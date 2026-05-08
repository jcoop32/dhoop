// ============================================================
// dhoop — Whoop BLE processing worker
// Consumes whoop_raw_stream from Redis, validates the custom
// Whoop CRC-32, and batch-inserts valid payloads into
// ClickHouse. HR / accelerometer slicing is a future iteration.
// ============================================================

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sw/redis++/redis++.h>

#include <clickhouse/client.h>
#include <clickhouse/columns/date.h>      // ColumnDateTime64 lives here in clickhouse-cpp
#include <clickhouse/columns/string.h>

// ── CRC-32 engine ─────────────────────────────────────────────────────────────
// Polynomial : 0x04C11DB7   (reflected → 0xEDB88320)
// ReflectIn  : true
// ReflectOut : true
// Init       : 0x00000000
// FinalXOR   : 0xF43F44AC
// ─────────────────────────────────────────────────────────────────────────────
static constexpr uint32_t kReflectedPoly = 0xEDB88320u;
static constexpr uint32_t kCrcInit       = 0x00000000u;
static constexpr uint32_t kCrcFinalXor   = 0xF43F44ACu;

static std::array<uint32_t, 256> buildCrcTable() noexcept {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int b = 0; b < 8; ++b)
            crc = (crc & 1u) ? ((crc >> 1) ^ kReflectedPoly) : (crc >> 1);
        t[i] = crc;
    }
    return t;
}
static const auto kCrcTable = buildCrcTable();

static uint32_t computeCrc32(const uint8_t* data, size_t len) noexcept {
    uint32_t crc = kCrcInit;
    for (size_t i = 0; i < len; ++i)
        crc = (crc >> 8) ^ kCrcTable[(crc ^ data[i]) & 0xFFu];
    return crc ^ kCrcFinalXor;
}

// ── Hex utilities ─────────────────────────────────────────────────────────────
static std::vector<uint8_t> hexToBytes(const std::string& hex) {
    if (hex.size() % 2 != 0)
        throw std::invalid_argument("Odd-length hex string: " + hex);
    std::vector<uint8_t> out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2)
        out.push_back(static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    return out;
}

// ── Decode result ─────────────────────────────────────────────────────────────
struct DecodeResult {
    bool     crc_valid;
    uint32_t computed_crc;
    uint32_t embedded_crc;      // last 4 bytes of frame, little-endian
    std::vector<uint8_t> raw;   // full frame bytes (for future slicing)
};

// Frame assumption: last 4 bytes are the CRC field (little-endian).
// The CRC is computed over all preceding bytes.
// This assumption will be validated / adjusted once the Whoop frame
// specification is confirmed in the next iteration.
DecodeResult decodePayload(const std::string& hex_string) {
    auto bytes = hexToBytes(hex_string);

    constexpr size_t kCrcLen = 4;
    if (bytes.size() <= kCrcLen)
        throw std::runtime_error("Frame too short to contain CRC");

    const size_t   data_len = bytes.size() - kCrcLen;
    const uint8_t* crc_ptr  = bytes.data() + data_len;

    uint32_t embedded =
          static_cast<uint32_t>(crc_ptr[0])
        | (static_cast<uint32_t>(crc_ptr[1]) << 8)
        | (static_cast<uint32_t>(crc_ptr[2]) << 16)
        | (static_cast<uint32_t>(crc_ptr[3]) << 24);

    uint32_t computed = computeCrc32(bytes.data(), data_len);

    return DecodeResult{
        .crc_valid    = (computed == embedded),
        .computed_crc = computed,
        .embedded_crc = embedded,
        .raw          = std::move(bytes),
    };
}

// ── ClickHouse insertion stub ─────────────────────────────────────────────────
// Inserts CRC-validated raw payloads into whoop_raw_data.
// TODO (next iteration): slice .raw into HR samples → whoop_hr
//                        and accelerometer frames → whoop_accelerometer
struct RawRecord {
    uint64_t    timestamp_ns;
    std::string hex_data;
};

static void insertBatch(clickhouse::Client& ch, const std::vector<RawRecord>& records) {
    if (records.empty()) return;

    auto ts_col   = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto data_col = std::make_shared<clickhouse::ColumnString>();

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        data_col->Append(r.hex_data);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("data",      data_col);

    ch.Insert("whoop_raw_data", block);
}

// ── Helpers ───────────────────────────────────────────────────────────────────
static uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
}

static std::string getenv_or(const char* key, const char* fallback) {
    const char* v = std::getenv(key);
    return v ? v : fallback;
}

// ── Entry point ───────────────────────────────────────────────────────────────
int main() {
    const std::string redis_url   = getenv_or("REDIS_URL",        "redis://redis:6379");
    const std::string ch_host     = getenv_or("CLICKHOUSE_HOST",  "clickhouse");
    const int         ch_port     = std::stoi(getenv_or("CLICKHOUSE_PORT", "9000"));
    const std::string stream_name = getenv_or("REDIS_STREAM",     "whoop_raw_stream");

    const std::string GROUP    = "whoop_workers";
    const std::string CONSUMER = "worker-1";
    constexpr long long BATCH    = 50;
    constexpr int       BLOCK_MS = 5000;

    // ── Connect ───────────────────────────────────────────────────────────────
    std::cout << "[worker] Redis    → " << redis_url << "\n";
    sw::redis::Redis redis(redis_url);

    std::cout << "[worker] ClickHouse → " << ch_host << ":" << ch_port << "\n";
    clickhouse::Client ch(
        clickhouse::ClientOptions().SetHost(ch_host).SetPort(ch_port)
    );

    // ── Bootstrap consumer group ──────────────────────────────────────────────
    // id "0" → replay all existing entries on first start.
    // Switch to "$" once you only want messages arriving after the worker boots.
    try {
        redis.xgroup_create(stream_name, GROUP, "0", true /* mkstream */);
        std::cout << "[worker] Consumer group '" << GROUP << "' created.\n";
    } catch (const sw::redis::Error& e) {
        std::cout << "[worker] Group already exists (" << e.what() << ") — continuing.\n";
    }

    std::cout << "[worker] Listening on '" << stream_name << "' ...\n";

    // ── Consumer loop ─────────────────────────────────────────────────────────
    using StreamMap = std::unordered_map<
        std::string,
        std::vector<std::pair<std::string, sw::redis::OptionalStringPairs>>
    >;

    while (true) {
        try {
            StreamMap result;
            redis.xreadgroup(
                GROUP, CONSUMER,
                stream_name, ">",      // ">" = only undelivered messages
                BATCH,
                std::chrono::milliseconds(BLOCK_MS),
                std::inserter(result, result.end())
            );

            auto it = result.find(stream_name);
            if (it == result.end() || it->second.empty()) continue;

            std::vector<RawRecord> batch;
            std::vector<std::string> ack_ids;
            batch.reserve(it->second.size());
            ack_ids.reserve(it->second.size());

            for (const auto& [msg_id, opt_fields] : it->second) {
                if (!opt_fields) { ack_ids.push_back(msg_id); continue; }

                std::string hex_data;
                uint64_t    ts_ns = 0;

                // Field names emitted by the Python ingest service
                for (const auto& [k, v] : *opt_fields) {
                    if (k == "data" || k == "hex") hex_data = v;
                    else if (k == "ts")            ts_ns    = std::stoull(v);
                }

                if (hex_data.empty()) {
                    std::cerr << "[worker] WARN  " << msg_id << " no payload field — discarding\n";
                    ack_ids.push_back(msg_id);
                    continue;
                }
                if (ts_ns == 0) ts_ns = nowNs();

                try {
                    auto dec = decodePayload(hex_data);
                    if (!dec.crc_valid) {
                        std::cerr << "[worker] CRC   FAIL  " << msg_id
                                  << " computed=0x" << std::hex << std::uppercase << dec.computed_crc
                                  << " embedded=0x" << dec.embedded_crc << std::dec << "\n";
                        ack_ids.push_back(msg_id); // dead-letter: don't re-queue
                        continue;
                    }
                    batch.push_back({ts_ns, hex_data});
                    ack_ids.push_back(msg_id);
                } catch (const std::exception& ex) {
                    std::cerr << "[worker] PARSE " << msg_id << " " << ex.what() << " — discarding\n";
                    ack_ids.push_back(msg_id);
                }
            }

            // Insert first; ACK only on success so a crash doesn't lose data.
            if (!batch.empty()) {
                insertBatch(ch, batch);
                std::cout << "[worker] INSERT " << batch.size() << " rows → whoop_raw_data\n";
            }

            if (!ack_ids.empty())
                redis.xack(stream_name, GROUP, ack_ids.begin(), ack_ids.end());

        } catch (const sw::redis::Error& ex) {
            std::cerr << "[worker] REDIS ERR: " << ex.what() << " — retry in 2s\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        } catch (const std::exception& ex) {
            std::cerr << "[worker] ERR: " << ex.what() << " — retry in 2s\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
}
