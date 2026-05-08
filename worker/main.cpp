// ============================================================
// dhoop — Whoop BLE processing worker  (main.cpp)
// Redis xreadgroup loop → WhoopParser → Database layer.
// All CRC / parsing logic lives in WhoopParser.cpp.
// All ClickHouse I/O lives in Database.cpp.
// ============================================================

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sw/redis++/redis++.h>
#include <clickhouse/client.h>

#include "WhoopParser.h"
#include "Database.h"

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
    const std::string redis_url   = getenv_or("REDIS_URL",           "redis://redis:6379");
    const std::string ch_host     = getenv_or("CLICKHOUSE_HOST",     "clickhouse");
    const int         ch_port     = std::stoi(getenv_or("CLICKHOUSE_PORT",    "9000"));
    const std::string ch_user     = getenv_or("CLICKHOUSE_USER",     "dhoop_worker");
    const std::string ch_pass     = getenv_or("CLICKHOUSE_PASSWORD", "");
    const std::string ch_db       = getenv_or("CLICKHOUSE_DB",       "dhoop");
    const std::string stream_name = getenv_or("REDIS_STREAM",        "whoop_raw_stream");

    const std::string GROUP    = "whoop_workers";
    const std::string CONSUMER = "worker-1";
    constexpr long long BATCH    = 50;
    constexpr int       BLOCK_MS = 5000;

    // ── Connect ───────────────────────────────────────────────────────────────
    std::cout << "[worker] Redis     → " << redis_url << "\n";
    sw::redis::Redis redis(redis_url);

    std::cout << "[worker] ClickHouse → " << ch_host << ":" << ch_port << "\n";
    clickhouse::Client ch(
        clickhouse::ClientOptions()
            .SetHost(ch_host)
            .SetPort(ch_port)
            .SetUser(ch_user)
            .SetPassword(ch_pass)
            .SetDefaultDatabase(ch_db)
    );

    // ── Bootstrap consumer group ──────────────────────────────────────────────
    try {
        redis.xgroup_create(stream_name, GROUP, "0", true /* mkstream */);
        std::cout << "[worker] Consumer group '" << GROUP << "' created.\n";
    } catch (const sw::redis::Error& e) {
        std::cout << "[worker] Group already exists (" << e.what() << ") — continuing.\n";
    }

    std::cout << "[worker] Listening on '" << stream_name << "' ...\n";

    using Fields    = std::vector<std::pair<std::string, std::string>>;
    using Entry     = std::pair<std::string, std::optional<Fields>>;
    using StreamMap = std::unordered_map<std::string, std::vector<Entry>>;

    while (true) {
        try {
            StreamMap result;
            redis.xreadgroup(
                GROUP, CONSUMER,
                stream_name, ">",
                std::chrono::milliseconds(BLOCK_MS),
                BATCH,
                std::inserter(result, result.end())
            );

            auto it = result.find(stream_name);
            if (it == result.end() || it->second.empty()) continue;

            std::vector<db::RawRecord>       raw_batch;
            std::vector<whoop::HrRecord>     hr_batch;
            std::vector<whoop::AccelRecord>  accel_batch;
            std::vector<std::string>         ack_ids;

            raw_batch.reserve(it->second.size());
            hr_batch.reserve(it->second.size());
            accel_batch.reserve(it->second.size());
            ack_ids.reserve(it->second.size());

            for (const auto& [msg_id, opt_fields] : it->second) {
                if (!opt_fields) { ack_ids.push_back(msg_id); continue; }

                std::string hex_data;
                uint64_t    ts_ns = 0;

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
                    auto r = whoop::parse(hex_data, ts_ns);

                    if (!r.crc_valid) {
                        std::cerr << "[worker] CRC FAIL " << msg_id << " — dead-lettering\n";
                        ack_ids.push_back(msg_id);
                        continue;
                    }

                    raw_batch.push_back({ r.timestamp_ns, r.hex_data });
                    if (r.hr)    hr_batch.push_back(*r.hr);
                    if (r.accel) accel_batch.push_back(*r.accel);

                    ack_ids.push_back(msg_id);
                } catch (const std::exception& ex) {
                    std::cerr << "[worker] PARSE " << msg_id << " " << ex.what() << " — discarding\n";
                    ack_ids.push_back(msg_id);
                }
            }

            // Insert before ACK — crash safety: no data loss on restart.
            if (!raw_batch.empty()) {
                db::insertRawBatch(ch, raw_batch);
                std::cout << "[worker] INSERT " << raw_batch.size()   << " rows → whoop_raw_data\n";
            }
            if (!hr_batch.empty()) {
                db::insertHrBatch(ch, hr_batch);
                std::cout << "[worker] INSERT " << hr_batch.size()    << " rows → whoop_hr\n";
            }
            if (!accel_batch.empty()) {
                db::insertAccelBatch(ch, accel_batch);
                std::cout << "[worker] INSERT " << accel_batch.size() << " rows → whoop_accelerometer\n";
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
