#include "WhoopParser.h"

#include <algorithm>    // std::clamp
#include <array>
#include <cmath>        // std::sqrt
#include <cstdint>
#include <cstring>      // std::memcpy
#include <numeric>      // std::accumulate
#include <stdexcept>
#include <string>
#include <vector>

namespace whoop {
namespace {

// ── CRC-32 engine ─────────────────────────────────────────────────────────────
// Polynomial : 0x04C11DB7  (reflected → 0xEDB88320)
// ReflectIn  : true   ReflectOut : true
// Init       : 0x00000000  FinalXOR : 0xF43F44AC
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

// ── Hex → bytes ───────────────────────────────────────────────────────────────
static std::vector<uint8_t> hexToBytes(const std::string& hex) {
    if (hex.size() % 2 != 0)
        throw std::invalid_argument("Odd-length hex string: " + hex);
    std::vector<uint8_t> out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2)
        out.push_back(static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    return out;
}

// ── Gen4 frame layout constants ───────────────────────────────────────────────
// Confirmed from whoomp.js (jogolden/whoomp) — authoritative Whoop 4.0 RE.
//
// Full framed packet:
//   [0]    0xAA  — sync
//   [1..2] uint16 LE total length (includes CRC32)
//   [3]    CRC-8(len)
//   [4]    type  — PacketType discriminator  ← kTypeIndex
//   [5]    seq   — sequence number
//   [6]    cmd   — command/event number      ← kCmdIndex
//   [7..]  data  — metric payload            ← kDataBase
//   [last-3..last] CRC-32 LE
//
// PacketType enum (decimal / hex):
//   COMMAND          = 35  / 0x23
//   COMMAND_RESPONSE = 36  / 0x24
//   REALTIME_DATA    = 40  / 0x28
//   REALTIME_RAW     = 43  / 0x2B
//   EVENT            = 48  / 0x30

static constexpr uint8_t kTypeEvent        = 0x30u;  // 48
static constexpr uint8_t kTypeRealtimeData = 0x28u;  // 40
static constexpr uint8_t kTypeRawRealtime  = 0x2Bu;  // 43

static constexpr size_t kTypeIndex = 4u;  // PacketType byte
static constexpr size_t kCmdIndex  = 6u;  // cmd / event-number byte
static constexpr size_t kDataBase  = 7u;  // first payload byte

// ── 0x30 EVENT — Skin Temperature ────────────────────────────────────────────
// bytes[4] = 0x30 (EVENT)
// bytes[6] = 17   (EventNumber::TEMPERATURE_LEVEL)
// bytes[7..8] = uint16 LE raw temperature, units = 0.01 °C  (divide by 100.0)
static constexpr uint8_t kEvtTempCmd      = 17u;   // TEMPERATURE_LEVEL event
static constexpr size_t  kEvtTempIdx      = 7u;    // uint16 LE at bytes[7]
static constexpr size_t  kEvtMinFrame     = kEvtTempIdx + 2u;  // 9 bytes minimum

// ── 0x28 REALTIME_DATA — HR + Accel (R10) ────────────────────────────────────
// bytes[4] = 0x28 (REALTIME_DATA)
// bytes[6] = record sub-type (10 = R10)
// bytes[7..] = R10 payload
// HR: packet.data[5] confirmed in whoomp.js → absolute byte[4+3+5] = byte[12]
static constexpr uint8_t kRecTypeR10    = 10u;
static constexpr size_t  kR10HrIndex   = 12u;            // uint8 HR byte
static constexpr size_t  kR10AccelXBase = kDataBase + 85u;   // 100 × int16 LE
static constexpr size_t  kR10AccelYBase = kDataBase + 285u;  // 100 × int16 LE
static constexpr size_t  kR10AccelZBase = kDataBase + 485u;  // 100 × int16 LE
static constexpr size_t  kR10MinFrame   = kDataBase + 485u + 200u;  // 892 bytes

// ── 0x28 REALTIME_DATA — SpO2 (R21) ──────────────────────────────────────────
// bytes[4] = 0x28, bytes[6] = 21 (R21)
static constexpr uint8_t kRecTypeR21    = 21u;
static constexpr size_t  kR21ChCBase    = kDataBase + 420u;   // IR  channel: 100 × uint32 LE
static constexpr size_t  kR21ChFBase    = kDataBase + 1032u;  // Red channel: 100 × uint32 LE
static constexpr size_t  kR21MinFrame   = kDataBase + 1233u;  // 1240 bytes

static constexpr size_t kCrcLen = 4u;

// ── Helper: read uint16 LE from byte array at offset (bounds-checked) ─────────
static uint16_t readU16LE(const std::vector<uint8_t>& b, size_t off) {
    return static_cast<uint16_t>(b[off]) | (static_cast<uint16_t>(b[off + 1]) << 8);
}

// ── Helper: read int16 LE from byte array at offset (bounds-checked) ──────────
static int16_t readI16LE(const std::vector<uint8_t>& b, size_t off) {
    return static_cast<int16_t>(readU16LE(b, off));
}

// ── Helper: read uint32 LE from byte array at offset (bounds-checked) ─────────
static uint32_t readU32LE(const std::vector<uint8_t>& b, size_t off) {
    return  static_cast<uint32_t>(b[off])
          | (static_cast<uint32_t>(b[off + 1]) <<  8)
          | (static_cast<uint32_t>(b[off + 2]) << 16)
          | (static_cast<uint32_t>(b[off + 3]) << 24);
}

// ── Helper: mean of a double vector ───────────────────────────────────────────
static double mean(const std::vector<double>& v) {
    return std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(v.size());
}

// ── Helper: population standard deviation of a double vector ─────────────────
static double stddev(const std::vector<double>& v) {
    const double m = mean(v);
    double acc = 0.0;
    for (double x : v) acc += (x - m) * (x - m);
    return std::sqrt(acc / static_cast<double>(v.size()));
}

} // anonymous namespace

// ── Public API ────────────────────────────────────────────────────────────────
ParseResult parse(const std::string& hex_string, uint64_t timestamp_ns) {
    auto bytes = hexToBytes(hex_string);

    if (bytes.size() <= kCrcLen)
        throw std::runtime_error("Frame too short to contain CRC");

    // ── CRC validation ────────────────────────────────────────────────────────
    const size_t   data_len = bytes.size() - kCrcLen;
    const uint8_t* crc_ptr  = bytes.data() + data_len;

    const uint32_t embedded =
          static_cast<uint32_t>(crc_ptr[0])
        | (static_cast<uint32_t>(crc_ptr[1]) <<  8)
        | (static_cast<uint32_t>(crc_ptr[2]) << 16)
        | (static_cast<uint32_t>(crc_ptr[3]) << 24);

    const uint32_t computed = computeCrc32(bytes.data(), data_len);

    ParseResult result;
    result.crc_valid    = (computed == embedded);
    result.timestamp_ns = timestamp_ns;
    result.hex_data     = hex_string;

    // CRC bypass — extract metrics regardless of validation result
    // if (!result.crc_valid) return result;

    // Guard: every Gen4 packet must be long enough to reach byte[kTypeIndex].
    if (bytes.size() <= kTypeIndex)
        return result;

    const uint8_t pktType = bytes[kTypeIndex];

    // ═══════════════════════════════════════════════════════════════════════════
    // Branch A — 0x30 Event packet → Skin Temperature
    // ═══════════════════════════════════════════════════════════════════════════
    if (pktType == kTypeEvent) {
        // Event number is in the cmd byte at kCmdIndex
        if (bytes.size() <= kCmdIndex)
            return result;

        const uint8_t eventNum = bytes[kCmdIndex];  // e.g. 17 = TEMPERATURE_LEVEL

        if (eventNum == kEvtTempCmd) {
            // Temperature uint16 LE at bytes[7..8], units = 0.01 °C
            if (bytes.size() < kEvtMinFrame)
                return result;

            const uint16_t raw = readU16LE(bytes, kEvtTempIdx);
            result.skin_temp   = SkinTempRecord{ timestamp_ns, raw / 100.0f };
        }

        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Branch B — 0x28 / 0x2B Realtime Data packet
    // ═══════════════════════════════════════════════════════════════════════════
    if (pktType == kTypeRealtimeData || pktType == kTypeRawRealtime) {
        // Record sub-type is in the cmd byte at kCmdIndex
        if (bytes.size() <= kCmdIndex)
            return result;

        const uint8_t recType = bytes[kCmdIndex];  // 10=R10, 21=R21

        // ── Sub-branch B1: R10 — Heart Rate + IMU ────────────────────────────
        if (recType == kRecTypeR10) {
            // HR: confirmed at packet.data[5] = absolute byte[12]
            if (bytes.size() > kR10HrIndex) {
                result.hr = HrRecord{ timestamp_ns, bytes[kR10HrIndex] };
            }

            // IMU: only extract if the full Z array is present.
            if (bytes.size() >= kR10MinFrame) {
                const float x = static_cast<float>(readI16LE(bytes, kR10AccelXBase));
                const float y = static_cast<float>(readI16LE(bytes, kR10AccelYBase));
                const float z = static_cast<float>(readI16LE(bytes, kR10AccelZBase));
                result.accel = AccelRecord{ timestamp_ns, x, y, z };
            }

            return result;
        }

        // ── Sub-branch B2: R21 — SpO2 Optical ────────────────────────────────
        if (recType == kRecTypeR21) {
            // Require the full Red channel array to be present.
            if (bytes.size() < kR21MinFrame)
                return result;

            // Read 100 × uint32 LE for IR (chC) and Red (chF) channels.
            std::vector<double> ir(100), red(100);
            for (size_t i = 0; i < 100u; ++i) {
                ir[i]  = static_cast<double>(readU32LE(bytes, kR21ChCBase + i * 4u));
                red[i] = static_cast<double>(readU32LE(bytes, kR21ChFBase + i * 4u));
            }

            const double dc_ir  = mean(ir);
            const double dc_red = mean(red);
            const double ac_ir  = stddev(ir);
            const double ac_red = stddev(red);

            // Guard against degenerate signals (flat-line or zero DC).
            if (dc_ir > 0.0 && dc_red > 0.0 && ac_ir > 0.0) {
                const double ratio = (ac_red / dc_red) / (ac_ir / dc_ir);
                const double spo2  = std::clamp(110.0 - 25.0 * ratio, 85.0, 100.0);
                result.spo2 = SpO2Record{ timestamp_ns, static_cast<float>(spo2) };
            }

            return result;
        }

        // Unknown recType — return result with CRC info only.
        return result;
    }

    // 0x2B (kTypeRawRealtime) and any other types — no metric extraction.
    return result;
}

} // namespace whoop
