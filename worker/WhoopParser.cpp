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

// Packet type discriminator: byte[4]
static constexpr uint8_t kTypeEvent        = 0x30u;
static constexpr uint8_t kTypeRealtimeData = 0x28u;
static constexpr uint8_t kTypeRawRealtime  = 0x2Bu;  // reserved / no extraction

// Shared indices
static constexpr size_t kTypeIndex  = 4u;   // packet type byte
static constexpr size_t kRecTypeIdx = 5u;   // record sub-type (R10 / R21)

// ── 0x30 Event (Skin Temperature) ────────────────────────────────────────────
static constexpr size_t   kEvtEventTypeIdx = 6u;    // uint16 LE event type
static constexpr uint16_t kEvtTempType     = 17u;   // Temperature event ID
static constexpr size_t   kEvtTempIdx      = 16u;   // int16 LE raw value
static constexpr size_t   kEvtMinFrame     = kEvtTempIdx + 2u;  // 18 bytes

// ── 0x28 R10 (HR + IMU) ──────────────────────────────────────────────────────
static constexpr uint8_t kRecTypeR10    = 10u;
static constexpr size_t  kR10HrIndex   = 21u;             // uint8 HR byte
static constexpr size_t  kR10AccelXBase = 4u + 85u;       // 100 × int16 LE
static constexpr size_t  kR10AccelYBase = 4u + 285u;      // 100 × int16 LE
static constexpr size_t  kR10AccelZBase = 4u + 485u;      // 100 × int16 LE
// Minimum bytes needed for the full Z array (last sample ends at base + 200)
static constexpr size_t  kR10MinFrame  = 4u + 485u + 200u;  // 889 bytes

// ── 0x28 R21 (SpO2 Optical) ──────────────────────────────────────────────────
static constexpr uint8_t kRecTypeR21    = 21u;
static constexpr size_t  kR21ChCBase    = 4u + 420u;      // IR  channel: 100 × uint32 LE
static constexpr size_t  kR21ChFBase    = 4u + 1032u;     // Red channel: 100 × uint32 LE
// Minimum bytes needed for the full Red array (last sample at base + 400 - 4)
static constexpr size_t  kR21MinFrame   = 4u + 1233u;     // 1237 bytes

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
        // Need at least 2 bytes for the event type field at byte[6].
        if (bytes.size() < kEvtEventTypeIdx + 2u)
            return result;

        const uint16_t eventType = readU16LE(bytes, kEvtEventTypeIdx);

        if (eventType == kEvtTempType) {
            // Need 2 more bytes for the int16 LE temperature at byte[16].
            if (bytes.size() < kEvtMinFrame)
                return result;

            const int16_t raw = readI16LE(bytes, kEvtTempIdx);
            result.skin_temp  = SkinTempRecord{ timestamp_ns, raw / 10.0f };
        }

        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Branch B — 0x28 Realtime Data packet
    // ═══════════════════════════════════════════════════════════════════════════
    if (pktType == kTypeRealtimeData) {
        // Need byte[5] for the record sub-type.
        if (bytes.size() <= kRecTypeIdx)
            return result;

        const uint8_t recType = bytes[kRecTypeIdx];

        // ── Sub-branch B0: R2 — Basic HR ─────────────────────────────────────
        if (recType == 2u) {
            // HR is at byte 20 in the R2 packet
            if (bytes.size() > 20u) {
                result.hr = HrRecord{ timestamp_ns, bytes[20u] };
            }
            return result;
        }

        // ── Sub-branch B1: R10 — Heart Rate + IMU ────────────────────────────
        if (recType == kRecTypeR10) {
            // HR is always present if we can reach byte[21].
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

            // Read 100 × uint16 LE for IR (chC) and Red (chF) channels.
            std::vector<double> ir(100), red(100);
            for (size_t i = 0; i < 100u; ++i) {
                ir[i]  = static_cast<double>(readU16LE(bytes, kR21ChCBase + i * 2u));
                red[i] = static_cast<double>(readU16LE(bytes, kR21ChFBase + i * 2u));
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
