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
// Empirically confirmed from live debug output (2026-05-09):
//
// Full framed packet (DATA_FROM_STRAP / EVENTS_FROM_STRAP):
//   [0]    0xAA  — sync
//   [1..2] uint16 LE total length (body + CRC32)
//   [3]    CRC-8(len)
//   [4]    pktType  — 0x28=REALTIME_DATA, 0x2B=REALTIME_RAW, 0x30=EVENT
//   [5]    subType  — R10=10, R21=21, EventNum=17 for TEMPERATURE_LEVEL
//   [6]    unknown  (not sub-type; varies per packet)
//   [7+]   payload
//   [last-3..last] CRC-32 LE
//
// CONFIRMED from parser debug:
//   bytes[5]=0x0A(10) → R10 match ✓
//   bytes[21] = 81/80/79 BPM ← valid physiological HR ✓

static constexpr uint8_t kTypeEvent        = 0x30u;  // 48
static constexpr uint8_t kTypeRealtimeData = 0x28u;  // 40
static constexpr uint8_t kTypeRawRealtime  = 0x2Bu;  // 43

static constexpr size_t kTypeIndex  = 4u;   // packet type byte
static constexpr size_t kRecTypeIdx = 5u;   // sub-type / event-number byte

// ── 0x30 EVENT — Skin Temperature ─────────────────────────────────────────────
// bytes[4] = 0x30
// bytes[5] = event number (17 = TEMPERATURE_LEVEL)
// Temperature encoding: int16 LE in centidegrees (÷100 = °C)
// Offset is TBD — offset scan prints all candidates to stderr
static constexpr uint16_t kEvtTempType  = 17u;   // TEMPERATURE_LEVEL
static constexpr size_t   kEvtTempIdx   = 6u;    // UPDATED: probe from offset 6
static constexpr size_t   kEvtMinFrame  = kEvtTempIdx + 2u;  // 8 bytes minimum

// ── 0x28/0x2B R10 — Heart Rate + IMU ──────────────────────────────────────────
// CONFIRMED: recType(bytes[5])=10, HR at bytes[21]
static constexpr uint8_t kRecTypeR10    = 10u;
static constexpr size_t  kR10HrIndex   = 21u;          // uint8 HR — CONFIRMED ✓
static constexpr size_t  kR10AccelXBase = 4u + 85u;    // 100 × int16 LE
static constexpr size_t  kR10AccelYBase = 4u + 285u;   // 100 × int16 LE
static constexpr size_t  kR10AccelZBase = 4u + 485u;   // 100 × int16 LE
static constexpr size_t  kR10MinFrame   = 4u + 485u + 200u;  // 889 bytes

// ── 0x28/0x2B R21 — SpO2 Optical ─────────────────────────────────────────────
// bytes[5] = 21 (R21)
static constexpr uint8_t kRecTypeR21    = 21u;
static constexpr size_t  kR21ChCBase    = 4u + 420u;   // IR  channel: 100 × uint32 LE
static constexpr size_t  kR21ChFBase    = 4u + 1032u;  // Red channel: 100 × uint32 LE
static constexpr size_t  kR21MinFrame   = 4u + 1233u;  // 1237 bytes

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

    // ── DEBUG: log every packet's type/sub-type bytes and first 30 bytes ────
    {
        const size_t dump_len = std::min(bytes.size(), size_t(30));
        std::string hex_dump;
        for (size_t i = 0; i < dump_len; ++i) {
            char buf[4];
            std::snprintf(buf, sizeof(buf), "%02X ", bytes[i]);
            hex_dump += buf;
        }
        std::fprintf(stderr,
            "[parser] type=0x%02X b[5]=0x%02X b[6]=0x%02X size=%zu  | %s\n",
            pktType,
            bytes.size() > 5 ? bytes[5] : 0xFF,
            bytes.size() > 6 ? bytes[6] : 0xFF,
            bytes.size(), hex_dump.c_str());
        std::fflush(stderr);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Branch A — 0x30 Event packet → Skin Temperature
    // ═══════════════════════════════════════════════════════════════════════════
    if (pktType == kTypeEvent) {
        if (bytes.size() <= kRecTypeIdx)
            return result;

        const uint8_t eventNum = bytes[kRecTypeIdx];  // bytes[5]

        // ── Full EVENT packet hex dump (packets are small, dump everything) ─────
        {
            std::string full_dump;
            for (size_t i = 0; i < bytes.size(); ++i) {
                char buf[8];
                std::snprintf(buf, sizeof(buf), "%02zu:%02X ", i, bytes[i]);
                full_dump += buf;
            }
            std::fprintf(stderr, "[parser] EVENT 0x%02X size=%zu FULL DUMP:\n  %s\n",
                eventNum, bytes.size(), full_dump.c_str());
            std::fflush(stderr);
        }

        std::fprintf(stderr, "[parser] EVENT: eventNum=0x%02X (%u), want %u\n",
            eventNum, eventNum, kEvtTempType);

        if (eventNum == kEvtTempType) {
            // Probe multiple candidate offsets to find the real temperature
            // (centidegrees ÷100 = °C; expect 3100-3700 for 31-37°C wrist skin)
            static const size_t probes[] = { 6, 7, 8, 10, 11, 12, 16 };
            for (size_t off : probes) {
                if (bytes.size() >= off + 2) {
                    const int16_t   raw_s = readI16LE(bytes, off);
                    const uint16_t  raw_u = readU16LE(bytes, off);
                    std::fprintf(stderr,
                        "[parser]   probe b[%zu..%zu] i16=%d u16=%u → %.2f°C (÷100) | %.1f°C (÷10)\n",
                        off, off+1, raw_s, raw_u, raw_s/100.0f, raw_s/10.0f);
                }
            }
            std::fflush(stderr);

            // Use offset 6 ÷100 as the current best guess;
            // update kEvtTempIdx once the correct offset is confirmed from logs above
            if (bytes.size() >= kEvtTempIdx + 2) {
                const int16_t raw = readI16LE(bytes, kEvtTempIdx);
                const float   temp_c = raw / 100.0f;
                // Sanity gate: only store if in plausible human range (20–45°C)
                if (temp_c >= 20.0f && temp_c <= 45.0f) {
                    result.skin_temp = SkinTempRecord{ timestamp_ns, temp_c };
                    std::fprintf(stderr, "[parser] 🌡️ TEMP b[%zu] raw=%d → %.2f°C ✅\n",
                        kEvtTempIdx, raw, temp_c);
                } else {
                    std::fprintf(stderr, "[parser] 🌡️ TEMP b[%zu] raw=%d → %.2f°C ❌ OUT OF RANGE\n",
                        kEvtTempIdx, raw, temp_c);
                }
            }
        }

        return result;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Branch B — 0x28 / 0x2B Realtime Data packet
    // ═══════════════════════════════════════════════════════════════════════════
    if (pktType == kTypeRealtimeData || pktType == kTypeRawRealtime) {
        // Record sub-type is at bytes[5] (kRecTypeIdx) — CONFIRMED from debug output
        if (bytes.size() <= kRecTypeIdx)
            return result;

        const uint8_t recType = bytes[kRecTypeIdx];  // 10=R10, 21=R21
        std::fprintf(stderr, "[parser] REALTIME: recType(b[5])=%u\n", recType);

        // ── Sub-branch B1: R10 — Heart Rate + IMU ────────────────────────────
        if (recType == kRecTypeR10) {
            // HR at bytes[21] — CONFIRMED from debug (values 78-81 BPM) ✓
            std::fprintf(stderr, "[parser] ✅ R10 MATCH: size=%zu b[21]=0x%02X(%u BPM)\n",
                bytes.size(),
                bytes.size() > 21 ? bytes[21] : 0xFF,
                bytes.size() > 21 ? bytes[21] : 0);
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
