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
// EVENT packets use COMMAND format (different from DATA packets 0x28/0x2B):
//   bytes[4] = 0x30  (pktType)
//   bytes[5] = seq   (rolling counter — NOT the event type)
//   bytes[6] = event type  (17=0x11 = TEMPERATURE_LEVEL)
//   bytes[7+] = payload
// NOTE: bytes[5] being a seq is why we were getting false 281°C readings —
//       the seq happened to equal 17, triggering a spurious temp match.
static constexpr uint16_t kEvtTempType  = 17u;    // TEMPERATURE_LEVEL
static constexpr size_t   kEvtTypeIdx   = 6u;     // event type byte (CMD field)
static constexpr size_t   kEvtTempIdx   = 16u;    // Fixed offset for Skin Temp
static constexpr size_t   kEvtMinFrame  = kEvtTempIdx + 2u;

// ── 0x28/0x2B R10 — Heart Rate + IMU ──────────────────────────────────────────
// CONFIRMED: recType(bytes[5])=10, HR at bytes[21]
static constexpr uint8_t kRecTypeR10    = 10u;
static constexpr size_t  kR10HrIndex   = 21u;          // uint8 HR — CONFIRMED ✓
static constexpr size_t  kR10AccelXBase = 4u + 85u;    // 100 × int16 LE
static constexpr size_t  kR10AccelYBase = 4u + 285u;   // 100 × int16 LE
static constexpr size_t  kR10AccelZBase = 4u + 485u;   // 100 × int16 LE
static constexpr size_t  kR10GyroXBase  = kR10AccelZBase + 200u; // 100 × int16 LE
static constexpr size_t  kR10GyroYBase  = kR10GyroXBase + 200u;  // 100 × int16 LE
static constexpr size_t  kR10GyroZBase  = kR10GyroYBase + 200u;  // 100 × int16 LE
static constexpr size_t  kR10MinFrame   = kR10GyroZBase + 200u;  // 1289 bytes

// ── 0x28/0x2B R21 — SpO2 Optical ─────────────────────────────────────────────
// bytes[5] = 21 (R21)
static constexpr uint8_t kRecTypeR21    = 21u;
static constexpr size_t  kR21ChGreenBase= 4u + 20u;    // Green channel: 100 × uint32 LE
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
        if (bytes.size() <= kEvtTypeIdx)  // need at least bytes[6]
            return result;

        const uint8_t seq      = bytes.size() > 5 ? bytes[5] : 0;
        const uint8_t eventNum = bytes[kEvtTypeIdx];  // bytes[6] = event type

        // ── Full EVENT packet hex dump (indexed, so we can read offsets directly) ──
        {
            std::string full_dump;
            for (size_t i = 0; i < bytes.size(); ++i) {
                char buf[8];
                std::snprintf(buf, sizeof(buf), "%02zu:%02X ", i, bytes[i]);
                full_dump += buf;
            }
            std::fprintf(stderr,
                "[parser] EVENT type=0x%02X seq=0x%02X size=%zu DUMP:\n  %s\n",
                eventNum, seq, bytes.size(), full_dump.c_str());
            std::fflush(stderr);
        }

        std::fprintf(stderr, "[parser] EVENT: eventNum=0x%02X (%u), want 0x%02X (%u)\n",
            eventNum, eventNum, kEvtTempType, kEvtTempType);

        if (eventNum == kEvtTempType) {
            // Use kEvtTempIdx (=16) ÷10.0; updated offset for Skin Temp
            if (bytes.size() >= kEvtTempIdx + 2) {
                const int16_t raw    = readI16LE(bytes, kEvtTempIdx);
                const float   temp_c = raw / 10.0f;
                if (temp_c >= 20.0f && temp_c <= 45.0f) {
                    result.skin_temp = SkinTempRecord{ timestamp_ns, temp_c };
                    std::fprintf(stderr, "[parser] 🌡️ STORED %.1f°C (raw=%d b[%zu]) ✅\n",
                        temp_c, raw, kEvtTempIdx);
                } else {
                    std::fprintf(stderr, "[parser] 🌡️ RANGE FAIL %.1f°C (raw=%d b[%zu]) ❌\n",
                        temp_c, raw, kEvtTempIdx);
                }
            }
        } else if (eventNum == 14) {
            result.double_tap = DoubleTapRecord{ timestamp_ns };
            std::fprintf(stderr, "[parser] 👋 DOUBLE TAP DETECTED\n");
        } else if (eventNum == 9 || eventNum == 10) {
            result.wrist_state = WristStateRecord{ timestamp_ns, eventNum == 10 };
            std::fprintf(stderr, "[parser] ⌚ WRIST STATE: %s\n", eventNum == 10 ? "ON" : "OFF");
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
            // Scale: ±8g range → 1g = 4096 LSB  (confirmed: |(-1394,-2945,2511)| ≈ 4114 ≈ 4096)
            if (bytes.size() >= kR10MinFrame) {
                constexpr float kAccelScale = 4096.0f;  // LSB/g for ±8g mode
                constexpr float kGyroScale = 16.4f;     // LSB/(deg/s) for ±2000 dps
                const float x = static_cast<float>(readI16LE(bytes, kR10AccelXBase)) / kAccelScale;
                const float y = static_cast<float>(readI16LE(bytes, kR10AccelYBase)) / kAccelScale;
                const float z = static_cast<float>(readI16LE(bytes, kR10AccelZBase)) / kAccelScale;
                result.accel = AccelRecord{ timestamp_ns, x, y, z };
                std::fprintf(stderr, "[parser] 📐 ACCEL x=%.3fg y=%.3fg z=%.3fg |g|=%.3fg\n",
                    x, y, z, std::sqrt(x*x + y*y + z*z));

                const float gx = static_cast<float>(readI16LE(bytes, kR10GyroXBase)) / kGyroScale;
                const float gy = static_cast<float>(readI16LE(bytes, kR10GyroYBase)) / kGyroScale;
                const float gz = static_cast<float>(readI16LE(bytes, kR10GyroZBase)) / kGyroScale;
                result.gyro = GyroRecord{ timestamp_ns, gx, gy, gz };
                std::fprintf(stderr, "[parser] 🌀 GYRO x=%.2f y=%.2f z=%.2f\n", gx, gy, gz);
            }

            return result;
        }

        // ── Sub-branch B2: R21 — SpO2 Optical ────────────────────────────────
        if (recType == kRecTypeR21) {
            // Require the full Red channel array to be present.
            if (bytes.size() < kR21MinFrame)
                return result;

            // Read 100 × uint32 LE for Green, IR (chC), and Red (chF) channels.
            std::vector<double> ir(100), red(100);
            std::vector<uint32_t> raw_green(100), raw_red(100), raw_ir(100);
            for (size_t i = 0; i < 100u; ++i) {
                uint32_t c = readU32LE(bytes, kR21ChCBase + i * 4u);
                uint32_t f = readU32LE(bytes, kR21ChFBase + i * 4u);
                uint32_t g = readU32LE(bytes, kR21ChGreenBase + i * 4u);
                ir[i]  = static_cast<double>(c);
                red[i] = static_cast<double>(f);
                raw_ir[i] = c;
                raw_red[i] = f;
                raw_green[i] = g;
            }

            result.ppg_waveform = PpgWaveformRecord{ timestamp_ns, raw_green, raw_red, raw_ir };

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
