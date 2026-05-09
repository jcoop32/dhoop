#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace whoop {

// ── Records ───────────────────────────────────────────────────────────────────

struct HrRecord {
    uint64_t timestamp_ns;
    uint8_t  hr;
};

struct AccelRecord {
    uint64_t timestamp_ns;
    float    x;
    float    y;
    float    z;
};

struct SkinTempRecord {
    uint64_t timestamp_ns;
    float    temp_c;      // int16 LE at event byte[16] / 10.0 → degrees Celsius
};

struct GyroRecord {
    uint64_t timestamp_ns;
    float    x;
    float    y;
    float    z;
};

struct DoubleTapRecord {
    uint64_t timestamp_ns;
};

struct WristStateRecord {
    uint64_t timestamp_ns;
    bool     on_wrist; // true if placed on (10), false if removed (9)
};

struct PpgWaveformRecord {
    uint64_t timestamp_ns;
    std::vector<uint32_t> green;
    std::vector<uint32_t> red;
    std::vector<uint32_t> infrared;
};

// SpO2: floating-point result from the AC/DC ratio method applied to R21 optical data.
struct SpO2Record {
    uint64_t timestamp_ns;
    float    spo2;        // clamped to [85.0, 100.0] %
};

struct RRIntervalRecord {
    uint64_t timestamp_ns;
    uint16_t rr_ms;       // milliseconds between successive heartbeats
};

// ── Parse result ──────────────────────────────────────────────────────────────
// Returned by parse() for every message consumed from the Redis stream.
// crc_valid == false means the payload failed integrity check.
// iOS sends COMPLETE Gen4 frames (AA lenLo lenHi crc8 | 0x23 seq cmd payload crc32).
// Metric optionals are populated based on Gen4 packet type at byte[6]:
//   0x30 (Event, eventType==17 at byte[8]) → skin_temp (raw int16 LE at byte[18], /10 = °C)
//   0x28 recType==10 at byte[7] (R10)      → hr at byte[23], accel (if size >= 891 bytes)
//   0x28 recType==21 at byte[7] (R21)      → spo2      (if size >= 1239 bytes)
// rr_intervals is reserved for future use; currently always empty.

struct ParseResult {
    bool        crc_valid;
    uint64_t    timestamp_ns;
    std::string hex_data;                    // original hex (for raw insert)

    std::optional<HrRecord>       hr;
    std::optional<AccelRecord>    accel;
    std::optional<GyroRecord>     gyro;
    std::optional<SkinTempRecord> skin_temp;
    std::optional<SpO2Record>     spo2;
    std::optional<DoubleTapRecord> double_tap;
    std::optional<WristStateRecord> wrist_state;
    std::optional<PpgWaveformRecord> ppg_waveform;
    std::vector<RRIntervalRecord> rr_intervals; // empty = not present
};

// ── Primary entry point ───────────────────────────────────────────────────────
// Converts hex_string to bytes, validates CRC-32, and extracts metrics based
// on Gen4 packet type (byte[4]):
//   0x30 → Temperature event
//   0x28 → Realtime data (R10: HR/IMU, R21: SpO2)
//   0x2B → Raw realtime (reserved, no extraction)
// Throws std::invalid_argument if hex_string has an odd length.
// Throws std::runtime_error   if the frame is too short to contain a CRC.
ParseResult parse(const std::string& hex_string, uint64_t timestamp_ns);

} // namespace whoop
