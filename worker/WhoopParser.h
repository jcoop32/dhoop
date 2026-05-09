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

// Accel: single scalar representing the mean vector magnitude (m/s² or raw LSB)
// computed across the 100-sample IMU window in an R10 packet.
struct AccelRecord {
    uint64_t timestamp_ns;
    float    magnitude;   // mean of sqrt(x²+y²+z²) over 100 int16 LE samples
};

struct SkinTempRecord {
    uint64_t timestamp_ns;
    float    temp_c;      // int16 LE at event byte[16] / 10.0 → degrees Celsius
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
// Metric optionals are populated based on Gen4 packet type:
//   0x30 (Event, eventType==17) → skin_temp
//   0x28 recType==10  (R10)     → hr, accel (if size >= 889 bytes)
//   0x28 recType==21  (R21)     → spo2      (if size >= 1237 bytes)
// rr_intervals is reserved for future use; currently always empty.

struct ParseResult {
    bool        crc_valid;
    uint64_t    timestamp_ns;
    std::string hex_data;                    // original hex (for raw insert)

    std::optional<HrRecord>       hr;
    std::optional<AccelRecord>    accel;
    std::optional<SkinTempRecord> skin_temp;
    std::optional<SpO2Record>     spo2;
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
