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
    float    temp_c;       // raw 16-bit ADC / 100 → degrees Celsius
};

struct SpO2Record {
    uint64_t timestamp_ns;
    uint8_t  spo2;         // blood oxygen saturation percentage (0–100)
};

struct RRIntervalRecord {
    uint64_t timestamp_ns;
    uint16_t rr_ms;        // milliseconds between successive heartbeats
};

// ── Parse result ──────────────────────────────────────────────────────────────
// Returned by parse() for every message consumed from the Redis stream.
// crc_valid == false means the payload failed integrity check.
// Metric optionals populated only for packet types that contain those fields.
// rr_intervals is a flat vector — one entry per beat found in the packet tail.

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
// on packet type: HR from 0xFF packets, full metrics from 0x05 packets.
// Throws std::invalid_argument if hex_string has an odd length.
// Throws std::runtime_error   if the frame is too short to contain a CRC.
ParseResult parse(const std::string& hex_string, uint64_t timestamp_ns);

} // namespace whoop

