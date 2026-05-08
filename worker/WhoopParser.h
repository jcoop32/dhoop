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

// ── Parse result ──────────────────────────────────────────────────────────────
// Returned by parse() for every message consumed from the Redis stream.
// crc_valid == false means the payload failed integrity check and should be
// dead-lettered. hr / accel are only populated for SyncBatchData packets
// (packet-type byte 0x05 at raw frame index 6) that are long enough to
// contain the respective fields.

struct ParseResult {
    bool     crc_valid;
    uint64_t timestamp_ns;
    std::string hex_data;                // original hex string (for raw insert)
    std::optional<HrRecord>    hr;
    std::optional<AccelRecord> accel;
};

// ── Primary entry point ───────────────────────────────────────────────────────
// Converts hex_string to bytes, validates CRC-32, and — when the packet type
// is 0x05 — extracts HR and accelerometer fields.
// Throws std::invalid_argument if hex_string has an odd length.
// Throws std::runtime_error   if the frame is too short to contain a CRC.
ParseResult parse(const std::string& hex_string, uint64_t timestamp_ns);

} // namespace whoop
