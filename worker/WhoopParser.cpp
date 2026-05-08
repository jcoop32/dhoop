#include "WhoopParser.h"

#include <array>
#include <cstdint>
#include <cstring>        // std::memcpy
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

// ── Frame layout constants ────────────────────────────────────────────────────
static constexpr uint8_t kHealthMonitorType = 0xFFu;
static constexpr size_t  kPacketTypeIndex   = 3u;
static constexpr size_t  kHrIndex           = 12u;
static constexpr size_t  kAccelXIndex       = 40u;
static constexpr size_t  kAccelYIndex       = 44u;
static constexpr size_t  kAccelZIndex       = 48u;
static constexpr size_t  kMinFrameForHr     = kHrIndex + 1u;
static constexpr size_t  kMinFrameForAccel  = kAccelZIndex + 4u;
static constexpr size_t  kCrcLen            = 4u;

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
        | (static_cast<uint32_t>(crc_ptr[1]) << 8)
        | (static_cast<uint32_t>(crc_ptr[2]) << 16)
        | (static_cast<uint32_t>(crc_ptr[3]) << 24);

    const uint32_t computed = computeCrc32(bytes.data(), data_len);

    ParseResult result;
    result.crc_valid    = (computed == embedded);
    result.timestamp_ns = timestamp_ns;
    result.hex_data     = hex_string;

    if (!result.crc_valid) return result;

    // ── Metric extraction: only for Health Monitor streams (0xFF) ────────────
    if (bytes.size() >= kMinFrameForHr && bytes[kPacketTypeIndex] == kHealthMonitorType) {
        // Heart rate: uint8_t
        result.hr = HrRecord{ timestamp_ns, bytes[kHrIndex] };

        // Accelerometer: three little-endian IEEE-754 floats at indices 40, 44, 48.
        // std::memcpy avoids strict-aliasing UB when type-punning raw bytes → float.
        /*
        if (bytes.size() >= kMinFrameForAccel) {
            float x = 0.f, y = 0.f, z = 0.f;
            std::memcpy(&x, bytes.data() + kAccelXIndex, sizeof(float));
            std::memcpy(&y, bytes.data() + kAccelYIndex, sizeof(float));
            std::memcpy(&z, bytes.data() + kAccelZIndex, sizeof(float));
            result.accel = AccelRecord{ timestamp_ns, x, y, z };
        }
        */
    }

    return result;
}

} // namespace whoop
