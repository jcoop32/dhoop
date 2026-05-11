// ============================================================
// dhoop — Whoop BLE processing worker  (main.cpp)
// UDP socket listener → WhoopParser → Database layer.
// All CRC / parsing logic lives in WhoopParser.cpp.
// All ClickHouse I/O lives in Database.cpp.
// ============================================================

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

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
    const std::string ch_host = getenv_or("CLICKHOUSE_HOST",     "clickhouse");
    const int         ch_port = std::stoi(getenv_or("CLICKHOUSE_PORT", "9000"));
    const std::string ch_user = getenv_or("CLICKHOUSE_USER",     "dhoop_worker");
    const std::string ch_pass = getenv_or("CLICKHOUSE_PASSWORD", "");
    const std::string ch_db   = getenv_or("CLICKHOUSE_DB",       "dhoop");

    std::cout << "[worker] ClickHouse → " << ch_host << ":" << ch_port << "\n";
    clickhouse::Client ch(
        clickhouse::ClientOptions()
            .SetHost(ch_host)
            .SetPort(ch_port)
            .SetUser(ch_user)
            .SetPassword(ch_pass)
            .SetDefaultDatabase(ch_db)
    );

    // ── Setup UDP Socket ──────────────────────────────────────────────────────
    int udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_socket < 0) {
        std::cerr << "[worker] Failed to create UDP socket\n";
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(9001); // Match iOS target port

    if (bind(udp_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "[worker] Failed to bind UDP port 9001\n";
        close(udp_socket);
        return 1;
    }
    std::cout << "[worker] Listening for raw UDP packets on port 9001...\n";

    uint8_t buffer[4096];
    while (true) {
        int len = recvfrom(udp_socket, buffer, sizeof(buffer), 0, nullptr, nullptr);
        if (len <= 0) continue;

        // Convert raw bytes to hex string for parser (maintains existing parser API).
        std::string hex_data;
        hex_data.reserve(static_cast<size_t>(len) * 2);
        static constexpr const char* kHexChars = "0123456789ABCDEF";
        for (int i = 0; i < len; ++i) {
            hex_data.push_back(kHexChars[buffer[i] >> 4]);
            hex_data.push_back(kHexChars[buffer[i] & 0x0F]);
        }

        try {
            auto r = whoop::parse(hex_data, nowNs());

            if (r.hr)        db::insertHrBatch(ch,       {*r.hr});
            if (r.accel)     db::insertAccelBatch(ch,    {*r.accel});
            if (r.gyro)      db::insertGyroBatch(ch,     {*r.gyro});
            if (r.skin_temp) db::insertSkinTempBatch(ch, {*r.skin_temp});
            if (r.spo2)      db::insertSpO2Batch(ch,     {*r.spo2});
            if (r.double_tap)  db::insertDoubleTapBatch(ch,  {*r.double_tap});
            if (r.wrist_state) db::insertWristStateBatch(ch, {*r.wrist_state});
            if (!r.rr_intervals.empty()) db::insertRRBatch(ch, r.rr_intervals);

            // Raw insert for debugging / replay.
            db::insertRawBatch(ch, {{r.timestamp_ns, hex_data}});

        } catch (const std::exception& ex) {
            std::cerr << "[worker] PARSE ERR: " << ex.what() << "\n";
        }
    }

    close(udp_socket);
    return 0;
}
