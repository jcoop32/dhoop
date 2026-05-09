#pragma once

#include <vector>
#include <string>
#include <cstdint>

#include <clickhouse/client.h>

#include "WhoopParser.h"

namespace db {

// Raw hex payloads → whoop_raw_data (timestamp, data)
struct RawRecord {
    uint64_t    timestamp_ns;
    std::string hex_data;
};

void insertRawBatch      (clickhouse::Client& ch, const std::vector<RawRecord>&                 records);
void insertHrBatch       (clickhouse::Client& ch, const std::vector<whoop::HrRecord>&           records);
void insertAccelBatch    (clickhouse::Client& ch, const std::vector<whoop::AccelRecord>&        records);
void insertSkinTempBatch (clickhouse::Client& ch, const std::vector<whoop::SkinTempRecord>&     records);
void insertSpO2Batch     (clickhouse::Client& ch, const std::vector<whoop::SpO2Record>&         records);
void insertRRBatch       (clickhouse::Client& ch, const std::vector<whoop::RRIntervalRecord>&   records);

} // namespace db

