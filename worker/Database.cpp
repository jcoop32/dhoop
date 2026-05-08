#include "Database.h"

#include <clickhouse/columns/date.h>      // ColumnDateTime64
#include <clickhouse/columns/string.h>    // ColumnString
#include <clickhouse/columns/numeric.h>   // ColumnUInt8, ColumnFloat32

namespace db {

void insertRawBatch(clickhouse::Client& ch, const std::vector<RawRecord>& records) {
    if (records.empty()) return;

    auto ts_col   = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto data_col = std::make_shared<clickhouse::ColumnString>();

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        data_col->Append(r.hex_data);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("data",      data_col);

    ch.Insert("whoop_raw_data", block);
}

void insertHrBatch(clickhouse::Client& ch, const std::vector<whoop::HrRecord>& records) {
    if (records.empty()) return;

    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto hr_col = std::make_shared<clickhouse::ColumnUInt8>();

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        hr_col->Append(r.hr);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("hr",        hr_col);

    ch.Insert("whoop_hr", block);
}

void insertAccelBatch(clickhouse::Client& ch, const std::vector<whoop::AccelRecord>& records) {
    if (records.empty()) return;

    auto ts_col   = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto acc0_col = std::make_shared<clickhouse::ColumnFloat32>();
    auto acc1_col = std::make_shared<clickhouse::ColumnFloat32>();
    auto acc2_col = std::make_shared<clickhouse::ColumnFloat32>();

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        acc0_col->Append(r.x);
        acc1_col->Append(r.y);
        acc2_col->Append(r.z);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("acc0",      acc0_col);
    block.AppendColumn("acc1",      acc1_col);
    block.AppendColumn("acc2",      acc2_col);

    ch.Insert("whoop_accelerometer", block);
}

} // namespace db
