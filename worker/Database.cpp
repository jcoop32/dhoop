#include "Database.h"

#include <clickhouse/columns/date.h>      // ColumnDateTime64
#include <clickhouse/columns/string.h>    // ColumnString
#include <clickhouse/columns/numeric.h>   // ColumnUInt8, ColumnUInt16, ColumnFloat32

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

void insertSkinTempBatch(clickhouse::Client& ch, const std::vector<whoop::SkinTempRecord>& records) {
    if (records.empty()) return;

    auto ts_col   = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto temp_col = std::make_shared<clickhouse::ColumnFloat32>();

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        temp_col->Append(r.temp_c);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("temp_c",    temp_col);

    ch.Insert("whoop_skin_temp", block);
}

void insertSpO2Batch(clickhouse::Client& ch, const std::vector<whoop::SpO2Record>& records) {
    if (records.empty()) return;

    auto ts_col   = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto spo2_col = std::make_shared<clickhouse::ColumnFloat32>();  // float — preserves decimal precision

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        spo2_col->Append(r.spo2);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("spo2",      spo2_col);

    ch.Insert("whoop_spo2", block);
}

void insertRRBatch(clickhouse::Client& ch, const std::vector<whoop::RRIntervalRecord>& records) {
    if (records.empty()) return;

    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto rr_col = std::make_shared<clickhouse::ColumnUInt16>();

    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        rr_col->Append(r.rr_ms);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("rr_ms",     rr_col);

    ch.Insert("whoop_rr_intervals", block);
}

void insertGyroBatch(clickhouse::Client& ch, const std::vector<whoop::GyroRecord>& records) {
    if (records.empty()) return;
    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto x_col  = std::make_shared<clickhouse::ColumnFloat32>();
    auto y_col  = std::make_shared<clickhouse::ColumnFloat32>();
    auto z_col  = std::make_shared<clickhouse::ColumnFloat32>();
    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        x_col->Append(r.x); y_col->Append(r.y); z_col->Append(r.z);
    }
    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("gx", x_col); block.AppendColumn("gy", y_col); block.AppendColumn("gz", z_col);
    ch.Insert("whoop_gyro", block);
}

void insertDoubleTapBatch(clickhouse::Client& ch, const std::vector<whoop::DoubleTapRecord>& records) {
    if (records.empty()) return;
    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    for (const auto& r : records) ts_col->Append(r.timestamp_ns);
    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    ch.Insert("whoop_double_tap", block);
}

void insertWristStateBatch(clickhouse::Client& ch, const std::vector<whoop::WristStateRecord>& records) {
    if (records.empty()) return;
    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto state_col = std::make_shared<clickhouse::ColumnUInt8>();
    for (const auto& r : records) {
        ts_col->Append(r.timestamp_ns);
        state_col->Append(r.on_wrist ? 1 : 0);
    }
    clickhouse::Block block;
    block.AppendColumn("timestamp", ts_col);
    block.AppendColumn("on_wrist", state_col);
    ch.Insert("whoop_wrist_state", block);
}

} // namespace db

