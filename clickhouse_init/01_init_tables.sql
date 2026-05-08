-- ============================================================
-- dhoop ClickHouse schema
-- Engine: ReplacingMergeTree — deduplicates on full ORDER BY key.
-- All timestamps are nanosecond-precision UTC (DateTime64(9)).
-- ============================================================
USE dhoop;
-- Raw BLE hex payloads from the iOS bridge
CREATE TABLE IF NOT EXISTS whoop_raw_data
(
    timestamp DateTime64(9),
    data      String
)
ENGINE = ReplacingMergeTree()
ORDER BY timestamp;

-- Decoded heart-rate samples
CREATE TABLE IF NOT EXISTS whoop_hr
(
    timestamp DateTime64(9),
    hr        UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY timestamp;

-- Decoded 3-axis accelerometer samples
CREATE TABLE IF NOT EXISTS whoop_accelerometer
(
    timestamp DateTime64(9),
    acc0      Float32,
    acc1      Float32,
    acc2      Float32
)
ENGINE = ReplacingMergeTree()
ORDER BY timestamp;
