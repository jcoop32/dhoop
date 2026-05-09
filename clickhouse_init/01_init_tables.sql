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

-- Aggregated daily metrics for historical baselining
CREATE TABLE IF NOT EXISTS whoop_daily_summary
(
    date                 Date,
    sleep_score          Float32,
    daily_strain         Float32,
    resting_hr           Float32,
    hrv_rmssd            Float32,
    sleep_duration_min   Float32,
    time_in_bed_min      Float32,
    disturbances         UInt16
)
ENGINE = ReplacingMergeTree()
ORDER BY date;

-- Skin temperature samples (raw 16-bit ADC / 100 → Celsius)
CREATE TABLE IF NOT EXISTS whoop_skin_temp
(
    timestamp DateTime64(9),
    temp_c    Float32
)
ENGINE = ReplacingMergeTree()
ORDER BY timestamp;

-- Blood oxygen saturation samples
CREATE TABLE IF NOT EXISTS whoop_spo2
(
    timestamp DateTime64(9),
    spo2      UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY timestamp;

-- Beat-to-beat RR intervals in milliseconds (one row per beat — HRV source)
CREATE TABLE IF NOT EXISTS whoop_rr_intervals
(
    timestamp DateTime64(9),
    rr_ms     UInt16
)
ENGINE = ReplacingMergeTree()
ORDER BY (timestamp, rr_ms);