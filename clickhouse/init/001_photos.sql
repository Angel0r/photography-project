CREATE DATABASE IF NOT EXISTS photos;
CREATE TABLE IF NOT EXISTS photos.raw_files (
  file_id UUID, path String, relpath String, size_bytes UInt64,
  mtime DateTime, ctime DateTime, sha1 FixedString(40), phash String, mime LowCardinality(String),
  width UInt32, height UInt32, orientation LowCardinality(String),
  camera_make LowCardinality(String), camera_model LowCardinality(String), lens_model LowCardinality(String),
  iso UInt32, f_number Float32, exposure_time String, focal_length_mm Float32,
  taken_at_utc DateTime, tz_offset_minutes Int16,
  gps_lat Nullable(Float64), gps_lon Nullable(Float64), gps_alt_m Nullable(Float32),
  subjects Array(String), rating Nullable(UInt8), description String,
  import_ts DateTime DEFAULT now(), ingestion_run_id String
) ENGINE=MergeTree PARTITION BY toYYYYMM(taken_at_utc) ORDER BY (taken_at_utc, file_id);
CREATE TABLE IF NOT EXISTS photos.enrichments (
  file_id UUID, place_country LowCardinality(String), place_region LowCardinality(String),
  place_city LowCardinality(String), place_locality String, tz_name LowCardinality(String),
  ml_labels Array(String), faces Array(String), updated_at DateTime DEFAULT now()
) ENGINE=ReplacingMergeTree(updated_at) ORDER BY file_id;
CREATE VIEW IF NOT EXISTS photos.v_photos AS
SELECT r.*, e.place_country, e.place_region, e.place_city, e.place_locality, e.tz_name, e.ml_labels, e.faces
FROM photos.raw_files r LEFT JOIN photos.enrichments e USING (file_id);
