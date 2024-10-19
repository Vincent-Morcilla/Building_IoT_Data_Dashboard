CREATE TABLE streams(
    id      TEXT PRIMARY KEY,
    site    TEXT NOT NULL,
    brick_class  TEXT,
    units   TEXT
);
CREATE INDEX ON streams(site, brick_class);

CREATE TABLE data(
    time        TIMESTAMPTZ,
    stream_id   TEXT REFERENCES streams(id),
    value       FLOAT NOT NULL,
    PRIMARY KEY(time, stream_id)
);
CREATE INDEX ON data (stream_id, time DESC);
SELECT * FROM create_hypertable('data', 'time');

ALTER TABLE  data
  SET (timescaledb.compress,
      timescaledb.compress_orderby = 'time DESC',
      timescaledb.compress_segmentby = 'stream_id');
-- Timescale 1.x
-- SELECT add_compress_chunks_policy('data', INTERVAL '14 days');

-- Timescale 2.x
SELECT add_compression_policy('data', INTERVAL '14 days');

CREATE VIEW unified AS
    SELECT time, value, stream_id, brick_class, site, units
    FROM data LEFT JOIN streams ON data.stream_id = streams.id;


-- https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates
-- use MATERIALIZED for Timescale 2.x
CREATE MATERIALIZED VIEW hourly_summaries
 WITH (timescaledb.continuous) AS
 SELECT stream_id,
        time_bucket(INTERVAL '1 hour', time) AS bucket,
        COUNT(value) as count,
        MAX(value) as max,
        MIN(value) as min,
        AVG(value) as mean
 FROM data
 GROUP BY stream_id, bucket;
-- timescale 1.x
-- ALTER VIEW hourly_summaries SET (timescaledb.refresh_interval = '30 min');

-- timescale 2.x
SELECT add_continuous_aggregate_policy('hourly_summaries',
    start_offset => NULL,
    end_offset => INTERVAL '1 h',
    schedule_interval => INTERVAL '1 h');
