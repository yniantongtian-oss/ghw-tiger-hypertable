# Hypertable run on Tiger Cloud

Server: `PostgreSQL 18.6 (Ubuntu 18.6-1.pgdg22.04+2) on aarch64-unknown-linux-gnu`  
TimescaleDB: `2.31.0`  
Run at: 2026-11-09 07:58:19 UTC

## 1. Imported sample dataset into a plain table

140,352 hourly rows (2024-01-01 00:00:00+00:00 .. 2025-12-31 23:00:00+00:00), 8 cities, COPY took 2.3s.

## 2. Converted the standard table into a hypertable (in place)

```sql
SELECT create_hypertable('ghw23.weather', by_range('time', INTERVAL '7 days'), migrate_data => true);
```

Migration took 1.6s.

### timescaledb_information.hypertables

| hypertable_schema | hypertable_name | num_dimensions | num_chunks |
|---|---|---|---|
| ghw23 | weather | 1 | 105 |

### First 5 chunks (7-day ranges)

| chunk_name | range_start | range_end |
|---|---|---|
| _hyper_1_1_chunk | 2023-12-28 00:00:00+00:00 | 2024-01-04 00:00:00+00:00 |
| _hyper_1_2_chunk | 2024-01-04 00:00:00+00:00 | 2024-01-11 00:00:00+00:00 |
| _hyper_1_3_chunk | 2024-01-11 00:00:00+00:00 | 2024-01-18 00:00:00+00:00 |
| _hyper_1_4_chunk | 2024-01-18 00:00:00+00:00 | 2024-01-25 00:00:00+00:00 |
| _hyper_1_5_chunk | 2024-01-25 00:00:00+00:00 | 2024-02-01 00:00:00+00:00 |

## 3. Time + space partitioning (hash on city)

```sql
CREATE TABLE ghw23.weather_ts (LIKE ghw23.weather);
SELECT create_hypertable('ghw23.weather_ts', by_range('time', INTERVAL '30 days'));
SELECT add_dimension('ghw23.weather_ts', by_hash('city', 4));
INSERT INTO ghw23.weather_ts SELECT * FROM ghw23.weather;
```

Re-ingested 140,352 rows in 1.0s (140,537 rows/s, server-side).

### Dimensions of ghw23.weather_ts

| dimension_number | column_name | dimension_type | time_interval | num_partitions |
|---|---|---|---|---|
| 1 | time | Time | 30 days, 0:00:00 |  |
| 2 | city | Space |  | 4 |

`weather_ts` has 100 chunks (time x city-hash).

## 4. Columnstore (compression)

Compressed 100 chunks older than 2025-12-01. Hypertable size 29 MB -> 15 MB (2.0x smaller).

## 5. Query timings (server-side EXPLAIN ANALYZE execution time, ms)

| query | plain table | hypertable | hypertable + columnstore |
|---|---|---|---|
| one_week_one_city | 0.18 | 0.15 | 1.2 |
| daily_avg_one_month | 3.13 | 2.47 | 2.5 |
| latest_reading_per_city | 0.11 | 0.11 | 0.11 |

### Chunk exclusion: plan for the one-week query touches only the matching chunk(s)

```
Finalize Aggregate  (cost=38.38..38.39 rows=1 width=16)
  ->  Append  (cost=19.17..38.37 rows=2 width=40)
        ->  Custom Scan (VectorAgg)  (cost=19.17..19.18 rows=1 width=40)
              ->  Custom Scan (ColumnarScan) on _hyper_1_79_chunk  (cost=14.17..14.17 rows=1000 width=16)
                    Vectorized Filter: (("time" >= '2025-07-01 00:00:00+00'::timestamp with time zone) AND ("time" < '2025-07-08 00:00:00+00'::timestamp with time zone))
                    ->  Index Scan using _hyper_1_79_chunk_compressed_city__ts_meta_v2_first_time__t_idx on _hyper_1_79_chunk_compressed  (cost=0.15..4.17 rows=1 width=148)
                          Index Cond: ((city = 'Tokyo'::text) AND (_ts_meta_v2_first_time >= '2025-07-01 00:00:00+00'::timestamp with time zone) AND (_ts_meta_v2_last_time < '2025-07-08 00:00:00+00'::timestamp with time zone))
        ->  Custom Scan (VectorAgg)  (cost=19.17..19.18 rows=1 width=40)
              ->  Custom Scan (ColumnarScan) on _hyper_1_80_chunk  (cost=14.17..14.17 rows=1000 width=16)
                    Vectorized Filter: (("time" >= '2025-07-01 00:00:00+00'::timestamp with time zone) AND ("time" < '2025-07-08 00:00:00+00'::timestamp with time zone))
                    ->  Index Scan using _hyper_1_80_chunk_compressed_city__ts_meta_v2_first_time__t_idx on _hyper_1_80_chunk_compressed  (cost=0.15..4.17 rows=1 width=148)
                          Index Cond: ((city = 'Tokyo'::text) AND (_ts_meta_v2_first_time >= '2025-07-01 00:00:00+00'::timestamp with time zone) AND (_ts_meta_v2_last_time < '2025-07-08 00:00:00+00'::timestamp with time zone))
Allocated Memory: allocated_by_plan=437kB base_allocation=13606kB base_allocation_increase=2048kB
```

### Sample analytic result: daily average temperature, March 2025 (first 8 rows)

| d | city | avg_temp_c |
|---|---|---|
| 2025-03-01 00:00:00+00:00 | London | 4.00 |
| 2025-03-01 00:00:00+00:00 | Nairobi | 23.59 |
| 2025-03-01 00:00:00+00:00 | New York | 7.96 |
| 2025-03-01 00:00:00+00:00 | Reykjavik | 3.69 |
| 2025-03-01 00:00:00+00:00 | Sao Paulo | 25.55 |
| 2025-03-01 00:00:00+00:00 | Singapore | 27.08 |
| 2025-03-01 00:00:00+00:00 | Sydney | 24.64 |
| 2025-03-01 00:00:00+00:00 | Tokyo | 13.99 |
