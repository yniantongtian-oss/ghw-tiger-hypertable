"""Challenge: Launch Your First Tiger Data Hypertable.

Loads a real hourly weather dataset (Open-Meteo, 8 cities, 2024-2025) into a
plain PostgreSQL table on Tiger Cloud, benchmarks it, converts it in place into
a TimescaleDB hypertable (time partitioning), builds a second hypertable with
time + space (hash on city) partitioning, enables the columnstore, and records
every result to results/.

Reads the connection string from the DATABASE_URL environment variable.
"""
import gzip, json, os, statistics, time
from pathlib import Path
import psycopg

ROOT = Path(__file__).parent
OUT = ROOT / "results"
OUT.mkdir(exist_ok=True)
DATA = ROOT / "data" / "weather_hourly_2024_2025.csv.gz"
log_lines = []

def log(s=""):
    print(s, flush=True)
    log_lines.append(s)

def table(cur, sql, params=None, title=None):
    cur.execute(sql, params)
    cols = [d.name for d in cur.description]
    rows = cur.fetchall()
    if title:
        log(f"\n### {title}\n")
    log("| " + " | ".join(cols) + " |")
    log("|" + "---|" * len(cols))
    for r in rows:
        log("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return rows

def bench(cur, sql, n=5):
    cur.execute(sql)  # warm-up
    cur.fetchall()
    ts = []
    for _ in range(n):
        t = time.perf_counter(); cur.execute(sql); cur.fetchall(); ts.append((time.perf_counter() - t) * 1000)
    return round(statistics.median(ts), 2)

def plan_time(cur, sql):
    cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
    p = cur.fetchone()[0][0]
    return round(p["Execution Time"], 2)

QUERIES = {
    "one_week_one_city": "SELECT avg(temperature_c), max(wind_kmh) FROM {t} WHERE city='Tokyo' AND time >= '2025-07-01' AND time < '2025-07-08'",
    "daily_avg_one_month": "SELECT time_bucket('1 day', time) d, city, round(avg(temperature_c)::numeric, 2) AS avg_temp_c FROM {t} WHERE time >= '2025-03-01' AND time < '2025-04-01' GROUP BY 1,2 ORDER BY 1,2",
    "latest_reading_per_city": "SELECT DISTINCT ON (city) city, time, temperature_c FROM {t} WHERE time > '2025-12-30' ORDER BY city, time DESC",
}

def main():
    url = os.environ["DATABASE_URL"]
    results = {}
    with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SET TimeZone = 'UTC'")
        cur.execute("SELECT version(), (SELECT extversion FROM pg_extension WHERE extname='timescaledb')")
        v, tsv = cur.fetchone()
        log(f"# Hypertable run on Tiger Cloud\n\nServer: `{v.split(',')[0]}`  \nTimescaleDB: `{tsv}`  \nRun at: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        results["postgres"] = v.split(",")[0]; results["timescaledb"] = tsv

        cur.execute("DROP SCHEMA IF EXISTS ghw23 CASCADE; CREATE SCHEMA ghw23")
        cur.execute("""CREATE TABLE ghw23.weather (
            time timestamptz NOT NULL, city text NOT NULL, temperature_c double precision,
            humidity_pct double precision, wind_kmh double precision, precipitation_mm double precision)""")
        t = time.perf_counter()
        with gzip.open(DATA, "rt") as f, cur.copy("COPY ghw23.weather FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
            while chunk := f.read(1 << 16):
                cp.write(chunk)
        load_s = time.perf_counter() - t
        cur.execute("CREATE INDEX ON ghw23.weather (city, time DESC)")
        cur.execute("ANALYZE ghw23.weather")
        cur.execute("SELECT count(*), min(time), max(time) FROM ghw23.weather")
        n, tmin, tmax = cur.fetchone()
        log(f"\n## 1. Imported sample dataset into a plain table\n\n{n:,} hourly rows ({tmin} .. {tmax}), 8 cities, COPY took {load_s:.1f}s.")
        results["rows"] = n

        plain = {k: plan_time(cur, q.format(t="ghw23.weather")) for k, q in QUERIES.items()}

        log("\n## 2. Converted the standard table into a hypertable (in place)\n")
        log("```sql\nSELECT create_hypertable('ghw23.weather', by_range('time', INTERVAL '7 days'), migrate_data => true);\n```")
        t = time.perf_counter()
        cur.execute("SELECT create_hypertable('ghw23.weather', by_range('time', INTERVAL '7 days'), migrate_data => true)")
        log(f"\nMigration took {time.perf_counter()-t:.1f}s.")
        cur.execute("ANALYZE ghw23.weather")
        table(cur, """SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks
                      FROM timescaledb_information.hypertables WHERE hypertable_schema='ghw23'""", title="timescaledb_information.hypertables")
        table(cur, """SELECT chunk_name, range_start, range_end FROM timescaledb_information.chunks
                      WHERE hypertable_schema='ghw23' AND hypertable_name='weather' ORDER BY range_start LIMIT 5""", title="First 5 chunks (7-day ranges)")

        log("\n## 3. Time + space partitioning (hash on city)\n")
        log("```sql\nCREATE TABLE ghw23.weather_ts (LIKE ghw23.weather);\nSELECT create_hypertable('ghw23.weather_ts', by_range('time', INTERVAL '30 days'));\nSELECT add_dimension('ghw23.weather_ts', by_hash('city', 4));\nINSERT INTO ghw23.weather_ts SELECT * FROM ghw23.weather;\n```")
        cur.execute("CREATE TABLE ghw23.weather_ts (LIKE ghw23.weather)")
        cur.execute("SELECT create_hypertable('ghw23.weather_ts', by_range('time', INTERVAL '30 days'))")
        cur.execute("SELECT add_dimension('ghw23.weather_ts', by_hash('city', 4))")
        t = time.perf_counter()
        cur.execute("INSERT INTO ghw23.weather_ts SELECT * FROM ghw23.weather")
        ins_s = time.perf_counter() - t
        log(f"\nRe-ingested {n:,} rows in {ins_s:.1f}s ({n/ins_s:,.0f} rows/s, server-side).")
        table(cur, """SELECT dimension_number, column_name, dimension_type, time_interval, num_partitions
                      FROM timescaledb_information.dimensions WHERE hypertable_schema='ghw23' AND hypertable_name='weather_ts'
                      ORDER BY dimension_number""", title="Dimensions of ghw23.weather_ts")
        cur.execute("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='ghw23' AND hypertable_name='weather_ts'")
        log(f"\n`weather_ts` has {cur.fetchone()[0]} chunks (time x city-hash).")

        hyper = {k: plan_time(cur, q.format(t="ghw23.weather")) for k, q in QUERIES.items()}

        cur.execute("SELECT pg_size_pretty(hypertable_size('ghw23.weather')), hypertable_size('ghw23.weather')")
        before_pretty, before = cur.fetchone()
        log("\n## 4. Columnstore (compression)\n")
        cur.execute("ALTER TABLE ghw23.weather SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = 'city', timescaledb.orderby = 'time DESC')")
        cur.execute("SELECT count(*) FROM (SELECT compress_chunk(c) FROM show_chunks('ghw23.weather', older_than => '2025-12-01'::timestamptz) c) s")
        nc = cur.fetchone()[0]
        cur.execute("SELECT pg_size_pretty(hypertable_size('ghw23.weather')), hypertable_size('ghw23.weather')")
        after_pretty, after = cur.fetchone()
        log(f"Compressed {nc} chunks older than 2025-12-01. Hypertable size {before_pretty} -> {after_pretty} ({before/after:.1f}x smaller).")
        results["size_before"] = before_pretty; results["size_after"] = after_pretty; results["compression_ratio"] = round(before/after, 1)
        compressed = {k: plan_time(cur, q.format(t="ghw23.weather")) for k, q in QUERIES.items()}

        log("\n## 5. Query timings (server-side EXPLAIN ANALYZE execution time, ms)\n")
        log("| query | plain table | hypertable | hypertable + columnstore |")
        log("|---|---|---|---|")
        for k in QUERIES:
            log(f"| {k} | {plain[k]} | {hyper[k]} | {compressed[k]} |")
        results["timings_ms"] = {"plain": plain, "hypertable": hyper, "columnstore": compressed}

        cur.execute("EXPLAIN " + QUERIES["one_week_one_city"].format(t="ghw23.weather"))
        log("\n### Chunk exclusion: plan for the one-week query touches only the matching chunk(s)\n\n```")
        for (line,) in cur.fetchall():
            log(line)
        log("```")
        table(cur, QUERIES["daily_avg_one_month"].format(t="ghw23.weather") .replace("ORDER BY 1,2", "ORDER BY 1,2 LIMIT 8"), title="Sample analytic result: daily average temperature, March 2025 (first 8 rows)")

    (OUT / "RESULTS.md").write_text("\n".join(log_lines) + "\n")
    (OUT / "results.json").write_text(json.dumps(results, indent=2, default=str))

if __name__ == "__main__":
    main()
