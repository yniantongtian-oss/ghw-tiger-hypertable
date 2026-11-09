# ghw-tiger-hypertable — my first Tiger Data hypertable

MLH Global Hack Week: Builders Week · Tiger Data Challenge 1 *"Launch Your First Tiger Data Hypertable"*.

A real public dataset — **140,352 hourly weather observations** (Open-Meteo archive, 8 cities on 6 continents, 2024-01-01 → 2025-12-31,
`data/weather_hourly_2024_2025.csv.gz`) — is imported into a **plain PostgreSQL table on a free Tiger Cloud service**, benchmarked,
then converted **in place** into a TimescaleDB hypertable:

1. `COPY` the CSV into a standard table `ghw23.weather`.
2. Time a few typical queries on the plain table.
3. `SELECT create_hypertable('ghw23.weather', by_range('time', INTERVAL '7 days'), migrate_data => true);` → 7-day chunks.
4. Build `ghw23.weather_ts` with **time + space partitioning** (`by_range('time', '30 days')` + `add_dimension(by_hash('city', 4))`).
5. Enable the **columnstore** (`segmentby = city`, `orderby = time DESC`) and compress older chunks; compare size and query time.
6. Show chunk exclusion in `EXPLAIN` — a one-week query only touches the chunks for that week.

Everything the script prints is saved to [`results/RESULTS.md`](results/RESULTS.md) and `results/results.json`.

## Run it

```bash
cp .env.example .env            # put your Tiger Cloud connection string in DATABASE_URL
export $(grep -v '^#' .env | xargs)
pip install -r requirements.txt
python hypertable_demo.py       # re-runnable: it drops and recreates schema ghw23
```

Create a free service at <https://console.cloud.tigerdata.com> (Quickstart: <https://www.tigerdata.com/docs/get-started>).
The script only needs `DATABASE_URL`; credentials are never committed (`.env` is git-ignored).

### Run from GitHub Actions
`ci/run-live.yml` runs the demo against the live service using a `DATABASE_URL` repository secret and commits fresh results.
Copy it to `.github/workflows/` to enable it.

## Stack
Tiger Cloud (TimescaleDB on PostgreSQL 17) · hypertables · hash space partitioning · columnstore compression · Python 3 · psycopg 3 · Open-Meteo data
