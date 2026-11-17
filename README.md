# 📞 Contact Center Analytics Platform

> Real-time and batch data platform for a multi-market contact center (UK · DE · ES).
> Call and agent events stream through **Kafka**, land in an **S3 lakehouse** (RustFS), are processed with **PySpark** and **dbt**,
> and feed live and historical dashboards with the KPIs a real-time analyst watches all day:
> service level, abandon rate, occupancy, schedule adherence and end-of-day projections.

All data is **simulated** by a generator in this repository. It mimics the shape of contact center exports
(contact records, agent states, schedules, forecasts) but contains no real company or customer data.

> 🚧 Work in progress: this README grows with each step.

## Architecture (target)

```
generator (Python)
   │  call + agent events
   ▼
Redpanda (Kafka) ──► stream processor ──► PostgreSQL "realtime" ──► live dashboard
   │
   ▼  daily files
RustFS (S3): bronze ──► PySpark ──► silver ──► gold ──► dbt star schema ──► Power BI
```

## Roadmap

| Step | What | Tech | Status |
|---|---|---|---|
| 0 | Setup: repo, Docker Compose (Redpanda, RustFS S3 storage, PostgreSQL) | Docker | ✅ |
| 1 | Realistic data generator: contacts, agent states, schedules, forecast | Python | ✅ |
| 2 | Real time: events into Kafka, KPIs every 15 minutes | Redpanda, Python | ✅ |
| 3 | Batch lakehouse: bronze → silver → gold | RustFS (S3), PySpark | ⏳ |
| 4 | Star schema + data tests | dbt | |
| 5 | Real-time analyst logic: EOD service level projection, Erlang C staffing, adherence colour code | Python, SQL | |
| 6 | Automatic intraday status message | Python | |
| 7 | Live dashboard + historical dashboard | Grafana / Streamlit, Power BI | |
| 8 | Orchestration + CI | Airflow, GitHub Actions | |

## Run it locally

Requirements: **Docker Desktop** (nothing else to install).

```bash
git clone https://github.com/AYMANETBAIK98/CONTACT-CENTER-ANALYTICS.git
cd CONTACT-CENTER-ANALYTICS

docker compose up -d                                          # start Redpanda, RustFS (S3), PostgreSQL
docker compose run --rm tools python scripts/check_setup.py   # check everything is ready
```

Expected output:

```
[OK]   PostgreSQL reachable: PostgreSQL 16.x
[OK]   PostgreSQL schemas: ['marts', 'monitoring', 'realtime', 'staging']
[OK]   Redpanda reachable: 1 broker(s) on redpanda:9092
[OK]   Kafka topics: agent-events (3 partitions), contact-events (3 partitions)
[OK]   S3 storage (RustFS) reachable on http://rustfs:9000
[OK]   S3 buckets: ['bronze', 'gold', 'silver']

Environment ready. On to step 1!
```

| Service | URL / port | Login |
|---|---|---|
| Redpanda Console (Kafka UI) | http://localhost:8080 | – |
| RustFS console (S3 UI) | http://localhost:9001/rustfs/console/ | `lakehouse` / `lakehouse123` |
| PostgreSQL | `localhost:5435`, database `contact_center` | `cc` / `cc` |
| Kafka API (from your PC) | `localhost:19092` | – |

Stop with `docker compose down` (data is kept). `docker compose down -v` also deletes the data.

## Step 1: the data generator

`generator/` simulates a contact center day by day, for 3 markets, and writes the files a real-time analyst
works with. Same `--seed` = same data.

```bash
docker compose run --rm tools python -m generator.generate --start 2026-11-02 --days 14
docker compose run --rm tools pytest -v tests/test_generator.py
```

| Dataset | Format | One row = | Like in real life |
|---|---|---|---|
| `forecast` | CSV | a 15-minute interval: expected contacts and handle time | the WFM forecast |
| `schedule` | CSV | a planned shift, break or lunch | the WFM schedule |
| `contacts` | JSON Lines | a call, shaped like an Amazon Connect contact trace record | the telephony export |
| `agent_events` | JSON Lines | an agent state change (LOGIN, AVAILABLE, ON_CONTACT, ACW, BREAK, LUNCH, LOGOUT) | the agent event stream |

Files land in `data/landing/<dataset>/date=YYYY-MM-DD/market=XX/`, partitioned like a data lake.

**What makes it realistic**

| Market | Opening hours (local) | Weekday volume | Talk + wrap-up |
|---|---|---|---|
| UK | Mon-Fri 8-20, Sat 9-17 | ~2,400 | 330 s + 45 s |
| DE | Mon-Fri 8-20, Sat 9-17 | ~1,800 | 360 s + 50 s |
| ES | Mon-Fri 9-21 | ~1,500 | 300 s + 40 s |

- Intraday curve with a late-morning peak, a lunch dip and an afternoon peak; Monday busiest, Saturday half.
- Actual volume misses the forecast (6 % per day, 10 % per interval), like any forecast.
- Staffing from **Erlang C** (the agents each interval needs for 80/20), minus up to 40 % (lean staffing),
  with 8-hour shifts and staggered breaks and lunches.
- Agents are sometimes absent (6 %), late (12 %), overrun breaks (18 %) or log out early (5 %).
- Calls are answered in arrival order by the first free agent; callers hang up when their patience runs out.

Result over two weeks: daily service level between ~69 % and ~96 % (80/20 target), abandon rate 2-12 %;
every Saturday misses the target. About 1 interval in 5 is below 80 %: openings, peaks and Saturdays.
All tunable in `generator/config.py`.

## Step 2: real time with Kafka

A **producer** replays a simulated day into Redpanda (Kafka) at accelerated speed, in time order.
A **consumer** reads the events as they arrive and keeps the KPIs of every market and 15-minute interval
up to date in PostgreSQL (`realtime.interval_kpis`), printing the last closed interval like a live wallboard.

```bash
# terminal 1: start the KPI consumer (Ctrl+C to stop)
docker compose run --rm tools python -m streaming.consumer
# terminal 2: replay Wednesday 4 November at x120 (a 12-hour day in ~7 minutes)
docker compose run --rm tools python -m streaming.producer --date 2026-11-04 --speed 120
```

```
[11:00 UTC] UK LOW SL  62% abd  10% occ  97% adh  88% | DE OK  SL 100% abd   0% occ  40% adh  91% | ES OK  SL 100% abd   0% occ  43% adh  87%
```

| Topic | Events | Key |
|---|---|---|
| `contact-events` | `CONTACT_QUEUED`, `CONTACT_ANSWERED`, `CONTACT_ABANDONED`, `CONTACT_ENDED` (built from each contact record) | market |
| `agent-events` | agent state changes | market |

The key is the market: Kafka keeps the order of messages with the same key, so each market's events
arrive in the order they happened.

| KPI | Definition |
|---|---|
| Service level | contacts answered within 20 s / contacts offered |
| Abandon rate | contacts abandoned / contacts offered |
| ASA | average wait of answered contacts |
| AHT | average talk + wrap-up |
| Occupancy | time on contact or wrap-up / productive time (available + on contact + wrap-up) |
| Adherence | time doing what the schedule planned / scheduled time (work + breaks) |

Contacts are counted in the interval they were queued in; agent time is cut at interval boundaries.

**What if it crashes or runs twice?**
- The consumer rebuilds its state from the Kafka log at every start (new consumer group, read from the beginning).
- Every event has an id; an event already counted is ignored, so replaying a day never double-counts.
- Rows are written with an UPSERT on `(market, interval_start)`: rewriting an interval replaces it.

Tests (`tests/test_streaming.py`) check that KPIs counted event by event equal the batch truth,
that a replayed day gives the same numbers, and that adherence drops when a break runs long.
`tests/test_erlang.py` checks the Erlang C formula against published table values.

## Project structure

```
├── docker-compose.yml   # infrastructure: Redpanda, RustFS (S3), PostgreSQL + a Python "tools" container
├── Dockerfile.tools     # the tools container (Python 3.11 + libraries)
├── sql/init.sql         # PostgreSQL schemas, created on first start
├── scripts/             # setup check, bucket creation
├── generator/           # step 1: simulated contact center data + Erlang C
├── streaming/           # step 2: Kafka producer, KPI consumer, shared KPI logic
├── spark/               # step 3: PySpark lakehouse jobs
├── dbt/                 # step 4: star schema + tests
├── dashboards/          # step 7
├── tests/               # pytest
└── docs/                # documentation
```

## Notes
- **Why RustFS and not MinIO?** MinIO is the usual local S3, but its Docker images were removed in 2026.
  RustFS speaks the same S3 API, so the code (boto3, Spark `s3a://`) is identical to what would run on AWS S3.
