# Sport Data Solution — POC Avantages Sportifs
 
## Context
 
Sport Data Solution is a startup founded by Alexandre (cyclist) and Juliette (marathoner). The company develops performance monitoring solutions for amateur and semi-professional athletes.
 
This POC implements two employee benefits:
 
- **Prime sportive (5% salary bonus):** For employees commuting to the office by walking (max 15 km) or cycling (max 25 km), validated via Google Maps API.
- **5 well-being days per year:** For employees with 15+ sport activities in a rolling 12-month period.
 
Each sport activity triggers a congratulatory Slack notification. All business parameters are adjustable via SCD2 pattern for full auditability.
 
## Tech Stack
 
| Component | Tool | Version | Purpose |
|---|---|---|---|
| Database | PostgreSQL | 15.7 | Primary data store (3 databases: sportdb, kestradb, metabasedb) |
| Message broker | Redpanda | 24.2.7 | Kafka-compatible event streaming |
| Orchestration | Kestra | 0.18.0 | Pipeline scheduling and execution |
| Dashboard | Metabase | 0.50.24 | KPI visualization |
| Language | Python | 3.11 | ETL pipelines and data processing |
| Validation | Pydantic | v2 | Row-level data validation |
| Data quality | Great Expectations | 1.15 | Dataset-level quality checks (31 expectations) |
| Testing | Pytest | 9.0.2 | Unit tests (76 tests) |
| ORM | SQLAlchemy | 2.x | Database connection and session management |
| Streaming | confluent-kafka | — | Redpanda producer/consumer |
| Address validation | Google Maps API | — | Geocoding + Distance Matrix |
| Notifications | Slack Incoming Webhook | — | Activity notifications |
| Containerization | Docker Compose | — | All services on sport_network |
 
## Architecture
 
```
Sources                    Pipeline (Kestra)                    Outputs
─────────                  ─────────────────                    ───────
DonneesRH.xlsx     ──►  Phase 1: HR Pipeline               ──►  PostgreSQL (employees)
DonneesSportive.xlsx       Extract → Validate → Transform
                           → Google Maps → Encrypt → Upsert
 
                       Phase 2: Activity Pipeline           ──►  PostgreSQL (sport_activities)
                           Generate → Redpanda → Consumer   ──►  Slack notifications
 
                       Phase 3: Benefit Pipeline            ──►  PostgreSQL (employee_benefits)
                           Read rules → Compute eligibility
                           → Upsert benefits
                                                             ──►  Metabase dashboard
```
 
## Database Schema
 
Four tables in sportdb (public schema):
 
**employees** — 161 rows. PII columns (name, salary, address) encrypted with pgcrypto. Soft delete via rh_is_active flag.
 
**sport_activities** — 7,240 rows (7,237 historical + live injections). Unique constraint on (employee_id, start_date). Soft delete via sp_is_active flag.
 
**benefit_rules** — SCD2 configuration table. Stores business parameters (prime_rate, min_activities, well_being_days, distance thresholds). INSERT-only, never updated — new rows with new effective_date preserve full history.
 
**employee_benefits** — 161 rows. Computed results from Phase 3. UPSERT on (employee_id, period_start, period_end) for idempotent re-runs.
 
## Project Structure
 
```
Projet_12/
├── data/raw/                    Source Excel files
├── docker/postgres/init.sh      Database initialization (roles, schemas)
├── docker-compose.yml           5 containers on sport_network
├── docs/                        ADRs, architecture diagram, quality report
├── kestra/flows/                4 Kestra YAML flows
├── migrations/                  V1-V9 SQL migrations
├── notebooks/                   EDA notebooks (RH + Sport data)
├── src/
│   ├── consumers/               Redpanda → PostgreSQL + Slack
│   ├── generators/              Deterministic activity generator (seed=42)
│   ├── pipelines/               HR pipeline, benefit computation, live injection
│   ├── producers/               Redpanda topic publisher
│   ├── quality/                 Great Expectations (31 expectations)
│   ├── utils/                   db, logger, encryption, gmaps
│   └── validators/              Pydantic schemas (employee + activity)
├── tests/unit/                  76 unit tests
├── pyproject.toml               Poetry dependency management
├── requirements.txt             Kestra pip compatibility
└── .env.example                 All environment variables
```
 
## Quick Start
 
### Prerequisites
 
- Docker and Docker Compose
- Python 3.11 (via pyenv recommended)
- Poetry
- Google Maps API key (free tier)
- Slack workspace with incoming webhook
 
### 1. Clone and configure
 
```bash
git clone https://github.com/alexqtn/oc_data_engineer_p12.git
cd Projet_12
cp .env.example .env
# Edit .env with your credentials
```
 
### 2. Start infrastructure
 
```bash
docker-compose up -d
```
 
Wait for all 5 containers to be healthy:
 
```bash
docker ps
```
 
### 3. Apply database migrations
 
```bash
for f in migrations/V*.sql; do
    docker exec -i sport_postgres psql -U sport_admin -d sportdb < "$f"
done
```
 
### 4. Install Python dependencies
 
```bash
poetry install
```
 
### 5. Run the full pipeline
 
Option A — via Kestra UI (http://localhost:8080):
 
Execute the `full_pipeline` flow.
 
Option B — via Python:
 
```bash
# Phase 1: Load employees
poetry run python -m src.pipelines.load_employees
 
# Phase 2: Generate and publish activities
poetry run python -m src.generators.generate_activities
poetry run python -m src.producers.publish_activities
poetry run python -m src.consumers.consumer_postgres
 
# Phase 3: Compute benefits
poetry run python -m src.pipelines.compute_benefits
```
 
### 6. Run tests
 
```bash
# Unit tests (76 tests, no Docker needed)
poetry run pytest tests/unit/ -v
 
# Data quality (Great Expectations, Docker must be running)
poetry run python -m src.quality.run_quality_checks
```
 
### 7. View dashboard
 
Open http://localhost:3000 (Metabase).
 
## Kestra Flows
 
| Flow | Purpose | Trigger |
|---|---|---|
| full_pipeline | Runs all 3 phases sequentially | Manual / Schedule |
| recompute_benefits | Phase 3 only (after parameter change) | Manual |
| update_benefit_rules | SCD2 INSERT + auto recompute | Manual (live demo) |
| inject_live_activity | Generate N activities → Redpanda → Slack → DB → Recompute | Manual (live demo) |
 
## Data Quality — 4 Layers
 
| Layer | Tool | Scope | When |
|---|---|---|---|
| Row-level | Pydantic v2 | Field types, ranges, cross-field rules | Before DB insert |
| Database-level | PostgreSQL | ENUMs, NOT NULL, UNIQUE, FK, CHECK | At DB insert |
| Dataset-level | Great Expectations | 31 expectations (completeness, uniqueness, ranges) | After pipeline run |
| Code-level | Pytest | 76 unit tests on transform, eligibility, validators | CI / manual |
 
## Security
 
- PII encrypted at rest with pgcrypto (symmetric encryption): name, salary, birth date, address
- Three PostgreSQL roles: sport_admin (DDL), sport_writer (DML), sport_reader (SELECT only)
- Metabase connects as sport_reader — cannot modify data
- All credentials in .env (gitignored), .env.example documents required variables
- Docker services communicate on isolated bridge network (sport_network)
- No SSL in POC (all traffic local). Production recommendation: enable SSL + certificate auth.
 
## Key Architectural Decisions
 
| Decision | Choice | Alternative considered | Rationale |
|---|---|---|---|
| Message broker | Redpanda | Apache Kafka | Simpler (no ZooKeeper), Kafka-compatible API, ARM-native |
| Kafka client | confluent-kafka | kafka-python | kafka-python had silent consumer failures with Redpanda |
| Orchestrator | Kestra (OSS) | Airflow | Lighter, YAML-based, built-in UI, no DAG Python boilerplate |
| Dashboard | Metabase | Streamlit | Zero-code dashboarding, built-in filters, SQL native queries |
| Database | PostgreSQL | DuckDB/SQLite | Multi-user, pgcrypto for encryption, production-grade |
| Encryption | pgcrypto | Application-level | PostgreSQL-native, Metabase can decrypt at query time |
| Parameter history | SCD2 (INSERT-only) | UPDATE in place | Full audit trail, live demo shows parameter evolution |
| Delete strategy | Soft delete | Hard delete | Preserves history, reversible, audit-friendly |
| Pipeline pattern | UPSERT | INSERT + DELETE | Idempotent re-runs, no duplicate risk |
| Data generation | Deterministic (seed=42) | Random | Reproducible results across environments |
 
## Live Demo Sequence
 
### Demo 1 — Inject activities + Slack
 
1. Open Metabase dashboard
2. In Kestra UI → execute inject_live_activity (count=3)
3. Show Slack: 3 congratulatory messages appear
4. Refresh Metabase: activity counts updated
 
### Demo 2 — Change prime rate
 
1. Show current total: ~172,482 EUR (5%)
2. In Kestra UI → execute update_benefit_rules (prime_rate, 0.07)
3. Flow inserts new rule + recomputes benefits
4. Refresh Metabase: new total ~241,475 EUR (7%)
5. Show benefit_rules table: both 0.05 and 0.07 rows visible (SCD2)
 
## Environment Variables
 
See `.env.example` for the complete list. Variables are grouped by service: PostgreSQL, Redpanda, Slack, Google Maps, Kestra, Metabase, Encryption.