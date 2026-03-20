# Airflow Docker Environment

**Project:** DHAP-34 | **Author:** naga_poluri

This folder has everything needed to run Airflow locally using Docker Compose.
It sets up Airflow plus two Postgres databases — one for Airflow internals
and one as the dev target where pipeline data gets loaded.

---

## Requirements

- Docker Desktop installed and running
- Python 3.9+ (for generating keys)
- Git

---

## Setup (first time only)

**1. Copy the env file**
```bash
cp .env.example .env
```

**2. Generate a Fernet key and add it to .env**
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
Paste the output as `AIRFLOW__CORE__FERNET_KEY` in your `.env`

**3. Generate a secret key and add it to .env**
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```
Paste the output as `AIRFLOW_WEBSERVER_SECRET_KEY` in your `.env`

**4. Start everything**
```bash
docker compose up -d
```
First run takes about 2-3 minutes to pull images and set up.

**5. Check containers are running**
```bash
docker ps
```

**6. Open Airflow**
Go to http://localhost:8080
Login: `admin` / `admin`

---

## Daily Commands

```bash
# start
docker compose up -d

# stop (keeps data)
docker compose down

# full reset (deletes everything)
docker compose down -v

# see logs
docker compose logs -f

# see logs for one service
docker compose logs -f airflow-scheduler
```

---

## Services

| Service | Port | What it does |
|---------|------|-------------|
| airflow-webserver | localhost:8080 | Airflow UI |
| airflow-scheduler | internal | watches DAGs and runs tasks |
| airflow-db | internal | Airflow's internal metadata |
| target-db | localhost:5433 | where pipeline data gets loaded |

---

## Connect to Target DB directly

```bash
docker exec -it target_postgres_db psql -U pg_user -d customer_db
```
