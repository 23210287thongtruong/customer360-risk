# Customer 360 & Risk Scoring Data Warehouse

A hands-on data engineering project for building a Customer 360 view and risk scoring system using Apache Spark, Airflow, and Metabase.

---

## Project Overview

This repository walks through the process of:

- Ingesting and syncing data from CSV sources with **Apache Spark**
- Generating synthetic customer, transaction, and credit bureau data
- Transforming and scoring data using Apache Spark ETL pipelines
- Automating workflows with Apache Airflow
- Storing data in a layered PostgreSQL warehouse
- Visualizing results in Metabase dashboards

---

## Architecture

```
[Faker CSV Data] → [Spark Ingestion] → [PostgreSQL Staging] → [Spark ETL] → [Data Warehouse] → [Metabase Dashboard]
                            ↓
                    [Airflow Orchestration]
```

---

## Technology Stack

| Component              | Technology            | Purpose                      |
| ---------------------- | --------------------- | ---------------------------- |
| Data Ingestion         | Apache Spark 4.0.1    | Batch ingestion & validation |
| Data Generation        | Python Faker + Pandas | Create synthetic datasets    |
| Data Transformation    | Apache Spark 4.0.1    | ETL and risk scoring         |
| Workflow Orchestration | Apache Airflow        | Pipeline automation          |
| Data Warehouse         | PostgreSQL 15         | Layered data storage         |
| Visualization          | Metabase              | Dashboards & analytics       |
| Package Management     | uv + pyproject.toml   | Python dependencies          |
| Containerization       | Docker Compose        | Service orchestration        |

---

## Data Model

**Source Datasets**

- `customers.csv`: Demographics
- `transactions.csv`: Transaction history
- `credit_scores.csv`: Credit bureau data

**Database Layers**

- Staging (`staging.*`): Raw CSV via Spark ingestion
- Warehouse (`warehouse.*`): Cleaned and validated data
- Analytics (`analytics.*`): Aggregated KPIs, Customer 360 view

**Customer 360 Table Example**

```sql
CREATE TABLE analytics.customer_360 (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    annual_income DECIMAL(12,2),
    employment_status VARCHAR(50),
    total_transactions INTEGER,
    total_spent DECIMAL(15,2),
    avg_transaction_amount DECIMAL(10,2),
    credit_score INTEGER,
    risk_score DECIMAL(5,2),
    risk_category VARCHAR(20),
    last_transaction_date TIMESTAMP,
    customer_since DATE
);
```

---

## Quick Start

### Prerequisites

- Docker Desktop (8GB+ memory recommended)
- Git

### Setup

#### Option 1: Local Development (Recommended for Development)

For local development without Docker Swarm features:

```bash
# Clone the repository
git clone <repository-url>
cd customer360-risk

# Start all services in local mode
docker-compose -f docker-compose.local.yml up -d
```

#### Option 2: Production Deployment (Docker Swarm)

For production deployment on VPS with Docker Swarm:

```bash
# Clone the repository
git clone <repository-url>
cd customer360-risk

# Initialize Docker Swarm (if not already done)
docker swarm init

# Create overlay network
docker network create --driver overlay customer360-network

# Start all services in Swarm mode
docker stack deploy -c docker-compose.yml customer360-stack
```

### One-Time Airflow Setup (First-Time Only)

Before running the project for the first time, you need to initialize Airflow's metadata database and create an admin user.

#### 1. Initialize Airflow Database

```bash
# For local development
docker-compose -f docker-compose.local.yml run --rm airflow-webserver airflow db init

# For production Swarm
docker-compose run --rm airflow-webserver airflow db init
```

#### 2. Start All Services

```bash
# For local development
docker-compose -f docker-compose.local.yml up -d

# For production Swarm
docker stack deploy -c docker-compose.yml customer360-stack
```

#### 3. Create Airflow Admin User

Once the services are running, create the Airflow admin user:

```bash
# For local development
docker-compose -f docker-compose.local.yml exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# For production Swarm
docker-compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

> **Note:** These steps are only required during the initial setup.

---

## Usage

### Data Generation

Data generation is handled automatically by the Airflow pipeline, or you can trigger it manually:

```bash
# Generate data using the pipeline (recommended)
# Go to http://localhost:8080 and trigger the customer360_risk_pipeline DAG

# Or generate data manually (for testing)
docker-compose exec airflow-webserver python /opt/airflow/scripts/generate_data.py --customers 1000 --transactions 20 --output /opt/airflow/data/raw
```

### Access Services

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Metabase**: http://localhost:3000
- **Spark Master UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432 (postgres/postgres)

---

## Project Structure

```
customer360-risk/
├── dags/             # Airflow DAGs
├── spark_jobs/       # Spark ETL scripts
├── sql/              # Database schemas
├── scripts/          # Data generation scripts
├── data/             # Data files
├── docker-compose.yml# Services
├── pyproject.toml    # Project configuration & dependencies
├── uv.lock          # Locked dependency versions
└── README.md         # This file
```

---

## Development

All development is done within Docker containers. Dependencies are managed via `pyproject.toml` and automatically installed using `uv` in the Docker images.

### Local Development (Optional)

If you prefer to develop locally instead of using Docker:

```bash
# Install uv (modern Python package manager)
pip install uv

# Install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Run development commands
uv run pytest tests/
uv run black scripts/ spark_jobs/ dags/
uv run ruff check scripts/ spark_jobs/ dags/
```

### Running the Pipeline

1. **Start services**: `docker-compose up -d`
2. **Access Airflow**: http://localhost:8080
3. **Trigger DAG**: Run the `customer360_risk_pipeline` DAG
4. **View results**: Check Metabase at http://localhost:3000

### Useful Commands

```bash
# Local Development Commands
docker-compose -f docker-compose.local.yml logs -f airflow-scheduler
docker-compose -f docker-compose.local.yml logs -f spark-master
docker-compose -f docker-compose.local.yml exec postgres psql -U postgres -d customer360_dw
docker-compose -f docker-compose.local.yml restart
docker-compose -f docker-compose.local.yml down -v  # Removes all data!

# Production Swarm Commands
docker service logs customer360-stack_airflow-scheduler
docker service logs customer360-stack_spark-master
docker exec -it $(docker ps -q -f name=customer360-postgres) psql -U postgres -d customer360_dw
docker stack ps customer360-stack  # Check service status
docker stack rm customer360-stack  # Remove stack
```

---

## Testing & Quality

```bash
# Run tests
pytest tests/

# Code style
black scripts/ spark_jobs/ dags/
ruff check scripts/ spark_jobs/ dags/

# Or use Make commands
make test         # Run all tests
make format       # Format code
make lint         # Lint code
make quality      # Run all quality checks
```

---

## Sample Queries

```sql
-- Risk distribution
SELECT risk_category, COUNT(*), AVG(total_spent)
FROM analytics.customer_360
GROUP BY risk_category;

-- Top customers
SELECT customer_id, name, total_spent, risk_score
FROM analytics.customer_360
ORDER BY total_spent DESC LIMIT 10;
```

---

## Resources

- [Spark Guide](https://spark.apache.org/docs/latest/)
- [Airflow Docs](https://airflow.apache.org/docs/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Metabase Docs](https://www.metabase.com/docs/)

---

## Contributing

1. Fork the repo and create a feature branch
2. Follow code standards (black, ruff, pytest)
3. Open a Pull Request

---

## License

MIT License.
