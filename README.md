# Customer 360 & Risk Scoring Data Warehouse

A hands-on data engineering project for building a Customer 360 view and risk scoring system using SeaTunnel, Apache Spark, Airflow, and Metabase.

---

## Project Overview

This repository walks through the process of:

- Ingesting and syncing data from CSV and other sources with **SeaTunnel**
- Generating synthetic customer, transaction, and credit bureau data
- Transforming and scoring data using Apache Spark ETL pipelines
- Automating workflows with Apache Airflow
- Storing data in a layered PostgreSQL warehouse
- Visualizing results in Metabase dashboards

---

## Architecture

```
[Faker CSV Data] → [SeaTunnel Ingestion] → [PostgreSQL Staging] → [Spark ETL] → [Data Warehouse] → [Metabase Dashboard]
                            ↓
                    [Airflow Orchestration]
```

---

## Technology Stack

| Component              | Technology             | Purpose                     |
| ---------------------- | ---------------------- | --------------------------- |
| Data Ingestion         | SeaTunnel              | Batch & streaming ingestion |
| Data Generation        | Python Faker + Pandas  | Create synthetic datasets   |
| Data Transformation    | Apache Spark           | ETL and risk scoring        |
| Workflow Orchestration | Apache Airflow         | Pipeline automation         |
| Data Warehouse         | PostgreSQL 15          | Layered data storage        |
| Visualization          | Metabase               | Dashboards & analytics      |
| Package Management     | pip + requirements.txt | Python dependencies         |
| Containerization       | Docker Compose         | Service orchestration       |

---

## Data Model

**Source Datasets**

- `customers.csv`: Demographics
- `transactions.csv`: Transaction history
- `credit_scores.csv`: Credit bureau data

**Database Layers**

- Staging (`staging.*`): Raw CSV via SeaTunnel
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

````bash
# Clone the repository
git clone <repository-url>
cd customer360-risk

# Start all services
docker-compose up -d

### One-Time Airflow Setup (First-Time Only)

Before running the project for the first time, you need to initialize Airflow's metadata database and create an admin user.

#### 1. Initialize Airflow Database

```bash
# Initialize the Airflow metadata database
docker compose run --rm airflow-webserver airflow db init

# Ensure all containers are stopped
docker compose down
```

#### 2. Start All Services

```bash
docker compose up -d
```

#### 3. Create Airflow Admin User

Once the services are running, create the Airflow admin user:

```bash
docker compose exec airflow-webserver airflow users create \
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
````

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
├── seatunnel/        # SeaTunnel configs
├── spark_jobs/       # Spark ETL scripts
├── sql/              # Database schemas
├── scripts/          # Data generation scripts
├── data/             # Data files
├── docker-compose.yml# Services
├── requirements.txt  # Dependencies
└── README.md         # This file
```

---

## Development

All development is done within Docker containers. Dependencies are managed via `requirements.txt` and automatically installed in the Docker images.

### Running the Pipeline

1. **Start services**: `docker-compose up -d`
2. **Access Airflow**: http://localhost:8080
3. **Trigger DAG**: Run the `customer360_risk_pipeline` DAG
4. **View results**: Check Metabase at http://localhost:3000

### Useful Commands

```bash
# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master

# Connect to database
docker-compose exec postgres psql -U postgres -d customer360_dw

# Restart services
docker-compose restart

# Clean up
docker-compose down -v  # Removes all data!
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
