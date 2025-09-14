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
- Python 3.9+ (tested with 3.13)
- UV package manager (optional)

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd customer360-risk

# Set up Python environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Generate synthetic data
python scripts/generate_data.py --customers 10000 --transactions 50

# Start core services
docker-compose up -d postgres metabase

# Apply schema
docker-compose exec postgres psql -U postgres -d customer360_dw -f /docker-entrypoint-initdb.d/init_schema.sql

# (Optional) Start Airflow
docker-compose up -d airflow-webserver airflow-scheduler
```

---

## Usage

### Data Generation

```bash
python scripts/generate_data.py --customers 1000 --transactions 20 --output ./data/raw
```

### Dashboards

- Metabase: http://localhost:3000
- Airflow: http://localhost:8080 (admin/admin)

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

## Dependency Management

Dependencies are managed with `pip` and `requirements.txt`:

```bash
# Install dependencies
pip install -r requirements.txt

# Add a new dependency
echo "package-name>=1.0.0" >> requirements.txt
pip install package-name

# Update dependencies
pip install --upgrade -r requirements.txt
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
