FROM apache/airflow:2.7.3-python3.11

# Install system dependencies as root
USER root

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jdk-headless \
  curl \
  && apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Install uv for modern Python dependency management
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
  mv ~/.local/bin/uv /usr/local/bin/uv

# Set Java environment variables for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user (best practice for security)
USER airflow

WORKDIR /opt/airflow

# Copy Python project configuration
COPY pyproject.toml .

# Install Python dependencies using uv (modern, fast dependency resolver)
RUN uv sync --no-install-project

# Explicitly install Apache Spark provider (uv may not handle Airflow providers correctly)
RUN pip install apache-airflow-providers-apache-spark==4.4.0