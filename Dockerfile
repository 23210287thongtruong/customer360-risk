FROM apache/airflow:2.7.3-python3.11

# Install system dependencies as root
USER root

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jdk-headless \
  curl \
  && apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Set Java environment variables for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user (best practice for security)
USER airflow

WORKDIR /opt/airflow

# Copy Python dependencies
COPY requirements.txt .

# Install Python dependencies into Airflow environment
RUN pip install --no-cache-dir -r requirements.txt