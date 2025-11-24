FROM apache/airflow:2.9.0-python3.10

# Set working directory inside container
ENV AIRFLOW_HOME=/opt/airflow

# Switch to airflow user (recommended best practice)
USER airflow

# Copy dependency list
COPY requirements.txt /requirements.txt

# Install Python packages needed by Airflow + your ETL scripts
RUN pip install --no-cache-dir -r /requirements.txt
