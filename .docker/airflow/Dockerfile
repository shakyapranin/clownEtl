# Use the official Apache Airflow image
FROM apache/airflow:2.1.0

# Set environment variables
# ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
# ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
# ENV AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0

# Copy DAGs and other documents into the container
COPY ../dags /opt/airflow/dags
COPY ../docs /opt/airflow/docs

# Install any additional dependencies if needed
# RUN pip install ...

# Set up the entrypoint script
# COPY ../entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh

CMD [ "airflow" ]

# ENTRYPOINT ["/entrypoint.sh"]
