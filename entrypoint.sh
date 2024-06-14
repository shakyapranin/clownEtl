#!/bin/bash

# Initialize the database
airflow db init

# Create admin user (if not exists)
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin || true

# Start scheduler and webserver
exec airflow scheduler &
exec airflow webserver
