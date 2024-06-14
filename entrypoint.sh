#!/bin/bash

# Initialize the database, create user, run scheduler and run webserver
# airflow db init &&
# airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin &&
# airflow scheduler & airflow webserver



#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Initialize the database
# airflow db init

# Create an admin user
# airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

# Start the scheduler in the background
# airflow scheduler

# Start the webserver in the foreground (blocking call)
# exec airflow webserver
