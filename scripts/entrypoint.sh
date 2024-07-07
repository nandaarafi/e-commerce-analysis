#!/bin/bash

# Initialize the Airflow database
airflow db init

# Update the webserver configuration
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py

# Check if the connection already exists
if ! airflow connections get 'postgres_main'; then
  # Add the Postgres connection if it doesn't exist
  airflow connections add 'postgres_main' \
  --conn-type 'postgres' \
  --conn-login $POSTGRES_USER \
  --conn-password $POSTGRES_PASSWORD \
  --conn-host $POSTGRES_CONTAINER_NAME \
  --conn-port $POSTGRES_PORT \
  --conn-schema $POSTGRES_DB
fi

airflow connections add 'google_cloud_main' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"extra__google_cloud_platform__key_path": "/opt/airflow/credentials/gcp.json"}'


# Start the Airflow webserver
exec airflow webserver
