#!/bin/bash

# Start Cloud SQL Proxy in Cloud Code environment
if [ "$CLOUD_CODE_ENV" = "true" ]; then
    echo "Starting Cloud SQL Proxy..."

    # Read connection name from env, fallback to default
    CONNECTION_NAME=${INSTANCE_CONNECTION_NAME:-"curioustore:asia-northeast3:curioustore"}

    echo "Using connection name: $CONNECTION_NAME"
    cloud_sql_proxy -instances=$CONNECTION_NAME=tcp:5432 &
    sleep 5  # wait for proxy startup
fi

# Start app
echo "Starting Auto Trader Bot..."
exec gunicorn -k uvicorn.workers.UvicornWorker main:app \
    --bind 0.0.0.0:8080 \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 3600 \
    --access-logfile - \
    --error-logfile -
