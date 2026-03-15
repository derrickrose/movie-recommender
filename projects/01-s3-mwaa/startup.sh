#!/bin/bash

################################################################################
# MWAA Startup Script
################################################################################
# Purpose:
#   This script runs automatically when the MWAA environment starts.
#   It adds the lib/ folder to PYTHONPATH so custom libraries can be imported
#   in DAGs without manual path manipulation.
#
# Location:
#   Upload this file to: s3://dev-miketriky-eu-west-3-mwaa/startup.sh
#
# MWAA Configuration:
#   The MWAA environment references this via StartupScriptS3Path property
#
# How it works:
#   1. MWAA syncs S3 bucket structure to /usr/local/airflow/
#      - s3://bucket/lib/ → /usr/local/airflow/lib/
#      - s3://bucket/dags/ → /usr/local/airflow/dags/
#      - s3://bucket/plugins/ → /usr/local/airflow/plugins/
#
#   2. This script adds lib/ to PYTHONPATH at environment startup
#
#   3. Your DAGs can now import directly:
#      from helpers import my_function
#      from transformations import process_data
#
# Execution:
#   - Runs once when MWAA environment starts (cold init)
#   - Runs again when environment is updated/restarted
#
# Troubleshooting:
#   - Check MWAA startup logs in CloudWatch if imports fail
#   - Verify lib/ folder exists in S3 bucket root
#   - Ensure Python files in lib/ have __init__.py if using packages
################################################################################

echo "================================================================"
echo "MWAA Startup Script - Initializing Custom Library Path"
echo "================================================================"

# Step 1: Display current PYTHONPATH before modification
echo "Current PYTHONPATH: ${PYTHONPATH}"

# Step 2: Add lib folder to Python path
# This allows importing from s3://bucket/lib/ which maps to /usr/local/airflow/lib/
export PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/lib"

# Step 3: Confirm the update
echo "Updated PYTHONPATH: ${PYTHONPATH}"

# Step 4: Verify lib directory exists
if [ -d "/usr/local/airflow/lib" ]; then
    echo "✓ lib/ directory found at /usr/local/airflow/lib"
    echo "  Available modules:"
    ls -la /usr/local/airflow/lib/ || echo "  (directory is empty or not accessible)"
else
    echo "⚠ Warning: lib/ directory not found at /usr/local/airflow/lib"
    echo "  Make sure to upload your library files to s3://bucket/lib/"
fi

echo "================================================================"
echo "Startup script completed successfully"
echo "================================================================"
