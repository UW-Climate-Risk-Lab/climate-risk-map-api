#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    source .env
else
    echo ".env file not found!"
    exit 1
fi

# Directory containing the SQL files
DIR="migrations"

# Check required environment variables
required_vars=("PGDATABASE" "PGUSER" "PGPASSWORD" "PGHOST" "PGPORT")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: $var environment variable is not set"
        exit 1
    fi
done

# Database connection details are now from environment variables
# PGDATABASE, PGUSER, PGPASSWORD, PGHOST, and PGPORT should be set in the environment

# Loop over each SQL file in the directory
for FILE in $(ls $DIR/*.sql | sort)
do
    if [[ $FILE == *"init_db"* ]]; then
        continue
    fi  
    # Execute the SQL file with explicit connection parameters
    echo "Executing $FILE..."
    if ! psql -U "$PGUSER" -d "$PGDATABASE" -h "$PGHOST" -p "$PGPORT" -f "$FILE"; then
        echo "Error executing $FILE"
        exit 1
    fi
done