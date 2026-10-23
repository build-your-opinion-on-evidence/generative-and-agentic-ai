#!/usr/bin/env bash
if [ "$1" == "--wait" ]; then
    echo "Waiting for services to be healthy..."
    MAX_RETRIES=60
    for ((i=1; i<=MAX_RETRIES; i++)); do
        UNHEALTHY=$(docker ps --filter "health=starting" --filter "health=unhealthy" -q | wc -l)
        if [ "$UNHEALTHY" -eq "0" ]; then
            echo "All services healthy."
            exit 0
        fi
        sleep 2
    done
    echo "Timeout waiting for services."
    docker compose ps
    exit 1
else
    docker compose ps
fi
