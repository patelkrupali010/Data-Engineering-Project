#!/bin/bash

# This script starts the containers defined in the Docker Compose file, and runs a command in the 'app' service.

#local testing only
# Remove all existing containers, if any.
if [ "$(docker ps -q)" ]; then
    docker rm -f $(docker ps -aq)
fi

# Remove all existing images, if any.
if [ "$(docker images -q)" ]; then
    docker rmi -f $(docker images -q)
fi

# Remove all unused cache.
docker system prune -f..



# Change to the directory containing the Docker Compose file.
cd .

# Start the containers defined in the Docker Compose file in detached mode and without using cached images.
docker-compose up -d

# Wait for the containers to start up.
sleep 10

# Check the status of the containers.
docker-compose ps

# Run the specified command in the 'app' service.
docker-compose run app





