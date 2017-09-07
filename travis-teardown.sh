#!/bin/sh -ev

# Kill and remove Docker containers
sudo docker kill minio || true
sudo docker rm minio || true
sudo docker kill azurite || true
sudo docker rm azurite || true

# Destroy zpool
sudo zpool destroy -f tank || true

# Remove temp file
sudo rm ${VDEV} || true

# Clear env variables
export VDEV=
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
