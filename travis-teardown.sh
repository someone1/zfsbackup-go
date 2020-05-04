#!/bin/sh -ev

# Kill and remove Docker containers
sudo docker rm -f minio || true
sudo docker rm -f azurite || true
sudo docker rm -f fake-gcs-server || true

# Destroy zpool
sudo zpool destroy -f tank || true

# Remove temp file
sudo rm ${VDEV} || true

# Remove PGP keyrings
rm *.pgp || true

# Clear env variables
unset VDEV
