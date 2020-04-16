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
rm *.pgp

# Clear env variables
export VDEV=
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
