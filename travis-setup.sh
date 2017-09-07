#!/bin/sh -ev

# Setup ZFS Pool
export VDEV=$(mktemp /tmp/testdevXXXX)
dd if=/dev/zero of=${VDEV} bs=2048 count=1048576
sudo zpool create tank ${VDEV}
sudo zfs allow -ld -e send,receive,create,mount,mountpoint tank
sudo zfs create tank/data
sudo dd if=/dev/urandom of=/tank/data/test bs=1024 count=409600
sudo zfs snapshot tank/data@a

# Install Go dependencies
go get github.com/mattn/goveralls
go get github.com/alecthomas/gometalinter
go get github.com/mitchellh/gox

# Setup Docker containers
sudo docker pull arafato/azurite
sudo docker pull minio/minio
sudo docker run -d -p 10000:10000 --name azurite --restart=on-failure arafato/azurite
sudo docker run -d -p 9000:9000 --name minio minio/minio server /data

# Setup env variables from Docker containers
sleep 30
export AWS_ACCESS_KEY_ID=$(sudo docker exec -it minio cat /root/.minio/config.json | python -c "import sys, json; print(json.load(sys.stdin)['credential']['accessKey'])")
export AWS_SECRET_ACCESS_KEY=$(sudo docker exec -it minio cat /root/.minio/config.json | python -c "import sys, json; print(json.load(sys.stdin)['credential']['secretKey'])")
