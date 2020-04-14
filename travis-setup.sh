#!/bin/sh -ev

# Setup ZFS Pool
export VDEV=$(mktemp)
dd if=/dev/zero of=${VDEV} bs=2048 count=1048576
sudo zpool create tank ${VDEV}
sudo zfs create tank/data
sudo dd if=/dev/urandom of=/tank/data/a bs=1024 count=409600
sudo zfs snapshot tank/data@a
sudo dd if=/dev/urandom of=/tank/data/b bs=256 count=409600
sudo zfs snapshot tank/data@b
sudo dd if=/dev/urandom of=/tank/data/c bs=256 count=409600
sudo zfs snapshot tank/data@c

# Setup Docker containers
sudo docker pull arafato/azurite
sudo docker pull minio/minio
sudo docker run -d -p 10000:10000 --rm --name azurite arafato/azurite
sudo docker run -d -p 9000:9000 --rm --name minio minio/minio server /data

# Setup env variables from Docker containers
sleep 10
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# PGP Test keys
cat >test <<EOF
     %echo Generating a basic OpenPGP key
     Key-Type: RSA
     Key-Length: 2048
     Subkey-Type: RSA
     Subkey-Length: 2048
     %echo Generating a basic OpenPGP key
     Name-Real: Example Tester
     Name-Comment: with no passphrase
     Name-Email: test@example.com
     Expire-Date: 0
     %no-protection
     %commit
     %echo done
EOF
gpg2 --batch --gen-key test
gpg2 --output public.pgp --armor --export test@example.com
gpg2 --output private.pgp --armor --export-secret-key test@example.com
rm test