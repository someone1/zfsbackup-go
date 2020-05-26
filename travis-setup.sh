#!/bin/sh -ev

# Setup ZFS Pool
dd if=/dev/zero of=${VDEV} bs=2048 count=1572864
sudo zpool create tank ${VDEV}
sudo zfs set snapdir=visible compression=lz4 atime=off tank
sudo zfs create tank/data
sudo dd if=/dev/urandom of=/tank/data/a bs=1024 count=409600
sudo zfs snapshot tank/data@a
sudo dd if=/dev/urandom of=/tank/data/b bs=256 count=409600
sudo zfs snapshot tank/data@b
sudo dd if=/dev/urandom of=/tank/data/c bs=256 count=409600
sudo zfs snapshot tank/data@c

# Setup Docker containers
sudo docker pull mcr.microsoft.com/azure-storage/azurite
sudo docker pull minio/minio
sudo docker pull fsouza/fake-gcs-server
sudo docker run -d -p 10000:10000 --rm --name azurite mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
sudo docker run -d -p 9000:9000 --rm --name minio minio/minio server /data
sudo docker run -d -p 4443:4443 --rm --name fake-gcs-server fsouza/fake-gcs-server -public-host localhost

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
gpg2 --output public.pgp --batch --yes --no-tty --armor --export test@example.com
gpg2 --output private.pgp --batch --yes --no-tty --armor --export-secret-key test@example.com
rm test