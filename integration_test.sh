#!/bin/sh -ev

SCRATCHDIR=$(mktemp -d)
mkdir -p $SCRATCHDIR/integrationtest

TARGET="file://$SCRATCHDIR/integrationtest/"
USER="test@example.com"

echo "Basic send/recieve test with encryption and compression"
go build ./

sudo ./zfsbackup-go send \
    --logLevel=debug \
    --workingDirectory=$SCRATCHDIR \
    --compressor=xz \
    --compressionLevel=2 \
    --publicKeyRingPath=public.pgp \
    --encryptTo=$USER \
    "tank/data@a" \
    $TARGET

echo "Test manifest file"
gpg2 --output data.manifest.gz --decrypt $SCRATCHDIR/integrationtest/manifests\|tank/data\|a.manifest.gz.pgp
gzip -d data.manifest.gz
cat data.manifest | jq '.VolumeName' | grep -q 'tank/data'

echo "Test data files"
gpg2 --output data.vol1.xz --decrypt $SCRATCHDIR/integrationtest/tank/data\|a.zstream.xz.pgp.vol1
gpg2 --output data.vol2.xz --decrypt $SCRATCHDIR/integrationtest/tank/data\|a.zstream.xz.pgp.vol2
xz -d data.vol1.xz
xz -d data.vol2.xz
cat data.vol1 data.vol2 | sudo zfs receive -F tank/integrationtest

echo "Cleaning up"
rm data.vol1 data.vol2 data.manifest "zfsbackup-go"
sudo rm -rf $SCRATCHDIR
sudo zfs destroy -f -r tank/integrationtest
