#!/bin/sh -ev

export TARGET="file://scratch/integrationtest/"
export USER="test@example.com"

mkdir -p ./scratch/integrationtest
sudo ./zfsbackup-go send --logLevel=debug --workingDirectory=./scratch --secretKeyRingPath=private.pgp --publicKeyRingPath=public.pgp --encryptTo=$USER "tank/data@a" $TARGET
gpg --output data.manifest.gz --decrypt ./scratch/integrationtest/manifests\|tank/data\|a.manifest.gz.pgp
gzip -d data.manifest
cat data.manifest | js '.VolumeName' | grep -q 'tank/data'