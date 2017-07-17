# ZFSBackup

DISCLAIMER: This is a work in progress - USE AT YOUR OWN RISK!

## Overview:

This backup software was designed for the secure, long-term storage of ZFS snapshots on remote storage. Backup jobs are resilient to network failures and can be stopped/resumed. It works by splitting the ZFS send stream (the format for which is committed and can be received on future versions of ZFS as per the [man page](https://www.freebsd.org/cgi/man.cgi?zfs(8))) into chunks and then optionally compresses, encrypts, and signs each chunk before uploading it to your remote storage location(s) of choice. Backup chunks are validated using SHA256 and CRC32C checksums (along with the many integrity checks builtin to compression algorithms, SSL/TLS transportation protocols, and the ZFS stream format itself). The software is completely self-contained and has no external dependencies.

This project was inspired by the [duplicity project](http://duplicity.nongnu.org/).

### Highlights:
* Written in Go
* Backup jobs are resumeable and resilient to network failures
* Backup files can be compressed and optionally encrypyted and/or signed.
* Concurrent by design, enable multiple cores for parallel processing
* Configurable Operation - Limit bandwidth usage, space usage, CPU usage, etc.
* Backup to multiple destinations at once, just comma separate destination URIs
* Uses familiar ZFS send/receive options

### Supported Backends:
* Google Cloud Storage (gs://)
  - Auth details: https://developers.google.com/identity/protocols/application-default-credentials
* Amazon AWS S3 (s3://) (Glacier supported indirectly via lifecycle rules)
  - Auth details: https://godoc.org/github.com/aws/aws-sdk-go/aws/session#hdr-Environment_Variables
* Any S3 Compatible Storage Provider (e.g. Minio, StorageMadeEasy, Ceph, etc.)
  - Set the AWS_S3_CUSTOM_ENDPOINT environmental variable to the compatible target API URI


### Compression:
The compression algorithm builtin to the software is a parallel gzip ([pgzip](https://github.com/klauspost/pgzip)) compressor. There is support for 3rd party compressors so long as the binary is available on the host system and is compatible with the standard gzip binary command line options (e.g. xz, bzip2, lzma, etc.)

### Encryption/Signing:
The PGP algorithm is used for encryption/signing. The cipher used is AES-256.

## Usage

Full backup example:

	$ ./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc Tank/Dataset@snapshot-20170101 gs://backup-bucket-target

Incremental backup example:

	$ ./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc -i Tank/Dataset@snapshot-20170101 Tank/Dataset@snapshot-20170201 gs://backup-bucket-target,s3://another-backup-target


Full restore example:

	$ ./zfsbackup receive --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc -d Tank/Dataset@snapshot-20170201 gs://backup-bucket-target Tank

Incremental restore example:

	$ ./zfsbackup receive --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc -d -F -i Tank/Dataset@snapshot-20170101 Tank/Dataset@snapshot-20170201 gs://backup-bucket-target Tank

Notes:
* Create keyring files: https://keybase.io/crypto
* PGP Passphrase will be prompted during execution if it is not found in the PGP_PASSPHRASE environmental variable.
* `--maxFileBuffer=0` will disable parallel processing, chunking, multiple destinations, and upload hash verification but will use virtually no disk space.
* For S3: Specify Standard/Bulk/Expedited in the AWS_S3_GLACIER_RESTORE_TIER environmental variable to change Glacier restore option (default: Bulk)

Example output for top level command:

```shell
$ ./zfsbackup
zfsbackup is a tool used to do off-site backups of ZFS volumes.
It leverages the built-in snapshot capabilities of ZFS in order to export ZFS
volumes for long-term storage.

zfsbackup uses the "zfs send" command to export, and optionally compress, sign,
encrypt, and split the send stream to files that are then transferred to a
destination of your choosing.

Usage:
  zfsbackup [command]

Available Commands:
  clean       Clean will delete any objects in the destination that are not found in the manifest files found in the destination.
  help        Help about any command
  list        List all backup sets found at the provided destination.
  receive     receive will restore a snapshot of a ZFS volume similar to how the "zfs recv" command works.
  send        send will backup of a ZFS volume similar to how the "zfs send" command works.
  verify      Verify will ensure that the backupset for the given snapshot exists in the destination
  version     Print the version of zfsbackup in use and relevant compile information

Flags:
      --compressor string          specify to use the internal (parallel) gzip implementation or an external binary (e.g. gzip, bzip2, pigz, lzma, xz, etc. Syntax must be similiar to the gzip compression tool) to compress the stream for storage. Please take into consideration time, memory, and CPU usage for any of the compressors used. (default "internal")
      --encryptTo string           the email of the user to encrypt the data to from the provided public keyring.
  -h, --help                       help for zfsbackup
      --logLevel string            this controls the verbosity level of logging. Possible values are critical, error, warning, notice, info, debug. (default "notice")
      --manifestPrefix string      the prefix to use for all manifest files. (default "manifests")
      --numCores int               number of CPU cores to utilize. Do not exceed the number of CPU cores on the system. (default 2)
      --publicKeyRingPath string   the path to the PGP public key ring
      --secretKeyRingPath string   the path to the PGP secret key ring
      --signFrom string            the email of the user to sign on behalf of from the provided private keyring.
      --workingDirectory string    the working directory path for zfsbackup. (default "~/.zfsbackup")

Use "zfsbackup [command] --help" for more information about a command.
```

## TODOs:
* Make PGP cipher configurable.
* Finish the verify command
* Build out more robust restore options (e.g. cascading, parent verification, etc.)
* Refactor
* Test Coverage
* Add more backends (e.g. Azure, BackBlaze, etc.)
* Fix error handling (at least omit panic dumps!)
* Add delete feature
* Appease linters


