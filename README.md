# ZFSBackup [![Build Status](https://travis-ci.org/someone1/zfsbackup-go.svg?branch=master)](https://travis-ci.org/someone1/zfsbackup-go) [![Go Report Card](https://goreportcard.com/badge/github.com/someone1/zfsbackup-go)](https://goreportcard.com/report/github.com/someone1/zfsbackup-go) [![Coverage Status](https://coveralls.io/repos/github/someone1/zfsbackup-go/badge.svg?branch=master)](https://coveralls.io/github/someone1/zfsbackup-go?branch=master)

DISCLAIMER: This is a work in progress in still considered beta though I personally use this in a production environment and have tested it for my own use cases (looking for feedback on other people's experience before considering this "production ready").

## Overview

This backup software was designed for the secure, long-term storage of ZFS snapshots on remote storage. Backup jobs are resilient to network failures and can be stopped/resumed. It works by splitting the ZFS send stream (the format for which is committed and can be received on future versions of ZFS as per the [man page](<https://www.freebsd.org/cgi/man.cgi?zfs(8)>)) into chunks and then optionally compresses, encrypts, and signs each chunk before uploading it to your remote storage location(s) of choice. Backup chunks are validated using SHA256 and CRC32C checksums (along with the many integrity checks builtin to compression algorithms, SSL/TLS transportation protocols, and the ZFS stream format itself). The software is completely self-contained and has no external dependencies.

This project was inspired by the [duplicity project](http://duplicity.nongnu.org/).

### Highlights

- Written in Go
- No external dependencies - Just drop in the binary on your system and you're all set!
- Backup jobs are resumeable and resilient to network failures
- Backup files can be compressed and optionally encrypyted and/or signed.
- Concurrent by design, enable multiple cores for parallel processing
- Configurable Operation - Limit bandwidth usage, space usage, CPU usage, etc.
- Backup to multiple destinations at once, just comma separate destination URIs
- Uses familiar ZFS send/receive options

### Supported Backends

- Google Cloud Storage (gs://)
  - Auth details: <https://developers.google.com/identity/protocols/application-default-credentials>
  - [99.999999999% durability](https://cloud.google.com/storage/docs/storage-classes) - Using erasure encodings
- Amazon AWS S3 (s3://) (Glacier supported indirectly via lifecycle rules)
  - Auth details: <https://godoc.org/github.com/aws/aws-sdk-go/aws/session#hdr-Environment_Variables>
  - [99.999999999% durability](https://aws.amazon.com/s3/faqs/#data-protection) - Using replication and checksums on the data for integrity validation and repair
- Any S3 Compatible Storage Provider (e.g. Minio, StorageMadeEasy, Ceph, etc.)
  - Set the AWS_S3_CUSTOM_ENDPOINT environmental variable to the compatible target API URI
- Azure Blob Storage (azure://)
  - Auth: Set the AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY environmental variables to the appropiate values or if using SAS set AZURE_SAS_URI to a container authorized SAS URI
  - Point to a custom endpoint by setting the AZURE_CUSTOM_ENDPOINT envrionmental variable
  - Although no durability target is provided, there is an in-depth explanation of their architecture [here](http://sigops.org/sosp/sosp11/current/2011-Cascais/printable/11-calder.pdf) - Using the Reed-Solomon erasure encoding and user-configurable redundancy settings
- BackBlaze B2 (b2://)
  - Auth: Set the B2_ACCOUNT_ID and B2_ACCOUNT_KEY environmental variables to the appropiate values
  - [99.999999999% durability](https://help.backblaze.com/hc/en-us/articles/218485257-B2-Resiliency-Durability-and-Availability) - Using the Reed-Solomon erasure encoding
- Local file path (file://[relative|/absolute]/local/path)
- SSH/SFTP (ssh://)
  - Auth: username & password, public key or ssh-agent.
  - For username & password set the SSH_USERNAME and SSH_PASSWORD environment variables or use the url format: `ssh://username:password@example.org/remote/path`.
  - For public key auth set the SSH_KEY_FILE environment variable. By default zfsbackup tries to use common key names from the users home directory.  
  - ssh-agent auth is activated when SSH_AUTH_SOCK exists.
  - By default zfsbackup also uses the known hosts file from the users home directory. To disable host key checking set SSH_KNOWN_HOSTS to `ignore`. You can also specify the path to your own known hosts file.

### Compression

The compression algorithm builtin to the software is a parallel gzip ([pgzip](https://github.com/klauspost/pgzip)) compressor. There is support for 3rd party compressors so long as the binary is available on the host system and is compatible with the standard gzip binary command line options (e.g. xz, bzip2, lzma, etc.)

### Encryption/Signing

The PGP algorithm is used for encryption/signing. The cipher used is AES-256.

## Installation

Download the latest binaries from the [releases](https://github.com/someone1/zfsbackup-go/releases) section or compile your own by:

```shell
go get github.com/someone1/zfsbackup-go
```

The compiled binary should be in your $GOPATH/bin directory.

## Usage

### "Smart" Backup Options

Use the `--full` option to auto select the most recent snapshot on the target volume to do a full backup of:

```bash
./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc --full Tank/Dataset gs://backup-bucket-target,s3://another-backup-target
```

Use the `--increment` option to auto select the most recent snapshot on the target volume to do an incremental snapshot of the most recent snapshot found in the target destination:

```bash
./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc --increment Tank/Dataset gs://backup-bucket-target,s3://another-backup-target
```

Use the `--fullIfOlderThan` option to auto select the most recent snapshot on the target volume to do an incremental snapshot of the most recent snapshot found in the target destination, unless the last full backup is older than the provided duration, in which case do a full backup:

```bash
./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc --fullIfOlderThan 720h Tank/Dataset gs://backup-bucket-target,s3://another-backup-target
```

### "Smart" Restore Options

Add the `--auto` option to automatically restore to the snapshot if one is given, or detect the latest snapshot for the filesystem/volume given and restore to that. It will figure out which snapshots are missing from the local_volume and select them all to restore to get to the desired snapshot. Note: snapshot comparisons work using the name of the snapshot, if you restored a snapshot to a different name, this application won't think it is available and it will break the restore process.

Auto-detect latest snapshot:

```bash
./zfsbackup receive --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc --auto -d Tank/Dataset gs://backup-bucket-target Tank
```

Auto restore to snapshot provided:

```bash
./zfsbackup receive --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc --auto -d Tank/Dataset@snapshot-20170201 gs://backup-bucket-target Tank
```

### Manual Options

Full backup example:

```bash
./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc Tank/Dataset@snapshot-20170101 gs://backup-bucket-target
```

Incremental backup example:

 ```bash
 ./zfsbackup send --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc -i Tank/Dataset@snapshot-20170101 Tank/Dataset@snapshot-20170201 gs://backup-bucket-target,s3://another-backup-target
  ```

Full restore example:

  ```bash
  ./zfsbackup receive --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc -d Tank/Dataset@snapshot-20170201 gs://backup-bucket-target Tank
  ```

Incremental restore example:

```bash
./zfsbackup receive --encryptTo user@domain.com --signFrom user@domain.com --publicKeyRingPath pubring.gpg.asc --secretKeyRingPath secring.gpg.asc -d -F -i Tank/Dataset@snapshot-20170101 Tank/Dataset@snapshot-20170201 gs://backup-bucket-target Tank
```

Notes:

- Create keyring files:

```bash
gpg2 --gen-key
gpg2 --output public.pgp --armor --export test@example.com
gpg2 --output private.pgp --armor --export-secret-key test@example.com
```

- PGP Passphrase will be prompted during execution if it is not found in the PGP_PASSPHRASE environmental variable.
- `--maxFileBuffer=0` will disable parallel uploading for some backends, multiple destinations, and upload hash verification but will use virtually no disk space.
- For S3: Specify Standard/Bulk/Expedited in the AWS_S3_GLACIER_RESTORE_TIER environmental variable to change Glacier restore option (default: Bulk)
- A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

Help Output:

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
  clean       Clean will delete any objects in the target that are not found in the manifest files found in the target.
  help        Help about any command
  list        List all backup sets found at the provided target.
  receive     receive will restore a snapshot of a ZFS volume similar to how the "zfs recv" command works.
  send        send will backup of a ZFS volume similar to how the "zfs send" command works.
  version     Print the version of zfsbackup in use and relevant compile information

Flags:
      --encryptTo string           the email of the user to encrypt the data to from the provided public keyring.
  -h, --help                       help for zfsbackup
      --jsonOutput                 dump results as a JSON string - on success only
      --logLevel string            this controls the verbosity level of logging. Possible values are critical, error, warning, notice, info, debug. (default "notice")
      --manifestPrefix string      the prefix to use for all manifest files. (default "manifests")
      --numCores int               number of CPU cores to utilize. Do not exceed the number of CPU cores on the system. (default 2)
      --publicKeyRingPath string   the path to the PGP public key ring
      --secretKeyRingPath string   the path to the PGP secret key ring
      --signFrom string            the email of the user to sign on behalf of from the provided private keyring.
      --workingDirectory string    the working directory path for zfsbackup. (default "~/.zfsbackup")
      --zfsPath string             the path to the zfs executable. (default "zfs")

Use "zfsbackup [command] --help" for more information about a command.
```

Send Options:

```shell
$ ./zfsbackup send
Usage:
  zfsbackup send [flags] filesystem|volume|snapshot uri(s)

Flags:
      --compressionLevel int       the compression level to use with the compressor. Valid values are between 1-9. (default 6)
      --compressor string          specify to use the internal (parallel) gzip implementation or an external binary (e.g. gzip, bzip2, pigz, lzma, xz, etc.) Syntax must be similar to the gzip compression tool) to compress the stream for storage. Please take into consideration time, memory, and CPU usage for any of the compressors used. All manifests utilize the internal compressor. If value is zfs, the zfs stream will be created compressed. See the -c flag on zfs send for more information. (default "internal")
  -D, --deduplication              See the -D flag for zfs send for more information.
      --full                       set this flag to take a full backup of the specified volume using the most recent snapshot.
      --fullIfOlderThan duration   set this flag to do an incremental backup of the most recent snapshot from the most recent snapshot found in the target unless the it's been greater than the time specified in this flag, then do a full backup. (default -1m0s)
  -h, --help                       help for send
      --increment                  set this flag to do an incremental backup of the most recent snapshot from the most recent snapshot found in the target.
  -i, --incremental string         See the -i flag on zfs send for more information
  -I, --intermediary string        See the -I flag on zfs send for more information
      --maxBackoffTime duration    the maximum delay you'd want a worker to sleep before retrying an upload. (default 30m0s)
      --maxFileBuffer int          the maximum number of files to have active during the upload process. Should be set to at least the number of max parallel uploads. Set to 0 to bypass local storage and upload straight to your destination - this will limit you to a single destination and disable any hash checks for the upload where available. (default 5)
      --maxParallelUploads int     the maximum number of uploads to run in parallel. (default 4)
      --maxRetryTime duration      the maximum time that can elapse when retrying a failed upload. Use 0 for no limit. (default 12h0m0s)
      --maxUploadSpeed uint        the maximum upload speed (in KB/s) the program should use between all upload workers. Use 0 for no limit
  -p, --properties                 See the -p flag on zfs send for more information.
  -w, --raw                        See the -w flag on zfs send for more information.
  -R, --replication                See the -R flag on zfs send for more information
      --resume                     set this flag to true when you want to try and resume a previously cancled or failed backup. It is up to the caller to ensure the same command line arguments are provided between the original backup and the resumed one.
      --separator string           the separator to use between object component names. (default "|")
  -s, --skip-missing               See the -s flag on zfs send for more information
      --snapshotPrefix string      Only consider snapshots starting with the given snapshot prefix
      --uploadChunkSize int        the chunk size, in MiB, to use when uploading. A minimum of 5MiB and maximum of 100MiB is enforced. (default 10)
      --volsize uint               the maximum size (in MiB) a volume should be before splitting to a new volume. Note: zfsbackup will try its best to stay close/under this limit but it is not guaranteed. (default 200)

Global Flags:
      --encryptTo string           the email of the user to encrypt the data to from the provided public keyring.
      --jsonOutput                 dump results as a JSON string - on success only
      --logLevel string            this controls the verbosity level of logging. Possible values are critical, error, warning, notice, info, debug. (default "notice")
      --manifestPrefix string      the prefix to use for all manifest files. (default "manifests")
      --numCores int               number of CPU cores to utilize. Do not exceed the number of CPU cores on the system. (default 2)
      --publicKeyRingPath string   the path to the PGP public key ring
      --secretKeyRingPath string   the path to the PGP secret key ring
      --signFrom string            the email of the user to sign on behalf of from the provided private keyring.
      --workingDirectory string    the working directory path for zfsbackup. (default "~/.zfsbackup")
      --zfsPath string             the path to the zfs executable. (default "zfs")
```

## TODOs

- Make PGP cipher configurable.
- Refactor
- Test Coverage
- Add more backends
- Add delete feature
- Appease linters
- Track intermediary snaps as part of backup jobs
- Parity archives?
