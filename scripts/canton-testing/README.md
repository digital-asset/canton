Nightly Performance Tests
=========================

# Overview

This folder contains scripts for running performance tests.
The nightly performance test suite is started with `nightly-performance-test.sh`.
Individual tests can be started with `test-*.sh`.
To reset configuration of the machine, use the `setup-*.sh` scripts.
The `*.env` files contain setting for performance tests.

The `util` subfolder contains utility scripts for running tests that are not meant to be called directly.
The `config` subfolder contains configuration files. Change them to do ad hoc config changes.
The `deployment` subfolder contains scripts for invoking tests from `canton-testing.da-int.net` automatically.

More information can be found at the headers of the individual files.
If you want to perform a specific task, consider to read the following "How To..." section.

# How To...

## Run nightly performance tests

```
scripts/canton-testing/nightly-performance-test.sh scripts/canton-testing/canton-testing.env
```
Passing `canton-testing.env` will first load the parameters in `default-settings.env` and then those in `canton-testing.env`.
So the parameters from `canton-testing.env` will effectively overwrite those in `default-settings.env`.
The file `canton-testing.env` contains parameters that are suitable for performance testing (e.g. host the db on an ssd disk).

## Quickly test whether the nightly performance test is broken

```
scripts/canton-testing/nightly-performance-test.sh
```
This will use parameters from `default-settings.env` only.
The test will terminate within a few minutes, but the performance data won't be useful.

## Set the log level to debug

Environment variables are passed through to Canton.
Before starting the test, do:
```
export LOG_LEVEL_CANTON=DEBUG
```

## Run only the test with a replicated db

First, make sure that Canton has been compiled.
```
sbt Test/compile performance/package community-app/bundle
```
Then, run
```
scripts/canton-testing/test-with-replicated-db.sh scripts/canton-testing/canton-testing.env
```

## Run participant replay tests

First do a recording.
```
sbt Test/compile performance/package community-app/bundle
export RUN_ID=mytest # or some other magic string
scripts/canton-testing/test-with-recording.sh scripts/canton-testing/canton-testing.env
```
You need to set `RUN_ID` to make sure that both the recording and the replay step use the same data folders.
(By default, `RUN_ID` is the time when the test was started; so it would be different for record and replay.)

To replay the recording:
```
scripts/canton-testing/test-participant-replay.sh scripts/canton-testing/canton-testing.env
```
You can repeat the replay test as often as you want.

Note that there are other replay tests than the participant one that can only be run manually. Check the source code for more details.

### Running locally for troubleshooting

Make sure that you are connected to the `vpn-cn-devnet.da-int.net` VPN or configure and run Graphite on your own.
On macOS: comment out all `bail-out-on-unrelated-processes.sh` usages or at least lines that use commands that may not exist.

Run the tests as described above using the default settings:

```
scripts/canton-testing/<script-name>.sh
```

You might want to tune test durations not to waste time or get more meaningful results.

## Find the log files

Check the configuration file (e.g., `canton-testing.env`).
```
HDD_BASEDIR="/mnt/data/nightly-performance-tests/$RUN_ID"
LOGS_DIR="$HDD_BASEDIR/logs"
```

For `RUN_ID`, check `default-settings.env`:
```
RUN_ID="${RUN_ID:-$(date "+%Y-%m-%d-%H-%M-%S")}"
```

# Port Allocation

## Ad-Hoc Tests

Postgresql (see `/usr/lib/postgresql`):
- 5432

Replicated postgresql (see `docker/replicated-postgres`):
- Postgresql: 4431, 4432, 4433, 4439
- Toxiproxy: 8474

**Canton:**
Ports are listed in `community/app/src/test/resources/README.md`.
This should not lead to collisions, as long as you stop Canton when you are done.

## Nightly Tests

Replicated postgresql (see `docker/replicated-postgres`):
- Postgresql: 4531, 4532, 4533, 4539
- Toxiproxy: 8374

Canton:
- Sequencers: 8028, 8029
- Mediators: 8038, 8039
- Participants: 8011, 8012, 8021, 8022
