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
```
scripts/canton-testing/nightly-performance-test.sh scripts/canton-testing/local-dev.env
```
Passing `local-dev.env` will load in the same way as `canton-testing.env` but it is optimized for local runs. For example, it loads environment variables which bypass Slack, Datadog, Telegraf reporting and by default it loads environment variables not to perform `sbt clean` for each nightly run. Overall it allows us to have a quick check on the nightly test (note that an installed `Toxiproxy server` is still needed for Mac users).

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

### Sequencer catchup performance test
It kicks off the sbt test `com.digitalasset.canton.integration.tests.nightly.sequencer.SequencerCatchUpPerformanceIntegrationTest` that runs 2 participants, 4 sequencers and 5 mediators.
It measures how efficiently a newly bootstrapped or offline sequencer node reads and catches up on historical block/sequencing state from peer sequencers or the underlying database.
It uses `UsePostgres` and `UseToxiproxy` plugins, which - at the time of writing - requires default port usage so the whole test brings up a standalone containerized postgres - and a toxiproxy server.
Due to its irregular test topology setup, it produces some unique metrics file (for each node) and a common log file which is, at the metrics reporting phase, split into sequencer - and participant related log lines. This is important as the other tests mentioned above have separate participant and synchronizer log files.

### Mediator replay performance test
As its name implies, it measures how fast the mediator can replay events.
Similarly as the `Sequencer catchup performance test` it executes one Scala test in sbt:
`com.digitalasset.canton.integration.tests.benchmarks.MediatorReplayBenchmark` and it also produces a common log file that include both participant and synchronizer related log lines. The generated metrics are more limited because the test brings up a topology that doesn't have any participant. This means we don't have measured participant metrics. 

### Running locally for troubleshooting

Run the whole batch of nightly performance tests with
```
scripts/canton-testing/nightly-performance-test.sh scripts/canton-testing/local-dev.env
```
This doesn't clean the sbt project, saving minutes on the execution and the performance numbers are still produced but not reported to the different channels like Slack or Datadog.
It is recommended to comment out any tests in `scripts/canton-testing/nightly-performance-test.sh` that you are not troubleshooting on so you can further shorten the execution time of the script.

## Find the log files

Check the configuration file (e.g., `canton-testing.env`, `local-dev.env`).
```
HDD_BASEDIR="/mnt/data/nightly-performance-tests/$RUN_ID"
LOGS_DIR="$HDD_BASEDIR/logs"
```

For `RUN_ID`, check `default-settings.env`:
```
RUN_ID="${RUN_ID:-$(date "+%Y-%m-%d-%H-%M-%S")}"
```

## Find the metrics files
Check the configuration file (e.g., `canton-testing.env`, `local-dev.env`).
```
export METRICS_BASE_DIR="$DATA_DIR/metrics"
export METRICS_DIR="$METRICS_BASE_DIR/$CURRENT_JOB_NAME"
```
There is a special metrics file, `summary.json`, under `METRICS_BASE_DIR` that contains the summary TPS for each test that is completed in the same nightly run.
A list of `summary.json` file can be rendered to a markdown table by using the script
```
scripts/canton-testing/util/render-metrics-table.py <any_number_of_summary.json_file_even_wtih_glob_path>
```
An example output can look like this:
```markdown
| Test Name                                  |        TPS | Start Time           | Duration     | Version                             | Branch | Machine        |
|:-------------------------------------------|-----------:|:---------------------|:-------------|:------------------------------------|:-------|:---------------|
| `test-with-replicated-db`                  |  **91.77** | 2026-09-30 20:20:19Z | 3641 seconds | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-with-big-commands`                   |  **56.28** | 2026-09-30 21:23:26Z | 656 seconds  | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-participant-replay`                  | **174.10** | 2026-09-30 21:53:47Z | 428 seconds  | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-sequencer-catchup`                   |   **0.25** | 2026-09-30 22:06:49Z | 298 seconds  | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-mediator-replay`                     | **111.41** | 2026-09-30 22:20:36Z | 27 seconds   | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-sequencer-replay`                    |  **48.45** | 2026-09-30 22:30:09Z | 39 seconds   | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-bong-benchmark`                      |  **13.40** | 2026-09-30 22:34:33Z | 140 seconds  | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
| `test-sync-disconnect-reconnect-benchmark` |   **2.16** | 2026-09-30 22:41:42Z | 544 seconds  | `canton-open-source-3.7.0-SNAPSHOT` | `main` | canton-testing |
```

# Port Allocation

## Ad-Hoc Tests

Postgresql (see `/usr/lib/postgresql` or `scripts/canton-testing/util/setup-standalone-services.sh`):
- 5432

Toxiproxy (see `scripts/canton-testing/util/setup-standalone-services.sh`):
- 8474

Replicated postgresql (see `docker/replicated-postgres`):
- Postgresql: 4431, 4432, 4433, 4439
- Toxiproxy: 8474

**Canton:**
Ports are listed in `community/app/src/test/resources/README.md`.
This should not lead to collisions, as long as you stop Canton when you are done.
The scripts are written in a way that they call setup and teardown scripts that ensure for setting up - and tearing down the required services.

## Nightly Tests

Standalone postgresql (see `scripts/canton-testing/util/setup-standalone-services.sh`):
- 5432

Standalone Toxiproxy (see `scripts/canton-testing/util/setup-standalone-services.sh`):
- 8474

Replicated postgresql (see `docker/replicated-postgres`):
- Postgresql: 4531, 4532, 4533, 4539
- Toxiproxy: 8374

Canton:
- Sequencers: 8028, 8029
- Mediators: 8038, 8039
- Participants: 8011, 8012, 8021, 8022

