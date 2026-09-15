# Ledger API scheduled performance tests

## The scripts in canton-testing

- `lapi_performance_test.sh`
- `deployment/lapi_performance_test.sh`
- `util/lapi_perf_test_metrics.py`
- `lapi_performance_test_config.json`

Are specific to the stream-level performance measurements of the Ledger API.
These scripts are scheduled in cron with the following command:

```bash
from-cron.sh $LAPI_CRON_OUTPUT_PATH \
    ./lapi_performance_test.sh <branch> <config.json>
```

The schedule already exists on `canton-network-testing-1.da-int.net`

## Deployment

There is a folder created (`automated-performance-tests`) where the
`canton/scripts/canton-testing/deployment/from-cron.sh` and the
`deployment/lapi-performance-test.sh` scripts
and the configuration file are linked via symbolic links.

The script under deployment folder checks out the target branch from Git
and starts the test script.

### Additional environment variables to be set in profile

- `LAPI_PERF_TEST_SLACK_CHANNEL_ID` - Slack channel id for the notifications
- `LAPI_SLACK_API_KEY` - Slack API key for the LAPI performance reporter app
- `LAPI_DATADOG_API_KEY` - Datadog API key
- `LAPI_CRON_OUTPUT_PATH` path used in from-cron.sh to redirect the entire stdout

If API keys are not set the payload will be printed and not sent to Slack or Datadog.
The cron path is stored in the variable so that the cleanup can be executed.

## Test script (`lapi-performance-test.sh`)

The test script builds and runs the ScalaTest tests from the `Perf` task using
`sbt`, and upon finishing executes cleanup and sends notifications (provided
that notification settings are configured).

Test execution is based on the configuration file passed as an argument.
The configuration file is a JSON file with the following structure:

```json
{
  "db_name": "lapi_load_test",
  "retention_days": 14,
  "report_path": "/tmp/lapi_reports",
  "stages": [
    {
      "stage_name": "fetch_1",
      "description": "Fetch 1",
      "file_pattern": "acs_fetch",
      "scala_test": "AcsFetchPerfTest",
      "xmx": "32G",
      "new_db": false
    }
    ...
  ],
  "metrics": [
    {
      "metric_name": "fetch_time_seconds",
      "description": "Time taken to fetch data from the ledger API",
      "lower": 0,
      "upper": 1000
    }
    ...
  ]
}
```

Stages are executed in the order defined in the configuration file.
Stage name is an identifier used to distinguish the reported metrics,
so if the same scala test is executed and the tests themselves log in
files that would match the same file pattern, we still can label the points.
Currently Datadog `source:` tag contains that information.  
If `new_db` is set to `true`, the database is recreated before the stage.
The `file_pattern` field is used to specify the location of test results
in the output folder (the `file_pattern` corresponds to the `filenameSuffix`
field of the test classes),
`scala_test` specifies the name of the ScalaTest test to run.
The `description` field is used when reporting metrics to Slack.

Each stage runs as a separate `sbt` task. The `xmx` setting is passed as a
property to `sbt`, which is necessary because certain tests require significant
memory to run. We avoid setting this high value as the global default. (This
high memory consumption is required by test data generation itself, not by the
system under test, and will be optimized in future releases).

Cleanup relies on `retention_days`: logs older than the specified retention
period are automatically deleted. Log archiving is currently not configured.

## Tests

Tests themselves are implemented in ScalaTest and are located in
ledger-api-core/perf module. The scala tests reuse the previous
load testing IndexComponrntLoadTest class and its infrastructure
with the addition of a reporting class that collects and saves data to CSV files.
The metrics collected are the same as were logged previously
and are described in the configuration file,
only those metrics are reported to Slack and Datadog.
The tests are run in the order defined in the configuration file.

Adding a test needs a new test class in the perf module.
You may then configure it for running in the configuration file as a new stage.

If you add new metrics that you want to see reported,
you need to extend the metrics configuration part.
Metric names are simply snake cased from the key passed to the reportMetric function.

The execution of test stages is preceded by an unmeasured warm-up run that
ingests 20% of the NFR data. (InsertPerfTest)

The test stages in the default config file are defined as follows:

- `fetch_acs_base`: Runs ACS fetch after an initial unmeasured
ingestion of 20% NFR
  data. (AcsFetchPerfTest)
- `insert_base`: Ingests an additional 20% of NFR random data on top of existing
  data. (InsertPerfTest)
- `insert_with_high_archival_rate`:
Ingests 20% of NFR data with a higher archival rate on a fresh,
  clean database. (InsertWithIncreasedArchivalRatePerfTest)
- `fetch_with_high_archival_rate`: Executes ACS fetch once again. (AcsFetchPerfTest)

## Reporting

The Python script `util/lapi_perf_test_metrics.py` reports metrics to Slack and
Datadog. The ScalaTest suite produces a simple CSV file containing raw values:
`timestamp, metric_name, value`.

The Python script reads all CSV files for the current day. Values are
aggregated and averaged by `metric_name` for Slack notifications, while raw
values are forwarded directly to Datadog. The script also validates that
metrics remain within configured bounds, issuing a warning to Slack if any
thresholds are breached.

A time-series dashboard is configured in Datadog:

- [LAPI nightly performance tests][datadog-dashboard]

It features time-series graphs for each metric. Datadog automatically
highlights anomalies, and automated alerting can be configured if required.

[datadog-dashboard]: https://app.datadoghq.com/dashboard/rwe-rzq-zrf/lapi-nightly-performance-tests?fromUser=true&refresh_mode=sliding&from_ts=1785688563824&to_ts=1788366963824&live=true

## Troubleshooting

1. Run failure reproted in Slack

- ssh in to the test server
- if the failure reported after some metrics is logged the failure should be in
the next test stage log
- if the failure reported before any other metric would be logged the error
should be found in the cron output

1. the scripts can be run more or less independently manually,
to repete or fix test runs

- testing can be started with the lapi-performance-test.sh script (from canton/scripts/canton-testing)

```bash
canton/scripts/canton-testing/lapi-performance-test.sh <config.json>
```

This will use the current date for logging.

- reporting can be started for any given day independently,
  useful if you would like to send in specific measurements from a test
  with a special configuration file.

  ```bash
  python canton/scripts/canton-testing/util/lapi_perf_test_metrics.py <config.json> <date: YYYY-MM-DD> [<stage>]
  ```
