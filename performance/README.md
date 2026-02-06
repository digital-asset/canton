# Canton Performance Testing

The Canton performance test is a set of Daml models with a simplified
over-the-counter trading model and a Scala based automated test driver.

## How does it work?

There is a so called `PerformanceRunner` class, defining an application which
runs against one instance of a Ledger API. This application needs to be
deployed at least once per participant node.

On a per-application basis, you need to define the roles that the application
should take care. It's up to you to decide how many roles you want your
application to run.

The following roles are possible:

* Master: The master is the main organizer of the ceremony. He defines the
  test setup and when the test starts and when it is completed. All other
  nodes only need to know who the master is.
  The master's configuration contains information such as how many trades
  (named cycles) every participant to the test needs to complete or the quorum
  of ready participants that needs to be reached before the test can kick off.

* Issuers: A more passive role is the role of issuer. Issuers issue assets
  and these assets can be transferred between traders. There needs to be at
  least one issuer configured.

* Traders: The active role in the test, traders will propose trades to other
  traders (randomly picked) and they will accept trade proposals from other
  traders.

* Both: The role type both means that a party acts as both trader and issuer.

The performance test has the following phases:

* Setup: In a first step, the performance test will initialise itself by
  allocating new parties if necessary using the `PartyManagement` api endpoint
  and will upload the DAR file if necessary.

* Init: All roles will start up, read the ACS and based on the current state,
  decide if they need to create their initial contract. The Master will create
  a `TestRun` contract and the participants will create a `ParticipationRequest`
  contract.

* Onboarding: This is the initial phase. The Master role will approve any
  participation request, sharing the names with other parties by updating the
  `TestRun` contract. Once a sufficient quorum of parties has been reached, the
  `Master` will start the test.

* Throughput: In the throughput measurement phase, the traders will try to load
  the system as optimally as possible with trades. They do this by trying to
  increase the load such that they reach an average latency of 5s (which can be
  configured).

* Done: When everybody is done, the master signals to all parties that they
  can leave the performance test.

The performance runners can be restarted at any time and will resume an ongoing test.
This also means that finished runs and their results are persistent.
To truly run a new test, one needs to use a different `Master` configuration name.
The runners are resilient to failures, retrying failed commands and
trying to reconnect if a node goes down.

## How to run it?

1. If needed, install Postgres and make sure that `psql postgres` (i.e., opening a Postgres shell)
   works from the command line.
   In some environments, just after the installation and before any other user is created,
   you may need to first manually start the DB cluster; the installation procedure usually
   explains how.
   Note that in some environments `psql postgres` may not work but `sudo -u postgres psql`
   may work in its stead. You may also use docker based postgres.

2. Make sure that Postgres runs on port 5432 (default port): `sudo netstat -plunt | grep postgres` (or `sudo lsof -i -P | grep postgres` if `netstat` doesn't work).

3. If you have already built a bundled Canton binary locally, run `performance/bundle-performance.sh`,
   else `performance/pack-performance.sh`.
   If `bundle-performance.sh` fails, run `sbt clean` and then `pack-performance.sh`.

   Files should be packed in `community/app/target/release/canton/performance`

4. If you're setting up performance testing for the first time, you'll need to set up Postgres:
    - `cd community/app/target/release/canton/performance/postgres`
    - Create a performance test user using `../../config/utils/postgres/db.sh create-user`
    - Run `../../config/utils/postgres/db.sh setup`

5. Cd to the performance directory: `cd ..`

6. Run `./run.sh -h` to see the available options; the top portion of the source contains a more detailed description.

Run `./run.sh` with selected flags.

This will start the canton console and kick off the runners. There are a few utilities in MissionControl, such as:

* `stats` - display the current progress
* `disable` - pause all runners
* `enable` - enable all runners again after they were disabled

Currently, only the `performance.canton` script supports all of these operations.

Further utilities and scripts can be found in `performance/src/main/console`:

* `performance.canton` - the main bootstrap script which starts two performance runner instances, one for
  participant1 and the other for participant2.
* `persistence.conf` - a persistence mixin configuration for the `simple-topology.conf` example. You can
  add this configuration file to the command line to make the simple topology use Postgres persistence.
* `shared.conf` - a set of parameters that should be set to properly run the performance test.
* `metrics.conf` - a configuration file used to send metrics to our grafana server
* `light-profile.conf` and `heavy-profile.conf` - configuration mixins that allocate resources for the test.
* `logback.xml` - a logback configuration file for Canton which rolls and compresses log-files hourly.
* `run.sh` - a bash script which will start a performance run. To get using run `./run.sh -h`
  The `light` vs. `heavy` profile is tailored for a machine with 4 vs. 16 virtual cores.

## Steps to run the long-running test

### Build the package
On your machine, check out the release-line and build the bundle:

```
    cd performance && ./pack-performance.sh
```

This will build a `canton-performance.tar.gz` in the base folder.

Next, you need to transfer the file to our long-running test box:

```
    rsync -avz --progress canton-performance.tar.gz canton@sgx-testing.da-int.net:
```

You might need some colleague to drop your ssh key into .ssh/authorized_keys.

On the target server, you unpack the archive:

```
    tar zxvf canton-performance.tar.gz
```
and then rename the folder name

```
    mv canton canton-X.Y.Z-rc3
    cd canton-X.Y.Z-rc3
```

### Set up the Database

You need to set up the databases first. There is a database server (db-testing.da-int.net). It is
very powerful and already configured. We are running Postgres. Check the README.md
on the server if you have questions. Postgres is running as a local process.

But setting up the databases is easy. Likely, there is already old data, so you also need to delete them.
You can do that in a single step:

```
cd performance/postgres
../../config/utils/postgres/db.sh reset
```

The `db.sh` will read the files `databases` and `db.env` in the working directory and initialise them for you.

### Start the Process

We run the process in interactive mode such that we can poke around in the console to see if something is fishy.
To make sure that the process remains active once your ssh session terminates, we use `screen` to run it in a
background attachable console.

You can connect to an existing screen session running:
```
screen -r
There is no screen to be resumed.
```

In this case, there was none running, so we received an error. Screens have a name. If there are several screens,
then you need to add a few characters of the screen name, e.g. `screen -r perf` to get into the session. If your
choice is ambiguous, you'll get a list of running screens.

We start a new screen session using

```
screen -S performance
```

To exit the screen, you hit `CTRL-a` and thereafter `d`. Be careful to not send `CTRL-d` ...

You need to set the protocol version via an environment variable (or edit the config files ...):
```
export PROTOCOL_VERSION=6
```

The next step you need to do is to link the log directory to the big raid disk, as otherwise,
you will fill up the home directory with log files. Just run
```
ln -s /mnt/ssddisk/log
```

There are quite a few possible topologies that you can run. Check out the config files in `performance/topology/`.
Normally, we just use the `distributed-reference.conf`. You can start it using

```
    ./performance/run.sh -n distributed-reference
```

Once it is started up, you will get the console, where you can run the `stats` command to see what it is doing.

### Checking in Regularly

What do you need to do next?

1. Let it run. Check in daily to see what it is doing.
2. If there are WARN / ERRORS in the logs, you need to investigate.
3. Make sure it's active and submitting. The `report-age` should be recent (seconds). There should be `pending` commands. `failed` should not grow. The latency should be ~5s (as configured).
4. After 10 to 20 minutes, check the `tps` number and remember it. Check daily whether the tps number is going down. If it goes down, we have a scaling problem.
5. After a couple of days, kill the process and restart it (with the same command). It will take some time to read the ACS and reinit the system, but it should come back. If things take too long, we might have a problem.
6. Watch for anything that seems fishy to you: things that take longer than they should, errors that get printed to the console etc.

Remember, you can always just exit the screen out using `CTRL-a d`. Don't exit the console ...

## How to run with TLS

By default, we copy the test certs we have from `community/app/src/test/resources/tls` into the performance tar
package. You can use these certificates out of the box.

In order to enable TLS (with simple-topology.conf), you just need to mix in the tls.conf AND you need to set the
environment variable TLS_ROOT_CERT to the root certificate `tls/root-ca.crt`.

If you include tls.conf in your profile, the environment variable will be set automatically in the `run.sh` script.

## How to run against an external Canton deployment

You can start the performance runner using "remote participants" by using the `-n` flag to define
another topology. This allows you to run the performance runner application separate from the participant
node. This comes in handy if you want to continue after an upgrade, as the performance runner jar
is tied to a particular Canton version.

### Running with a constant rate

In order to run at a constant rate and test a certain throughput rather than the tool discovering it,
set the `RATE_ADJUST_FACTOR` environment variable to `1`, and `START_RATE_PER_SEC` to the desired throughput.

As opposed to other configurations, the `pure-ledger-api` configuration sets `zeroTpsReportOnFinish` to false,
so the actual `tps` number reported at the end is meaningful.
Otherwise, there is a chance that not all the statuses are zeroed resulting in a miscalculated `tps` number.

## Troubleshooting

### Incorrect Java version
If the performance runner fails with
```
Exception in thread "main" java.lang.UnsupportedClassVersionError: com/daml/grpc/adapter/ExecutionSequencerFactory has
been compiled by a more recent version of the Java Runtime (class file version 61.0), this version of the Java Runtime
only recognizes class file versions up to 55.0
```
you need to ensure that you run Java >= 17.
On Ubuntu, this can be done by changing the JAVA_HOME:
```
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
```

## Observability Stack

We have one observability stack for all NY machines (deployed to db-testing). To make sure it's running:

1. Run `performance/observability/install-stack.sh <your_ssh_username>`. This will build the dashboard and upload all the files to db-testing.
2. SSH to the db-testing machine and go to the `/home/canton/observability` folder.
3. Make sure the containers are running: `sudo docker compose ps`, `sudo docker compose up -d`.
4. Also, make sure all the prometheus config is correct for the performance tests (sgx-testing, canton-testing-2). The `metrics/prometheus.conf` file should look like in https://github.com/DACH-NY/canton/commit/71e546e86a42b8696ffb35972a0ac30ddd7436e8. You can skip this point if you're using the latest version of the repo.

The dashboards are available at http://db-testing.da-int.net:3000/dashboards.

