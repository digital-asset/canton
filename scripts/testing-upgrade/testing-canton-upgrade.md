# Testing a Canton upgrade

This guide walks you through the steps to test a Canton upgrade (without new protocol version).
The goal is to perform the following steps:

- Starting Canton using the old binaries and create some contracts with the performance runner.
- Starting Canton using the new binaries
- If needed, applying database migrations.
- Starting the performance runner again, with the new nodes running.

## Identify the tested versions

Before getting started, you want to understand for which versions you would like to test the
upgrade procedure. Usually, when testing a new version in the release candidate phase, you
will likely want to test the latest patch of the previous minor with the latest RC of the
new minor (e.g. `2.7.6` with `2.8.0-rc1`). If in doubt, ask.

## Initial steps

Ensure that you've got two canton repositories which are checked out at the commit/tag for which you'd like to test a canton upgrade,
i.e. the *old version* from which you want to upgrade to the *new version*.
Define these two environment variables pointing to the repository directories containing the *old* and *new* canton version.

```
export OLD_VERSION_DIR=""
export NEW_VERSION_DIR=""
```

Then, compile and bundle, for the two versions (making sure that the git submodules are initialized and up to date):
```
cd $OLD_VERSION_DIR
git submodule update --init --recursive
sbt "compile;bundle"
performance/pack-performance.sh

cd $NEW_VERSION_DIR
git submodule update --init --recursive
sbt "compile;bundle"
performance/pack-performance.sh
```

## Set up the database

Run the following commands:
```
cd $OLD_VERSION_DIR
docker compose -f scripts/testing-upgrade/postgres-docker-compose.yml up
performance/src/main/console/postgres/db-setup.sh
```

This will keep the current terminal busy. Open a different terminal to move further.

## General setup

Now that the database is running, we will use two more terminal windows:

- One to administrate the nodes (admin).
- One to use the performance runner (runner), accessing the nodes as remote nodes.

The reason is that we don't want the `Performance.dar` to be recompiled.
Hence, we will upgrade Canton nodes binaries but we will keep the same performance runner during the whole test.


## Start the nodes

In admin terminal, start the Canton console that will be use to administrate the nodes:
```
cd $OLD_VERSION_DIR/community/app/target/release/canton
source performance/postgres/db.env
./bin/canton -c performance/postgres/persistence.conf -c performance/topology/distributed-topology.conf
```

Then, bootstrap the synchronizer and connect the participants:
and connect the nodes to the synchronizer:
```
val activeMediator = mediators.all.find(_.health.status.isActive.getOrElse(false)).get
mysynchronizer.setup.bootstrap_synchronizer(sequencers.all, Seq(activeMediator))
participants.local.foreach(_.synchronizers.connect_local(sequencer1))
participant1.health.ping(participant2)
```

## Create contracts using the old version

In the runner terminal, we use the performance runner to put some load on the database:
```
cd $OLD_VERSION_DIR/community/app/target/release/canton/performance
./run.sh -t notls -m none -n remote-distributed-topology
```

About the parameters:

- `-t notls` disable TLS
- `-m none` disable reporting (so that you don't need graphite)
- `-n remote-simple-topology` information on how to connect to remote nodes


You can follow the status of the performance runner using the command
```
stats
```

Note: you may see degradation warning related to the DB queue, this is expected and not an issue.

After a while, you can see the `stats` indicating that contracts were created.
The command will report lines which look like

```
trader(name=Trader0-3-yg, report-age=282ms,  mode=Throughput, rate(c/m)=(1,0/2,8), latencyMs=4874.917694095647, pending=13, proposals=(s/o/q)=690/678/41, accepts=(s/o/q)=663/639/0, backpressured=0, failed=0)
```

The part `proposals=(s/o/q)=690/678/41` indicates statistics for the proposal:

- 690 were submitted
- 678 were observed
- 41 are open (not yet accepted)

You can also check that the number of contracts of the ACS is growing:
```
participant1.ledger_api.acs.of_all().size
res7: Int = 1000
```

Now wait until enough contracts have been created. Usually, that means a few thousands
for each party listed in the `stats` output. That take 20-30 minutes on a typical dev
laptop.

Once enough contracts have been created, stop the runners and exit:
```
disable
exit
```

## Upgrading Canton

Next, you want to stop the admin console. You can do it the hard way (`SIGKILL`) to
make sure you are testing possible corner-cases.

Alternatively, you can just exit the admin console (that will stop the nodes beforehand):
```
exit
```

Now, you can start Canton using the new version:

```
cd $NEW_VERSION_DIR/community/app/target/release/canton
source performance/postgres/db.env
./bin/canton -c performance/postgres/persistence.conf -c performance/topology/distributed-topology.conf --manual-start
```

Note the parameter `--manual-start` that prevents the nodes to be started automatically.

Start the nodes:
```
sequencers.local.foreach(_.start())
mediators.local.start()
participants.local.start()
```

> [!NOTE]
>
> If you see an error such as
> ```
> ERROR c.d.c.c.EnterpriseConsoleEnvironment - failed to initialize participant1: There are 1 pending migrations to get to database schema version 5. Currently on version 4. Please run `participant1.db.migrate` to apply pending migrations
>   Command LocalParticipantReference.start invoked from cmd0.sc:1
> com.digitalasset.canton.console.CommandFailure: Command execution failed.
> ```
>
> then you need to apply the pending migrations
> ```
> nodes.local.foreach(_.db.migrate())
> ```
>
> Please also check that upgrading instructions mention that migrations need to be applied.

After the migrations, you should be able to start the nodes
```
sequencers.local.foreach(_.start())
mediators.local.start()
participants.local.start()
```

Finally, reconnect the nodes and issue a simple ping
```
participants.local.foreach(_.synchronizers.reconnect_all())
participant1.health.ping(participant2)
```

Note that you can check that the previously created contracts are still in the ACS:
```
participant1.ledger_api.acs.of_all().size
res7: Int = 1000
```

## Restart the performance runner

Finally, you should be able to restart the performance runner (use the old version again)

```
./run.sh -t notls -m none -n remote-distributed-topology
```

# Cleaning up

If the performance runner restart completed without any errors or warnings, you can stop the runners and exit:

```
disable
exit
```

In the admin console, stop the nodes and exit:

```
exit
```

Finally, you can shutdown Postgres:

```
cd $OLD_VERSION_DIR/community/app/target/release/canton/performance
docker-compose -f scripts/testing-upgrade/postgres-docker-compose.yml down
```
