
# Postgres Database for (Performance) Testing

This docker compose project allows you to deploy PostgreSQL for testing purposes.
It will spin up a primary and secondary PostgreSQL instance, where the secondary replicates the primary instance.
Additionally, it will use Toxiproxy to introduce latency on the network connection to the primary PostgreSQL instance
and between the primary and secondary instance.

Background knowledge:
- Replication in PostgreSQL: https://www.postgresql.org/docs/17/high-availability.html
- Toxiproxy: https://github.com/Shopify/toxiproxy

## Prerequisites

To run the project, you must install:
- Docker & Docker-compose
  - Linux: \
    https://docs.docker.com/engine/install/ubuntu/ \
    https://docs.docker.com/compose/install/ \
    Consult stackoverflow to resolve further problems.
  - Mac: https://hub.docker.com/editions/community/docker-ce-desktop-mac/

For managing the service, it is convenient to install:
- Postgres 17 client
  - Linux: https://www.postgresql.org/download/linux/ubuntu/
  - Mac: `brew install postgresql@17`
- Toxiproxy 2.1.4
  - Linux: `sudo apt install -y toxiproxy-cli`
  - Mac: `brew install toxiproxy`

## Getting Started

### The Very Basics
To start the service, simply run `start.sh:`
```
$ ./start.sh
Loading default settings from default-settings.env...
...
```

In case you have previously run the service and want to purge the old state before restarting, run `restart-with-purge.sh` instead.

This will spin up several docker containers:
```
Creating network "replicated-postgres_default" with the default driver
Creating replicated-postgres_toxiproxy ... done
Creating replicated-postgres_secondary ... done
Creating replicated-postgres_primary   ... done
```
It will configure Toxiproxy:
```
Created new proxy application_to_primary
Added upstream latency toxic 'latency_upstream' on proxy 'application_to_primary'
Added downstream latency toxic 'latency_downstream' on proxy 'application_to_primary'
Created new proxy replication_to_primary
Added upstream latency toxic 'latency_upstream' on proxy 'replication_to_primary'
Added downstream latency toxic 'latency_downstream' on proxy 'replication_to_primary'
```
Then, it will wait until PostgreSQL is responding on all ports:
```
Waiting until replicated-postgres_primary is serving postgres on port 4431. (On Mac this may take a while.)	... done
Waiting until replicated-postgres_toxiproxy is serving postgres on port 4432. (On Mac this may take a while.)	... done
Waiting until replicated-postgres_toxiproxy is serving postgres on port 4433. (On Mac this may take a while.)	... done
Waiting until replicated-postgres_secondary is serving postgres on port 4439. (On Mac this may take a while.)	... done
```
---
**Note:** Depending on the host machine, it may occur that PostgreSQL cannot allocate enough memory.
If you hit this, try the following:
1. In `default-settings.env`, set `PERFORMANCE_PROFILE=performance-light.conf`. Restart by using
2. If the problem remains, visit https://pgtune.leopard.in.ua/ enter the parameter of your machine and
   paste the resulting PostgreSQL configuration parameters into `scripts/setup-postgres/performance-light.conf`.

---

Finally, it will test whether the replication is working:
```
Testing whether the replication works as expected.

Testing replication of empty table:
- Creating non-empty table testtable...
- Testing replication...
- Success!

Testing replication of non-empty table:
- Removing rows from table testtable...
- Testing replication...
- Success!

Testing replication of non-empty table:
- Deleting table testtable...
- Testing replication...
- Success!
```

Now you can access the primary PostgreSQL instance as follows:
```
$ psql -h localhost -p 4432 -U test postgres
psql (11.10, server 11.9 (Debian 11.9-1.pgdg90+1))
Type "help" for help.

postgres=# select 1; -- or whatever...
 ?column?
----------
        1
(1 row)

postgres=# exit
```
By default, you do not need to provide a password.

You can similarly access the secondary instance by changing the port to `4439`.
Note that the secondary instance has only read access to the database.

If you are not sure about the status of the system, use `test-replication.sh` to find out:
```
$ ./test-replication.sh
Testing whether the replication works as expected.
...
- Success!
```

### Changing Toxics

You can use `toxiproxy-cli` to inspect toxics:
```
$ toxiproxy-cli ls
Listen		Upstream                Name                    Enabled	Toxics
======================================================================
[::]:4432	postgres_primary:5432	application_to_primary	true	2
[::]:4433	postgres_primary:5432	replication_to_primary	true	2
```
You see that there are toxics enabled behind port `4432` and `4433`.

Let's further inspect the toxics behind port `4432`:
```
$ toxiproxy-cli inspect application_to_primary
Name: application_to_primary	Listen: [::]:4432	Upstream: postgres_primary:5432
======================================================================
Upstream toxics:
latency_upstream:	type=latency	stream=upstream	toxicity=1.00	attributes=[jitter=0 latency=1]

Downstream toxics:
latency_downstream:	type=latency	stream=downstream	toxicity=1.00	attributes=[jitter=0 latency=1]
```

Now you can also change toxics, e.g., change the latency to 25 and the jitter to 10 for the upstream toxic.
```
$ toxic update -n latency_upstream -a latency=25 -a jitter=10 application_to_primary
Updated toxic 'latency_upstream' on proxy 'application_to_primary'
```

### Troubleshooting Docker Containers

To enable using `docker-compose` from the command line, you need to first export environment variables used in
the `docker-compose.yml`:
```
. scripts/load-settings.sh
```

In case you need to troubleshoot the created docker containers, first run `docker-compose ps`
to see the running containers (including port mappings!):
```
$ docker-compose ps
            Name                           Command               State                                   Ports
-----------------------------------------------------------------------------------------------------------------------------------------------
replicated-postgres_primary     docker-entrypoint.sh postgres    Up      0.0.0.0:4431->5432/tcp
replicated-postgres_secondary   docker-entrypoint.sh postgres    Up      0.0.0.0:4439->5432/tcp
replicated-postgres_toxiproxy   /go/bin/toxiproxy -host=0. ...   Up      0.0.0.0:4432->4432/tcp, 0.0.0.0:4433->4433/tcp, 0.0.0.0:8474->8474/tcp
```

Use `docker-compose config` to lookup service names:
```
$ docker-compose config --services
toxiproxy
postgres_secondary
postgres_primary
```

Use `docker-compose logs` to inspect the log file of, say, `postgres_secondary`:
```
$ docker-compose logs postgres_secondary
...
```

You can also log into a container:
```
$ docker-compose exec postgres_secondary bash
root@fd6ab90c7e03:/# ...
# Allows you to inspect files and processes.
...
root@fd6ab90c7e03:/# exit
```

### Purging Data

Before you purge any data, first stop the services:
```
$ ./stop.sh
...
```

Then you can purge data as follows:
```
$ ./purge-all.sh
...
```
On Linux, this will prompt you to confirm and enter the root password,
because PostgreSQL removes write permissions from the data directories.

You can also do more fine-grained purging by calling `purge-data.sh`, `purge-primary.sh`, `purge-secondary.sh`, and `purge-replication-data.sh`.

## Customization

The services can be customized in various ways:
- postgres user name, password, database name
- PostgreSQL performance parameters
- Toxiproxy settings
- PostgreSQL replication settings
- Ports
- Docker compose project name
- Mountpoints for data and log file directories

These settings are defined and documented in `default-settings.env`.
Please refer to this file for further details.

### Settings Files

The settings in `default-settings.env` can be changed, by simply adjusting the definitions in that file.
This is the way to go, if you want to permanently change, say, the database name from `postgres` to `mydb`.

Sometimes, it is however desirable to keep the default values and have several settings files that can be used on demand.
Several such files have already been created:
- `canton-testing.env:` Defines mointpoints tailored to `canton-testing.da-int.net`.
- `disable-replication.env:` Disables all PostgreSQL replication features.
- `disable-toxiproxy.env:` Disable Toxiproxy.

These files can be combined arbitrarily:
```
$ ./restart-with-purge.sh disable-replication.env canton-testing.env
Loading default settings from default-settings.env...
Loading custom settings from disable-replication.env...
Loading custom settings from canton-testing.env...
...
```
As the output indicates, this will first load the default settings, and then overwrite them with `disable-replication.env` and `canton-testing.env`.
As a result the service will be started with replication disabled and mount points tailored to `canton-testing.da-int.net`.
Toxiproxy remains enabled.

Additionally, it is also possible to inherit settings from the bash environment:
```
$ set -o allexport                            # Enable auto-export of variable definitions.
$ . default-settings.env                      # Load default settings.
$ . disable-replication.env                   # Disable replication.
$ APPLICATION_TO_DB_REQUEST_LATENCY=55        # Set latency to 55 milliseconds.
$ ./restart-with-purge.sh --inherit-settings  # Now restart the service while using the settings stored in the environment of the calling shell.
```

### PostgreSQL Settings

The PostgreSQL configuration is only to some degree exposed through `default-settings.env`.
If you need to change it, you have to modify the files in `scripts/setup-postgres`.
For example:
- Change `performance.conf` to modify performance parameters.
  Alternatively, you may create a new configuration file `scripts/setup-postgres` and
  change `PERFORMANCE_PROFILE` to point to that file.
- Change `pg_hba.conf` to customize authentication.
- Change `setup-logging-config.sh` to customize logging.

### Toxiproxy Settings

After starting the service, you can customize Toxiproxy by using `toxiproxy-cli`.
If you want to change the default toxics, you can modify `scripts/setup-toxiproxy/setup-toxiproxy.sh`.
