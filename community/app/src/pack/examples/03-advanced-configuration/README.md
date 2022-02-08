# Running Canton Participants and Domains in Separate Processes

While most examples are given for a single process setup for simplicity, the purpose of Canton is to run it in
multi-party workflows across enterprises and trust boundaries. Therefore, the following example features 
configurations for two [participant](nodes/participant1.conf) nodes and one for the [domain](nodes/domain.conf)
which can run on different servers. You can find these configurations in the [nodes](nodes) directory and you 
can use them for your setups.

## Persistence Mixins

In order to run the advanced configuration examples, you need to decide how and if you want to persist the 
data. You currently have three choices: don't persist and just use in-memory stores, persist using H2-file 
based stores, or persist to `Postgres` databases.

For this purpose, there are [storage mixin configurations](storage/) defined. 
The storage mixins in `memory.conf` and `postgres.conf` can be combined with `participant3.conf`.
The storage mixins in `h2.conf`, `memory.conf`, `postgres-alternative.conf` can be combined with the other nodes
(namely, `domain.conf`, `participant1.conf`, `participant2.conf`, `particiant4.conf`, `unique-contract-key-domain.conf`).
Please consult the documentation within the storage mixin files and 
the installation section of the Canton manual for further help.

If you ever see the following error: `Could not resolve substitution to a value: ${_shared.storage}`, then 
you forgot to add the persistence mixin configuration file.

## Domain

Navigate into the directory where you unpacked Canton. We'll be using the configuration files 
found in `examples/03-advanced-configuration`. Then start the domain with the following command:
```
    ./bin/canton -c examples/03-advanced-configuration/storage/h2.conf,examples/03-advanced-configuration/nodes/domain.conf
```
The domain can be started without any script, as it self-initialises by default, waiting for incoming connections. In 
this example, we used the [h2.conf](storage/h2.conf) persistence mixin, but you can also use the 
[memory.conf](storage/memory.conf) or the [postgres.conf](storage/postgres.conf) (once you've set the database up).

If you pass in multiple configuration files, they will be combined. It doesn't matter if you separate the 
configurations using `,` or if you pass them with several `-c` options.

NOTE: If you unpacked the zip directory, then you might have to make the canton startup script executable
 (`chmod u+x bin/canton`).

## Participants

The participant(s) can be started the same way, just by pointing to the participant configuration file. 
However, before we can use the participant for any Daml processing, we need to connect it to a domain. You can 
connect to the domain interactively, or use the [initialisation script](participant-init.canton).

```
    ./bin/canton -c examples/03-advanced-configuration/storage/h2.conf \
        -c examples/03-advanced-configuration/nodes/participant1.conf,examples/03-advanced-configuration/nodes/participant2.conf \
        --bootstrap=examples/03-advanced-configuration/participant-init.canton
```

The initialisation script assumes that the domain can be reached via `localhost`, which needs to change if the domain
runs on a different server.

A setup with more participant nodes can be created using the [participant](nodes/participant1.conf) as a template. 
The same applies to the domain configuration. The instance names should be changed (`participant1` to something else), 
as otherwise, distinguishing the nodes in a trial run will be difficult. 

## Test Your Setup

Assuming that you have started both participants and a domain, you can verify that the system works by having
participant2 pinging participant1 (the other way around also works). A ping here is just a built-in Daml 
contract which gets sent from one participant to another, and the other responds by exercising a choice.

First, just make sure that the `participant2` is connected to the domain by testing whether the following command 
returns `true`
```
@ participant2.domains.active("mydomain")
```

In order to ping participant1, participant2 must know participant1's `ParticipantId`. You could obtain this from 
participant1's instance of the Canton console using the command `participant1.id` and copy-pasting the resulting 
`ParticipantId` to participant2's Canton console. Another option is to lookup participant1's ID directly using
participant2's console:
```
@ val participant1Id = participant2.parties.list(filterParticipant="participant1").head.participants.head.participant
```
Using the console for participant2, you can now get the two participants to ping each other:
```
@ participant2.health.ping(participant1Id)
```

## Running as Background Process

If you start Canton with the commands above, you will always be in interactive mode within the Canton console. 
You can start Canton as well as a non-interactive process using 
```
    ./bin/canton daemon -c examples/03-advanced-configuration/storage/memory.conf \
                        -c examples/03-advanced-configuration/nodes/participant1.conf \
                        --bootstrap examples/03-advanced-configuration/participant-init.canton
```

## Connect To Remote Nodes

In many cases, the nodes will run in a background process, started as `daemon`, while the user would 
still like the convenience of using the console. This can be achieved by defining remote domains and 
participants in the configuration file. 

A participant or domain configuration can be turned into a remote config using

```
    ./bin/canton generate remote-config -c examples/03-advanced-configuration/storage/memory.conf,examples/03-advanced-configuration/nodes/participant1.conf
```

Then, if you start Canton using
```
    ./bin/canton -c remote-participant1.conf
```
you will have a new instance `participant1`, which will expose most but not all commands
that a node exposes. As an example, run:
```
    participant1.health.status
```

Please note that depending on your setup, you might have to adjust the target ip address.

