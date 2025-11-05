..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Review completely. model tuning is a bit outdated with pv=7 and dirty requests has been renamed into inflight validation requests
    Consider to move general performance advice/introduction to explanations, move concrete actions into howtos for caching/batching/storage.

.. note::
    Ensure that the following performance tuning parameters (in ParticipantNodeParameterConfig or CantonParameters) are covered somewhere in the optimize group of docs
    - startup parallelism
    - timeouts
    - startup memory checks
    - batching config
    - caching config
    - warn if overloaded
    - journal GC delay
    - package metadata view

.. _scaling_and_performance:

Scaling and Performance
=======================

Network Scaling
---------------

The scaling and performance characteristics of a Canton-based system
are determined by many factors. The simplest approach is to deploy Canton as a simple monolith where vertical scaling would
add more CPUs, memory, etc. to the compute resource. However, the most frequent and expected deployment of Canton is as a distributed, micro-service architecture, running in
different data centers of different organizations, with many opportunities to
incrementally increase throughput. This is outlined below.

The ledger state in Canton does not exist globally so there is no
single node that, by design, hosts all contracts. Instead, participant nodes are
involved in transactions that operate on the ledger state on a strict
need-to-know basis (data minimization), only exchanging (encrypted)
information on the synchronizers used as coordination points for the given
input contracts. For example, if participants Alice and Bank transact
on an i-owe-you contract on synchronizer A, another participant Bob, or
another synchronizer B, does not receive a single bit related to this
transaction. This is in contrast to blockchains, where each node has to
process each block regardless of how active or directly affected
they are by a given transaction. This lends itself to a
micro-service approach that can scale horizontally.

The micro-services deployment of Canton includes the set of
participant nodes (hereafter, "participant" or
"participants") and synchronizers, as well as the
services internal to the synchronizer (e.g., Topology Manager). In general,
each Canton micro-service follows the best practice of having its own
local database which increases throughput. Deploying a service
to its
own compute server increases throughput because of the additional CPU and
disk capacity. A vertical scaling approach can be used to
increase throughput if a single service becomes a bottleneck, along
with the option of horizontal scaling that is discussed next.

An initial Canton deployment can increase its scaling in multiple
ways that build on each other. If a single participant node has many
parties, then throughput can be increased by migrating parties off to
a new, additional participant node (currently supported as a manual
early access feature). For example, if 100 parties are performing
multi-lateral transactions with each other, then the system can
reallocate parties to 10 participants with 10 parties each, or 100
participants with 1 party each. As most of the computation occurs on
the participants, a synchronizer can sustain a very substantial load from
multiple participants. If the synchronizer were to be a bottleneck then the
sequencer(s), topology manager, and mediator can be run on their own
compute server which increases the synchronizer throughput. Therefore, new
compute servers with additional Canton nodes can be added to the
network when needed, allowing the entire system to scale horizontally.

If even more throughput is needed then the multi-synchronizer feature of
Canton can be leveraged to increase throughput. In a large and active network
where a synchronizer reaches the capacity limit, additional synchronizers can be
rolled out, such that the workflows can be sharded over the available
synchronizers (early access). This is a standard technique for load
balancing where the client application does the load balancing via sharding.

If a single party is a bottleneck then the throughput can be increased
by sharding the workflow across multiple parties hosted on separate participants. If a workflow
is involving some large operator (i.e. an exchange), then an option
would be to shard the operator by creating two operator parties and
distribute the workflows evenly over the two operators (eventually hosted on different participants), and by adding
some intermediate steps for the few cases where the workflows would
span across the two shards.

Some anti-patterns need to be avoided for the
maximum scaling opportunity. For example, having
almost all of the parties on a single participant is an anti-pattern to
be avoided since that participant will be a bottleneck. Similarly,
the design of the Daml model has a strong impact on the degree to which sharding
is possible. For example, having a Daml application that introduces a
synchronization party
through which all transactions need to be validated introduces a
bottleneck so it is also an anti-pattern to avoid.

The bottom line is that a Canton system can scale out horizontally if
commands involve only a small number of participants and synchronizers.


.. enterprise-only::

Node Scaling
------------

The Daml Enterprise edition of Canton supports the following scaling of nodes:

- The database-backed drivers (Postgres and Oracle) can run in an active-active setup with parallel processing,
  supporting multiple writer and reader processes. Thus, such nodes can scale horizontally.

- The enterprise participant node processes transactions in parallel (except the process of conflict detection which
  by definition must be sequential), allowing much higher throughput than the community version. The community version
  processes each transaction sequentially.
  Canton processes make use of multiple CPUs and will detect the number of available CPUs automatically.
  The number of parallel threads can be controlled by setting the JVM properties
  `scala.concurrent.context.numThreads` to the desired value.

Generally, the performance of Canton nodes is currently storage I/O bound. Therefore, their performance depends on the
scaling behavior and throughput performance of the underlying storage layer, which can be a database or a distributed
ledger for some drivers. Therefore, appropriately sizing the database is key to achieving the necessary performance.

On a related note: the Daml interpretation is a pure operation, without side-effects. Therefore, the interpretation
of each transaction can run in parallel, and only the conflict detection between transactions must run sequentially.

Performance and Sizing
----------------------

A Daml workflow can be computationally arbitrarily complex, performing lots of computation (cpu!) or fetching many
contracts (io!), and involve different numbers of parties, participants, and synchronizers. Canton nodes store their entire
data in the storage layer (database), with additional indexes. Every workflow and topology is different,
and therefore, sizing requirements depend on the Daml application that is going to run, and on the resource
requirements of the storage layer. Therefore, to obtain sizing estimates you must measure the resource usage
of dominant workflows using a representative topology and setup of your use case.

Batching
--------

As every transaction comes with an overhead (signatures, symmetric encryption keys, serialization and wrapping into
messages for transport, HTTP headers, etc), we recommend designing the applications submitting commands in a way that
batches smaller requests together into a single transaction.

Optimal batch sizes depend on the workflow and the topology and need to be determined experimentally.

Asynchronous Submissions
------------------------

In order to achieve the best performance, we suggest that you use asynchronous command submissions. However,
please note that the async submission is only partially asynchronous, as the initial command interpretation
and transaction building are included in that step, while the transaction validation and result finalization are
not. This means that an async submission takes between 50 to 1000 ms, depending on command size and complexity.
In the extreme case with a single thread submitting transactions, this would mean that you would only achieve a rate of
one command per second.

If you use synchronous command submissions, the system will wait for the entire transaction to complete, which will
require even more threads. Also, please note that the synchronous command submission has a default upper limit of 256
in flight commands, which can be reconfigured using

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/command-service-max-commands-in-flight.conf


Storage Estimation
------------------
A priori storage estimation of a Canton installation is tricky. As explained above, storage usage depends on topology, payload, Daml models used, and what type
of storage layer is configured. However, the following example may help you understand the storage usage for your use case:

First, a command submitted through the gRPC Ledger API is sent to the participant as a serialized gRPC request.

This command is first interpreted and translated into a Daml-LF transaction. The interpreted transaction is next translated into a Canton transaction view-decomposition, which is a privacy-preserving
representation of the full transaction tree structure.
A transaction typically consists of several transaction views; in the worst case, every action node in the transaction tree becomes a separate transaction view.
Each view contains the full set of arguments required by that view, including the contract arguments of the input contracts.
So the data representation can be multiplied quite a bit. Here, we cannot estimate the resulting size without having a concrete example.
For simplicity, let us consider the simple case where a participant is exercising a simple "Transfer" choice on an
typical "Iou" contract to a new owner, preserving the other contract arguments.
We assume that the old and new owners of the IOU are hosted on the same participant whereas the IOU issuer is hosted on a second participant.

The resulting Canton transaction consists of two views (one for the **Exercise** node of the Transfer choice and one for the **Create** node of the transferred IOU).
Both views contain some metadata such as the package and template identifiers, contract keys, stakeholders, and involved participants.
The view for the **Exercise** node contains the contract arguments of the input IOU, say of size `Y`.
The view for the **Create** node contains the updated contract arguments for the created contract, again of size `Y`.
Note that there is no fixed relation between the command size `X` and the size of the input contracts `Y`.
Typically `X` only contains the receiver of the transfer, but not the contract arguments that are stored on the ledger.

Then, we observe the following storage usage:

- Two encrypted envelopes with payload `Y` each, one view seed per view, one symmetric key per
  informee group, two root hashes for each
  participant and the participant IDs as recipients at the sequencer
  store, and the informee tree for the mediator (informees and
  transaction metadata, but no payload), together with the sequencer
  database indexes.
- Two encrypted envelopes with payload `Y` each and the symmetric keys for the views, in the participant events table of each participant (as both receive the data)
- Decrypted new resulting contract of size `Y` in the private contract store and some status information of that contract on the active contract journal of the sync service.
- The full decrypted transaction with a payload of size `Y` for the created contract, in the sync service linear event log. This transaction does not contain the input contract arguments.
- The full decrypted transaction with `Y` in the indexer events table, excluding input contracts, but including newly divulged input contracts.

If we assume that payloads dominate the storage requirements, we conclude that the storage requirement is given by the payload multiplication
due to the view decomposition. In our example, the transaction requires `5*Y` storage on each participant and `2*Y` on the sequencer.
For the two participants and the sequencer, this makes `12*Y` in total.

Additionally to this, some indexes have to be built by the database to serve the contracts and events efficiently.
The exact estimation of the size usage of such indexes for each database layer is beyond the scope of our documentation.

.. note::

   Please note that we do have plans to remove the storage duplication between the sync service and the indexer. Ideally,
   will be able to reduce the storage on the participant for this example from `5*Y` down to `3*Y`: once for the unencrypted created contract and twice for the two encrypted transaction views.

Generally, to recover used storage, a participant and a synchronizer can be pruned. Pruning is available on Canton Enterprise
through a :ref:`set of console commands <ledger-pruning-commands>` and allows removal of past events and archived contracts
based on a timestamp. The storage usage of a Canton deployment can be kept constant by continuously removing
obsolete data. Non-repudiation and auditability of the unpruned history are preserved due to the bilateral commitments.


Set Up Canton to Get the Best Performance
-----------------------------------------

In this section, the findings from internal performance tests are outlined to help you achieve optimal performance for your Canton application.

System Design / Architecture
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We recommend the version of Canton included in the Daml Enterprise edition, which is heavily optimized when compared with the community edition.

Plan your topology such that your Daml parties can be partitioned into independent blocks.
That means most of your Daml commands involve parties of a single block only.
It is ok if some commands involve parties of several (or all) blocks, as long as this happens only very rarely.
In particular, avoid having a single master party that is involved in every command, because that party bottlenecks the system.

If your participants are becoming a bottleneck, add more participant nodes to your system.
Make sure that each block runs on its own participant.
If your synchronizer(s) are becoming a bottleneck, add more synchronizer nodes and distribute the load evenly over all synchronizers.

Prefer sending big commands with multiple actions (creates / exercises) over sending numerous small commands.
Avoid sending unnecessary commands through the gRPC Ledger API.
Try to minimize the payload of commands.

Further information can be found in Section :ref:`scaling_and_performance`.

Hardware and Database
~~~~~~~~~~~~~~~~~~~~~

Do not run Canton nodes with an in-memory storage or with an H2 storage in production or during performance tests.
You may observe very good performance in the beginning, but performance can degrade substantially once the data stores fill up.

Measure memory usage, CPU usage and disk throughput and improve your hardware as needed.
For simplicity, it makes sense to start on a single machine.
Once the resources of a machine are becoming a bottleneck, distribute your nodes and databases to different machines.

Try to make sure that the latency between a Canton node and its database is
very low (ideally in the order of microseconds). The latency between Canton
nodes has a much lower impact on throughput than the latency between a Canton
node and its database.

Please check the :ref:`Postgres persistence section <postgres-performance-tuning>` for tuning instructions.

.. _performance_configuration:

Configuration
~~~~~~~~~~~~~

In the following, we go through the parameters with known impact on performance.

**Timeouts.** Under high load, you may observe that commands timeout.
This will negatively impact throughput, because the commands consume resources without contributing to the number of accepted commands.
To avoid this situation increase timeout parameters from the Canton console:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/scripts/canton-testing/config/run-synchronizers.canton
    start-after: user-manual-entry-begin: BumpSynchronizerTimeouts
    end-before: user-manual-entry-end: BumpSynchronizerTimeouts

If timeouts keep occurring, change your setup to submit commands at a lower rate.
In addition, take the next paragraph on resource limits into account.

.. _tuning_resource_limits:

**Tune resource limits at the Canton protocol level.** Resource limits are used to prevent ledger applications from overloading Canton by sending
commands at an excessive rate.
While resource limits are necessary to protect the system from denial of service attacks in a production environment,
they can prevent Canton from achieving maximum throughput.
Resource limits at the Canton protocol level can be configured as follows from the Canton console:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/BackpressureIntegrationTest.scala
   :start-after: user-manual-entry-begin: SetResourceLimits
   :end-before: user-manual-entry-end: SetResourceLimits
   :dedent:

As a rule of thumb, configure ``maxDirtyRequests`` to be slightly larger than ``throughput * latency``, where

- ``throughput`` is the number of requests per second Canton needs to handle and
- ``latency`` is the time taken to process a single request while Canton is receiving requests at rate ``throughput``.

You should run performance tests to ensure that ``throughput`` and ``latency`` are actually realistic.
Otherwise, an application may overload Canton by submitting more requests than Canton can handle.

Configure the ``maxRate`` parameter to be slightly higher than the expected maximal ``throughput``.

If you need to support command bursts, configure the ``maxBurstFactor`` accordingly. Then,
the ``maxRate`` limitation will only start to enforce the rate after having received the initial burst
of ``maxBurstFactor * maxRate``.

To find optimal resource limits you need to run performance tests.
The ``maxDirtyRequest`` parameter will protect Canton from being overloaded, if requests are arriving at a constant rate.
The ``maxRate`` parameter offers additional protection, if requests are arriving at a variable rate.

If you choose higher resource limits, you may observe a higher throughput, at the risk of a higher latency.
In the extreme case however, latency grows so much that commands will timeout;
as a result, the command processing consumes resources even though some commands are not committed to the ledger.

If you choose lower resource limits, you may observe a lower latency, at the cost of lower throughput and
commands getting rejected with the error code ``PARTICIPANT_BACKPRESSURE``.

.. _tuning_ledger_api_limits:

**Tune resource limits at the gRPC Ledger API level.** Resource limits can also be imposed on the gRPC Ledger API level.
As these settings are applied closer to the ledger applications, they can be used for protecting the resources
of individual participants rather than the entire Canton system.

You can modify the following configuration options:

.. code::

    canton.participants.<participant-name>.ledger-api.rate-limit {
      max-streams = 333
      max-api-services-queue-size = 444
      max-api-services-index-db-queue-size = 555
      max-used-heap-space-percentage = 66
      min-free-heap-space-bytes = 7777777
    }

You can cap the number of gRPC Ledger API streams open at any given time by modifying the
``max-streams`` parameter. When the number of simultaneously open transaction, transaction-tree, completion, or acs
streams reaches the maximum, it doesn't accept any additional get stream requests and returns
a ``MAXIMUM_NUMBER_OF_STREAMS`` error code instead.

You can cap the number of items pending in the thread pools serving the Ledger API and the index
database read requests by modifying the ``max-api-services-queue-size`` and ``max-api-services-index-db-queue-size``
respectively. When the CPU worker thread pool or the database communication thread pool is overloaded, the server
responds with a ``THREADPOOL_OVERLOADED`` error code.

You can cap the percentage of the memory heap used by changing the
``max-used-heap-space-percentage`` parameter. If this percentage is exceeded following a garbage collection of
the ``tenured`` memory pool, the system is rate-limited until additional space is freed up.

Similarly, you can set the minimum heap space in absolute terms by changing the ``min-free-heap-space-bytes`` parameter.
If the amount of free space is below this value following a garbage collection of the ``tenured`` memory pool,
the system is rate-limited until additional space is freed up. When the maximum memory thresholds are exceeded
the server responds to gRPC Ledger API requests with a ``HEAP_MEMORY_OVER_LIMIT`` error code.

The following configuration values are the defaults:

.. code::

    max-streams = 1000
    max-api-services-queue-size = 10000
    max-api-services-index-db-queue-size = 1000
    max-used-heap-space-percentage = 100
    min-free-heap-space-bytes = 0

The memory-related settings of 100 for ``max-used-heap-space-percentage`` and 0 for ``min-free-heap-space-bytes``
render them effectively inactive. This is done on purpose. They are highly sensitive to the operating environment
and should only be configured where memory profiling has highlighted spikes in memory usage that need to be flattened.

It is possible to turn off rate limiting at the gRPC Ledger API level:

.. code::

    canton.participants.<participant-name>.ledger-api {
      rate-limit = null
    }

.. _tuning_stream_api_limits:

**Number of Open Streams**: Similarly to the Ledger API, the number of open streams on the Admin API and the Sequencer API can be configured using the ``limits.active`` section of the configuration:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/sequencer-api-limits.conf

.. _size_of_connection_pools:

**Size of connection pools.** Make sure that every node uses a connection pool to communicate with the database.
This avoids the extra cost of creating a new connection on every database query.
Canton chooses a suitable connection pool by default, but for performance sensitive applications you may want to optimize.
Configure the maximum number of connections such that the database is fully loaded, but not overloaded. Allocating
too many database connections will lead to resource waste (each thread costs), context switching and contention on the
database system, slowing the overall system down. If this occurs, the query latencies reported by Canton go
up (:ref:`check the troubleshooting guide<how_to_diagnose_slow_db_queries>`).

Detailed instructions on handling this issue can also be found in :ref:`max_connection_settings`.

**Size of database task queue.** If you are seeing frequent ``RejectedExecutionExceptions`` when Canton queries the database,
increase the size of the task queue, as described in :ref:`database_task_queue_full`. The rejection is otherwise
harmless. It just points out that the database is overloaded.

**Database Latency.** Ensure that the database latency is low. The higher the database latency, the lower the actual
bandwidth and the lower the throughput of the system.

**Throttling configuration for SequencerClient.**
The ``SequencerClient`` is the component responsible for managing the connection of any member (participant,
mediator, or topology manager) in a Canton network to the synchronizer. Each synchronizer can have multiple sequencers,
and the ``SequencerClient`` connects to one of them. However, there is a possibility that the ``SequencerClient``
can become overwhelmed and struggle to keep up with the incoming messages. To address this issue, a configuration
parameter called ``maximum-in-flight-event-batches`` is available:

.. literalinclude:: CANTON/enterprise/app/src/test/resources/documentation-snippets/sequencer-client-throttling.conf

By setting the ``maximum-in-flight-event-batches`` parameter, you can control the maximum number of event batches
that the system processes concurrently. This configuration helps prevent overload and ensures that the
system can handle the workload effectively.

It's important to note that the value you choose for ``maximum-in-flight-event-batches`` impacts the ``SequencerClient``'s
performance in several ways. A higher value can potentially increase the ``SequencerClient``'s throughput, allowing it to
handle more events simultaneously. However, this comes at the cost of higher memory consumption and longer
processing times for each batch.

On the other hand, a lower value for ``maximum-in-flight-event-batches`` might limit the throughput, as it
can process fewer events concurrently. However, this approach can result in more stable and predictable ``SequencerClient``
behavior.

To monitor the performance of the ``SequencerClient`` and ensure it is operating within the desired limits, you
can observe the metric ``sequencer-client.handler.actual-in-flight-event-batches``. This metric provides the
current value of the in-flight event batches, indicating how close it is to the configured limit. Additionally,
you can also reference the metric ``sequencer-client.handler.max-in-flight-event-batches`` to determine the
configured maximum value.

By monitoring these metrics, you can gain insights into the actual workload being processed and assess whether
it is approaching the specified limit. This information is valuable for maintaining optimal ``SequencerClient``
performance and preventing any potential bottlenecks or overload situations.

**Turn on High-Throughput Sequencer.** The database sequencer has a number of parameters that can be tuned. The trade-off
is low-latency or high-throughput. In the low-latency setting, every submission will be immediately processed as a single
item. In the high-throughput setting, the sequencer will accumulate a few events before writing them together at once.
While the latency added is only a few ms, it does make a difference during development and testing of your Daml applications.
Therefore, the default setting is ``low-latency``. A production deployment with high throughput demand should choose the
``high-throughput`` setting by configuring:

.. literalinclude:: CANTON/enterprise/app/src/test/resources/documentation-snippets/high-throughput-sequencer.conf

There are additional parameters that can in theory be fine-tuned, but we recommend to leave the defaults and use either
high-throughput or low-latency. In our experience, a high-throughput sequencer can handle several thousand submissions
per second.

**JVM heap size.** In case you observe ``OutOfMemoryErrors`` or high overhead of garbage collection, you must increase the heap size of the JVM,
as described in Section :ref:`jvm_arguments`.
Use tools of your JVM provider (such as VisualVM) to monitor the garbage collector to check whether the heap size is tight.

**Size of thread pools.** Every Canton process has a thread pool for executing internal tasks.
By default, the size of the thread-pool is configured as the number of (virtual) cores of the underlying (physical) machine.
If the underlying machine runs other processes (e.g., a database) or if Canton runs inside of a container,
the thread-pool may be too big, resulting in excessive context switching.
To avoid that, configure the size of the thread pool explicitly like this:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/release/CliIntegrationTest.scala
   :start-after: user-manual-entry-begin: SetNumThreads
   :end-before: user-manual-entry-end: SetNumThreads
   :dedent:

As a result, Canton will log the following line:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/release/CliIntegrationTest.scala
   :start-after: user-manual-entry-begin: LogNumThreads
   :end-before: user-manual-entry-end: LogNumThreads
   :dedent:

**Asynchronous commits.** If you are using a Postgres database, configure the Participant's Ledger API server
to commit database transactions asynchronously by including the following line into your Canton configuration:

.. literalinclude:: CANTON/scripts/canton-testing/config/participants.conf
   :start-after: user-manual-entry-begin: AsynchronousCommitMode
   :end-before: user-manual-entry-end: AsynchronousCommitMode

**Logging Settings.** Make sure that Canton outputs log messages only at level INFO and above
and turn off immediate log flushing using the ``--log-immediate-flush=false`` commandline flag,
at the risk of missing log entries during a host system crash.

**Replication.** If (and **only if**) using single nodes for participant, sequencer, and/or mediator, replication can be turned off by setting
``replication.enabled = false`` in their respective configuration.

.. warning::

    While replication can be turned off to try to obtain performance gains, it must **not** be disabled when running multiple nodes
    for HA.

.. _caching_configuration:

**Caching Configuration.** In some cases, you might also want to tune caching configurations and either
reduce or increase them, depending on your situation. This can also be helpful if you need to reduce the memory
foot-print of Canton, which can be large, as the default cache configurations are tailored for high-throughput,
high-memory and small transaction sizes.

Generally, the caches that usually matter with respect to size are the contract caches and the in-memory
fan-out event buffer. You can tune these using the following configurations. The values depicted here are
the ones recommended for smaller memory-footprints and are therefore also helpful if you run into out-of-memory
issues:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/caching-configs.conf

.. _model_tuning:

Model Tuning
------------

How you write your Daml model has a large impact on the performance of your system. There are instances of good models running with 3000 ledger events/second (v2.7)
on a single participant node. A bad model will reach a fraction of that. Therefore, it is important to understand the connection between the model
and the performance implications. This section aims to give a few guidelines.

.. _model_tuning_views:

Reduce the Number of Views
~~~~~~~~~~~~~~~~~~~~~~~~~~
One key performance driver in Canton is the transaction structure resulting from the model. A Daml command is effectively
a "program" that computes a transaction structure. This transaction structure is then broken up by Canton into
pieces called "transaction views". The transaction views are then encrypted and sent to the participants who then confirm
each view. Each view requires cryptography and creates additional payload that needs to be processed and validated.

As a result, the performance of your system directly depends on how many views the transaction creates. The number of views
of a transaction is logged on the participant side as a DEBUG log message:

.. code::

    Computed transaction tree with total=27 for #root-nodes=8

If you submit a Ledger API command (which can have multiple Daml commands), then every Daml command will create one so-called root-node,
(two for the ``CreateAndExercise`` command). Each root node creates one view.

Just putting all Daml commands into a single batch command does not help, as you might still create lots of views.

Currently, Canton creates a view for every action node in the transaction tree if the
**participants that host their informees are not a subset of their parent view's informee participants**.
A view's informee participants are all the participants that must be informed about the view.
Therefore, to reduce the number of views, you should try to reduce either the number of times the set of informees grows
or make sure most of your actions share the same pool of participants. Alternatively, you can add informees to the
action nodes close to the root (e.g., by including choice observers) so that their children nodes
are aggregated in the initial view, since these children nodes target only a subset of those participants.

One concrete way to reduce the number of views being generated by a Daml model is to write batch commands and group the
operations on the ledger based on their informees' participants. In other words, you can create a single choice and
batch all the operations that share the same informee participant group.

You can also reduce the number of views being generated for the children of a node (i.e., parent-child views) when
creating your Daml model.
With the below example as reference, instead of sending a batch with 20 exercise commands `Foo`,
you can group your contracts by a set of stakeholders that cover the exercise's informee participants and send
one exercise command on a generator contract with the list of contracts that in turn exercises the Foo choice
per stakeholder group. This way you can perform these operations in a single view, instead of having them spawn multiple
different views.

For example, if the `doUpdate` below is called from a choice visible to the `owner` party alone, then whether the `efficient`
flag is set or not has a huge impact on the number of views created.
In the choice `Foo` of the `Example` contract, even though the participants to be informed are a subset of the
contract's informee participants they are not merged into a single view if this choice is called independently each time.
On the contrary, you will produce multiple root actions each belonging to their own view. Finally, aligning the choice observer of `Run`
to its template contract means that either the choice observer is the same in each case or the new observer is hosted by any of the contract's informee
participants.

.. code:: daml

   doUpdate : Bool -> Party -> Party -> [ContractId Example] -> Update ()

   doUpdate efficient owner obs batch = do
     if efficient then do
       batcher <- create BatchFoo with owner
       -- This only works out if obs is the same observer on all the exercised Example contracts
       batcher `exercise` Run with batch = batch; obs = obs
     else do
       -- Canton will create one view per exercise
       forA_ batch (`exercise` Foo)
     pure ()

   template Example with
       owner : Party
       obs: Party
     where
       signatory owner
       observer obs

       choice Foo : ()
         controller owner
         do
           return ()

   template BatchFoo with
       owner : Party
     where
       signatory owner

       choice Run : () with
           batch : [ContractId Example]
           obs : Party
         -- The observer here is a choice observer. Therefore, Canton will
         -- ship a single view with the top node of this choice if the observer of the
         -- choice aligns with the observer of the contract.
         observer obs
         controller owner
         do
           forA_ batch (`exercise` Foo)

As a rule, the number of views should depend on the number of groups of informee participants
you have in your batch choice, not the number of "batches" you process in parallel within one command.
An informee's participants group is formed by aggregating the participants that host each stakeholder, or in other
words, the set of participants that need to see a particular view.

The informees for the different type of transaction tree nodes are (also see :externalref:`da-model-projections`):
   * create: signatories, observers
   * consuming exercise: signatories, observers, stakeholders of the choice (controller, choice observers)
   * nonconsuming exercise: signatories, stakeholders of the choice (controller, choice observers), but not the observers of the contract
   * fetch: signatories + actors of the fetch, which are all stakholders which are in the authorization context that the fetch executed in
   * lookupByKey: only key maintainers

.. _model_tuning_events:

Reduce Ledger Events
~~~~~~~~~~~~~~~~~~~~
The best optimisation is always to just not do something. A ledger is meant to store relevant data and coordinate different
parties. If you use the ledger as processing queue, you run into performance issues, as each transaction view is cryptographically
secured and processed extensively to ensure privacy and integrity, which is unnecessary for intermediate steps. The rule
is: if the output of a transaction submitted by a party is immediately consumed by the same party, then you are using the
ledger as a processing queue. Instead, restructure your command such that the two steps happen as one.

Furthermore, each `create` creates a contract, causing data to be written to the ledger. Each `fetch` causes the interpreter to halt
interpretation, asking the ledger for a contract, which in the worst case requires a lookup in the database. As the interpretation
must happen sequentially, this means a one-by-one lookup of contracts, causing load and latency. Fetching data is important and
a key feature, but you should apply the same reasoning as you would for a database: A database lookup is expensive and should only be
done if necessary, ideally caching repetive computing results.

As an example, if you have a high throughput process that always resolves some data by a chain `A -> B -> C -> D` (four fetches), then
change your model to use a single cache contract `ABCD`, only fetch that contract and ensure that there is a process that whenever one
of the contracts changes, `ABCD` is updated.

Also, avoid unnecessary transient contracts, as they may cause additional views. `createAndExercise` is not a single command,
but translated to one create and an exercise. Use a `nonconsuming` choice instead, possibly with a choice observer if you need
to leave a trace on the ledger for audit purposes.

Generally, using `lookupKey` is also discouraged. If you use `lookupByKey` for on-ledger deduplication, the lookup requires a
database lookup, as the lookupByKey will resolve to `None` in most cases. Instead, use command deduplication
on the Ledger API. Second, the current `lookupByKey` will not be supported in a multi-synchronizer deployment, as the current semantic
only works on a single-synchronizer deployment and can not be translated 1:1 to multi-synchronizer.
