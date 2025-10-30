..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Review and update content.
    Link to participant HA docs for general configuration.
    Link to internal architecture/explanation of replication vs. multiple independent nodes.
    Mediator: cover both replicated node with shared DB as well as multiple mediators in a group.
    Sequencer: cover db sequencer HA using shared database. cover multiple sequencer nodes with BFT orderer.

.. _synchronizer-ha:

High Availability in Synchronizer
=================================

.. _ha_mediator:

Mediator
--------

The mediator service uses a hot standby mechanism with an arbitrary number of replicas.
During a mediator fail-over, all in-flight requests get purged.
As a result, these requests will timeout at the participants.
The applications need to retry the underlying commands.

Running a Stand-Alone Mediator Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A synchronizer may be statically configured with a single embedded mediator node or it may be configured to work with external mediators.
Once the synchronizer has been initialized further mediators can be added at runtime.

By default, a synchronizer will run an embedded mediator node itself.
This is useful in simple deployments where all synchronizer functionality can be co-located on a single host.
In a distributed setup where synchronizer services are operated over many machines,
you can instead configure a synchronizer manager node and bootstrap the synchronizer with mediator(s) running externally.

Mediator nodes can be defined in the same manner as Canton participants and synchronizers.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/external-mediators.conf
   :start-after: user-manual-entry-begin: ExternalMediatorNode
   :end-before: user-manual-entry-end: ExternalMediatorNode
   :dedent:

When the synchronizer starts it will automatically provide the embedded mediator information about the synchronizer.
External mediators have to be initialized using runtime administration in order to complete the synchronizer initialization.

HA Configuration
~~~~~~~~~~~~~~~~

HA mediator support is only available in the Daml Enterprise version of Canton and
only PostgreSQL and Oracle-based storage are supported for HA.

Mediator node replicas are configured in the Canton configuration file as
individual stand-alone mediator nodes with two required changes for each
mediator node replica:

- Using the same storage configuration to ensure access to the shared database.
- Set ``replication.enabled = true`` for each mediator node replica.

.. note::

    Starting from canton 2.4.0, mediator replication is enabled by default when using supported storage.

Only the active mediator node replica has to be initialized through the synchronizer
bootstrap commands. The passive replicas observe the initialization via the
shared database.

Further replicas can be started at runtime without any additional setup. They
remain passive until the current active mediator node replica fails.

.. _ha_sequencer:

Sequencer
---------

The database-based sequencer can be horizontally scaled and placed behind a load balancer to provide
high availability and performance improvements.

Deploy multiple sequencer nodes for the synchronizer with the following configuration:

 - All sequencer nodes share the same database so ensure that the storage configuration for each sequencer matches.
 - All sequencer nodes must be configured with `high-availability.enabled = true`.

.. note::

    Starting from Canton 2.4.0, sequencer high availability is enabled by default when using supported storage.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/external-ha-sequencers.conf
   :start-after: user-manual-entry-begin: SequencerHAConfig
   :end-before: user-manual-entry-end: SequencerHAConfig

The synchronizer node only supports embedded sequencers, so a distributed setup using a synchronizer manager node must then be
configured to use these sequencer nodes by pointing it at these external services.

Once configured the synchronizer must be bootstrapped with the new external sequencer using the
:ref:`bootstrap_synchronizer <synchronizer-bootstrap>` operational process.
These sequencers share a database so just use a single instance for bootstrapping and the replicas
will come online once the shared database has a sufficient state for starting.

As these nodes are likely running in separate processes you could run this command entirely externally using a remote
administration configuration.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/external-ha-sequencers-remote-admin.conf
   :start-after: user-manual-entry-begin: SequencerHARemoteConfig
   :end-before: user-manual-entry-end: SequencerHARemoteConfig

There are two methods available for exposing the horizontally scaled sequencer instances to participants.

.. _total_node_count:

Total node count
~~~~~~~~~~~~~~~~
The ``sequencer.high-availability.total-node-count`` parameter is used to divide up time among the database sequencers.
The parameter should not be changed once a set of sequencer nodes has been deployed. Because each message sequenced must
have a unique timestamp, a sequencer node will use timestamps `modulo` the ``total-node-count`` plus its own index to create timestamps that do not conflict with other sequencer nodes while sequencing the messages in a parallel
database insertion process. Canton uses microseconds, which yields a theoretical max throughput of 1 million messages
per second per synchronizer. Now, this theoretical throughput is divided equally among all sequencer nodes
(``total-node-count``). Therefore, if you set ``total-node-count`` too high, then a sequencer might not be able to
operate at the maximum theoretical throughput. We recommend keeping the default value of ``10``, as all above explanations
are only theoretical and we have not yet seen a database/hard disk that can handle the theoretical throughput.
Also note that a message might contain multiple events, such that we are talking about high numbers here.

External load balancer
~~~~~~~~~~~~~~~~~~~~~~

Using a load balancer is recommended when you have a http2+grpc supporting load balancer available, and can't/don't
want to expose details of the backend sequencers to clients.
An advanced deployment could also support elastically scaling the number of sequencers available and dynamically
reconfigure the load balancer for this updated set.

An example `HAProxy <http://www.haproxy.org/>`__ configuration for exposing GRPC services without TLS looks like::

  frontend domain_frontend
    bind 1234 proto h2
    default_backend domain_backend

  backend domain_backend
    option httpchk
    http-check connect
    http-check send meth GET uri /health
    balance roundrobin
    server sequencer1 sequencer1.local:1234 proto h2 check port 8080
    server sequencer2 sequencer2.local:1234 proto h2 check port 8080
    server sequencer3 sequencer3.local:1234 proto h2 check port 8080

Please note that for quick failover, you also need to add HTTP health checks, as
otherwise, you have to wait for the TCP timeout to occur before failover happens. The Public API of the sequencer
exposes the standard `GRPC health endpoints <https://github.com/grpc/grpc/blob/master/doc/health-checking.md>`__, but
these are currently not supported by HAProxy, hence you need to fall back on the HTTP/health endpoint.

.. _client-side-load-balancing:

Client-side load balancing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Using client-side load balancing is recommended where an external load-balancing service is unavailable (or lacks http2+grpc
support), and the set of sequencers is static and can be configured at the client.

To simply specify multiple sequencers use the ``synchronizers.connect_multi`` console command when registering/connecting to the synchronizer::

  myparticipant.synchronizers.connect_multi(
    "my_synchronizer_alias",
    Seq("https://sequencer1.example.com", "https://sequencer2.example.com", "https://sequencer3.example.com")
  )

See the :externalref:`sequencer connectivity documentation <synchronizer-connections>` for more details on how to add many sequencer URLs
when combined with other synchronizer connection options.
The synchronizer connection configuration can also be changed at runtime to add or replace configured sequencer connections.
Note the synchronizer will have to be disconnected and reconnected at the participant for the updated configuration to be used.
