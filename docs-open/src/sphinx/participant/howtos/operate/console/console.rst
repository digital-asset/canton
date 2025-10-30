..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    - Split between howto (setup a remote console) and explanation (node references, help)
    - Explain how to configure TLS with a remote console

.. _canton_console:

Canton Console
==============

Canton offers a console (REPL) where entities can be dynamically started and stopped, and a variety of administrative
or debugging commands can be run.

All console commands must be valid Scala (the console is built on `Ammonite <http://ammonite.io>`__ - a Scala-based
scripting and `REPL <https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop>`__ framework). Note that we also
define `a set of implicit type conversions <https://docs.daml.com/__VERSION__/canton/scaladoc/com/digitalasset/canton/console/ConsoleEnvironment$$Implicits.html>`__ to improve the console usability:
notably, whenever a console command requires a `SynchronizerAlias <https://docs.daml.com/__VERSION__/canton/scaladoc/com/digitalasset/canton/SynchronizerAlias.html>`__, `Fingerprint <https://docs.daml.com/__VERSION__/canton/scaladoc/com/digitalasset/canton/crypto/Fingerprint.html>`__ or `Identifier <https://docs.daml.com/__VERSION__/canton/scaladoc/com/digitalasset/canton/topology/Identifier.html>`__, you can instead also call it with a ``String`` which will be automatically converted to the correct type.

The ``examples/`` sub-directories contain some sample scripts, with the extension ``.canton``.

.. contents:: Contents
   :local:

Commands are organized by thematic groups. Some commands also need to be explicitly turned on via
configuration directives to be accessible.

Some operations are available on both types of nodes, whereas some operations are specific to either participant nodes
or synchronizers. For consistency, we organize the manual by node type, which means that some commands will
appear twice. However, the detailed explanations are only given within the participant documentation.

.. _canton_remote_console:

Remote Administration
---------------------

The Canton console works with both
local in-process nodes and remote nodes.
Once you've configured the network address, port, and authentication information,
you can use a single Canton console to administer all the nodes in your system.

As an example, you might have previously started a Canton instance in daemon mode using something like the following:

.. code-block:: bash

    ./bin/canton daemon -c <some config>

You can then execute commands against this up-and-running participant
using a ``remote-participant`` configuration such as:

.. literalinclude:: CANTON/community/app/src/pack/config/remote/participant.conf

Given a remote config file, start a local Canton console configured to execute commands on a remote Canton instance like this:

.. code-block:: bash

    ./bin/canton -c config/remote-participant1.conf

Additionally, you can use the remote configuration to run a script:

.. code-block:: bash

    ./bin/canton run <some-canton-script> -c config/remote-participant1.conf

Note that most Canton commands can be executed from a remote console.
However, a few commands can only be called
from the local console of the node itself.

Given a participant's config file, you can generate a skeleton remote config file
using the ``generate`` command:

.. code-block:: bash

    ./bin/canton generate remote-config -c participant1.conf

Depending on your network, you might need to manually edit the auto-generated configuration to adjust the hostname.
To access multiple remote nodes, consolidate the auto-generated configurations into a single remote configuration file
or include all configuration files on the command line:

.. code-block:: bash

    ./bin/canton -c participant1.conf,participant2.conf,mysynchronizer.conf

TLS and Authorization
^^^^^^^^^^^^^^^^^^^^^

For production use cases, in particular if the Admin API is not just bound to localhost, we recommend to enable
:ref:`TLS <tls-configuration>` with mutual authentication.

The remote console can be used in installations that utilize authorization, so long as it has a valid access token. This can be achieved by modifying the configuration or by adding
an option to the remote console's launch command as in the following snippet:

.. code-block:: bash

    ./bin/canton daemon \
       -c remote-participant1.conf \
       -C canton.remote-participants.<remote-participant-name>.token="<encoded-and-signed-access-token-as-string>" \
       --bootstrap <some-script>

The remote console uses the token in its interactions with the Ledger API of the target participant.
It also extracts the user ID from the token and uses it to populate the userId field
in the command submission and completion subscription requests. This affects the following console commands:

- ledger_api.commands.submit
- ledger_api.commands.submit_flat
- ledger_api.commands.submit_async
- ledger_api.completions.list
- ledger_api.completions.list_with_checkpoint
- ledger_api.completions.subscribe

.. todo:: `#22917: Fix broken ref <https://github.com/DACH-NY/canton/issues/22917>`_
    note:: If you want to know more about the authorization please read the following article
    about the ref:`authorization tokens <user-access-tokens>`

Node References
---------------

To issue the command on a particular node, you must refer to it via its reference, which is a Scala variable.
Named variables are created for all synchronizer entities and participants using their configured identifiers.
For example, the sample ``examples/01-simple-topology/simple-topology.conf`` configuration file references the
synchronizer ``mysynchronizer``, and participants ``participant1`` and ``participant2``.
These are available in the console as ``mysynchronizer``, ``participant1`` and ``participant2``.

The console also provides additional generic references that allow you to consult a list
of nodes by type. The generic node reference supports three subsets of each node type: local, remote
or all nodes of that type. For the participants, you can use:

.. code-block:: scala

    participants.local
    participants.remote
    participants.all

The generic node references can be used in a Scala syntactic way:

.. code-block:: scala

    participants.all.foreach(_.dars.upload("my.dar"))

but the participant references also support some :ref:`generic commands <participants-references>` for actions that often have to be performed for many
nodes at once, such as:

.. code-block:: scala

    participants.local.dars.upload("my.dar")

The available node references are:

<console-topic-marker: Generic Node References>


Help
----
Canton can be very helpful if you ask for help. Try to type

::

    help

or

::

    participant1.help()

to get an overview of the commands and command groups that exist. ``help()`` works on every level
(e.g. ``participant1.synchronizers.help()``) or can be used to search for particular functions (``help("list")``)
or to get detailed help explanation for each command (``participant1.parties.help("list")``).

.. _migrate_and_start_mode:

Lifecycle Operations
--------------------

These are supported by individual and sequences of synchronizers and participants.
If called on a sequence, operations will be called sequentially in the order of the sequence.
For example:

.. code-block:: bash

   nodes.local.start()

can be used to start all configured local synchronizers and participants.

If the node is running with database persistence, it will support the database migration command (``db.migrate``).
The migrations are performed automatically when the node is started for the first time.
However, new migrations added as part of new versions of the software must be by default run manually using the command.
In some rare cases, it may also be necessary to run ``db.repair_migration`` before running ``db.migrate`` - please
refer to the description of ``db.repair_migration`` for more details.
If desired, the database migrations can be performed also automatically by enabling the "migrate-and-start" mode
using the following configuration option:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/migrate-and-start.conf

Note that data continuity (and therefore database migration) is only guaranteed to work across minor and patch version updates.

The synchronizer, sequencer and mediator nodes might need extra setup to be fully functional.
Check :externalref:`synchronizer bootstrapping <synchronizer-bootstrap>` for more details.

Timeouts
--------

Console command timeouts can be configured using the respective console command timeout section in the configuration
file:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/console-timeouts.conf

The ``bounded`` argument is used for all commands that should finish once processing has completed, whereas the
``unbounded`` timeout is used for commands where we do not control the processing time. This is used in
particular for potentially very long-running commands.

Some commands have specific timeout arguments that can be passed explicitly as type ``NonNegativeDuration``. For convenience,
the console includes by default the implicits of ``scala.concurrent.duration._`` and an implicit conversion from
the Scala type ``scala.concurrent.duration.FiniteDuration`` to ``NonNegativeDuration``. As a result, you can use
`normal Scala expressions <https://www.scala-lang.org/api/2.12.4/scala/concurrent/duration/Duration.html>`_ and write
timeouts as

::

    participant1.health.ping(participant1, timeout = 10.seconds)

while the implicit conversion will take care of converting it to the right types.

Generally, there is no need to reconfigure the timeouts and we recommend using the safe default values.

.. _console-codegen:

Code-Generation in Console
--------------------------

The Daml SDK provides `code-generation utilities <https://docs.daml.com/tools/codegen.html>`__ which
create **Java** or **Scala** bindings for Daml models. These bindings are a convenient way to interact with
the ledger from the console in a typed fashion. The linked documentation explains how to create these
bindings using the ``daml`` command. The **Scala** bindings are not officially supported, so should not be used
for application development.

Once you have successfully built the bindings, you can then load the resulting ``jar`` into the Canton console using the
magic **Ammonite** import trick within console scripts:

.. code-block:: scala

    interp.load.cp(os.Path("codegen.jar", base = os.pwd))

    @ // the at triggers the compilation such that we can use the imports subsequently

    import ...

.. _administration_apis:

Canton Administration APIs
--------------------------

Canton provides the :ref:`console <canton_console>` as a builtin mode for administrative interaction. However, under the
hood, all administrative console actions are effected using the administration `gRPC <https://grpc.io/>`_ API. Therefore,
it is also possible to write your own administration application and connect it to the administration gRPC endpoints.

The ``protobuf/`` sub-directories in the release artifacts contain the gRPC underlying protocol buffers. In particular,
the administrative gRPC APIs are located within the ``admin`` sub-directories.

For example, the Ping Pong Service which implements a simple workflow to smoke-test a deployment is defined with the
protocol buffer ``*/protobuf/*/admin/*/ping_pong_service.proto`` (where ``*`` denotes intermediary directories).
This service is then used by the console command :ref:`health.ping <health.ping>`.

The protocol buffers are also available within the `repository <https://github.com/digital-asset/canton/tree/main/community>`__
following a similar sub-directory structure as mentioned.

