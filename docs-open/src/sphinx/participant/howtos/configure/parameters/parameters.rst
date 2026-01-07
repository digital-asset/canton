..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _parameters-configuration:

Configure Canton parameters
===========================

Participant Node protocol version
---------------------------------

On Participant Node connection to a Sequencer Node there is a handshake between the nodes to ensure that the Synchronizer protocol version is compatible. By default a Participant Node can connect to a Synchronizer running any stable protocol version.

Ensure a minimum protocol version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Setting a minimum protocol version ensures that the Participant Node can only connect to a Synchronizer running a protocol version at least as high as the configured minimum. Use this to ensure a Participant Node cannot connect to a Synchronizer running an protocol version known to have an security exploit or that doesn't support a required feature.

For example to ensure that the Synchronizer protocol version is at least version **33** set the ``minimum-protocol-version`` Participant Node parameter:

.. code-block::

    canton.participants.participant1.parameters.minimum-protocol-version=33


Enable early access protocol features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    The use of early access is intended for non-production environments and is not covered by support

Enable alpha protocol versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To allow a Participant Node to connect to a Synchronizer running an alpha protocol version, set the ``alpha-version-support`` parameter to ``true``. As this is an experimental feature, it's necessary to also set the ``non-standard-config`` parameter to ``true``:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/alpha-version-support.conf


Enable beta protocol versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To allow a Participant Node to connect to a Synchronizer running an beta protocol version, set the ``beta-version-support`` parameter to ``true``. As this is an experimental feature, it's necessary to also set the ``non-standard-config`` parameter to ``true``:

.. code-block::

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/beta-version-support.conf


Detect when Canton has started
------------------------------

When running Canton inside a test environment, it can be useful to detect when Canton has started.

To instruct Canton to write a file containing details of the running ports when initialized, configure a ports-file:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ports-file.conf

On startup, Canton writes to the ports-file the values of the ports its services are running on:

.. code-block:: json

    {
      "participant1" : {
        "ledgerApi" : 5021,
        "adminApi" : 5022,
        "jsonApi" : null
      }
    }


Enable preview commands
-----------------------

Canton console *preview commands* are commands that are not considered stable and are not listed in command group help requests.

To enable preview commands, set the ``enable-preview-commands`` parameter to ``yes`` in the configuration file. Preview commands are now listed in command group help requests and may be used.

.. warning::

    Preview commands are not guaranteed to be stable and may change in future releases.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/preview-commands.conf

