..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _dynamic-sync-params:

Manage dynamic Synchronizer parameters
======================================


.. _dynamic_synchronizer_parameters:

In addition to the :externalref:`static Synchronizer parameters<parameters-configuration>` that you specify during
:ref:`Synchronizer bootstrap<synchronizer-bootstrap>`, you can change some parameters at runtime (
while the Synchronizer is running); these are referred to as `dynamic synchronizer parameters`. When the Synchronizer is bootstrapped,
the default values are used for the dynamic Synchronizer parameters.

Get dynamic Synchronizer parameters
-----------------------------------

You can get the current parameters on a Synchronizer you are connected to using the following command:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/dynamicsynchronizerparameters/SynchronizerParametersChangeIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin:-begin: GetDynamicSynchronizerParameters
   :end-before: user-manual-entry-begin:-end: GetDynamicSynchronizerParameters
   :dedent:

Change dynamic Synchronizer parameters
--------------------------------------

.. _change_dynamic_synchronizer_parameters:

You can set several dynamic parameters at the same time:

.. todo::
    #. `Link to reference with all dynamic synchronizer parameters <https://github.com/DACH-NY/canton/issues/25806>`_

.. todo:: <https://github.com/DACH-NY/canton/issues/25684>
    #. Explain the concept of synchronizer owner and that for changes to become affective one needs a threshold of them to submit the same proposal

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/dynamicsynchronizerparameters/SynchronizerParametersChangeIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin:-begin: SetDynamicSynchronizerParameters
   :end-before: user-manual-entry-begin:-end: SetDynamicSynchronizerParameters
   :dedent:

.. note::
    When increasing `max request size`, the sequencer nodes need to be restarted
    for the new value to be taken into account.

Recover from too-small *max request size*
-----------------------------------------

`MaxRequestSize` is a dynamic parameter. This parameter configures both the gRPC channel size on the Sequencer node
and the maximum size that a Sequencer client is allowed to transfer.

If the parameter is set to a very small value (roughly under `30kb`), Canton can crash because all messages are rejected by the sequencer client or
by the sequencer node. This cannot be corrected by setting a higher value within the console, because this change request needs to be sent via the sequencer and will
also be rejected.

To recover from this crash, you need to configure `override-max-request-size` on both the Sequencer node and all Sequencer clients.

This means you need to modify both the Synchronizer and the Participant Node configuration as follows:

.. code-block:: none

    mediators {
      mediator1 {
        sequencer-client.override-max-request-size = 30000
      }
    }
    participants {
      participant1 {
        sequencer-client.override-max-request-size = 30000
      }
      participant2 {
        sequencer-client.override-max-request-size = 30000
      }
    }
    mediators {
      mediator1 {
        sequencer-client.override-max-request-size = 30000
      }
    }
    sequencers {
      sequencer1 {
        # overrides the maxRequestSize in bytes on the sequencer node
        public-api.override-max-request-size = 30000
      }
    }

After the configuration is modified, disconnect all the Participant Nodes from the Synchronizer and then restart all nodes.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/MaxRequestSizeCrashIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: StopCanton
   :end-before: user-manual-entry-end: StopCanton
   :dedent:

Then perform the restart:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/MaxRequestSizeCrashIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: RestartCanton
   :end-before: user-manual-entry-end: RestartCanton
   :dedent:

Once Canton has recovered, use the admin command to set the `maxRequestSize` value, then delete the added configuration
in the previous step, and finally perform the restart again.
