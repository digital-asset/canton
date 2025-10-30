..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _sequencer-resource-limits:

Set sequencer resource limits
=============================

Protect against large requests
------------------------------

Max request size is a dynamic Synchronizer parameter. You update the `maxRequestSize` field when you
:ref:`change the dynamic Synchronizer parameters<change_dynamic_synchronizer_parameters>`.

Protect the Sequencer via rate limiting
---------------------------------------

Confirmation request max rate is a dynamic Synchronizer parameter. You update the `confirmationRequestsMaxRate` field when you
:ref:`change the dynamic Synchronizer parameters<change_dynamic_synchronizer_parameters>`.

Protect the Byzantine Fault Tolerance orderer from large requests
-----------------------------------------------------------------

When starting the Byzantine Fault Tolerance (BFT) orderer, you can provide limits for both `max-request-payload-bytes` how many
bytes one transaction can handle, and `max-request-in-batch` for how many transactions can be handled in a batch.

.. literalinclude:: CANTON/community/app/src/pack/examples/11-bft-sequencer/set-batch-size.conf
   :language: scala
   :start-after: user-manual-entry-begin: BftSequencerSetBatchSizeConfig
   :end-before: user-manual-entry-end: BftSequencerSetBatchSizeConfig
   :dedent:

.. note::

    Do note that `max-requests-in-batch` is a network wide parameter and should be the same for all orderers in the network.


Protect the Sequencer from too many acknowledgements
----------------------------------------------------

To limit the amount of acknowledgements on the network, the Sequencer can conflate acknowledgements that come from
the same member and are too close in time to each other. You can configure the window by setting the following value
in the config.

.. code-block:: none

    sequencers.sequencer1.acknowledgements-conflate-window = "1 minute"


Limit Sequencer submissions via traffic management
--------------------------------------------------

You can protect a Synchronizer from excessive traffic from its members by :ref:`enabling traffic management<sequencer-traffic>`.
