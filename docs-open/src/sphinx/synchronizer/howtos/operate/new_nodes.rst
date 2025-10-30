..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

Add New Nodes
=============

.. _dynamically_adding_sequencers:

Add a new Sequencer to a distributed Synchronizer
-------------------------------------------------

You can either initialize Sequencers as part of the regular
:ref:`distributed synchronizer bootstrapping process<synchronizer-bootstrap>`, or dynamically add a new Sequencer
at a later point as described in this section.

The reverse procedure is documented in the :ref:`Sequencer decommissioning section<decommissioning-sequencers>`.

Database Sequencer
^^^^^^^^^^^^^^^^^^

The Database Sequencer is currently unsupported.

BFT Sequencer
^^^^^^^^^^^^^

#. Assuming that at least one existing Sequencer is accessible, :ref:`prepare a new Sequencer<synchronizer-bootstrap>` and make sure it's running.
#. Run the following `bootstrap` command using instance references of the new Sequencer, the existing Sequencer, and the owners of the current Synchronizer:

    .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/util/OnboardsNewSequencerNode.scala
       :language: scala
       :start-after: user-manual-entry-begin: DynamicallyOnboardBftSequencer
       :end-before: user-manual-entry-end: DynamicallyOnboardBftSequencer
       :dedent:

#. Set up new connections in either or both directions for all Sequencers using the following commands:

    .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/sequencer/bftordering/BftOrderingDynamicOnboardingIntegrationTest.scala
       :language: scala
       :start-after: user-manual-entry-begin: BftSequencerAddPeerEndpoint
       :end-before: user-manual-entry-end: BftSequencerAddPeerEndpoint
       :dedent:

    For the newly-onboarded Sequencer, the endpoints can be :ref:`configured as part of the initial network<sequencer-backend-bft-initial-peers>`.

#. Wait for the new Sequencer to get initialized:

    .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/util/OnboardsNewSequencerNode.scala
       :language: scala
       :start-after: user-manual-entry-begin: DynamicallyOnboardBftSequencer-wait-for-initialized
       :end-before: user-manual-entry-end: DynamicallyOnboardBftSequencer-wait-for-initialized
       :dedent:

At this point, other nodes should be able to connect to the new Sequencer. To avoid problems, the best practice is to wait at least for the "maximum decision duration"
(the sum of the `participant_response_timeout` and `mediator_reaction_timeout` dynamic synchronizer parameters with a default of 30 seconds each) before connecting
nodes to a newly-onboarded Sequencer.

If you encounter issues, refer to the :ref:`troubleshooting guide<synchronizer-troubleshoot-new-sequencer-onboarding>`.

.. todo::
    #. Link the reference documentation. <https://github.com/DACH-NY/canton/issues/25963>

For details on the necessary admin commands, check the reference documentation.

Use separate consoles
^^^^^^^^^^^^^^^^^^^^^

Similarly to :ref:`initializing a distributed synchronizer with separate consoles<synchronizer_bootstrapping_separate_consoles>`,
dynamically onboarding new Sequencers can be achieved in separate consoles as follows:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/sequencer/SynchronizerBootstrapWithMultipleConsolesAndSequencersIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: DynamicallyOnboardSequencerWithSeparateConsoles
   :end-before: user-manual-entry-end: DynamicallyOnboardSequencerWithSeparateConsoles
   :dedent:

.. _dynamically_adding_mediators:

Add a new Mediator to a distributed Synchronizer
------------------------------------------------

You can either initialize Mediators as part of the regular
:ref:`distributed synchronizer bootstrapping process<synchronizer-bootstrap>`, or dynamically add a new Mediator
at a later point as described in this section.

#. :ref:`Prepare a new Mediator<synchronizer-bootstrap>` node and make sure it's running.
#. Save the new Mediator's identity and load it to relevant Sequencers:

    .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/bftsynchronizer/MediatorOnboardingTest.scala
       :language: scala
       :start-after: user-manual-entry-begin: DynamicallyOnboardMediator-LoadIdentity
       :end-before: user-manual-entry-end: DynamicallyOnboardMediator-LoadIdentity
       :dedent:

#. Propose a new Mediator state with active Mediators including the newly-onboarding Mediator:

    .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/bftsynchronizer/MediatorOnboardingTest.scala
       :language: scala
       :start-after: user-manual-entry-begin: DynamicallyOnboardMediator-Propose
       :end-before: user-manual-entry-end: DynamicallyOnboardMediator-Propose
       :dedent:

#. Initialize the new Mediator:

    .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/bftsynchronizer/MediatorOnboardingTest.scala
       :language: scala
       :start-after: user-manual-entry-begin: DynamicallyOnboardMediator-Initialize
       :end-before: user-manual-entry-end: DynamicallyOnboardMediator-Initialize
       :dedent:

The reverse procedure is documented in the :ref:`Mediator decommissioning section<decommissioning-mediators>`.

.. todo::
    #. Link the reference documentation. <https://github.com/DACH-NY/canton/issues/25832>

For details on the necessary admin commands, check the reference documentation.
