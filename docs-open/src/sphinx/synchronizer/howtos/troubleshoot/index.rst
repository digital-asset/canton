..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Link to participant docs for general troubleshooting guidelines.

.. _synchronizer-troubleshoot:

Synchronizer Troubleshooting
============================

.. _synchronizer-troubleshoot-new-sequencer-onboarding:

Sequencer subscriptions fail for newly onboarded sequencers
-----------------------------------------------------------

Newly onboarded Sequencers only serve events more recent than the "onboarding snapshot" taken during the onboarding. In addition, some events may belong to transactions initiated before
a Sequencer was onboarded, but the Sequencer is not in a position to sign such events and replaces them with "tombstones". If a participant (or mediator)
connects to a newly onboarded Sequencer too soon and the subscription encounters a tombstone, the Sequencer subscription aborts with a `FAILED_PRECONDITION` error
specifying `InvalidCounter` or `SEQUENCER_TOMBSTONE_ENCOUNTERED`. If this occurs, the participant or mediator should connect to another Sequencer with a longer
history of sequenced events before switching to the newly onboarded Sequencer.
