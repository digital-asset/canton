..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _optimize_achs:

Active Contracts Head Snapshot (ACHS)
=====================================

The Active Contracts Head Snapshot (ACHS) is an optional feature that maintains a continuously updated snapshot of
the currently active contracts. When enabled, the ACHS accelerates ``GetActiveContracts`` (ACS) queries by allowing
them to read directly from a pre-computed snapshot rather than scanning the full event log to reconstruct the active
contracts set.

ACHS is disabled by default.

Enabling ACHS
-------------

To enable ACHS, configure the ``achs-config`` block under the participant's indexer settings:

.. code-block:: none

    canton.participants.<participant>.parameters.ledger-api-server.indexer.achs-config {
        valid-at-distance-target = 1000000
        last-populated-distance-target = 500000
    }

Tuning Parameters
-----------------

valid-at-distance-target
^^^^^^^^^^^^^^^^^^^^^^^^

The ``valid-at-distance-target`` parameter controls how far behind the ledger end (in event sequential IDs) the
snapshot's validity point is maintained. Any ACS query targeting an offset before this validity point will not use
the ACHS and will fall back to the filter tables (original approach).

The value should be large enough to cover the typical ACS query offsets.
If the value is too small, long-running ACS
queries may observe the ACHS validity point moving past their requested offset (mid-stream), causing the stream to
fall back to the filter tables.
If the value is too large, the tail portion of the ACS (between the
ACHS validity point and the requested offset) must be resolved from the filter tables, making that last segment
more expensive.

last-populated-distance-target
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``last-populated-distance-target`` parameter controls how far behind the snapshot's validity point (in event
sequential IDs) contracts are populated into the ACHS. Since the validity point itself already trails ledger end by
``valid-at-distance-target``, the total distance from ledger end at which population occurs is
``valid-at-distance-target`` + ``last-populated-distance-target``. This additional lag limits short-lived
contracts stored in ACHS since any contract that is both created and archived within this window (from the snapshot's
validity point and ``valid-at-distance-target`` events before) will never be inserted into the snapshot.

Setting this value too low means short-lived contracts get written to the snapshot only to be immediately deleted,
wasting I/O. Setting it too high delays the point at which newly created (and still active) contracts appear in the
snapshot, which increases the cost of the remaining ACS tail (more data must be fetched from the filter tables to
cover the gap between the last populated point and the ACHS validity point).

Use the ``daml.participant.api.indexer.deactivation_distances`` histogram to see the distribution of contract
lifetimes. Ideally, ``last-populated-distance-target`` should be large enough so that most short-lived contracts
are already deactivated and thus not added to the snapshot.

Additional Parameters
^^^^^^^^^^^^^^^^^^^^^

The following parameters allow fine-tuning of ACHS maintenance:

- ``population-parallelism``: number of parallel threads for adding activations to the ACHS during normal operation.
- ``removal-parallelism``: number of parallel threads for removing deactivated activations from the ACHS during normal operation.
- ``aggregation-threshold``: minimum batch size (in event sequential IDs) before ACHS maintenance work is emitted. It controls the frequency of ACHS updates.
- ``init-parallelism``: number of parallel threads for ACHS population and removal during initialization.
- ``init-aggregation-threshold``: minimum batch size (in event sequential IDs) for ACHS maintenance during initialization.
- ``buffer-size``: size of the internal buffer between the indexer pipeline and the ACHS maintenance flow.

Monitoring
----------

Log Messages
^^^^^^^^^^^^

When the ACHS validity point is past the requested offset at query time, the following INFO-level message is logged:

.. code-block:: none

    ACHS for <filter> skipped since validAt (...) already surpassed requested activeAt (...)

When a long-running ACS query observes the ACHS validity point moving past its requested offset mid-stream, it falls
back to the filter tables and logs at INFO level:

.. code-block:: none

    ACHS stream for <filter> fell back to filter tables from (...) since validAt (...) surpassed activeAtEventSeqId (...)

Metrics
^^^^^^^

The following metrics are available to monitor ACHS behavior and guide tuning:

Under ``daml.participant.api.index``:

- ``achs_skips``: number of queries that skipped ACHS entirely (targeted before the validity point).
- ``achs_midstream_fallbacks``: number of queries that started on ACHS but fell back to filter tables mid-stream.

If either counter is increasing, consider raising ``valid-at-distance-target``.

Under ``daml.participant.api.indexer``:

- ``achs_valid_at``: the event sequential ID up to which the ACHS is currently valid. ACS queries whose requested offset maps to an event sequential ID at or after this value can be served directly from the ACHS.
- ``achs_last_populated``: the last event sequential ID for which activations were added to the ACHS.
- ``achs_last_removed``: the last event sequential ID for which deactivations were looked up and the corresponding activations were removed from the ACHS.
- ``deactivation_distances``: histogram of contract lifetimes (event sequential ID distance between activation and deactivation), useful for tuning ``last-populated-distance-target``.
