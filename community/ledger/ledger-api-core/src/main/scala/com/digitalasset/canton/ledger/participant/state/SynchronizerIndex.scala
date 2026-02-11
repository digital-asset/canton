// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.CantonTimestamp

/** SynchronizerIndex is a composite type describing the index for a synchronizer.
  *
  * SynchronizerIndex-es for the various type of updates can be created with the respective factory
  * function:
  *   - [[SynchronizerIndex.forRepairUpdate]] creates a SynchronizerIndex for repair update and
  *     setting the recordTime to the timestamp of the repair.
  *
  *   - [[SynchronizerIndex.forSequencedUpdate]] creates a SynchronizerIndex for sequenced update
  *     and setting the recordTime to the sequencer timestamp.
  *
  *   - [[SynchronizerIndex.forFloatingUpdate]] creates a SynchronizerIndex for floating update and
  *     setting the recordTime to the timestamp of the floating update. Floating event are for
  *     example: topology transactions emitted at effective time, timely rejection events.
  *
  * SynchronizerIndex-es can be aggregated with the [[max]] function which builds the maximum of
  * each parameter individually. This also means that all parameters can increase independently of
  * each other. For example
  *
  * {{{
  * SynchronizerIndex(
  *   repairIndex = Some(RepairIndex(T2, 13))
  *   sequencerIndex = Some(T1)
  *   recordTime = Some(T3)
  * )
  * }}}
  *
  * with timestamps T1 < T2 < T3 means:
  *   - the last sequenced event observed by the indexer is at T1
  *
  *   - after that record time moved ahead because of some floating events observed to T2
  *
  *   - then repair operations happened pushing the RepairCounter to 13
  *
  *   - and then additional floating events pushed the record time to T3
  *
  * @param repairIndex
  *   references the last known timestamp and repair counter for a repair update. The repair
  *   timestamp must be less or equal to the recordTime.
  * @param sequencerIndex
  *   references the last known timestamp for a sequenced update. This timestamp must be less or
  *   equal to the recordTime.
  * @param recordTime
  *   references the timestamp of the last update.
  */
final case class SynchronizerIndex(
    repairIndex: Option[RepairIndex],
    sequencerIndex: Option[CantonTimestamp],
    recordTime: CantonTimestamp,
) {
  def max(otherSynchronizerIndex: SynchronizerIndex): SynchronizerIndex =
    new SynchronizerIndex(
      repairIndex = repairIndex.iterator
        .++(otherSynchronizerIndex.repairIndex.iterator)
        .maxByOption(identity),
      sequencerIndex = sequencerIndex.iterator
        .++(otherSynchronizerIndex.sequencerIndex.iterator)
        .maxOption,
      recordTime = recordTime max otherSynchronizerIndex.recordTime,
    )

  override def toString: String =
    s"SynchronizerIndex(sequencerIndex=$sequencerIndex, repairIndex=$repairIndex, recordTime=$recordTime)"
}

object SynchronizerIndex {
  def forRepairUpdate(repairIndex: RepairIndex): SynchronizerIndex =
    SynchronizerIndex(
      Some(repairIndex),
      None,
      repairIndex.timestamp,
    )

  def forSequencedUpdate(sequencerTimestamp: CantonTimestamp): SynchronizerIndex =
    SynchronizerIndex(
      None,
      Some(sequencerTimestamp),
      sequencerTimestamp,
    )

  def forFloatingUpdate(recordTime: CantonTimestamp): SynchronizerIndex =
    SynchronizerIndex(
      None,
      None,
      recordTime,
    )
}

final case class RepairIndex(timestamp: CantonTimestamp, counter: RepairCounter)

object RepairIndex {
  implicit val orderingRepairIndex: Ordering[RepairIndex] =
    Ordering.by[RepairIndex, (CantonTimestamp, RepairCounter)](repairIndex =>
      (repairIndex.timestamp, repairIndex.counter)
    )
}
