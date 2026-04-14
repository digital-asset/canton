// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{PositiveDouble, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters
import io.scalaland.chimney.dsl.*

/** Async block sequencer writer control parameters
  *
  * @param trafficBatchSize
  *   the maximum number of traffic events to batch in a single write
  * @param aggregationBatchSize
  *   the maximum number of inflight aggregations to batch in a single write
  * @param blockInfoBatchSize
  *   the maximum number of block info updates to batch in a single write
  */
final case class AsyncWriterConfig(
    trafficBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    aggregationBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    blockInfoBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
) {
  def toParameters: AsyncWriterParameters = this.transformInto[AsyncWriterParameters]
}

/** Various parameters for non-standard sequencer settings
  *
  * @param alphaVersionSupport
  *   if true, then dev version will be turned on, but we will brick this sequencer node if it is
  *   used for production.
  * @param dontWarnOnDeprecatedPV
  *   if true, then this sequencer will not emit a warning when configured to use protocol version
  *   2.0.0.
  * @param maxConfirmationRequestsBurstFactor
  *   how forgiving the rate limit is in case of bursts (so rate limit starts after observing an
  *   initial burst of factor * max_rate commands)
  * @param asyncWriter
  *   controls the async writer
  * @param producePostOrderingTopologyTicks
  *   temporary flag to enable topology ticks produced post-ordering by sequencers (feature in
  *   development)
  * @param lsuRepair
  *   Config values that are used in disaster recovery scenarios after LSU. Those values SHOULD be
  *   set only in very specific cases and MUST be synchronized across sequencers.
  */
final case class SequencerNodeParameterConfig(
    override val alphaVersionSupport: Boolean = false,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    maxConfirmationRequestsBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.5),
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val watchdog: Option[WatchdogConfig] = None,
    unsafeSequencerChannelSupport: Boolean = false,
    asyncWriter: AsyncWriterConfig = AsyncWriterConfig(),
    timeAdvancingTopology: TimeAdvancingTopologyConfig = TimeAdvancingTopologyConfig(),
    // TODO(#30769) remove this flag once the feature is complete
    producePostOrderingTopologyTicks: Boolean = true,
    lsuRepair: LsuRepair = LsuRepair(),
) extends ProtocolConfig
    with LocalNodeParametersConfig

/** @param lsuSequencingBoundsOverride
  *   Override the LSU sequencing bounds computed from the topology store. Should be used ONLY in a
  *   disaster recovery scenario (roll forward) and the value MUST be identical across sequencer
  *   nodes.
  * @param globalMaxSequencingTimeExclusive
  *   Allows to specify an upper bound on the sequencing times: any message with sequencing time
  *   strictly greater to this value will not be delivered. Important notes:
  *   - SHOULD be set only in disaster recovery scenarios.
  *   - MUST be the same value in all sequencers of a synchronizer.
  */
final case class LsuRepair(
    lsuSequencingBoundsOverride: Option[LsuSequencingBoundsOverride] = None,
    globalMaxSequencingTimeExclusive: Option[CantonTimestamp] = None,
)

/** Used to override values usually derived from the LSU announcement. MUST be used only for roll
  * forward in DR scenarios.
  *
  * @param lowerBoundSequencingTimeExclusive
  *   Defined as the highest effective time of the topology store on the broken synchronizer.
  * @param upgradeTime
  *   The value should be chosen as follows:
  *   - At least lowerBoundSequencingTimeExclusive
  *   - At least the latest sequencing time on the broken synchronizer
  *   - At least the latest effective time of the latest non-rejected topology transaction on the
  *     broken synchronizer.
  *   - Not too far in the future, as it acts as a strict lower bound on the new synchronizer.
  */
final case class LsuSequencingBoundsOverride(
    lowerBoundSequencingTimeExclusive: CantonTimestamp,
    upgradeTime: CantonTimestamp,
)
