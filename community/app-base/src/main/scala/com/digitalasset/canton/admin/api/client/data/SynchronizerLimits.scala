// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.{
  AcsCommitmentLimits as AcsCommitmentLimitsInternal,
  SynchronizerLimits as SynchronizerLimitsInternal,
  TopologyLimits as TopologyLimitsInternal,
  TransactionProtocolLimits as TransactionProtocolLimitsInternal,
}
import io.scalaland.chimney.dsl.*

final case class SynchronizerLimits(
    transactionProtocolLimits: TransactionProtocolLimits,
    topologyLimits: TopologyLimits,
    acsCommitmentLimits: AcsCommitmentLimits,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[SynchronizerLimits] = SynchronizerLimits

  def toInternal: SynchronizerLimitsInternal = this.transformInto[SynchronizerLimitsInternal]
}

object SynchronizerLimits extends PrettyPrintingCompanion[SynchronizerLimits] {
  override protected val pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits),
    param("topology limits", _.topologyLimits),
    param("acs commitment limits", _.acsCommitmentLimits),
  )

  lazy val default: SynchronizerLimits =
    SynchronizerLimitsInternal.default.transformInto[SynchronizerLimits]
}

final case class AcsCommitmentLimits(
    maxCommitmentSizeBytes: PositiveInt,
    maxCommitmentSummarySizeBytes: PositiveInt,
    maxAddressedCounterparticipants: PositiveInt,
    maxUnsentDigests: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[AcsCommitmentLimits] = AcsCommitmentLimits

  def toInternal: AcsCommitmentLimitsInternal = this.transformInto[AcsCommitmentLimitsInternal]
}

object AcsCommitmentLimits extends PrettyPrintingCompanion[AcsCommitmentLimits] {
  override protected val pretty: Pretty[AcsCommitmentLimits] = prettyOfClass(
    param("max commitment size bytes", _.maxCommitmentSizeBytes),
    param("max commitment summary size bytes", _.maxCommitmentSummarySizeBytes),
    param("max addressed counterparticipants", _.maxAddressedCounterparticipants),
    param("max unsent digests", _.maxUnsentDigests),
  )

  lazy val default: AcsCommitmentLimits =
    AcsCommitmentLimitsInternal.default.transformInto[AcsCommitmentLimits]
}

final case class TransactionProtocolLimits(
    maxActAs: PositiveInt,
    maxEnvelopes: PositiveInt,
    maxRecipientsPerBatch: PositiveInt,
    maxRecipientsTrees: PositiveInt,
    maxRecipientsPerRecipientsTreeLevel: PositiveInt,
    maxChildrenPerRecipientsTreeLevel: PositiveInt,
    maxRecipientsPerEnvelope: PositiveInt,
    maxRecipientsTreeDepth: PositiveInt,
    maxTransactionRootViews: PositiveInt,
    maxTransactionSubViews: PositiveInt,
    maxTransactionTreeDepth: PositiveInt,
    maxSignaturesPerEnvelope: PositiveInt,
    maxQuorumSize: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[TransactionProtocolLimits] =
    TransactionProtocolLimits

  def toInternal: TransactionProtocolLimitsInternal =
    this.transformInto[TransactionProtocolLimitsInternal]
}

object TransactionProtocolLimits extends PrettyPrintingCompanion[TransactionProtocolLimits] {
  override protected val pretty: Pretty[TransactionProtocolLimits] = prettyOfClass(
    param("max actAs", _.maxActAs),
    param("max envelopes", _.maxEnvelopes),
    param("max recipients trees", _.maxRecipientsTrees),
    param("max recipients per batch", _.maxRecipientsPerBatch),
    param("max recipients per recipients tree level", _.maxRecipientsPerRecipientsTreeLevel),
    param("max children per recipients tree level", _.maxChildrenPerRecipientsTreeLevel),
    param("max recipients per envelope", _.maxRecipientsPerEnvelope),
    param("max recipients tree depth", _.maxRecipientsTreeDepth),
    param("max transaction root views", _.maxTransactionRootViews),
    param("max transaction sub views", _.maxTransactionSubViews),
    param("max transaction tree depth", _.maxTransactionTreeDepth),
    param("max signatures per envelope", _.maxSignaturesPerEnvelope),
    param("max quorum size", _.maxTransactionTreeDepth),
  )

  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimitsInternal.default.transformInto[TransactionProtocolLimits]
}

final case class TopologyLimits(
    maxSigningKeysPerEntity: PositiveInt,
    maxOwnersPerDecentralizedNamespace: PositiveInt,
    maxHostingParticipantsPerParty: PositiveInt,
    maxVettedPackagesPerParticipant: PositiveInt,
    maxSequencersPerSynchronizer: PositiveInt,
    maxMediatorsPerGroup: PositiveInt,
    maxSignaturesPerTransaction: PositiveInt,
    maxTransactionHashesPerMultiSignature: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[TopologyLimits] = TopologyLimits

  def toInternal: TopologyLimitsInternal = this.transformInto[TopologyLimitsInternal]
}

object TopologyLimits extends PrettyPrintingCompanion[TopologyLimits] {
  override protected val pretty: Pretty[TopologyLimits] = prettyOfClass(
    param("max signing keys per entity", _.maxSigningKeysPerEntity),
    param("max owners per decentralized namespace", _.maxOwnersPerDecentralizedNamespace),
    param("max hosting participants per party", _.maxHostingParticipantsPerParty),
    param("max vetted packages per participant", _.maxVettedPackagesPerParticipant),
    param("max sequencers per synchronizer", _.maxSequencersPerSynchronizer),
    param("max mediators per group", _.maxMediatorsPerGroup),
    param("max signatures per transaction", _.maxSignaturesPerTransaction),
    param("max transaction hashes per multi signature", _.maxTransactionHashesPerMultiSignature),
  )

  lazy val default: TopologyLimits =
    TopologyLimitsInternal.default.transformInto[TopologyLimits]
}
