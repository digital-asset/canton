// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.{v31 as protoV31, v32 as protoV32, v33 as protoV33}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class SynchronizerLimits(
    transactionProtocolLimits: TransactionProtocolLimits,
    topologyLimits: TopologyLimits,
    acsCommitmentLimits: AcsCommitmentLimits,
    bftOrdererLimits: BftOrdererLimits,
    maxSequencersPerSynchronizer: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[SynchronizerLimits] = SynchronizerLimits

  /** Narrow views for components that need limits shared across `SynchronizerLimits`. */
  def topologyLimitsView: TopologyLimitsView =
    TopologyLimitsView(topologyLimits, maxSequencersPerSynchronizer)

  def bftOrdererLimitsView: BftOrdererLimitsView =
    BftOrdererLimitsView(bftOrdererLimits, maxSequencersPerSynchronizer)

  // v31 has no topology limits, so they fall back to the default on the way back in
  def toProtoV31: protoV31.SynchronizerLimits = protoV31.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV31)
  )

  // v31 and v32 have no BFT orderer limits or max sequencers per synchronizer, so they fall back to
  // the default on the way back in
  def toProtoV32: protoV32.SynchronizerLimits = protoV32.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV32),
    topologyLimits = Some(topologyLimits.toProtoV32),
    acsCommitmentLimits = Some(acsCommitmentLimits.toProtoV32),
  )

  def toProtoV33: protoV33.SynchronizerLimits = protoV33.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV33),
    topologyLimits = Some(topologyLimits.toProtoV33),
    acsCommitmentLimits = Some(acsCommitmentLimits.toProtoV33),
    bftOrdererLimits = Some(bftOrdererLimits.toProtoV33),
    maxSequencersPerSynchronizer = maxSequencersPerSynchronizer.value,
  )
}

object SynchronizerLimits extends PrettyPrintingCompanion[SynchronizerLimits] {

  override protected val pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits),
    param("topology limits", _.topologyLimits),
    param("acs commitment limits", _.acsCommitmentLimits),
    param("bft orderer limits", _.bftOrdererLimits),
    param("max sequencers per synchronizer", _.maxSequencersPerSynchronizer),
  )

  /** Default upper bound on the number of sequencers in a synchronizer, used where the value is not
    * serialized (proto versions before v33).
    */
  val DefaultMaxSequencersPerSynchronizer: PositiveInt = PositiveInt.tryCreate(1000)

  /** The same on every protocol version: each limit check states the protocol version it applies
    * from through its ValidationParam. Applied where no synchronizer can be asked for its own
    * limits, such as the authorized store and the admin API.
    */
  lazy val default: SynchronizerLimits =
    SynchronizerLimits(
      transactionProtocolLimits = TransactionProtocolLimits.default,
      topologyLimits = TopologyLimits.default,
      acsCommitmentLimits = AcsCommitmentLimits.default,
      bftOrdererLimits = BftOrdererLimits.default,
      maxSequencersPerSynchronizer = DefaultMaxSequencersPerSynchronizer,
    )

  def fromProtoV31(
      synchronizerLimitsP: protoV31.SynchronizerLimits
  ): ParsingResult[SynchronizerLimits] = {
    val protoV31.SynchronizerLimits(transactionProtocolLimitsP) = synchronizerLimitsP

    for {
      transactionProtocolLimits <- ProtoConverter
        .required("transaction_protocol_limits", transactionProtocolLimitsP)
        .flatMap(TransactionProtocolLimits.fromProtoV31)
    } yield SynchronizerLimits(
      transactionProtocolLimits,
      TopologyLimits.default,
      AcsCommitmentLimits.default,
      BftOrdererLimits.default,
      DefaultMaxSequencersPerSynchronizer,
    )
  }

  def fromProtoV32(
      synchronizerLimitsP: protoV32.SynchronizerLimits
  ): ParsingResult[SynchronizerLimits] = {
    val protoV32.SynchronizerLimits(
      transactionProtocolLimitsP,
      topologyLimitsP,
      acsCommitmentLimitsP,
    ) =
      synchronizerLimitsP

    for {
      transactionProtocolLimits <- ProtoConverter
        .required("transaction_protocol_limits", transactionProtocolLimitsP)
        .flatMap(TransactionProtocolLimits.fromProtoV32)
      topologyLimits <- ProtoConverter
        .required("topology_limits", topologyLimitsP)
        .flatMap(TopologyLimits.fromProtoV32)
      acsCommitmentLimits <- ProtoConverter
        .required("acs_commitment_limits", acsCommitmentLimitsP)
        .flatMap(AcsCommitmentLimits.fromProtoV32)
    } yield SynchronizerLimits(
      transactionProtocolLimits,
      topologyLimits,
      acsCommitmentLimits,
      BftOrdererLimits.default,
      DefaultMaxSequencersPerSynchronizer,
    )
  }

  def fromProtoV33(
      synchronizerLimitsP: protoV33.SynchronizerLimits
  ): ParsingResult[SynchronizerLimits] = {
    val protoV33.SynchronizerLimits(
      transactionProtocolLimitsP,
      topologyLimitsP,
      acsCommitmentLimitsP,
      bftOrdererLimitsP,
      maxSequencersPerSynchronizerP,
    ) =
      synchronizerLimitsP

    for {
      transactionProtocolLimits <- ProtoConverter
        .required("transaction_protocol_limits", transactionProtocolLimitsP)
        .flatMap(TransactionProtocolLimits.fromProtoV33)
      topologyLimits <- ProtoConverter
        .required("topology_limits", topologyLimitsP)
        .flatMap(TopologyLimits.fromProtoV33)
      acsCommitmentLimits <- ProtoConverter
        .required("acs_commitment_limits", acsCommitmentLimitsP)
        .flatMap(AcsCommitmentLimits.fromProtoV33)
      bftOrdererLimits <- ProtoConverter
        .required("bft_orderer_limits", bftOrdererLimitsP)
        .flatMap(BftOrdererLimits.fromProtoV33)
      maxSequencersPerSynchronizer <- ProtoConverter.parsePositiveInt(
        "max_sequencers_per_synchronizer",
        maxSequencersPerSynchronizerP,
      )
    } yield SynchronizerLimits(
      transactionProtocolLimits,
      topologyLimits,
      acsCommitmentLimits,
      bftOrdererLimits,
      maxSequencersPerSynchronizer,
    )
  }
}

/** Size limits on collections within topology transactions.
  *
  * @param maxSigningKeysPerEntity
  *   the largest number of signing keys an entity can be mapped to, where an entity is a member in
  *   an [[com.digitalasset.canton.topology.transaction.OwnerToKeyMapping]] or a party in a
  *   [[com.digitalasset.canton.topology.transaction.PartyToKeyMapping]]
  * @param maxOwnersPerDecentralizedNamespace
  *   the largest number of owners of a
  *   [[com.digitalasset.canton.topology.transaction.DecentralizedNamespaceDefinition]]
  * @param maxHostingParticipantsPerParty
  *   the largest number of participants hosting a party in a
  *   [[com.digitalasset.canton.topology.transaction.PartyToParticipant]]
  * @param maxVettedPackagesPerParticipant
  *   the largest number of packages a participant can vet in a
  *   [[com.digitalasset.canton.topology.transaction.VettedPackages]]
  * @param maxMediatorsPerGroup
  *   the largest number of active and observer mediators in a
  *   [[com.digitalasset.canton.topology.transaction.MediatorSynchronizerState]]
  * @param maxSignaturesPerTransaction
  *   the largest number of signatures, single and multi transaction combined, on a
  *   [[com.digitalasset.canton.topology.transaction.SignedTopologyTransaction]]
  * @param maxTransactionHashesPerMultiSignature
  *   the largest number of transaction hashes a
  *   [[com.digitalasset.canton.topology.transaction.MultiTransactionSignature]] covers
  * @param maxTransactionsPerBroadcast
  *   the largest number of signed topology transactions in a
  *   [[com.digitalasset.canton.topology.transaction.SignedTopologyTransactions]], such as a
  *   topology transactions broadcast
  * @param maxEndpointsPerSequencerConnection
  *   the largest number of endpoints in the connection of a
  *   [[com.digitalasset.canton.topology.transaction.LsuSequencerConnectionSuccessor]]
  */
final case class TopologyLimits(
    maxSigningKeysPerEntity: PositiveInt,
    maxOwnersPerDecentralizedNamespace: PositiveInt,
    maxHostingParticipantsPerParty: PositiveInt,
    maxVettedPackagesPerParticipant: PositiveInt,
    maxMediatorsPerGroup: PositiveInt,
    maxSignaturesPerTransaction: PositiveInt,
    maxTransactionHashesPerMultiSignature: PositiveInt,
    maxTransactionsPerBroadcast: PositiveInt,
    maxEndpointsPerSequencerConnection: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[TopologyLimits] = TopologyLimits

  // v32 has no max vetted packages, mediators, signatures, transaction hashes,
  // transactions per broadcast or endpoints, so they fall back to the default on the way back in
  def toProtoV32: protoV32.TopologyLimits = protoV32.TopologyLimits(
    maxSigningKeysPerEntity = maxSigningKeysPerEntity.value,
    maxOwnersPerDecentralizedNamespace = maxOwnersPerDecentralizedNamespace.value,
    maxHostingParticipantsPerParty = maxHostingParticipantsPerParty.value,
  )

  def toProtoV33: protoV33.TopologyLimits = protoV33.TopologyLimits(
    maxSigningKeysPerEntity = maxSigningKeysPerEntity.value,
    maxOwnersPerDecentralizedNamespace = maxOwnersPerDecentralizedNamespace.value,
    maxHostingParticipantsPerParty = maxHostingParticipantsPerParty.value,
    maxVettedPackagesPerParticipant = maxVettedPackagesPerParticipant.value,
    maxMediatorsPerGroup = maxMediatorsPerGroup.value,
    maxSignaturesPerTransaction = maxSignaturesPerTransaction.value,
    maxTransactionHashesPerMultiSignature = maxTransactionHashesPerMultiSignature.value,
    maxTransactionsPerBroadcast = maxTransactionsPerBroadcast.value,
    maxEndpointsPerSequencerConnection = maxEndpointsPerSequencerConnection.value,
  )
}

/** The topology limits together with the limits shared across [[SynchronizerLimits]]. */
final case class TopologyLimitsView(
    limits: TopologyLimits,
    maxSequencersPerSynchronizer: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[TopologyLimitsView] = TopologyLimitsView

  val maxSigningKeysPerEntity: PositiveInt = limits.maxSigningKeysPerEntity
  val maxOwnersPerDecentralizedNamespace: PositiveInt = limits.maxOwnersPerDecentralizedNamespace
  val maxHostingParticipantsPerParty: PositiveInt = limits.maxHostingParticipantsPerParty
  val maxVettedPackagesPerParticipant: PositiveInt = limits.maxVettedPackagesPerParticipant
  val maxMediatorsPerGroup: PositiveInt = limits.maxMediatorsPerGroup
  val maxSignaturesPerTransaction: PositiveInt = limits.maxSignaturesPerTransaction
  val maxTransactionHashesPerMultiSignature: PositiveInt =
    limits.maxTransactionHashesPerMultiSignature
  val maxTransactionsPerBroadcast: PositiveInt = limits.maxTransactionsPerBroadcast
  val maxEndpointsPerSequencerConnection: PositiveInt = limits.maxEndpointsPerSequencerConnection
}

object TopologyLimitsView extends PrettyPrintingCompanion[TopologyLimitsView] {
  override protected val pretty: Pretty[TopologyLimitsView] = prettyOfClass(
    param("limits", _.limits),
    param("max sequencers per synchronizer", _.maxSequencersPerSynchronizer),
  )

  lazy val default: TopologyLimitsView =
    TopologyLimitsView(
      TopologyLimits.default,
      SynchronizerLimits.DefaultMaxSequencersPerSynchronizer,
    )
}

object TopologyLimits extends PrettyPrintingCompanion[TopologyLimits] {

  override protected val pretty: Pretty[TopologyLimits] = prettyOfClass(
    param("max signing keys per entity", _.maxSigningKeysPerEntity),
    param("max owners per decentralized namespace", _.maxOwnersPerDecentralizedNamespace),
    param("max hosting participants per party", _.maxHostingParticipantsPerParty),
    param("max vetted packages per participant", _.maxVettedPackagesPerParticipant),
    param("max mediators per group", _.maxMediatorsPerGroup),
    param("max signatures per transaction", _.maxSignaturesPerTransaction),
    param("max transaction hashes per multi signature", _.maxTransactionHashesPerMultiSignature),
    param("max transactions per broadcast", _.maxTransactionsPerBroadcast),
    param("max endpoints per sequencer connection", _.maxEndpointsPerSequencerConnection),
  )

  lazy val default: TopologyLimits = TopologyLimits(
    maxSigningKeysPerEntity = PositiveInt.tryCreate(20),
    maxOwnersPerDecentralizedNamespace = PositiveInt.tryCreate(100),
    maxHostingParticipantsPerParty = PositiveInt.tryCreate(100),
    maxVettedPackagesPerParticipant = PositiveInt.tryCreate(10_000),
    maxMediatorsPerGroup = PositiveInt.tryCreate(100),
    maxSignaturesPerTransaction = PositiveInt.tryCreate(1000),
    maxTransactionHashesPerMultiSignature = PositiveInt.tryCreate(1000),
    maxTransactionsPerBroadcast = PositiveInt.tryCreate(1000),
    maxEndpointsPerSequencerConnection = PositiveInt.tryCreate(10),
  )

  def fromProtoV32(
      topologyLimitsP: protoV32.TopologyLimits
  ): ParsingResult[TopologyLimits] = {
    val protoV32.TopologyLimits(
      maxSigningKeysPerEntityP,
      maxOwnersPerDecentralizedNamespaceP,
      maxHostingParticipantsPerPartyP,
    ) = topologyLimitsP

    for {
      maxSigningKeysPerEntity <- ProtoConverter.parsePositiveInt(
        "max_signing_keys_per_entity",
        maxSigningKeysPerEntityP,
      )
      maxOwnersPerDecentralizedNamespace <- ProtoConverter.parsePositiveInt(
        "max_owners_per_decentralized_namespace",
        maxOwnersPerDecentralizedNamespaceP,
      )
      maxHostingParticipantsPerParty <- ProtoConverter.parsePositiveInt(
        "max_hosting_participants_per_party",
        maxHostingParticipantsPerPartyP,
      )
    } yield TopologyLimits(
      maxSigningKeysPerEntity,
      maxOwnersPerDecentralizedNamespace,
      maxHostingParticipantsPerParty,
      default.maxVettedPackagesPerParticipant,
      default.maxMediatorsPerGroup,
      default.maxSignaturesPerTransaction,
      default.maxTransactionHashesPerMultiSignature,
      default.maxTransactionsPerBroadcast,
      default.maxEndpointsPerSequencerConnection,
    )
  }

  def fromProtoV33(
      topologyLimitsP: protoV33.TopologyLimits
  ): ParsingResult[TopologyLimits] = {
    val protoV33.TopologyLimits(
      maxSigningKeysPerEntityP,
      maxOwnersPerDecentralizedNamespaceP,
      maxHostingParticipantsPerPartyP,
      maxVettedPackagesPerParticipantP,
      maxMediatorsPerGroupP,
      maxSignaturesPerTransactionP,
      maxTransactionHashesPerMultiSignatureP,
      maxTransactionsPerBroadcastP,
      maxEndpointsPerSequencerConnectionP,
    ) = topologyLimitsP

    for {
      maxSigningKeysPerEntity <- ProtoConverter.parsePositiveInt(
        "max_signing_keys_per_entity",
        maxSigningKeysPerEntityP,
      )
      maxOwnersPerDecentralizedNamespace <- ProtoConverter.parsePositiveInt(
        "max_owners_per_decentralized_namespace",
        maxOwnersPerDecentralizedNamespaceP,
      )
      maxHostingParticipantsPerParty <- ProtoConverter.parsePositiveInt(
        "max_hosting_participants_per_party",
        maxHostingParticipantsPerPartyP,
      )
      maxVettedPackagesPerParticipant <- ProtoConverter.parsePositiveInt(
        "max_vetted_packages_per_participant",
        maxVettedPackagesPerParticipantP,
      )
      maxMediatorsPerGroup <- ProtoConverter.parsePositiveInt(
        "max_mediators_per_group",
        maxMediatorsPerGroupP,
      )
      maxSignaturesPerTransaction <- ProtoConverter.parsePositiveInt(
        "max_signatures_per_transaction",
        maxSignaturesPerTransactionP,
      )
      maxTransactionHashesPerMultiSignature <- ProtoConverter.parsePositiveInt(
        "max_transaction_hashes_per_multi_signature",
        maxTransactionHashesPerMultiSignatureP,
      )
      maxTransactionsPerBroadcast <- ProtoConverter.parsePositiveInt(
        "max_transactions_per_broadcast",
        maxTransactionsPerBroadcastP,
      )
      maxEndpointsPerSequencerConnection <- ProtoConverter.parsePositiveInt(
        "max_endpoints_per_sequencer_connection",
        maxEndpointsPerSequencerConnectionP,
      )
    } yield TopologyLimits(
      maxSigningKeysPerEntity,
      maxOwnersPerDecentralizedNamespace,
      maxHostingParticipantsPerParty,
      maxVettedPackagesPerParticipant,
      maxMediatorsPerGroup,
      maxSignaturesPerTransaction,
      maxTransactionHashesPerMultiSignature,
      maxTransactionsPerBroadcast,
      maxEndpointsPerSequencerConnection,
    )
  }
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
    maxViewsPerMessage: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[TransactionProtocolLimits] =
    TransactionProtocolLimits

  def toProtoV31: protoV31.TransactionProtocolLimits = protoV31.TransactionProtocolLimits(
    maxActAs = maxActAs.value,
    maxEnvelopes = maxEnvelopes.value,
    maxRecipientsPerBatch = maxRecipientsPerBatch.value,
    maxRecipientsTrees = maxRecipientsTrees.value,
    maxRecipientsPerRecipientsTreeLevel = maxRecipientsPerRecipientsTreeLevel.value,
    maxChildrenPerRecipientsTreeLevel = maxChildrenPerRecipientsTreeLevel.value,
    maxRecipientsPerEnvelope = maxRecipientsPerEnvelope.value,
    maxRecipientsTreeDepth = maxRecipientsTreeDepth.value,
    maxTransactionRootViews = maxTransactionRootViews.value,
    maxTransactionSubViews = maxTransactionSubViews.value,
    maxTransactionTreeDepth = maxTransactionTreeDepth.value,
  )

  def toProtoV32: protoV32.TransactionProtocolLimits = protoV32.TransactionProtocolLimits(
    maxActAs = maxActAs.value,
    maxEnvelopes = maxEnvelopes.value,
    maxRecipientsPerBatch = maxRecipientsPerBatch.value,
    maxRecipientsTrees = maxRecipientsTrees.value,
    maxRecipientsPerRecipientsTreeLevel = maxRecipientsPerRecipientsTreeLevel.value,
    maxChildrenPerRecipientsTreeLevel = maxChildrenPerRecipientsTreeLevel.value,
    maxRecipientsPerEnvelope = maxRecipientsPerEnvelope.value,
    maxRecipientsTreeDepth = maxRecipientsTreeDepth.value,
    maxTransactionRootViews = maxTransactionRootViews.value,
    maxTransactionSubViews = maxTransactionSubViews.value,
    maxTransactionTreeDepth = maxTransactionTreeDepth.value,
    maxSignaturesPerEnvelope = maxSignaturesPerEnvelope.value,
    maxQuorumSize = maxQuorumSize.value,
  )

  def toProtoV33: protoV33.TransactionProtocolLimits = protoV33.TransactionProtocolLimits(
    maxActAs = maxActAs.value,
    maxEnvelopes = maxEnvelopes.value,
    maxRecipientsPerBatch = maxRecipientsPerBatch.value,
    maxRecipientsTrees = maxRecipientsTrees.value,
    maxRecipientsPerRecipientsTreeLevel = maxRecipientsPerRecipientsTreeLevel.value,
    maxChildrenPerRecipientsTreeLevel = maxChildrenPerRecipientsTreeLevel.value,
    maxRecipientsPerEnvelope = maxRecipientsPerEnvelope.value,
    maxRecipientsTreeDepth = maxRecipientsTreeDepth.value,
    maxTransactionRootViews = maxTransactionRootViews.value,
    maxTransactionSubViews = maxTransactionSubViews.value,
    maxTransactionTreeDepth = maxTransactionTreeDepth.value,
    maxSignaturesPerEnvelope = maxSignaturesPerEnvelope.value,
    maxQuorumSize = maxQuorumSize.value,
    maxViewsPerMessage = maxViewsPerMessage.value,
  )
}

final case class AcsCommitmentLimits(
    maxCommitmentSizeBytes: PositiveInt,
    maxCommitmentSummarySizeBytes: PositiveInt,
    maxAddressedCounterparticipants: PositiveInt,
    maxUnsentDigests: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[AcsCommitmentLimits] = AcsCommitmentLimits

  def toProtoV32: protoV32.AcsCommitmentLimits = protoV32.AcsCommitmentLimits(
    maxAcsCommitmentSizeBytes = maxCommitmentSizeBytes.value,
    maxAcsCommitmentSummarySizeBytes = maxCommitmentSummarySizeBytes.value,
    maxAddressedCounterparticipants = maxAddressedCounterparticipants.value,
    maxUnsentDigests = maxUnsentDigests.value,
  )

  def toProtoV33: protoV33.AcsCommitmentLimits = protoV33.AcsCommitmentLimits(
    maxAcsCommitmentSizeBytes = maxCommitmentSizeBytes.value,
    maxAcsCommitmentSummarySizeBytes = maxCommitmentSummarySizeBytes.value,
    maxAddressedCounterparticipants = maxAddressedCounterparticipants.value,
    maxUnsentDigests = maxUnsentDigests.value,
  )
}

object AcsCommitmentLimits extends PrettyPrintingCompanion[AcsCommitmentLimits] {
  override protected val pretty: Pretty[AcsCommitmentLimits] = prettyOfClass(
    param("max commitment size bytes", _.maxCommitmentSizeBytes),
    param("max commitment summary size bytes", _.maxCommitmentSummarySizeBytes),
    param("max addressed counterparticipants", _.maxAddressedCounterparticipants),
    param("max unsent digests", _.maxUnsentDigests),
  )

  lazy val default: AcsCommitmentLimits = AcsCommitmentLimits(
    maxCommitmentSizeBytes = PositiveInt.tryCreate(10 * 1024), // 10KB
    maxCommitmentSummarySizeBytes = PositiveInt.tryCreate(1024 * 1024), // 1MB
    maxAddressedCounterparticipants =
      PositiveInt.tryCreate(10_000), // Same as old transaction default
    maxUnsentDigests = PositiveInt.tryCreate(10_000),
  )

  def fromProtoV32(limitsP: protoV32.AcsCommitmentLimits): ParsingResult[AcsCommitmentLimits] =
    for {
      maxCommitmentSizeBytes <- ProtoConverter.parsePositiveInt(
        "max_commitment_size_bytes",
        limitsP.maxAcsCommitmentSizeBytes,
      )
      maxCommitmentSummarySizeBytes <- ProtoConverter.parsePositiveInt(
        "max_commitment_summary_size_bytes",
        limitsP.maxAcsCommitmentSummarySizeBytes,
      )
      maxAddressedCounterparticipants <- ProtoConverter.parsePositiveInt(
        "max_addressed_counterparticipants",
        limitsP.maxAddressedCounterparticipants,
      )
      maxUnsentDigests <- ProtoConverter.parsePositiveInt(
        "max_unsent_digests",
        limitsP.maxUnsentDigests,
      )
    } yield AcsCommitmentLimits(
      maxCommitmentSizeBytes,
      maxCommitmentSummarySizeBytes,
      maxAddressedCounterparticipants,
      maxUnsentDigests,
    )

  def fromProtoV33(limitsP: protoV33.AcsCommitmentLimits): ParsingResult[AcsCommitmentLimits] =
    for {
      maxCommitmentSizeBytes <- ProtoConverter.parsePositiveInt(
        "max_commitment_size_bytes",
        limitsP.maxAcsCommitmentSizeBytes,
      )
      maxCommitmentSummarySizeBytes <- ProtoConverter.parsePositiveInt(
        "max_commitment_summary_size_bytes",
        limitsP.maxAcsCommitmentSummarySizeBytes,
      )
      maxAddressedCounterparticipants <- ProtoConverter.parsePositiveInt(
        "max_addressed_counterparticipants",
        limitsP.maxAddressedCounterparticipants,
      )
      maxUnsentDigests <- ProtoConverter.parsePositiveInt(
        "max_unsent_digests",
        limitsP.maxUnsentDigests,
      )
    } yield AcsCommitmentLimits(
      maxCommitmentSizeBytes,
      maxCommitmentSummarySizeBytes,
      maxAddressedCounterparticipants,
      maxUnsentDigests,
    )
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
    param("max quorum size", _.maxQuorumSize),
    param("max views per message", _.maxViewsPerMessage),
  )

  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimits(
      maxActAs = PositiveInt.tryCreate(1000),
      maxEnvelopes = PositiveInt.tryCreate(10_000),
      maxRecipientsPerBatch = PositiveInt.tryCreate(10_000),
      maxRecipientsTrees = PositiveInt.tryCreate(10_000),
      maxRecipientsPerRecipientsTreeLevel = PositiveInt.tryCreate(10_000),
      maxChildrenPerRecipientsTreeLevel = PositiveInt.tryCreate(10_000),
      maxRecipientsPerEnvelope = PositiveInt.tryCreate(10_000),
      maxRecipientsTreeDepth = PositiveInt.tryCreate(100),
      maxTransactionRootViews = PositiveInt.tryCreate(1_000_000),
      maxTransactionSubViews = PositiveInt.tryCreate(10_000_000),
      maxTransactionTreeDepth = PositiveInt.MaxValue,
      maxSignaturesPerEnvelope = PositiveInt.tryCreate(1_000),
      maxQuorumSize = PositiveInt.tryCreate(100_000_000),
      maxViewsPerMessage = PositiveInt.tryCreate(100_000),
    )

  /** Common parsing logic for all proto versions, which only differ in whether
    * maxSignaturesPerEnvelope and maxQuorumSize are carried on the wire.
    *
    * @param maxSignaturesPerEnvelope
    *   carried on v32 and later, not in v31, enforced on >= ProtocolVersion.newBoundsOnPV37
    * @param maxQuorumSize
    *   carried on v32 and later, not in v31, enforced on >= ProtocolVersion.newBoundsOnPV37
    * @param maxViewsPerMessage
    *   carried on v33 and later, not in v31 and v32, enforced on >=
    *   ProtocolVersion.nextValidationBoundPV
    */
  private def fromProtoCommon(
      maxActAsP: Int,
      maxEnvelopesP: Int,
      maxRecipientsPerBatchP: Int,
      maxRecipientsTreesP: Int,
      maxRecipientsPerRecipientsTreeLevelP: Int,
      maxChildrenPerRecipientsTreeLevelP: Int,
      maxRecipientsPerEnvelopeP: Int,
      maxRecipientsTreeDepthP: Int,
      maxTransactionRootViewsP: Int,
      maxTransactionSubViewsP: Int,
      maxTransactionTreeDepthP: Int,
      maxSignaturesPerEnvelope: PositiveInt,
      maxQuorumSize: PositiveInt,
      maxViewsPerMessage: PositiveInt,
  ): ParsingResult[TransactionProtocolLimits] =
    for {
      maxActAs <- ProtoConverter.parsePositiveInt("max_act_as", maxActAsP)
      maxEnvelopes <- ProtoConverter.parsePositiveInt("max_envelopes", maxEnvelopesP)
      maxRecipientsPerBatch <- ProtoConverter.parsePositiveInt(
        "max_recipients_per_batch",
        maxRecipientsPerBatchP,
      )
      maxRecipientsTrees <- ProtoConverter.parsePositiveInt(
        "max_recipients_trees",
        maxRecipientsTreesP,
      )
      maxRecipientsPerRecipientsTreeLevel <- ProtoConverter.parsePositiveInt(
        "max_recipients_per_recipients_tree_level",
        maxRecipientsPerRecipientsTreeLevelP,
      )
      maxChildrenPerRecipientsTreeLevel <- ProtoConverter.parsePositiveInt(
        "max_children_per_recipients_tree_level",
        maxChildrenPerRecipientsTreeLevelP,
      )
      maxRecipientsPerEnvelope <- ProtoConverter.parsePositiveInt(
        "max_recipients_per_envelope",
        maxRecipientsPerEnvelopeP,
      )
      maxRecipientsTreeDepth <- ProtoConverter.parsePositiveInt(
        "max_recipients_tree_depth",
        maxRecipientsTreeDepthP,
      )
      maxTransactionRootViews <- ProtoConverter.parsePositiveInt(
        "max_transaction_root_views",
        maxTransactionRootViewsP,
      )
      maxTransactionSubViews <- ProtoConverter.parsePositiveInt(
        "max_transaction_sub_views",
        maxTransactionSubViewsP,
      )
      maxTransactionTreeDepth <- ProtoConverter.parsePositiveInt(
        "max_transaction_tree_depth",
        maxTransactionTreeDepthP,
      )
    } yield TransactionProtocolLimits(
      maxActAs = maxActAs,
      maxEnvelopes = maxEnvelopes,
      maxRecipientsPerBatch = maxRecipientsPerBatch,
      maxRecipientsTrees = maxRecipientsTrees,
      maxRecipientsPerRecipientsTreeLevel = maxRecipientsPerRecipientsTreeLevel,
      maxChildrenPerRecipientsTreeLevel = maxChildrenPerRecipientsTreeLevel,
      maxRecipientsPerEnvelope = maxRecipientsPerEnvelope,
      maxRecipientsTreeDepth = maxRecipientsTreeDepth,
      maxTransactionRootViews = maxTransactionRootViews,
      maxTransactionSubViews = maxTransactionSubViews,
      maxTransactionTreeDepth = maxTransactionTreeDepth,
      maxSignaturesPerEnvelope = maxSignaturesPerEnvelope,
      maxQuorumSize = maxQuorumSize,
      maxViewsPerMessage = maxViewsPerMessage,
    )

  def fromProtoV31(
      transactionProtocolLimitsP: protoV31.TransactionProtocolLimits
  ): ParsingResult[TransactionProtocolLimits] = {
    val protoV31.TransactionProtocolLimits(
      maxActAsP,
      maxEnvelopesP,
      maxRecipientsPerBatchP,
      maxRecipientsTreesP,
      maxRecipientsPerRecipientsTreeLevelP,
      maxChildrenPerRecipientsTreeLevelP,
      maxRecipientsPerEnvelopeP,
      maxRecipientsTreeDepthP,
      maxTransactionRootViewsP,
      maxTransactionSubViewsP,
      maxTransactionTreeDepthP,
    ) = transactionProtocolLimitsP

    fromProtoCommon(
      maxActAsP,
      maxEnvelopesP,
      maxRecipientsPerBatchP,
      maxRecipientsTreesP,
      maxRecipientsPerRecipientsTreeLevelP,
      maxChildrenPerRecipientsTreeLevelP,
      maxRecipientsPerEnvelopeP,
      maxRecipientsTreeDepthP,
      maxTransactionRootViewsP,
      maxTransactionSubViewsP,
      maxTransactionTreeDepthP,
      // maxSignaturesPerEnvelope was introduced in proto V32
      maxSignaturesPerEnvelope = TransactionProtocolLimits.default.maxSignaturesPerEnvelope,
      // maxQuorumSize was introduced in Proto 32
      maxQuorumSize = TransactionProtocolLimits.default.maxQuorumSize,
      maxViewsPerMessage = TransactionProtocolLimits.default.maxViewsPerMessage,
    )
  }

  def fromProtoV32(
      transactionProtocolLimitsP: protoV32.TransactionProtocolLimits
  ): ParsingResult[TransactionProtocolLimits] = {
    val protoV32.TransactionProtocolLimits(
      maxActAsP,
      maxEnvelopesP,
      maxRecipientsPerBatchP,
      maxRecipientsTreesP,
      maxRecipientsPerRecipientsTreeLevelP,
      maxChildrenPerRecipientsTreeLevelP,
      maxRecipientsPerEnvelopeP,
      maxRecipientsTreeDepthP,
      maxTransactionRootViewsP,
      maxTransactionSubViewsP,
      maxTransactionTreeDepthP,
      maxSignaturesPerEnvelopeP,
      maxQuorumSizeP,
    ) = transactionProtocolLimitsP

    for {
      maxSignaturesPerEnvelope <- ProtoConverter.parsePositiveInt(
        "max_signatures_per_envelope",
        maxSignaturesPerEnvelopeP,
      )
      maxQuorumSize <- ProtoConverter.parsePositiveInt(
        "max_quorum_size",
        maxQuorumSizeP,
      )

      limits <- fromProtoCommon(
        maxActAsP,
        maxEnvelopesP,
        maxRecipientsPerBatchP,
        maxRecipientsTreesP,
        maxRecipientsPerRecipientsTreeLevelP,
        maxChildrenPerRecipientsTreeLevelP,
        maxRecipientsPerEnvelopeP,
        maxRecipientsTreeDepthP,
        maxTransactionRootViewsP,
        maxTransactionSubViewsP,
        maxTransactionTreeDepthP,
        maxSignaturesPerEnvelope,
        maxQuorumSize,
        // maxViewsPerMessage was introduced in proto V33
        TransactionProtocolLimits.default.maxViewsPerMessage,
      )
    } yield limits
  }

  def fromProtoV33(
      transactionProtocolLimitsP: protoV33.TransactionProtocolLimits
  ): ParsingResult[TransactionProtocolLimits] = {
    val protoV33.TransactionProtocolLimits(
      maxActAsP,
      maxEnvelopesP,
      maxRecipientsPerBatchP,
      maxRecipientsTreesP,
      maxRecipientsPerRecipientsTreeLevelP,
      maxChildrenPerRecipientsTreeLevelP,
      maxRecipientsPerEnvelopeP,
      maxRecipientsTreeDepthP,
      maxTransactionRootViewsP,
      maxTransactionSubViewsP,
      maxTransactionTreeDepthP,
      maxSignaturesPerEnvelopeP,
      maxQuorumSizeP,
      maxViewsPerMessageP,
    ) = transactionProtocolLimitsP

    for {
      maxSignaturesPerEnvelope <- ProtoConverter.parsePositiveInt(
        "max_signatures_per_envelope",
        maxSignaturesPerEnvelopeP,
      )
      maxQuorumSize <- ProtoConverter.parsePositiveInt(
        "max_quorum_size",
        maxQuorumSizeP,
      )
      maxViewsPerMessage <- ProtoConverter.parsePositiveInt(
        "max_views_per_message",
        maxViewsPerMessageP,
      )

      limits <- fromProtoCommon(
        maxActAsP,
        maxEnvelopesP,
        maxRecipientsPerBatchP,
        maxRecipientsTreesP,
        maxRecipientsPerRecipientsTreeLevelP,
        maxChildrenPerRecipientsTreeLevelP,
        maxRecipientsPerEnvelopeP,
        maxRecipientsTreeDepthP,
        maxTransactionRootViewsP,
        maxTransactionSubViewsP,
        maxTransactionTreeDepthP,
        maxSignaturesPerEnvelope,
        maxQuorumSize,
        maxViewsPerMessage,
      )
    } yield limits
  }
}

/** Size limits on the `repeated` fields of the BFT ordering service's internal P2P protocol.
  *
  * Only three values are configured. All other limits are derived from them and from
  * [[SynchronizerLimits.maxSequencersPerSynchronizer]] in [[BftOrdererLimitsView]], as the
  * legitimate size of every collection is bounded by the number of sequencers, the segment length,
  * or the sequencing parameters. Synchronizer owners must not configure topology values (number of
  * sequencers, segment length, requests per batch, batches per block proposal) above the limits
  * applied here, otherwise honest messages are rejected as malformed.
  *
  * @param maxSegmentLength
  *   upper bound on the number of blocks in an ISS segment
  * @param maxOrderingRequests
  *   upper bound on the number of requests in a batch (sequencing parameter maxRequestsInBatch)
  * @param maxProofs
  *   upper bound on the number of proofs of availability in a block proposal (sequencing parameter
  *   maxBatchesPerBlockProposal)
  */
final case class BftOrdererLimits(
    maxSegmentLength: PositiveInt,
    maxOrderingRequests: PositiveInt,
    maxProofs: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[BftOrdererLimits] = BftOrdererLimits

  def toProtoV33: protoV33.BftOrdererLimits = protoV33.BftOrdererLimits(
    maxSegmentLength = maxSegmentLength.value,
    maxOrderingRequests = maxOrderingRequests.value,
    maxProofs = maxProofs.value,
  )
}

/** The BFT orderer limits together with the limits shared across [[SynchronizerLimits]], and all
  * limits derived from them.
  */
final case class BftOrdererLimitsView(
    limits: BftOrdererLimits,
    maxSequencersPerSynchronizer: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[BftOrdererLimitsView] =
    BftOrdererLimitsView

  val maxSegmentLength: PositiveInt = limits.maxSegmentLength
  val maxOrderingRequests: PositiveInt = limits.maxOrderingRequests
  val maxProofs: PositiveInt = limits.maxProofs

  // Limits bounded by the number of sequencers
  val maxSegments: PositiveInt = maxSequencersPerSynchronizer
  val maxViewChangeMessagesPresent: PositiveInt = maxSequencersPerSynchronizer
  val maxPreparesPresent: PositiveInt = maxSequencersPerSynchronizer
  val maxCommitsPresent: PositiveInt = maxSequencersPerSynchronizer
  val maxAcks: PositiveInt = maxSequencersPerSynchronizer
  val maxViewChanges: PositiveInt = maxSequencersPerSynchronizer
  val maxCanonicalCommits: PositiveInt = maxSequencersPerSynchronizer
  val maxPrepares: PositiveInt = maxSequencersPerSynchronizer
  val maxCommits: PositiveInt = maxSequencersPerSynchronizer

  private val maxBlocksInEpoch: PositiveInt = PositiveInt.tryCreate(
    (maxSegmentLength.value.toLong * maxSequencersPerSynchronizer.value).min(Int.MaxValue).toInt
  )

  // Limits bounded by the number of blocks in an epoch (segment length * number of sequencers)
  val maxAreBlocksComplete: PositiveInt = maxBlocksInEpoch
  val maxBlockStatuses: PositiveInt = maxBlocksInEpoch
  val maxConsensusCerts: PositiveInt = maxBlocksInEpoch
  val maxPrePrepares: PositiveInt = maxBlocksInEpoch
  val maxCommitCertificates: PositiveInt = maxBlocksInEpoch
}

object BftOrdererLimitsView extends PrettyPrintingCompanion[BftOrdererLimitsView] {

  override protected val pretty: Pretty[BftOrdererLimitsView] = prettyOfClass(
    param("limits", _.limits),
    param("max sequencers per synchronizer", _.maxSequencersPerSynchronizer),
  )

  lazy val default: BftOrdererLimitsView =
    BftOrdererLimitsView(
      BftOrdererLimits.default,
      SynchronizerLimits.DefaultMaxSequencersPerSynchronizer,
    )
}

object BftOrdererLimits extends PrettyPrintingCompanion[BftOrdererLimits] {

  override protected val pretty: Pretty[BftOrdererLimits] = prettyOfClass(
    param("max segment length", _.maxSegmentLength),
    param("max ordering requests", _.maxOrderingRequests),
    param("max proofs", _.maxProofs),
  )

  // maxRequestsInBatch and maxBatchesPerBlockProposal are Short-typed sequencing parameters
  private val MaxShort: PositiveInt = PositiveInt.tryCreate(Short.MaxValue.toInt)

  lazy val default: BftOrdererLimits =
    BftOrdererLimits(
      maxSegmentLength = PositiveInt.tryCreate(10_000),
      maxOrderingRequests = MaxShort,
      maxProofs = MaxShort,
    )

  def fromProtoV33(
      bftOrdererLimitsP: protoV33.BftOrdererLimits
  ): ParsingResult[BftOrdererLimits] = {
    val protoV33.BftOrdererLimits(
      maxSegmentLengthP,
      maxOrderingRequestsP,
      maxProofsP,
    ) = bftOrdererLimitsP

    for {
      maxSegmentLength <- ProtoConverter.parsePositiveInt(
        "max_segment_length",
        maxSegmentLengthP,
      )
      maxOrderingRequests <- ProtoConverter.parsePositiveInt(
        "max_ordering_requests",
        maxOrderingRequestsP,
      )
      maxProofs <- ProtoConverter.parsePositiveInt("max_proofs", maxProofsP)
    } yield BftOrdererLimits(
      maxSegmentLength = maxSegmentLength,
      maxOrderingRequests = maxOrderingRequests,
      maxProofs = maxProofs,
    )
  }
}
