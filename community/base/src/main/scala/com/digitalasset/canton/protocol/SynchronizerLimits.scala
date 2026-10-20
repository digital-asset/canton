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
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[SynchronizerLimits] = SynchronizerLimits

  // v31 has no topology limits, so they fall back to the default on the way back in
  def toProtoV31: protoV31.SynchronizerLimits = protoV31.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV31)
  )

  def toProtoV32: protoV32.SynchronizerLimits = protoV32.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV32),
    topologyLimits = Some(topologyLimits.toProtoV32),
    acsCommitmentLimits = Some(acsCommitmentLimits.toProtoV32),
  )

  def toProtoV33: protoV33.SynchronizerLimits = protoV33.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV33),
    topologyLimits = Some(topologyLimits.toProtoV33),
    acsCommitmentLimits = Some(acsCommitmentLimits.toProtoV33),
  )
}

object SynchronizerLimits extends PrettyPrintingCompanion[SynchronizerLimits] {

  override protected val pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits),
    param("topology limits", _.topologyLimits),
    param("acs commitment limits", _.acsCommitmentLimits),
  )

  /** The same on every protocol version: each limit check states the protocol version it applies
    * from through its ValidationParam. Applied where no synchronizer can be asked for its own
    * limits, such as the authorized store and the admin API.
    */
  lazy val default: SynchronizerLimits =
    SynchronizerLimits(
      transactionProtocolLimits = TransactionProtocolLimits.default,
      topologyLimits = TopologyLimits.default,
      acsCommitmentLimits = AcsCommitmentLimits.default,
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

    } yield SynchronizerLimits(transactionProtocolLimits, topologyLimits, acsCommitmentLimits)
  }

  def fromProtoV33(
      synchronizerLimitsP: protoV33.SynchronizerLimits
  ): ParsingResult[SynchronizerLimits] = {
    val protoV33.SynchronizerLimits(
      transactionProtocolLimitsP,
      topologyLimitsP,
      acsCommitmentLimitsP,
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

    } yield SynchronizerLimits(transactionProtocolLimits, topologyLimits, acsCommitmentLimits)
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
  */
final case class TopologyLimits(
    maxSigningKeysPerEntity: PositiveInt,
    maxOwnersPerDecentralizedNamespace: PositiveInt,
    maxHostingParticipantsPerParty: PositiveInt,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[TopologyLimits] = TopologyLimits

  def toProtoV32: protoV32.TopologyLimits = protoV32.TopologyLimits(
    maxSigningKeysPerEntity = maxSigningKeysPerEntity.value,
    maxOwnersPerDecentralizedNamespace = maxOwnersPerDecentralizedNamespace.value,
    maxHostingParticipantsPerParty = maxHostingParticipantsPerParty.value,
  )

  def toProtoV33: protoV33.TopologyLimits = protoV33.TopologyLimits(
    maxSigningKeysPerEntity = maxSigningKeysPerEntity.value,
    maxOwnersPerDecentralizedNamespace = maxOwnersPerDecentralizedNamespace.value,
    maxHostingParticipantsPerParty = maxHostingParticipantsPerParty.value,
  )
}

object TopologyLimits extends PrettyPrintingCompanion[TopologyLimits] {

  override protected val pretty: Pretty[TopologyLimits] = prettyOfClass(
    param("max signing keys per entity", _.maxSigningKeysPerEntity),
    param("max owners per decentralized namespace", _.maxOwnersPerDecentralizedNamespace),
    param("max hosting participants per party", _.maxHostingParticipantsPerParty),
  )

  lazy val default: TopologyLimits = TopologyLimits(
    maxSigningKeysPerEntity = PositiveInt.tryCreate(20),
    maxOwnersPerDecentralizedNamespace = PositiveInt.tryCreate(100),
    maxHostingParticipantsPerParty = PositiveInt.tryCreate(100),
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
    )
  }

  def fromProtoV33(
      topologyLimitsP: protoV33.TopologyLimits
  ): ParsingResult[TopologyLimits] = {
    val protoV33.TopologyLimits(
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
    )

  /** Common parsing logic for all proto versions, which only differ in whether
    * maxSignaturesPerEnvelope and maxQuorumSize are carried on the wire.
    *
    * @param maxSignaturesPerEnvelope
    *   carried on v32 and later, not in v31, enforced on >= ProtocolVersion.newBoundsOnPV37
    * @param maxQuorumSize
    *   carried on v32 and later, not in v31, enforced on >= ProtocolVersion.newBoundsOnPV37
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
      )
    } yield limits
  }
}
