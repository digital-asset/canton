// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.{v31 as protoV31, v32 as protoV32}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

final case class SynchronizerLimits(transactionProtocolLimits: TransactionProtocolLimits)
    extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[SynchronizerLimits] = SynchronizerLimits

  def toProtoV31: protoV31.SynchronizerLimits = protoV31.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV31)
  )

  def toProtoV32: protoV32.SynchronizerLimits = protoV32.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV32)
  )
}

object SynchronizerLimits extends PrettyPrintingCompanion[SynchronizerLimits] {

  override protected val pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits)
  )

  private lazy val default: SynchronizerLimits =
    SynchronizerLimits(transactionProtocolLimits = TransactionProtocolLimits.default)
  private lazy val defaultForProtoVDev: SynchronizerLimits =
    SynchronizerLimits(transactionProtocolLimits = TransactionProtocolLimits.defaultForProtoVDev)
  lazy val max: SynchronizerLimits =
    SynchronizerLimits(transactionProtocolLimits = TransactionProtocolLimits.max)

  def defaultFor(protocolVersion: ProtocolVersion): SynchronizerLimits = protocolVersion match {
    // Before PV35, limits were not applied
    case pv if pv <= ProtocolVersion.v35 => max
    // TODO(i35890): Update default values for PVDev (to be upgraded to PV37 when set)
    case ProtocolVersion.dev => defaultForProtoVDev
    case _ => default
  }

  def fromProtoV31(
      synchronizerLimitsP: protoV31.SynchronizerLimits
  ): ParsingResult[SynchronizerLimits] = {
    val protoV31.SynchronizerLimits(transactionProtocolLimitsP) = synchronizerLimitsP

    for {
      transactionProtocolLimits <- ProtoConverter
        .required("transaction_protocol_limits", transactionProtocolLimitsP)
        .flatMap(TransactionProtocolLimits.fromProtoV31)
    } yield SynchronizerLimits(transactionProtocolLimits)
  }

  def fromProtoV32(
      synchronizerLimitsP: protoV32.SynchronizerLimits
  ): ParsingResult[SynchronizerLimits] = {
    val protoV32.SynchronizerLimits(transactionProtocolLimitsP) = synchronizerLimitsP

    for {
      transactionProtocolLimits <- ProtoConverter
        .required("transaction_protocol_limits", transactionProtocolLimitsP)
        .flatMap(TransactionProtocolLimits.fromProtoV32)
    } yield SynchronizerLimits(transactionProtocolLimits)
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
    )
  // TODO(i35890): Update default values for PVDev (to be upgraded to PV37 when set)
  lazy val defaultForProtoVDev: TransactionProtocolLimits = default
  lazy val max: TransactionProtocolLimits =
    TransactionProtocolLimits(
      maxActAs = PositiveInt.MaxValue,
      maxEnvelopes = PositiveInt.MaxValue,
      maxRecipientsPerBatch = PositiveInt.MaxValue,
      maxRecipientsTrees = PositiveInt.MaxValue,
      maxRecipientsPerRecipientsTreeLevel = PositiveInt.MaxValue,
      maxChildrenPerRecipientsTreeLevel = PositiveInt.MaxValue,
      maxRecipientsPerEnvelope = PositiveInt.MaxValue,
      maxRecipientsTreeDepth = PositiveInt.MaxValue,
      maxTransactionRootViews = PositiveInt.MaxValue,
      maxTransactionSubViews = PositiveInt.MaxValue,
      maxTransactionTreeDepth = PositiveInt.MaxValue,
    )

  /** Common parsing logic for proto versions 31 and 32, which have identical fields. */
  private def fromProtoV3132(
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

    fromProtoV3132(
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
    ) = transactionProtocolLimitsP

    fromProtoV3132(
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
    )
  }
}
