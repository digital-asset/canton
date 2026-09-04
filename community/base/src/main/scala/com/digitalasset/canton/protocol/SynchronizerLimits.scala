// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v31 as protoV31
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

final case class SynchronizerLimits(transactionProtocolLimits: TransactionProtocolLimits)
    extends PrettyPrinting {
  override protected def pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits)
  )

  def toProtoV31: protoV31.SynchronizerLimits = protoV31.SynchronizerLimits(
    transactionProtocolLimits = Some(transactionProtocolLimits.toProtoV31)
  )
}

object SynchronizerLimits {
  lazy val default: SynchronizerLimits =
    SynchronizerLimits(transactionProtocolLimits = TransactionProtocolLimits.default)
  lazy val max: SynchronizerLimits =
    SynchronizerLimits(transactionProtocolLimits = TransactionProtocolLimits.max)

  def defaultFor(protocolVersion: ProtocolVersion): SynchronizerLimits =
    if (protocolVersion >= ProtocolVersion.v36) default else max

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
) extends PrettyPrinting {
  override protected def pretty: Pretty[TransactionProtocolLimits] = prettyOfClass(
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
}

object TransactionProtocolLimits {
  // TODO(i35127): determine proper default values
  lazy val DefaultMaxActAs: PositiveInt = PositiveInt.tryCreate(1000)
  lazy val DefaultMaxEnvelopes: PositiveInt = PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxRecipientsPerBatch: PositiveInt = PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxRecipientsTrees: PositiveInt = PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxRecipientsPerRecipientsTreeLevel: PositiveInt =
    PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxChildrenPerRecipientsTreeLevel: PositiveInt =
    PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxRecipientsPerEnvelope: PositiveInt = PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxRecipientsTreeDepth: PositiveInt = PositiveInt.tryCreate(500)
  lazy val DefaultMaxTransactionRootViews: PositiveInt = PositiveInt.tryCreate(1_000_000)
  lazy val DefaultMaxTransactionSubViews: PositiveInt = PositiveInt.tryCreate(10_000_000)
  lazy val DefaultMaxTransactionTreeDepth: PositiveInt = PositiveInt.tryCreate(10_000)

  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimits(
      maxActAs = DefaultMaxActAs,
      maxEnvelopes = DefaultMaxEnvelopes,
      maxRecipientsPerBatch = DefaultMaxRecipientsPerBatch,
      maxRecipientsTrees = DefaultMaxRecipientsTrees,
      maxRecipientsPerRecipientsTreeLevel = DefaultMaxRecipientsPerRecipientsTreeLevel,
      maxChildrenPerRecipientsTreeLevel = DefaultMaxChildrenPerRecipientsTreeLevel,
      maxRecipientsPerEnvelope = DefaultMaxRecipientsPerEnvelope,
      maxRecipientsTreeDepth = DefaultMaxRecipientsTreeDepth,
      maxTransactionRootViews = DefaultMaxTransactionRootViews,
      maxTransactionSubViews = DefaultMaxTransactionSubViews,
      maxTransactionTreeDepth = DefaultMaxTransactionTreeDepth,
    )
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
  }
}
