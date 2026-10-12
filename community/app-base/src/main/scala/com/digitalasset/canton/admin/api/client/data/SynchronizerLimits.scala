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
  SynchronizerLimits as SynchronizerLimitsInternal,
  TransactionProtocolLimits as TransactionProtocolLimitsInternal,
}
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*

final case class SynchronizerLimits(transactionProtocolLimits: TransactionProtocolLimits)
    extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[SynchronizerLimits] = SynchronizerLimits

  def toInternal: SynchronizerLimitsInternal = this.transformInto[SynchronizerLimitsInternal]
}

object SynchronizerLimits extends PrettyPrintingCompanion[SynchronizerLimits] {
  override protected val pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits)
  )

  lazy val max: SynchronizerLimits =
    SynchronizerLimitsInternal.max.transformInto[SynchronizerLimits]

  def defaultFor(protocolVersion: ProtocolVersion): SynchronizerLimits =
    SynchronizerLimitsInternal.defaultFor(protocolVersion).transformInto[SynchronizerLimits]
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
  )

  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimitsInternal.default.transformInto[TransactionProtocolLimits]
}
