// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{
  SynchronizerLimits as SynchronizerLimitsInternal,
  TransactionProtocolLimits as TransactionProtocolLimitsInternal,
}
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*

final case class SynchronizerLimits(transactionProtocolLimits: TransactionProtocolLimits)
    extends PrettyPrinting {
  override protected def pretty: Pretty[SynchronizerLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits)
  )

  def toInternal: SynchronizerLimitsInternal = this.transformInto[SynchronizerLimitsInternal]
}

object SynchronizerLimits {
  lazy val default: SynchronizerLimits =
    SynchronizerLimitsInternal.default.transformInto[SynchronizerLimits]
  lazy val max: SynchronizerLimits =
    SynchronizerLimitsInternal.max.transformInto[SynchronizerLimits]

  def defaultFor(protocolVersion: ProtocolVersion): SynchronizerLimits =
    SynchronizerLimitsInternal.defaultFor(protocolVersion).transformInto[SynchronizerLimits]
}

final case class TransactionProtocolLimits(maxActAs: PositiveInt) extends PrettyPrinting {
  override protected def pretty: Pretty[TransactionProtocolLimits] = prettyOfClass(
    param("max actAs", _.maxActAs)
  )

  def toInternal: TransactionProtocolLimitsInternal =
    this.transformInto[TransactionProtocolLimitsInternal]
}

object TransactionProtocolLimits {
  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimitsInternal.default.transformInto[TransactionProtocolLimits]
}
