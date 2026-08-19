// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{
  SizeLimits as SizeLimitsInternal,
  TransactionProtocolLimits as TransactionProtocolLimitsInternal,
}
import io.scalaland.chimney.dsl.*

final case class SizeLimits(transactionProtocolLimits: TransactionProtocolLimits)
    extends PrettyPrinting {
  override protected def pretty: Pretty[SizeLimits] = prettyOfClass(
    param("transaction protocol limits", _.transactionProtocolLimits)
  )

  def toInternal: SizeLimitsInternal = this.transformInto[SizeLimitsInternal]
}

object SizeLimits {
  lazy val default: SizeLimits = SizeLimitsInternal.default.transformInto[SizeLimits]
  lazy val max: SizeLimits = SizeLimitsInternal.max.transformInto[SizeLimits]
}

final case class TransactionProtocolLimits(maxActAs: PositiveInt) extends PrettyPrinting {
  override protected def pretty: Pretty[TransactionProtocolLimits] = prettyOfClass(
    param("max actAs", _.maxActAs)
  )

  def toInternal: TransactionProtocolLimits = this.transformInto[TransactionProtocolLimits]
}

object TransactionProtocolLimits {
  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimitsInternal.default.transformInto[TransactionProtocolLimits]
}
