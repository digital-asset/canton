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

final case class TransactionProtocolLimits(maxActAs: PositiveInt) extends PrettyPrinting {
  override protected def pretty: Pretty[TransactionProtocolLimits] = prettyOfClass(
    param("max actAs", _.maxActAs)
  )

  def toProtoV31: protoV31.TransactionProtocolLimits = protoV31.TransactionProtocolLimits(
    maxActAs = maxActAs.value
  )
}

object TransactionProtocolLimits {
  lazy val DefaultMaxActAs: PositiveInt = PositiveInt.tryCreate(50)

  lazy val default: TransactionProtocolLimits =
    TransactionProtocolLimits(maxActAs = DefaultMaxActAs)
  lazy val max: TransactionProtocolLimits =
    TransactionProtocolLimits(maxActAs = PositiveInt.MaxValue)

  def fromProtoV31(
      transactionProtocolLimitsP: protoV31.TransactionProtocolLimits
  ): ParsingResult[TransactionProtocolLimits] = {
    val protoV31.TransactionProtocolLimits(maxActAsP) = transactionProtocolLimitsP

    for {
      maxActAs <- ProtoConverter.parsePositiveInt("max_act_as", maxActAsP)
    } yield TransactionProtocolLimits(maxActAs)
  }
}
