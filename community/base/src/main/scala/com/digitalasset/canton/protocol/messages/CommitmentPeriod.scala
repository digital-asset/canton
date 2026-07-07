// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.InvariantViolation
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.common.annotations.VisibleForTesting

final case class CommitmentPeriod private (
    fromExclusive: CantonTimestamp,
    toInclusive: CantonTimestamp,
) extends PrettyPrinting {

  override protected def pretty: Pretty[CommitmentPeriod] =
    prettyOfClass(
      param("fromExclusive", _.fromExclusive),
      param("toInclusive", _.toInclusive),
    )

  private[messages] def toProtoV32: v32.CommitmentPeriod = v32.CommitmentPeriod(
    fromExclusive = fromExclusive.toProtoPrimitive,
    toInclusive = toInclusive.toProtoPrimitive,
  )
}

object CommitmentPeriod {

  def fromProtoV32(protoMsg: v32.CommitmentPeriod): ParsingResult[CommitmentPeriod] = for {
    fromExclusive <- CantonTimestamp.fromProtoPrimitive(protoMsg.fromExclusive)
    toInclusive <- CantonTimestamp.fromProtoPrimitive(protoMsg.toInclusive)

    period <- create(fromExclusive, toInclusive).leftMap { error =>
      ProtoDeserializationError.InvariantViolation(
        field = None,
        error = error.message,
      )
    }
  } yield period

  def create(
      fromExclusive: CantonTimestamp,
      toInclusive: CantonTimestamp,
  ): Either[InvariantViolation, CommitmentPeriod] = Either.cond(
    toInclusive > fromExclusive,
    CommitmentPeriod(fromExclusive, toInclusive),
    InvariantViolation(s"Illegal commitment period length: $fromExclusive, $toInclusive"),
  )

  @VisibleForTesting
  def tryCreate(fromExclusive: CantonTimestamp, toInclusive: CantonTimestamp): CommitmentPeriod =
    create(fromExclusive, toInclusive).valueOr(err =>
      throw new IllegalArgumentException(err.message)
    )
}
