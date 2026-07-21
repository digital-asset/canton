// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.InvariantViolation
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.MergeableIntervalOps

final case class CommitmentPeriod private (
    fromExclusive: CantonTimestamp,
    toInclusive: CantonTimestamp,
) extends PrettyPrintingFromCompanion {

  private[messages] def toProtoV32: v32.CommitmentPeriod = v32.CommitmentPeriod(
    fromExclusive = fromExclusive.toProtoPrimitive,
    toInclusive = toInclusive.toProtoPrimitive,
  )

  override def prettyCompanion: PrettyPrintingCompanion[CommitmentPeriod] = CommitmentPeriod
}

object CommitmentPeriod extends PrettyPrintingCompanion[CommitmentPeriod] {

  override protected val pretty: Pretty[CommitmentPeriod] =
    prettyOfClass(
      param("fromExclusive", _.fromExclusive),
      param("toInclusive", _.toInclusive),
    )

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

  def tryCreate(fromExclusive: CantonTimestamp, toInclusive: CantonTimestamp): CommitmentPeriod =
    create(fromExclusive, toInclusive).valueOr(err =>
      throw new IllegalArgumentException(err.message)
    )

  implicit val intervalOpsCommitmentPeriod
      : MergeableIntervalOps.Aux[CommitmentPeriod, CantonTimestamp] =
    new MergeableIntervalOps[CommitmentPeriod] {
      override type Point = CantonTimestamp
      override def ordering: Ordering[CantonTimestamp] = implicitly
      override def pretty: Pretty[CommitmentPeriod] = implicitly

      override def startExclusive(period: CommitmentPeriod): CantonTimestamp = period.fromExclusive
      override def endInclusive(period: CommitmentPeriod): CantonTimestamp = period.toInclusive

      override def split(
          period: CommitmentPeriod,
          startExclusive: CantonTimestamp,
          endInclusive: CantonTimestamp,
      ): Seq[CommitmentPeriod] = {
        val leftRemainder = Option.when(startExclusive > period.fromExclusive)(
          period.copy(toInclusive = startExclusive)
        )
        val rightRemainder = Option.when(endInclusive < period.toInclusive)(
          period.copy(fromExclusive = endInclusive)
        )
        leftRemainder.toList ++ rightRemainder.toList
      }

      override def merge(x: CommitmentPeriod, y: CommitmentPeriod): CommitmentPeriod = {
        val start = x.fromExclusive.min(y.fromExclusive)
        val end = x.toInclusive.max(y.toInclusive)
        CommitmentPeriod(start, end)
      }
    }
}
