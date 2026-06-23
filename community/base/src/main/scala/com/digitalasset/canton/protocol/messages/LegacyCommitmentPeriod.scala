// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.time.PositiveSeconds
import slick.jdbc.{GetResult, GetTupleResult}

import scala.math.Ordering.Implicits.*

final case class LegacyCommitmentPeriod(
    fromExclusive: CantonTimestampSecond,
    periodLength: PositiveSeconds,
) extends PrettyPrinting {
  val toInclusive: CantonTimestampSecond = fromExclusive + periodLength

  def overlaps(other: LegacyCommitmentPeriod): Boolean =
    fromExclusive < other.toInclusive && toInclusive > other.fromExclusive

  override protected def pretty: Pretty[LegacyCommitmentPeriod] =
    prettyOfClassWithName("CommitmentPeriod")(
      param("fromExclusive", _.fromExclusive),
      param("toInclusive", _.toInclusive),
    )
}

object LegacyCommitmentPeriod {
  def create(
      fromExclusive: CantonTimestamp,
      periodLength: PositiveSeconds,
      interval: PositiveSeconds,
  ): Either[String, LegacyCommitmentPeriod] = for {
    from <- CantonTimestampSecond.fromCantonTimestamp(fromExclusive)
    _ <- Either.cond(
      periodLength.unwrap >= interval.unwrap || from == CantonTimestampSecond.MinValue,
      (),
      s"The period must be at least as large as the interval or start at MinValue, but is $periodLength and the interval is $interval",
    )
    _ <- Either.cond(
      from.getEpochSecond % interval.unwrap.getSeconds == 0 || from == CantonTimestampSecond.MinValue,
      (),
      s"The commitment period must start at a commitment tick or at MinValue, but it starts on $from, and the tick interval is $interval",
    )
    toInclusive = from + periodLength
    _ <- Either.cond(
      toInclusive.getEpochSecond % interval.unwrap.getSeconds == 0,
      (),
      s"The commitment period must end at a commitment tick, but it ends on $toInclusive, and the tick interval is $interval",
    )
  } yield LegacyCommitmentPeriod(
    fromExclusive = from,
    periodLength = periodLength,
  )

  def create(
      fromExclusive: CantonTimestamp,
      toInclusive: CantonTimestamp,
      interval: PositiveSeconds,
  ): Either[String, LegacyCommitmentPeriod] =
    PositiveSeconds
      .between(fromExclusive, toInclusive)
      .flatMap(LegacyCommitmentPeriod.create(fromExclusive, _, interval))

  def create(
      fromExclusive: CantonTimestampSecond,
      toInclusive: CantonTimestampSecond,
  ): Either[String, LegacyCommitmentPeriod] =
    PositiveSeconds
      .between(fromExclusive, toInclusive)
      .map(LegacyCommitmentPeriod(fromExclusive, _))

  implicit val getCommitmentPeriod: GetResult[LegacyCommitmentPeriod] =
    new GetTupleResult[(CantonTimestampSecond, CantonTimestampSecond)](
      GetResult[CantonTimestampSecond],
      GetResult[CantonTimestampSecond],
    ).andThen { case (from, to) =>
      PositiveSeconds
        .between(from, to)
        .map(LegacyCommitmentPeriod(from, _))
        .valueOr(err => throw new DbDeserializationException(err))
    }

}
