// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.config.{
  NonNegativeFiniteDuration => NonNegativeFiniteDurationConfig,
  PositiveDurationSeconds => ConfigPositiveSeconds,
}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{DurationConverter, ParsingResult}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.duration.{Duration => PbDuration}
import io.circe.Encoder
import io.scalaland.chimney.Transformer
import slick.jdbc.{GetResult, SetParameter}

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

sealed trait RefinedDuration {
  def duration: Duration
  def unwrap: Duration = duration

  def toProtoPrimitive: com.google.protobuf.duration.Duration =
    DurationConverter.toProtoPrimitive(duration)

  def toScala: FiniteDuration = duration.toScala

  def getMicros: Long = duration.getSeconds * 1000000L + duration.getNano / 1000L
}

trait RefinedDurationCompanion[RD <: RefinedDuration] {

  /** Factory method for creating the [[RefinedDuration]] from a [[java.time.Duration]]
    * @throws java.lang.IllegalArgumentException if the duration does not satisfy the refinement predicate
    */
  protected def apply(duration: Duration): RD

  def create(duration: Duration): Either[String, RD] =
    Either.catchOnly[IllegalArgumentException](apply(duration)).leftMap(_.getMessage)

  def between(from: CantonTimestamp, to: CantonTimestamp): Either[String, RD] =
    create(Duration.between(from.toInstant, to.toInstant))

  def between(from: CantonTimestampSecond, to: CantonTimestampSecond): Either[String, RD] =
    create(Duration.between(from.toInstant, to.toInstant))

  implicit val orderingRefinedDuration: Ordering[RD] = Ordering.by(_.duration)

  def fromProtoPrimitive(
      field: String
  )(durationP: PbDuration): ParsingResult[RD] =
    for {
      duration <- DurationConverter.fromProtoPrimitive(durationP)
      refinedDuration <- create(duration).leftMap(err => ValueConversionError(field, err))
    } yield refinedDuration

  def fromProtoPrimitiveO(
      field: String
  )(durationPO: Option[PbDuration]): ParsingResult[RD] =
    for {
      durationP <- ProtoConverter.required(field, durationPO)
      refinedDuration <- fromProtoPrimitive(field)(durationP)
    } yield refinedDuration

  def fromBytes(field: String)(bytes: Array[Byte]): ParsingResult[RD] =
    for {
      durationP <- ProtoConverter.protoParserArray(PbDuration.parseFrom)(bytes)
      res <- fromProtoPrimitive(field)(durationP)
    } yield res

  private def getResultFromBytes(bytes: Array[Byte]) =
    fromBytes("database field")(bytes).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize the duration: $err")
    )

  // JSON encoding using circe
  implicit val refinedDurationEncoder: Encoder[RD] =
    Encoder.forProduct1("duration")(_.duration)

  implicit val getResultRefinedDuration: GetResult[RD] =
    GetResult(r => getResultFromBytes(r.nextBytes()))

  implicit val getResultRefinedDurationOption: GetResult[Option[RD]] =
    GetResult(r => r.nextBytesOption().map(getResultFromBytes))

  implicit def setParameterRefinedDuration(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[RD] =
    (d, pp) => pp >> d.toProtoPrimitive.toByteArray

  implicit def setParameterRefinedDurationOption(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[RD]] = (d, pp) => pp >> d.map(_.toProtoPrimitive.toByteArray)

  def ofMicros(micros: Long): RD =
    apply(Duration.ofSeconds(micros / 1000000).withNanos((micros % 1000000L).toInt * 1000))

  def ofMillis(millis: Long): RD = apply(Duration.ofMillis(millis))

  def ofSeconds(secs: Long): RD = apply(Duration.ofSeconds(secs))

  def ofMinutes(minutes: Long): RD = apply(Duration.ofMinutes(minutes))

  def ofHours(hours: Long): RD = apply(Duration.ofHours(hours))

  def ofDays(days: Long): RD = apply(Duration.ofDays(days))
}

final case class PositiveFiniteDuration(duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative && !duration.isZero, s"Duration $duration must not be non-negative")

  override def pretty: Pretty[PositiveFiniteDuration] = prettyOfParam(_.duration)

  /** Returns the duration in seconds truncated to the size of Int, returns as a maximum Int.MaxValue.
    *
    * Usage: On the database/jdbc level many timeouts require to be specified in seconds as an integer, not a long.
    */
  def toSecondsTruncated(
      logger: TracedLogger
  )(implicit traceContext: TraceContext): PositiveNumeric[Int] = {
    val seconds = duration.getSeconds

    val result = if (seconds > Int.MaxValue) {
      logger.info(s"Truncating $duration to integer")
      Int.MaxValue
    } else
      seconds.toInt

    // Result must be positive due to assertion on duration
    checked(PositiveNumeric.tryCreate(result))
  }
}

object PositiveFiniteDuration extends RefinedDurationCompanion[PositiveFiniteDuration]

final case class NonNegativeFiniteDuration(duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative, s"Duration $duration must not be negative")

  override def pretty: Pretty[NonNegativeFiniteDuration] = prettyOfParam(_.duration)

  def +(other: NonNegativeFiniteDuration): NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    duration.plus(other.duration)
  )

  def *(multiplicand: NonNegativeInt): NonNegativeFiniteDuration = NonNegativeFiniteDuration(
    duration.multipliedBy(multiplicand.value.toLong)
  )

  def toConfig: NonNegativeFiniteDurationConfig = checked(
    NonNegativeFiniteDurationConfig.tryFromJavaDuration(duration)
  )
}

object NonNegativeFiniteDuration extends RefinedDurationCompanion[NonNegativeFiniteDuration] {
  implicit val forgetRefinementJDuration: Transformer[NonNegativeFiniteDuration, Duration] =
    _.duration
  implicit val forgetRefinementFDuration: Transformer[NonNegativeFiniteDuration, FiniteDuration] =
    _.toScala

  implicit val toNonNegativeDurationConfig
      : Transformer[NonNegativeFiniteDuration, NonNegativeFiniteDurationConfig] = _.toConfig

  val Zero: NonNegativeFiniteDuration = NonNegativeFiniteDuration(Duration.ZERO)
}

final case class NonNegativeSeconds(duration: Duration)
    extends RefinedDuration
    with PrettyPrinting {
  require(!duration.isNegative, s"Duration $duration must not be negative")
  require(duration.getNano == 0, s"Duration $duration must be rounded to the second")

  override def pretty: Pretty[NonNegativeSeconds.this.type] = prettyOfParam(_.duration)
}

object NonNegativeSeconds extends RefinedDurationCompanion[NonNegativeSeconds] {
  val Zero: NonNegativeSeconds = NonNegativeSeconds(Duration.ZERO)
}

final case class PositiveSeconds(duration: Duration) extends RefinedDuration with PrettyPrinting {
  require(!duration.isNegative && !duration.isZero, s"Duration $duration must be positive")
  require(duration.getNano == 0, s"Duration $duration must be rounded to the second")

  override def pretty: Pretty[PositiveSeconds.this.type] = prettyOfParam(_.duration)

  def toConfig: ConfigPositiveSeconds = checked(ConfigPositiveSeconds.tryFromJavaDuration(duration))
}

object PositiveSeconds extends RefinedDurationCompanion[PositiveSeconds] {
  implicit val toPositiveSecondsConfig: Transformer[PositiveSeconds, ConfigPositiveSeconds] =
    _.toConfig
}
