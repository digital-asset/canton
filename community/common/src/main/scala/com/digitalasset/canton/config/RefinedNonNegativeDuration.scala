// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.time.{
  NonNegativeFiniteDuration => DomainNonNegativeFiniteDuration,
  PositiveSeconds => DomainPositiveSeconds,
}
import com.digitalasset.canton.util.FutureUtil
import io.circe.Encoder
import org.slf4j.event.Level

import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration._

trait RefinedNonNegativeDuration[D <: RefinedNonNegativeDuration[D]] extends PrettyPrinting {
  this: {
    def update(newDuration: Duration): D
  } =>

  override def pretty: Pretty[RefinedNonNegativeDuration[D]] = prettyOfParam(_.duration)

  def duration: Duration

  def unwrap: Duration = duration

  def asFiniteApproximation: FiniteDuration

  def asJavaApproximation: JDuration = JDuration.ofMillis(asFiniteApproximation.toMillis)
  def minusSeconds(s: Int): D = update(duration.minus(Duration.fromNanos(s * 10e9)))

  def +(other: D): D = update(duration.plus(other.duration))
  def plusSeconds(s: Int): D = update(duration.plus(Duration.fromNanos(s * 10e9)))

  def multipliedBy(i: Int): D = update(duration * i.toDouble)
  def *(i: NonNegativeInt): D = update(duration * i.value.toDouble)

  def retries(interval: Duration): Int = {
    if (interval.isFinite && duration.isFinite)
      Math.max(0, duration.toMillis / Math.max(1, interval.toMillis)).toInt
    else Int.MaxValue
  }

  /** Same as Await.result, but with this timeout */
  def await[F](
      description: => String = "",
      logFailing: Option[Level] = None,
  )(fut: Future[F])(implicit loggingContext: ErrorLoggingContext): F = {
    FutureUtil.noisyAwaitResult(
      logFailing.fold(fut)(level => FutureUtil.logOnFailure(fut, description, level = level)),
      description,
      timeout = duration,
    )
  }

  /** Same as await, but not returning a value */
  def await_(
      description: => String = "",
      logFailing: Option[Level] = None,
  )(fut: Future[_])(implicit loggingContext: ErrorLoggingContext): Unit = {
    await(description, logFailing)(fut).discard
  }

  /** Await the completion of `future`. Log a message if the future does not complete within `timeout`.
    * If the `future` fails with an exception within `timeout`, this method rethrows the exception.
    *
    * @return The completed value of `future` if it successfully completes in time.
    */
  def valueOrLog[T](description: => String = "", level: Level = Level.WARN)(fut: => Future[T])(
      implicit loggingContext: ErrorLoggingContext
  ): Option[T] =
    FutureUtil.valueOrLog(fut, description, duration, level)

  def toProtoPrimitive: com.google.protobuf.duration.Duration = {
    val d = asJavaApproximation
    com.google.protobuf.duration.Duration(d.getSeconds, d.getNano)
  }
}

trait RefinedNonNegativeDurationCompanion[D <: RefinedNonNegativeDuration[D]] {
  this: {
    def apply(newDuration: Duration): D
  } =>

  implicit val timeoutDurationEncoder: Encoder[D] =
    Encoder[String].contramap(_.unwrap.toString)

  implicit val orderingRefinedDuration: Ordering[D] = Ordering.by(_.duration)

  def fromDuration(duration: Duration): Either[String, D]

  def tryFromDuration(duration: Duration): D = fromDuration(duration) match {
    case Left(err) => throw new IllegalArgumentException(err)
    case Right(x) => x
  }

  def tryFromJavaDuration(duration: java.time.Duration): D =
    tryFromDuration(Duration.fromNanos(duration.toNanos))

  def ofMillis(millis: Long): D = apply(Duration(millis, TimeUnit.MILLISECONDS))

  def ofSeconds(secs: Long): D = apply(Duration(secs, TimeUnit.SECONDS))

  def ofMinutes(minutes: Long): D = apply(Duration(minutes, TimeUnit.MINUTES))

  def ofHours(hours: Long): D = apply(Duration(hours, TimeUnit.HOURS))

  def ofDays(days: Long): D = apply(Duration(days, TimeUnit.DAYS))
}

/** Duration class used for non-negative durations.
  *
  * There are two options: either it's a non-negative duration or an infinite duration
  */
final case class NonNegativeDuration(duration: Duration)
    extends RefinedNonNegativeDuration[NonNegativeDuration] {
  require(duration >= Duration.Zero, s"Duration ${duration} is negative")
  // Make sure that the finite approximation exists.
  asFiniteApproximation

  def update(newDuration: Duration): NonNegativeDuration = NonNegativeDuration(newDuration)

  def asFiniteApproximation: FiniteDuration = duration match {
    case fd: FiniteDuration => fd
    case Duration.Inf => NonNegativeDuration.maxTimeout
    case _ =>
      throw new IllegalArgumentException(s"Duration must be non-negative, but is $duration.")
  }
}

object NonNegativeDuration extends RefinedNonNegativeDurationCompanion[NonNegativeDuration] {
  val maxTimeout: FiniteDuration = 100000.days
  val Zero: NonNegativeDuration = NonNegativeDuration(Duration.Zero)

  def fromDuration(duration: Duration): Either[String, NonNegativeDuration] = duration match {
    case x: FiniteDuration =>
      Either.cond(x.length >= 0, NonNegativeDuration(x), s"Duration ${x} is negative!")
    case Duration.Inf => Right(NonNegativeDuration(Duration.Inf))
    case x => Left(s"Duration ${x} is not a valid duration that can be used for timeouts.")
  }
}

/** Duration class used for non-negative finite durations. */
final case class NonNegativeFiniteDuration(underlying: FiniteDuration)
    extends RefinedNonNegativeDuration[NonNegativeFiniteDuration] {

  require(underlying >= Duration.Zero, s"Duration ${duration} is negative")
  // Make sure that the finite approximation exists.
  asFiniteApproximation

  def duration: Duration = underlying
  def asJava: JDuration = JDuration.ofNanos(duration.toNanos)

  def update(newDuration: Duration): NonNegativeFiniteDuration = newDuration match {
    case _: Duration.Infinite =>
      throw new IllegalArgumentException(s"Duration must be finite, but is Duration.Inf")
    case duration: FiniteDuration => NonNegativeFiniteDuration(duration)
  }

  def asFiniteApproximation: FiniteDuration = underlying

  private[canton] def toDomain: DomainNonNegativeFiniteDuration = DomainNonNegativeFiniteDuration(
    asJava
  )
}

object NonNegativeFiniteDuration
    extends RefinedNonNegativeDurationCompanion[NonNegativeFiniteDuration] {
  val Zero: NonNegativeFiniteDuration = NonNegativeFiniteDuration(Duration.Zero)

  def apply(duration: Duration): NonNegativeFiniteDuration = NonNegativeFiniteDuration
    .fromDuration(duration)
    .fold(err => throw new IllegalArgumentException(err), identity)

  def fromDuration(duration: Duration): Either[String, NonNegativeFiniteDuration] = duration match {
    case x: FiniteDuration =>
      Either.cond(x.length >= 0, NonNegativeFiniteDuration(x), s"Duration $x is negative!")
    case Duration.Inf => Left(s"Expecting finite duration but found Duration.Inf")
    case x => Left(s"Duration $x is not a valid duration that can be used for timeouts.")
  }
}

/** Duration class used for positive durations that are rounded to the second. */
final case class PositiveDurationRoundedSeconds(underlying: FiniteDuration)
    extends RefinedNonNegativeDuration[PositiveDurationRoundedSeconds] {

  require(underlying > Duration.Zero, s"Duration ${duration} is not positive")
  require(
    PositiveDurationRoundedSeconds.isRoundedToTheSecond(underlying),
    s"Duration ${duration} is not rounded to the second",
  )

  // Make sure that the finite approximation exists.
  asFiniteApproximation

  def duration: Duration = underlying
  def asJava: JDuration = JDuration.ofNanos(duration.toNanos)

  def update(newDuration: Duration): PositiveDurationRoundedSeconds = newDuration match {
    case _: Duration.Infinite =>
      throw new IllegalArgumentException(s"Duration must be finite, but is Duration.Inf")
    case duration: FiniteDuration => PositiveDurationRoundedSeconds(duration)
  }

  def asFiniteApproximation: FiniteDuration = underlying

  private[canton] def toDomain: DomainPositiveSeconds = DomainPositiveSeconds(
    asJava
  )
}

object PositiveDurationRoundedSeconds
    extends RefinedNonNegativeDurationCompanion[PositiveDurationRoundedSeconds] {
  private def isRoundedToTheSecond(duration: FiniteDuration): Boolean =
    duration == Duration(duration.toSeconds, SECONDS)

  def apply(duration: Duration): PositiveDurationRoundedSeconds = PositiveDurationRoundedSeconds
    .fromDuration(duration)
    .fold(err => throw new IllegalArgumentException(err), identity)

  def fromDuration(duration: Duration): Either[String, PositiveDurationRoundedSeconds] =
    duration match {
      case x: FiniteDuration =>
        for {
          _ <- Either.cond(x.length > 0, (), s"Duration $x is not positive")
          _ <- Either.cond(
            isRoundedToTheSecond(x),
            (),
            s"Duration ${duration} is not rounded to the second",
          )
        } yield PositiveDurationRoundedSeconds(x)
      case Duration.Inf => Left(s"Expecting finite duration but found Duration.Inf")
      case x => Left(s"Duration $x is not a valid duration that can be used for timeouts.")
    }
}
