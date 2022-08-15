// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.TimeoutDuration.maxTimeout
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.FutureUtil
import com.google.common.annotations.VisibleForTesting
import io.circe.Encoder
import org.slf4j.event.Level

import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration._

/** Duration class used for timeouts.
  *
  * There are two options: either it's a non-negative duration or an infinite duration
  */
case class TimeoutDuration(duration: Duration) {
  def unwrap: Duration = duration
  require(duration >= Duration.Zero, s"Duration ${duration} is negative")
  // Make sure that the finite approximation exists.
  asFiniteApproximation

  def minusSeconds(s: Int): TimeoutDuration = TimeoutDuration(
    duration.minus(Duration.fromNanos(s * 10e9))
  )
  def plusSeconds(s: Int): TimeoutDuration = TimeoutDuration(
    duration.plus(Duration.fromNanos(s * 10e9))
  )

  def multipliedBy(i: Int): TimeoutDuration = TimeoutDuration(duration * i.toDouble)

  def asJavaApproximation: JDuration = JDuration.ofMillis(asFiniteApproximation.toMillis)

  def asFiniteApproximation: FiniteDuration = duration match {
    case fd: FiniteDuration => fd
    case Duration.Inf => maxTimeout
    case _ =>
      throw new IllegalArgumentException(s"Duration must be non-negative, but is $duration.")
  }

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

object TimeoutDuration {
  val Zero: TimeoutDuration = TimeoutDuration(Duration.Zero)

  implicit val timeoutDurationEncoder: Encoder[TimeoutDuration] =
    Encoder[String].contramap(_.unwrap.toString)

  def fromDuration(duration: Duration): Either[String, TimeoutDuration] = duration match {
    case x: FiniteDuration =>
      Either.cond(x.length >= 0, TimeoutDuration(x), s"Duration ${x} is negative!")
    case Duration.Inf => Right(TimeoutDuration(Duration.Inf))
    case x => Left(s"Duration ${x} is not a valid duration that can be used for timeouts.")
  }

  def tryFromDuration(duration: Duration): TimeoutDuration = fromDuration(duration) match {
    case Left(err) => throw new IllegalArgumentException(err)
    case Right(x) => x
  }

  def tryFromJavaDuration(duration: java.time.Duration): TimeoutDuration =
    tryFromDuration(Duration.fromNanos(duration.toNanos))

  def ofMillis(millis: Long): TimeoutDuration =
    TimeoutDuration(Duration(millis, TimeUnit.MILLISECONDS))

  def ofSeconds(secs: Long): TimeoutDuration =
    TimeoutDuration(Duration(secs, TimeUnit.SECONDS))

  def ofMinutes(minutes: Long): TimeoutDuration =
    TimeoutDuration(Duration(minutes, TimeUnit.MINUTES))

  def ofHours(hours: Long): TimeoutDuration =
    TimeoutDuration(Duration(hours, TimeUnit.HOURS))

  def ofDays(days: Long): TimeoutDuration =
    TimeoutDuration(Duration(days, TimeUnit.DAYS))

  /** Very long timeout that is still within the limits of `FiniteDuration`.
    */
  val maxTimeout: FiniteDuration = 100000.days
}

/** Configuration for internal await timeouts
  *
  * @param unbounded timeout on how long "unbounded" operations can run. should be infinite in theory.
  * @param io timeout for disk based operations
  * @param default default finite processing timeout
  * @param network timeout for things related to networking
  * @param shutdownProcessing timeout used for shutdown of some processing where we'd like to keep the result (long)
  * @param shutdownNetwork timeout used for shutdown where we interact with some remote system
  * @param shutdownShort everything else shutdown releated (default)
  * @param closing our closing time (which should be strictly larger than any of the shutdown values)
  * @param verifyActive how long should we wait for the domain to tell us whether we are active or not
  * @param inspection timeout for the storage inspection commands (can run a long long time)
  * @param storageMaxRetryInterval max retry interval for storage
  * @param activeInit how long a passive replica should wait for the initialization by the active replica
  * @param slowFutureWarn when using future supervision, when should we start to warn about a slow future
  */
final case class ProcessingTimeout(
    unbounded: TimeoutDuration = DefaultProcessingTimeouts.unbounded,
    io: TimeoutDuration = DefaultProcessingTimeouts.io,
    default: TimeoutDuration = DefaultProcessingTimeouts.default,
    network: TimeoutDuration = DefaultProcessingTimeouts.network,
    shutdownProcessing: TimeoutDuration = DefaultProcessingTimeouts.shutdownProcessing,
    shutdownNetwork: TimeoutDuration = DefaultProcessingTimeouts.shutdownNetwork,
    shutdownShort: TimeoutDuration = DefaultProcessingTimeouts.shutdownShort,
    closing: TimeoutDuration = DefaultProcessingTimeouts.closing,
    inspection: TimeoutDuration = DefaultProcessingTimeouts.inspection,
    storageMaxRetryInterval: TimeoutDuration = DefaultProcessingTimeouts.maxRetryInterval,
    verifyActive: TimeoutDuration = DefaultProcessingTimeouts.verifyActive,
    activeInit: TimeoutDuration = DefaultProcessingTimeouts.activeInit,
    slowFutureWarn: TimeoutDuration = DefaultProcessingTimeouts.slowFutureWarn,
)

/** Reasonable default timeouts */
object DefaultProcessingTimeouts {
  val unbounded: TimeoutDuration = TimeoutDuration.tryFromDuration(Duration.Inf)

  /** Allow unbounded processing for io operations. This is because we retry forever upon db outages.
    */
  val io: TimeoutDuration = unbounded

  val default: TimeoutDuration = TimeoutDuration.tryFromDuration(1.minute)

  val network: TimeoutDuration = TimeoutDuration.tryFromDuration(2.minute)

  val shutdownNetwork: TimeoutDuration = TimeoutDuration.tryFromDuration(9.seconds)

  val shutdownProcessing: TimeoutDuration = TimeoutDuration.tryFromDuration(60.seconds)

  val shutdownShort: TimeoutDuration = TimeoutDuration.tryFromDuration(3.seconds)

  val closing: TimeoutDuration = TimeoutDuration.tryFromDuration(10.seconds)

  val inspection: TimeoutDuration = TimeoutDuration.tryFromDuration(Duration.Inf)

  val maxRetryInterval: TimeoutDuration = TimeoutDuration.tryFromDuration(10.seconds)

  val verifyActive: TimeoutDuration = TimeoutDuration.tryFromDuration(10.seconds)

  val activeInit: TimeoutDuration = TimeoutDuration.tryFromDuration(1.minute)

  val warnUnbounded: TimeoutDuration = TimeoutDuration.tryFromDuration(30.seconds)

  val slowFutureWarn: TimeoutDuration = TimeoutDuration.tryFromDuration(5.seconds)

  @VisibleForTesting
  lazy val testing: ProcessingTimeout = ProcessingTimeout()

}
