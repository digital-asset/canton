// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import cats.data.OptionT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import org.slf4j.event.Level
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

private[bftordering] object Miscellaneous {

  // The Canton storage implementations will only retry retriable errors anyway, such as the executor being
  //  overloaded and rejecting a task, which must be retried indefinitely until the task is accepted.
  //  This retry value is the one also used in higher-level operations implemented by `DbStorage`, such as
  //  `query` or `update`.
  private val StorageRetries: Int = retry.Forever

  // Better name for `storage.queryAndUpdate` for our use case; we cannot use `update` because it assumes
  //  the operation is transactional, but bulk updates are not always transactional
  //  (even though they are in our use case) so the types don't work out.
  def updateBulk[A](
      storage: DbStorage,
      action: DBIOAction[A, NoStream, Effect.All],
      operationName: String,
      maxRetries: Int = StorageRetries,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    storage.runWrite(action, operationName, maxRetries)

  val TestBootstrapTopologyActivationTime: TopologyActivationTime =
    TopologyActivationTime(CantonTimestamp.MinValue)

  def abort(logger: TracedLogger, message: String)(implicit traceContext: TraceContext): Nothing = {
    logger.error(s"FATAL: $message", new RuntimeException(message))
    throw new RuntimeException(message)
  }

  def dequeueN[ElementT, NumberT](
      queue: mutable.Queue[ElementT],
      n: NumberT,
  )(implicit num: Numeric[NumberT]): Seq[ElementT] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var remaining = n
    queue.dequeueWhile { _ =>
      import num.*
      val left = remaining
      remaining = remaining - num.one
      left > num.zero
    }.toSeq
  }

  def objId(obj: Any): Int = System.identityHashCode(obj)

  def objIdC(obj: Any): String = s"[${obj.getClass.getName}@${objId(obj)}]"

  final case class ResultWithLogs[T](
      result: T,
      logs: (Level, () => String)*
  ) {

    def logAndExtract(logger: TracedLogger, prefix: => String)(implicit
        traceContext: TraceContext
    ): T = {
      logs.foreach { case (level, createAnnotation) =>
        lazy val text = prefix + createAnnotation()
        level match {
          case Level.ERROR => logger.error(s"[FATAL] $text")
          case Level.WARN => logger.warn(s"[UNEXPECTED] $text")
          case Level.INFO => logger.info(text)
          case Level.DEBUG => logger.debug(text)
          case Level.TRACE => logger.trace(text)
        }
      }
      result
    }
  }
  object ResultWithLogs {

    def prefixLogsWith(
        prefix: => String,
        logs: Seq[(Level, () => String)],
    ): Seq[(Level, () => String)] =
      logs.map { case (level, log) =>
        level -> (() => prefix + log())
      }
  }

  def toUnitFutureUS[X](optionT: OptionT[FutureUnlessShutdown, X])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Unit] =
    optionT.value.map(_ => ())
}
