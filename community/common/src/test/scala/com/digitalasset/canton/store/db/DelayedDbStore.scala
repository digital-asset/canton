// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.syntax.functorFilter.*
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction.{All, ReadTransactional}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** DbStorage instance that allows delaying some operations.
  *
  * An entry `(operationName, n)` indicates that the n-th time the operation `operationName`
  * is run, then its completion will be delayed.
  * Note: first time an operation is executed correspond to `n=0`.
  */
class DelayedDbStore(
    val underlying: DbStorage,
    delayAfterRead: Map[(String, Int), Future[Unit]],
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override protected implicit val ec: ExecutionContext)
    extends DbStorage
    with NamedLogging {
  override val profile: DbStorage.Profile = underlying.profile
  override def metrics: DbStorageMetrics = underlying.metrics
  override val dbConfig: DbConfig = underlying.dbConfig

  // Counter the number of times a read was done for a particular operation
  private val readCallsCounter = TrieMap[String, Int]()

  private def updateCounter(current: Option[Int]): Option[Int] =
    Some(current.fold(1)(_ + 1))

  override protected[canton] def runRead[A](
      action: ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] = {

    val newCounter = readCallsCounter.updateWith(operationName)(updateCounter).getOrElse(1)
    val currentCounter = newCounter - 1

    logger.debug(
      s"Calling read for operation `$operationName`, counter updated from $currentCounter to $newCounter"
    )

    val delayingFuture = delayAfterRead.getOrElse((operationName, currentCounter), Future.unit)

    for {
      res <- underlying.runRead(action, operationName, maxRetries)
      _ <- delayingFuture
    } yield res
  }

  override protected[canton] def runWrite[A](
      action: All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    underlying.runWrite(action, operationName, maxRetries)

  override def isActive: Boolean = underlying.isActive

  /** Non-empty return may indicate a programming error. */
  def uncalledReads(): Seq[(String, Int)] = {
    delayAfterRead.keys.toSeq.mapFilter { case (operation, index) =>
      val wasCalled = readCallsCounter.get(operation).fold(true)(_ > index)

      Option.when(!wasCalled)((operation, index))
    }
  }
}
