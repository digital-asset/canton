// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.store.{CursorPrehead, CursorPreheadStore}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.Ordered.orderingToOrdered
import scala.concurrent.{ExecutionContext, Future}

class InMemoryCursorPreheadStore[Counter](protected val loggerFactory: NamedLoggerFactory)(implicit
    order: Ordering[Counter]
) extends CursorPreheadStore[Counter]
    with NamedLogging {

  override private[store] implicit val ec: ExecutionContext = DirectExecutionContext(logger)

  private val preheadRef = new AtomicReference[Option[CursorPrehead[Counter]]](None)

  override def prehead(implicit
      traceContext: TraceContext
  ): Future[Option[CursorPrehead[Counter]]] =
    Future.successful(preheadRef.get())

  @VisibleForTesting
  private[canton] override def overridePreheadUnsafe(newPrehead: Option[CursorPrehead[Counter]])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(preheadRef.set(newPrehead))

  override def advancePreheadToTransactionalStoreUpdate(
      newPrehead: CursorPrehead[Counter]
  )(implicit traceContext: TraceContext): TransactionalStoreUpdate =
    TransactionalStoreUpdate.InMemoryTransactionalStoreUpdate {
      val _ = preheadRef.getAndUpdate {
        case None => Some(newPrehead)
        case old @ Some(oldPrehead) =>
          if (oldPrehead.counter < newPrehead.counter) Some(newPrehead) else old
      }
    }

  override def rewindPreheadTo(
      newPreheadO: Option[CursorPrehead[Counter]]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info(s"Rewinding prehead to $newPreheadO")
    newPreheadO match {
      case None => preheadRef.set(None)
      case Some(newPrehead) =>
        val _ = preheadRef.getAndUpdate {
          case None => None
          case old @ Some(oldPrehead) =>
            if (oldPrehead.counter > newPrehead.counter) Some(newPrehead) else old
        }
    }
    Future.unit
  }
}
