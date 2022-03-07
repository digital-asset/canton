// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Storage for a cursor prehead. */
trait CursorPreheadStore[Counter] extends AutoCloseable {
  private[store] implicit def ec: ExecutionContext

  /** Gets the prehead of the cursor. */
  def prehead(implicit traceContext: TraceContext): Future[Option[CursorPrehead[Counter]]]

  /** Forces an update to the cursor prehead. Only use this for maintenance and testing. */
  // Cannot implement this method with advancePreheadTo/rewindPreheadTo
  // because it allows to atomically overwrite the timestamp associated with the current prehead.
  private[canton] def overridePreheadUnsafe(newPrehead: Option[CursorPrehead[Counter]])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Sets the prehead counter to `newPrehead` unless it is already at the same or a higher value.
    * The prehead counter should be set to the counter before the head of the corresponding cursor.
    */
  def advancePreheadTo(newPrehead: CursorPrehead[Counter])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    advancePreheadToTransactionalStoreUpdate(newPrehead).runStandalone()

  /** [[advancePreheadTo]] as a [[com.digitalasset.canton.resource.TransactionalStoreUpdate]] */
  def advancePreheadToTransactionalStoreUpdate(newPrehead: CursorPrehead[Counter])(implicit
      traceContext: TraceContext
  ): TransactionalStoreUpdate

  /** Sets the prehead counter to `newPreheadO` if it is currently set to a higher value. */
  def rewindPreheadTo(newPreheadO: Option[CursorPrehead[Counter]])(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

/** Information for the prehead of a cursor.
  * The prehead of a cursor is the counter before the cursors' head, if any.
  *
  * @param counter The counter corresponding to the prehead
  * @param timestamp The timestamp corresponding to the prehead
  */
case class CursorPrehead[+Counter](counter: Counter, timestamp: CantonTimestamp)

object CursorPrehead {
  // CursorPrehead cannot extend `PrettyPrinting` because the type variable Counter is covariant
  implicit def prettyCursorHead[Counter: Pretty]: Pretty[CursorPrehead[Counter]] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil._
    prettyOfClass(
      param("counter", _.counter),
      param("timestamp", _.timestamp),
    )
  }
}
