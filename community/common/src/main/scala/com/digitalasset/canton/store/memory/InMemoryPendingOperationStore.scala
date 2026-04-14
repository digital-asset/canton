// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.store.PendingOperation.ConflictingPendingOperationError
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.Synchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasProtocolVersionedWrapper, VersioningCompanion}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.Try

class InMemoryPendingOperationStore[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer](
    override protected val opCompanion: VersioningCompanion[Op],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends PendingOperationStore[Op, SId]
    with NamedLogging {

  // Allows tests to bypass validation and insert malformed data into the store
  @VisibleForTesting
  private[memory] val store =
    TrieMap.empty[
      (SId, String, NonEmptyString),
      InMemoryPendingOperationStore.StoredPendingOperation[SId],
    ]

  override def insert(
      operation: PendingOperation[Op, SId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConflictingPendingOperationError, Unit] =
    EitherT.fromEither[FutureUnlessShutdown] {
      val serializedOp =
        InMemoryPendingOperationStore.StoredPendingOperation.fromPendingOperation(operation)
      val storedOperationO = store.updateWith(operation.compositeKey) {
        case Some(existingSerializedOp) => Some(existingSerializedOp)
        case None => Some(serializedOp)
      }

      storedOperationO match {
        case Some(existingSerializedOp) if existingSerializedOp != serializedOp =>
          val error = ConflictingPendingOperationError(
            operation.synchronizer,
            operation.key,
            operation.name,
          )
          logger.info(error.message)
          Left(error)
        case Some(_) => Right(())
        case None =>
          throw new IllegalStateException(
            s"Pending operation ${operation.key} was either removed from or never inserted into the in-memory store."
          )
      }

    }

  override def updateOperation(
      operation: Op,
      synchronizer: SId,
      name: NonEmptyString,
      key: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    store
      .updateWith((synchronizer, key, name))(
        _.map(_.copy(serializedOperation = operation.toByteString))
      )
      .discard
    FutureUnlessShutdown.unit
  }

  override def delete(
      synchronizer: SId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    store.remove((synchronizer, operationKey, operationName)).discard
    FutureUnlessShutdown.unit
  }

  override def get(
      synchronizer: SId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PendingOperation[Op, SId]] = {
    val resultF = FutureUnlessShutdown.fromTry(Try {
      store
        .get((synchronizer, operationKey, operationName))
        .map(_.tryToPendingOperation(opCompanion))
    })
    OptionT(resultF)
  }

  override def getAll(
      operationName: NonEmptyString,
      synchronizerId: Option[SId] = None,
      operationKey: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[PendingOperation[Op, SId]]] = FutureUnlessShutdown.pure {
    store.iterator.collect {
      case ((sync, key, `operationName`), op)
          if synchronizerId.forall(_ == sync) && operationKey.forall(_ == key) =>
        op.tryToPendingOperation(opCompanion)
    }.toSet
  }

  override def close(): Unit = ()
}

object InMemoryPendingOperationStore {

  /*
   * The following members are exposed with `private[memory]` visibility for testing only.
   * This allows tests to bypass validation and insert malformed data to verify
   * the store's behavior when reading corrupt records.
   */
  @VisibleForTesting
  private[memory] final case class StoredPendingOperation[SId <: Synchronizer](
      synchronizer: SId,
      key: String,
      name: String,
      serializedOperation: ByteString,
  ) {

    /** @throws DbDeserializationException
      *   Intentionally mimics the behaviour of the database persistence in order to fulfill the
      *   stated store API contract.
      */
    def tryToPendingOperation[Op <: HasProtocolVersionedWrapper[Op]](
        opCompanion: VersioningCompanion[Op]
    ): PendingOperation[Op, SId] =
      PendingOperation
        .create(
          name,
          key,
          serializedOperation,
          opCompanion.fromTrustedByteString,
          synchronizer,
        )
        .valueOr(errorMessage => throw new DbDeserializationException(errorMessage))
  }

  @VisibleForTesting
  private[memory] object StoredPendingOperation {
    def fromPendingOperation[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer](
        po: PendingOperation[Op, SId]
    ): StoredPendingOperation[SId] =
      StoredPendingOperation(
        po.synchronizer,
        po.key,
        po.name.unwrap,
        po.operation.toByteString,
      )
  }
}
