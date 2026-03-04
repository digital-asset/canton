// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.PendingOperation.ConflictingPendingOperationError
import com.digitalasset.canton.store.db.DbPendingOperationsStore
import com.digitalasset.canton.store.memory.InMemoryPendingOperationStore
import com.digitalasset.canton.topology.Synchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasProtocolVersionedWrapper, VersioningCompanion}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** @tparam Op
  *   A protobuf message that implements
  *   [[com.digitalasset.canton.version.HasProtocolVersionedWrapper]] that contains the relevant
  *   data for executing the pending operation.
  */
trait PendingOperationStore[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer] {

  protected def opCompanion: VersioningCompanion[Op]

  /** Atomically stores a pending operation, returning an error if a conflicting operation already
    * exists.
    *
    * This check-and-insert operation is performed within a serializable transaction to prevent race
    * conditions. The behavior depends on whether an operation with the same unique key
    * (`synchronizerId`, `key`, `name`) already exists in the store:
    *   - If no operation with the key exists, the new operation is inserted.
    *   - If an '''identical''' operation already exists, the operation succeeds without making
    *     changes.
    *   - If an operation with the same key but '''different''' data exists, the operation fails
    *     with an error.
    *
    * @param operation
    *   The `PendingOperation` to insert.
    * @param traceContext
    *   The context for tracing and logging.
    * @return
    *   An `EitherT` that completes with:
    *   - `Right(())` if the operation was successfully stored or an identical one already existed.
    *   - `Left(ConflictingPendingOperationError)` if a conflicting operation was found.
    */
  def insert(operation: PendingOperation[Op, SId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConflictingPendingOperationError, Unit]

  /** Updates a pending operation identified by its unique composite key (`synchronizerId`,
    * `operationKey`, `operationName`) if such a pending operation already exists, else no update is
    * applied.
    *
    * @param operation
    *   The new value of the operation to update.
    * @param synchronizer
    *   The synchronizer id (logical or physical) scoping the operation application.
    * @param operationName
    *   The name of the operation to be executed.
    * @param operationKey
    *   A key to distinguish between multiple instances of the same operation.
    * @return
    *   A future that completes when the update has finished.
    */
  def updateOperation(
      operation: Op,
      synchronizer: SId,
      operationName: NonEmptyString,
      operationKey: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Deletes a pending operation identified by its unique composite key (`synchronizerId`,
    * `operationKey`, `operationName`).
    *
    * This operation is '''idempotent'''. It succeeds regardless of whether the record existed prior
    * to the call.
    *
    * @param synchronizer
    *   The synchronizer id (logical or physical) scoping the operation application.
    * @param operationKey
    *   A key to distinguish between multiple instances of the same operation.
    * @param operationName
    *   The name of the operation to be executed.
    * @param traceContext
    *   The context for tracing and logging.
    * @return
    *   A future that completes when the deletion has finished.
    */
  def delete(
      synchronizer: SId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Fetches a pending operation by its unique composite key (`synchronizerId`, `operationKey`,
    * `operationName`).
    *
    * @param synchronizer
    *   The synchronizer id (logical or physical) scoping the operation application.
    * @param operationKey
    *   A key to distinguish between multiple instances of the same operation.
    * @param operationName
    *   The name of the operation to be executed.
    * @param traceContext
    *   The context for tracing and logging.
    * @return
    *   A future that completes with `Some(operation)` if found and valid, `None` if not found, or
    *   fails with a `DbDeserializationException` if the stored data is corrupt.
    */
  def get(
      synchronizer: SId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): OptionT[FutureUnlessShutdown, PendingOperation[Op, SId]]

  /** Fetches all pending operations matching `operationName`.
    *
    * @param operationName
    *   The name of the operation to be executed.
    * @return
    *   A future that completes with `Set(operations)` of operations matching the above criteria,
    *   fails with a `DbDeserializationException` if the stored data is corrupt.
    */
  def getAll(
      operationName: NonEmptyString
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PendingOperation[Op, SId]]]
}

object PendingOperationStore {
  def apply[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer](
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      opCompanion: VersioningCompanion[Op],
      sidParser: String => Either[String, SId],
  )(implicit executionContext: ExecutionContext): PendingOperationStore[Op, SId] =
    storage match {
      case _: MemoryStorage => new InMemoryPendingOperationStore[Op, SId](opCompanion)
      case jdbc: DbStorage =>
        new DbPendingOperationsStore[Op, SId](jdbc, timeouts, loggerFactory, opCompanion, sidParser)
    }
}

/** @tparam Op
  *   A protobuf message that implements
  *   [[com.digitalasset.canton.version.HasProtocolVersionedWrapper]] that contains the relevant
  *   data for executing the pending operation.
  */
final case class PendingOperation[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer](
    name: NonEmptyString,
    key: String,
    operation: Op,
    synchronizer: SId,
) {
  private[store] def compositeKey: (SId, String, NonEmptyString) =
    (synchronizer, key, name)
}

object PendingOperation {

  private[store] def create[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer](
      name: String,
      key: String,
      operationBytes: ByteString,
      operationDeserializer: ByteString => ParsingResult[Op],
      synchronizer: SId,
  ): Either[String, PendingOperation[Op, SId]] =
    for {
      validName <- NonEmptyString
        .create(name)
        .leftMap(_ => s"Missing pending operation name (blank): $name")
      validOperation <- operationDeserializer(operationBytes).leftMap(error =>
        s"Failed to deserialize pending operation byte string: $error"
      )
    } yield PendingOperation(
      validName,
      key,
      validOperation,
      synchronizer,
    )

  /** Signals a failed attempt to insert a pending operation because it conflicts with an existing
    * one.
    *
    * A conflict occurs when an operation with the same unique key (`synchronizerId`, `key`, `name`)
    * already exists in the store but contains different data.
    *
    * @param synchronizer
    *   The synchronizer id (logical or physical) that owns the operation.
    * @param key
    *   The key that uniquely identifies the pending operation within its scope.
    * @param name
    *   The name describing the type of pending operation.
    */
  final case class ConflictingPendingOperationError(
      synchronizer: Synchronizer,
      key: String,
      name: NonEmptyString,
  )
}
