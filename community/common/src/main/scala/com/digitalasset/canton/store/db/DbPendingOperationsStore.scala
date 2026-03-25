// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.PendingOperation.ConflictingPendingOperationError
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.Synchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasProtocolVersionedWrapper, VersioningCompanion}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter, TransactionIsolation}

import scala.annotation.unused
import scala.concurrent.ExecutionContext

class DbPendingOperationsStore[Op <: HasProtocolVersionedWrapper[Op], SId <: Synchronizer](
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val opCompanion: VersioningCompanion[Op],
    sidParser: String => Either[String, SId],
)(implicit val executionContext: ExecutionContext)
    extends DbStore
    with PendingOperationStore[Op, SId] {

  import storage.api.*
  import storage.converters.*

  private implicit val getResultSId: GetResult[SId] =
    DbPendingOperationsStore.getResultSId(sidParser)
  private implicit val setParameterSId: SetParameter[SId] = (v: SId, pp) => pp >> v.toProtoPrimitive

  implicit val tryPendingOperationGetResult: GetResult[PendingOperation[Op, SId]] =
    DbPendingOperationsStore.tryGetPendingOperationResult(opCompanion.fromTrustedByteString)

  override def insert(
      operation: PendingOperation[Op, SId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ConflictingPendingOperationError, Unit] = {

    val readAction =
      sql"""
        select operation_name, operation_key, operation, synchronizer_id
        from common_pending_operations
        where synchronizer_id = ${operation.synchronizer}
        and operation_key = ${operation.key}
        and operation_name = ${operation.name.unwrap}
      """.as[PendingOperation[Op, SId]].headOption

    val transaction = readAction.flatMap {
      case Some(existingOperation) if existingOperation != operation =>
        DBIO.successful(
          Left(
            ConflictingPendingOperationError(
              operation.synchronizer,
              operation.key,
              operation.name,
            )
          )
        )

      case Some(_) => DBIO.successful(Right(()))

      case None =>
        @unused
        implicit val setParameter: SetParameter[Op] = (v: Op, pp) => pp >> v.toByteString

        sqlu"""
          insert into common_pending_operations
            (operation_name, operation_key, operation, synchronizer_id)
          values
            (
              ${operation.name.unwrap},
              ${operation.key},
              ${operation.operation},
              ${operation.synchronizer}
            )
        """.map(_ => Right(()))
    }

    EitherT(
      storage.queryAndUpdate(
        transaction.transactionally.withTransactionIsolation(TransactionIsolation.Serializable),
        functionFullName,
      )
    )
  }

  override def updateOperation(
      operation: Op,
      synchronizer: SId,
      name: NonEmptyString,
      key: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    @unused
    implicit val setParameter: SetParameter[Op] = (v: Op, pp) => pp >> v.toByteString
    val updateAction =
      sqlu"""
          update common_pending_operations
          set operation = $operation
          where synchronizer_id = $synchronizer and operation_key = $key and operation_name = ${name.unwrap}
        """
    storage.update_(updateAction, functionFullName)
  }

  override def delete(
      synchronizer: SId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val deleteAction =
      sqlu"""
        delete from common_pending_operations
        where synchronizer_id = $synchronizer
        and operation_key = $operationKey
        and operation_name = ${operationName.unwrap}
      """
    storage.update_(deleteAction, functionFullName)
  }

  override def get(
      synchronizer: SId,
      operationKey: String,
      operationName: NonEmptyString,
  )(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PendingOperation[Op, SId]] = {
    val selectAction =
      sql"""
        select operation_name, operation_key, operation, synchronizer_id
        from common_pending_operations
        where synchronizer_id = $synchronizer
        and operation_key = $operationKey
        and operation_name = ${operationName.unwrap}
      """.as[PendingOperation[Op, SId]].headOption
    OptionT.apply(storage.query(selectAction, functionFullName))
  }

  override def getAll(
      operationName: NonEmptyString,
      synchronizerId: Option[SId] = None,
      operationKey: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[PendingOperation[Op, SId]]] = {

    import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.*

    val baseQuery =
      sql"""
        select operation_name, operation_key, operation, synchronizer_id
        from common_pending_operations
        where operation_name = ${operationName.unwrap}
      """

    val syncFilter = synchronizerId.fold(sql"")(id => sql" and synchronizer_id = $id")
    val keyFilter = operationKey.fold(sql"")(key => sql" and operation_key = $key")

    val selectAction = (baseQuery ++ syncFilter ++ keyFilter).as[PendingOperation[Op, SId]]
    storage.query(selectAction, functionFullName).map(_.toSet)
  }
}

private object DbPendingOperationsStore {

  private def getResultSId[SId](sidParser: String => Either[String, SId]): GetResult[SId] =
    GetResult { r =>
      val rawSynchronizer = r.<<[String]
      sidParser(rawSynchronizer).valueOr(err =>
        throw new DbDeserializationException(
          s"Unable to parse `$rawSynchronizer` as a synchronizer: $err"
        )
      )
    }

  /** @throws DbDeserializationException
    *   Slick's transactional boundaries are managed through DBIOAction, which ultimately produces a
    *   Future. A Future signals failure with an exception. Therefore, to make a DBIO transaction
    *   fail and roll back, we must throw an exception from within it.
    */
  private def tryGetPendingOperationResult[
      Op <: HasProtocolVersionedWrapper[Op],
      SId <: Synchronizer,
  ](
      operationDeserializer: ByteString => ParsingResult[Op]
  )(implicit
      getByteString: GetResult[ByteString],
      getSId: GetResult[SId],
  ): GetResult[PendingOperation[Op, SId]] =
    GetResult { r =>
      PendingOperation
        .create(
          name = r.<<[String],
          key = r.<<[String],
          operationBytes = r.<<[ByteString],
          operationDeserializer,
          synchronizer = r.<<[SId],
        )
        .valueOr(errorMessage => throw new DbDeserializationException(errorMessage))
    }
}
