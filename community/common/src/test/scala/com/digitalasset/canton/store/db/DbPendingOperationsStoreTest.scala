// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.PendingOperationStoreTest.TestPendingOperationMessage
import com.digitalasset.canton.store.{
  PendingOperation,
  PendingOperationStore,
  PendingOperationStoreTest,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.scalatest.BeforeAndAfterAll
import slick.jdbc.SetParameter

import scala.annotation.unused
import scala.concurrent.Future

sealed trait DbPendingOperationsStoreTest
    extends PendingOperationStoreTest[TestPendingOperationMessage]
    with BeforeAndAfterAll {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(
      sqlu"truncate table common_pending_operations restart identity",
      functionFullName,
    )
  }

  override protected def insertCorruptedData(
      op: PendingOperation[TestPendingOperationMessage, SynchronizerId],
      store: Option[PendingOperationStore[TestPendingOperationMessage, SynchronizerId]] = None,
      corruptOperationBytes: Option[ByteString] = None,
  ): Future[Unit] = {
    import storage.api.*
    import storage.converters.*
    @unused
    implicit val setParameter: SetParameter[TestPendingOperationMessage] =
      (v: TestPendingOperationMessage, pp) => pp >> v.toByteString

    val operationBytes = corruptOperationBytes.getOrElse(ByteString.empty())

    val upsertCorruptAction = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
        insert into common_pending_operations
          (operation_name, operation_key, operation, synchronizer_id)
        values
          (${op.name.unwrap}, ${op.key}, $operationBytes, ${op.synchronizer})
        on conflict (synchronizer_id, operation_key, operation_name) do update
          set operation = $operationBytes
      """
      case _: DbStorage.Profile.H2 =>
        sqlu"""
        merge into common_pending_operations
          (operation_name, operation_key, operation, synchronizer_id)
        key (synchronizer_id, operation_key, operation_name)
        values
          (${op.name.unwrap}, ${op.key}, $operationBytes, ${op.synchronizer})
      """
    }
    storage.update_(upsertCorruptAction, functionFullName).failOnShutdown

  }

  "DbPendingOperationsStore" should {
    behave like pendingOperationsStore(() =>
      new DbPendingOperationsStore(
        storage,
        timeouts,
        loggerFactory,
        TestPendingOperationMessage,
        SynchronizerId.fromString,
      )
    )
  }
}

final class PendingOperationsStoreTestH2 extends DbPendingOperationsStoreTest with H2Test

final class PendingOperationsStoreTestPostgres
    extends DbPendingOperationsStoreTest
    with PostgresTest
