// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.java.test
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.platform.store.backend.DataSourceStorageBackend.DataSourceConfig
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.OptionValues.*

import java.sql.Connection
import java.util.UUID
import scala.jdk.CollectionConverters.*

trait DbLockingSupport extends NamedLogging {
  def createContract(
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
  )(implicit traceContext: TraceContext): (String, String) = {
    val partyId = participant.id.adminParty
    val createCmd = new test.Dummy(partyId.toProtoPrimitive).create.commands.asScala.toSeq
    val createTx = participant.ledger_api.javaapi.commands
      .submit(
        Seq(partyId),
        createCmd,
        commandId = s"createContract-${UUID.randomUUID()}",
        synchronizerId = Some(synchronizerId),
      )
    val cid = createTx.getEvents.asScala.collectFirst {
      case x if x.toProtoEvent.hasCreated =>
        val contractId = x.toProtoEvent.getCreated.getContractId
        logger.info(s"Created contract $contractId at offset ${createTx.getOffset}")
        contractId
    }.value

    (createTx.getUpdateId, cid)
  }

  def withConnectionForTest(
      participant: LocalParticipantReference
  )(testFunction: Connection => Unit, onCommit: Connection => Unit = _ => ()) = {
    val ledgerApiStore =
      participant.underlying.value.sync.participantNodePersistentState.value.ledgerApiStore
    val conn =
      ledgerApiStore.ledgerApiDbSupport.storageBackendFactory.createDataSourceStorageBackend
        .createDataSource(
          dataSourceConfig = DataSourceConfig(ledgerApiStore.ledgerApiStorage.jdbcUrl),
          loggerFactory = loggerFactory,
        )
        .getConnection
    conn.setAutoCommit(false)
    QueryStrategy.withoutNetworkTimeout(testFunction(_))(conn, noTracingLogger)
    new Object {
      def commitAndClose(): Unit = {
        onCommit(conn)
        conn.commit()
        conn.close()
      }
    }
  }

  private def eventStorageBackend(participant: LocalParticipantReference) =
    participant.underlying.value.sync.participantNodePersistentState.value.ledgerApiStore.ledgerApiDbSupport.storageBackendFactory
      .createEventStorageBackend(
        ledgerEndCache = MutableLedgerEndCache(),
        stringInterning = new MockStringInterning,
        loggerFactory = loggerFactory,
      )

  def lockPruning(participant: LocalParticipantReference)(conn: Connection): Unit =
    eventStorageBackend(participant).lockExclusivelyPruningProcessingTable(conn)

  def readLockContract(participant: LocalParticipantReference, internalContractId: Long)(
      conn: Connection
  ): Set[Long] =
    eventStorageBackend(participant).readLockInternalContractIds(Set(internalContractId))(conn)

  def writeLockContract(participant: LocalParticipantReference, internalContractId: Long)(
      conn: Connection
  ): Unit =
    eventStorageBackend(participant).writeLockInternalContractIds(cSQL"= $internalContractId")(conn)

}
