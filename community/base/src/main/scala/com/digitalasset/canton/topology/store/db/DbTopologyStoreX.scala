// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionsX,
  TimeQueryX,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOpX,
  TopologyMappingX,
  TopologyTransactionX,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class DbTopologyStoreX[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
    maxItemsInSqlQuery: Int,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStoreX[StoreId]
    with DbStore {
  override def findProposalsByTxHash(
      effective: EffectiveTime,
      hashes: Seq[TxHash],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] = ???

  override def findTransactionsForMapping(effective: EffectiveTime, hashes: Seq[MappingHash])(
      implicit traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = ???

  override def findValidTransactions(
      asOfExclusive: EffectiveTime,
      filter: GenericSignedTopologyTransactionX => Boolean,
  ): Future[Seq[GenericSignedTopologyTransactionX]] = ???

  /** add validated topology transaction as is to the topology transaction table */
  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Set[TopologyMappingX.MappingHash],
      removeTxs: Set[TopologyTransactionX.TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = ???

  override def dumpStoreContent()(implicit traceContext: TraceContext): Unit = ???

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQueryX,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOpX],
      typ: Option[TopologyMappingX.Code],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] = ???
}
