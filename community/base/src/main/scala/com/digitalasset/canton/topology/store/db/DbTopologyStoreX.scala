// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionX.GenericStoredTopologyTransactionX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.{
  GenericStoredTopologyTransactionsX,
  PositiveStoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionsX,
  TimeQueryX,
  TopologyStore,
  TopologyStoreId,
  TopologyStoreX,
  *,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyChangeOpX.Replace
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.{
  GenericTopologyTransactionX,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.{
  DomainTrustCertificateX,
  MediatorDomainStateX,
  TopologyChangeOpX,
  TopologyMappingX,
  TopologyTransactionX,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class DbTopologyStoreX[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
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

  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = ???

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactionsX] = ???

  override def findFirstMediatorStateForMediator(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, MediatorDomainStateX]]] = ???

  override def findFirstTrustCertificateForParticipant(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[Replace, DomainTrustCertificateX]]] = ???

  override def findEssentialStateForMember(member: Member, asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = ???

  override def bootstrap(snapshot: GenericStoredTopologyTransactionsX)(implicit
      traceContext: TraceContext
  ): Future[Unit] = ???

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore.Change]] = ???

  override def maxTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = ???

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = ???

  override def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = ???

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = ???

  override def findStored(
      transaction: GenericSignedTopologyTransactionX
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] = ???

  override def findStoredForVersion(
      transaction: GenericTopologyTransactionX,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] = ???

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransactionX]] = ???
}
