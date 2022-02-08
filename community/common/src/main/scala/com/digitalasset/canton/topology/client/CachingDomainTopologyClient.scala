// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.traverse._
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.DomainTopologyClient.Subscriber
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  SequencedTime,
  TopologyTimestampPlusEpsilonTracker,
}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{DomainId, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class CachingDomainTopologyClient(
    protected val clock: Clock,
    parent: DomainTopologyClientWithInit,
    cachingConfigs: CachingConfigs,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DomainTopologyClientWithInit
    with NamedLogging {

  override def updateHead(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    if (snapshots.get().isEmpty) {
      appendSnapshot(approximateTimestamp.value)
    }
    if (potentialTopologyChange)
      appendSnapshot(effectiveTimestamp.value)
    parent.updateHead(effectiveTimestamp, approximateTimestamp, potentialTopologyChange)
  }

  // snapshot caching entry
  // this one is quite a special cache. generally, we want to avoid loading too much data from the database.
  // now, we know that if there was no identity update between tx and ty, then snapshot(ty) == snapshot(tx)
  // therefore, we remember the list of timestamps when updates happened and used that list in order to figure
  // out which snapshot we can use instead of loading the data again and again.
  // so we use the snapshots list to figure out the update timestamp and then we use the pointwise cache
  // to load that update timestamp.
  private class SnapshotEntry(val timestamp: CantonTimestamp) {
    def get(): CachingTopologySnapshot = pointwise.get(timestamp.immediateSuccessor)
  }
  private val snapshots = new AtomicReference[List[SnapshotEntry]](List.empty)

  private val pointwise = cachingConfigs.topologySnapshot
    .buildScaffeine()
    .build[CantonTimestamp, CachingTopologySnapshot] { ts: CantonTimestamp =>
      new CachingTopologySnapshot(
        parent.trySnapshot(ts)(TraceContext.empty),
        cachingConfigs,
        loggerFactory,
      )
    }

  private def appendSnapshot(timestamp: CantonTimestamp): Unit = {
    val item = new SnapshotEntry(timestamp)
    val _ = snapshots.updateAndGet { cur =>
      if (cur.headOption.exists(_.timestamp > timestamp))
        cur
      else
        item :: (cur.filter(
          _.timestamp.plusMillis(
            cachingConfigs.topologySnapshot.expireAfterAccess.duration.toMillis
          ) > timestamp
        ))
    }
  }

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (transactions.nonEmpty) {
      // if there is a transaction, we insert the effective timestamp as a snapshot
      appendSnapshot(effectiveTimestamp.value)
    } else if (snapshots.get().isEmpty) {
      // if we haven't seen any snapshot yet, we use the sequencer time to seed the first snapshot
      appendSnapshot(sequencedTimestamp.value)
    }
    parent.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)
  }

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): TopologySnapshotLoader = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, available snapshot=$topologyKnownUntilTimestamp",
    )
    // find a matching existing snapshot
    val cur =
      snapshots.get().find(_.timestamp < timestamp) // note that timestamps are asOf exclusive
    cur match {
      // we'll use the cached snapshot client which defines the time-period this timestamp is in
      case Some(snapshotEntry) =>
        new ForwardingTopologySnapshotClient(timestamp, snapshotEntry.get(), loggerFactory)
      // this timestamp is outside of the window where we have tracked the timestamps of changes.
      // so let's do this pointwise
      case None =>
        pointwise.get(timestamp)
    }

  }

  override def domainId: DomainId = parent.domainId
  override def subscribe(subscriber: Subscriber): Unit = parent.subscribe(subscriber)
  override def unsubscribe(subscriber: Subscriber): Unit = parent.unsubscribe(subscriber)

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    parent.snapshotAvailable(timestamp)
  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] =
    parent.awaitTimestamp(timestamp, waitForEffectiveTime)

  override def approximateTimestamp: CantonTimestamp = parent.approximateTimestamp

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader = trySnapshot(approximateTimestamp)

  override def topologyKnownUntilTimestamp: CantonTimestamp = parent.topologyKnownUntilTimestamp

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    parent.scheduleAwait(condition(currentSnapshotApproximation), timeout)

  override private[topology] def scheduleAwait(condition: => Future[Boolean], timeout: Duration) =
    parent.scheduleAwait(condition, timeout)

  override def close(): Unit = ()

  override def numPendingChanges: Int = parent.numPendingChanges

  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParameters.WithValidity]] = parent.listDynamicDomainParametersChanges()
}

object CachingDomainTopologyClient {

  def create(
      clock: Clock,
      domainId: DomainId,
      store: TopologyStore,
      initKeys: Map[KeyOwner, Seq[SigningPublicKey]],
      initialProcessedTimestamp: Option[CantonTimestamp],
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      cachingConfigs: CachingConfigs,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[CachingDomainTopologyClient] = {

    val dbClient =
      new StoreBasedDomainTopologyClient(
        clock,
        domainId,
        store,
        initKeys,
        packageDependencies,
        timeouts,
        loggerFactory,
      )
    val caching =
      new CachingDomainTopologyClient(
        clock,
        dbClient,
        cachingConfigs,
        timeouts,
        loggerFactory,
      )

    val initF = initialProcessedTimestamp match {
      case Some(ts) =>
        // find epsilon at this point in time to compute the effective time
        TopologyTimestampPlusEpsilonTracker
          .determineEpsilonFromStore(ts, store, loggerFactory)
          .map { epsilon =>
            Some((ApproximateTime(ts), EffectiveTime(ts.plus(epsilon.duration))))
          }
      case None =>
        // ts is "effective time", but during startup if we don't have an processed timestamp, we can use this one
        store.timestamp.map { tsO => tsO.map(ts => (ApproximateTime(ts), EffectiveTime(ts))) }
    }

    initF.map { initWithTsO =>
      initWithTsO.foreach { case (sequencedTs, effectiveTs) =>
        caching.updateHead(effectiveTs, sequencedTs, potentialTopologyChange = true)
      }
      caching
    }

  }
}

/** simple wrapper class in order to "override" the timestamp we are returning here */
private class ForwardingTopologySnapshotClient(
    override val timestamp: CantonTimestamp,
    parent: TopologySnapshotLoader,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader {

  override def referenceTime: CantonTimestamp = parent.timestamp
  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    parent.participants()
  override def allKeys(owner: KeyOwner): Future[KeyCollection] = parent.allKeys(owner)
  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] = parent.findParticipantState(participantId)
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] = parent.loadParticipantStates(participants)
  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    parent.loadActiveParticipantsOf(party, participantStates)
  override def findParticipantCertificate(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[LegalIdentityClaimEvidence.X509Cert]] =
    parent.findParticipantCertificate(participantId)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Map[PartyId, Map[ParticipantId, ParticipantAttributes]]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  ): EitherT[Future, PackageId, Set[PackageId]] =
    parent.findUnvettedPackagesOrDependencies(participantId, packages)

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependencies(participant, packageId)

  /** returns the list of currently known mediators */
  override def mediators(): Future[Seq[MediatorId]] = parent.mediators()

  override def findDynamicDomainParameters(implicit
      traceContext: TraceContext
  ): Future[Option[DynamicDomainParameters]] = parent.findDynamicDomainParameters

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ) = parent.loadBatchActiveParticipantsOf(parties, loadParticipantStates)
}

class CachingTopologySnapshot(
    parent: TopologySnapshotLoader,
    cachingConfigs: CachingConfigs,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends TopologySnapshotLoader
    with NamedLogging
    with NoTracing {

  override def timestamp: CantonTimestamp = parent.timestamp

  private val partyCache = cachingConfigs.partyCache
    .buildScaffeine()
    .buildAsyncFuture[PartyId, Map[ParticipantId, ParticipantAttributes]](
      loader = party => parent.loadActiveParticipantsOf(party, loadParticipantStates),
      allLoader =
        Some(parties => parent.loadBatchActiveParticipantsOf(parties.toSeq, loadParticipantStates)),
    )

  private val participantCache =
    cachingConfigs.participantCache
      .buildScaffeine()
      .buildAsyncFuture[ParticipantId, Option[ParticipantAttributes]](parent.findParticipantState)
  private val keyCache = cachingConfigs.keyCache
    .buildScaffeine()
    .buildAsyncFuture[KeyOwner, KeyCollection](parent.allKeys)

  private val packageVettingCache = cachingConfigs.packageVettingCache
    .buildScaffeine()
    .buildAsyncFuture[(ParticipantId, PackageId), Either[PackageId, Set[PackageId]]](x =>
      loadUnvettedPackagesOrDependencies(x._1, x._2).value
    )

  private val mediatorsCache = new AtomicReference[Option[Future[Seq[MediatorId]]]](None)

  private val domainParametersCache =
    new AtomicReference[Option[Future[Option[DynamicDomainParameters]]]](None)

  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    parent.participants()

  override def allKeys(owner: KeyOwner): Future[KeyCollection] = keyCache.get(owner)

  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] =
    participantCache.get(participantId)

  override def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    partyCache.get(party)

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ) = partyCache.getAll(parties)

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    participants
      .traverse(participant => participantState(participant).map((participant, _)))
      .map(_.toMap)

  override def findParticipantCertificate(
      participantId: ParticipantId
  )(implicit traceContext: TraceContext): Future[Option[LegalIdentityClaimEvidence.X509Cert]] = {
    // This one is not cached as we don't need during processing
    parent.findParticipantCertificate(participantId)
  }

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  ): EitherT[Future, PackageId, Set[PackageId]] =
    findUnvettedPackagesOrDependenciesUsingLoader(
      participantId,
      packages,
      (x, y) => EitherT(packageVettingCache.get((x, y))),
    )

  private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependencies(participant, packageId)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Map[PartyId, Map[ParticipantId, ParticipantAttributes]]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  /** returns the list of currently known mediators */
  override def mediators(): Future[Seq[MediatorId]] = mediatorsCache.get().getOrElse {
    // simple cache implementation using a reference
    val responseF = parent.mediators()
    mediatorsCache.set(Some(responseF))
    responseF
  }

  override def findDynamicDomainParameters(implicit
      traceContext: TraceContext
  ): Future[Option[DynamicDomainParameters]] =
    domainParametersCache.get().getOrElse {
      val responseF = parent.findDynamicDomainParameters
      domainParametersCache.set(Some(responseF))
      responseF
    }

}
