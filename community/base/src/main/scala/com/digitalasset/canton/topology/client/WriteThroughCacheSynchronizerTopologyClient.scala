// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  ProcessingTimeout,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.SigningKeysWithThreshold
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParametersWithValidity,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  TopologyStore,
  TopologyStoreId,
  UnknownOrUnvettedPackages,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  ParticipantAttributes,
  SequencerConnectionSuccessor,
  VettedPackage,
}
import com.digitalasset.canton.topology.{
  KeyCollection,
  MediatorGroup,
  Member,
  MemberCode,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SequencerGroup,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.atomic.AtomicReference
import scala.Ordering.Implicits.*
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** A topology client that produces topology snapshots backed by the topology state write through
  * cache.
  *
  * Since the cache transparently loads data on demand into the cache and can inspect the topology
  * state at any time, there is no need to manage snapshots at certain timestamps. We do return new
  * instances of the snapshot to get the timestamps correct, but the actual underlying data is
  * shared through the `stateLookup`.
  */
class WriteThroughCacheSynchronizerTopologyClient(
    delegate: StoreBasedSynchronizerTopologyClient,
    stateLookup: TopologyStateLookup,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    packageDependencyResolver: PackageDependencyResolver,
    cachingConfigs: CachingConfigs,
    enableConsistencyChecks: Boolean,
    val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends SynchronizerTopologyClientWithInit
    with NamedLogging {

  // The write through cache is already very fast, but in order to avoid any regression and excessive recomputation,
  // we also preserve a caffeine cache based version of the snapshot (which was tuned on 2.x) to reduce the amount
  // of computation even more. If we invalidate the cache, we'll just rebuild it from the topology state cache.
  private val cachedHeadState =
    new AtomicReference[Option[(CantonTimestamp, CachingTopologySnapshot)]](None)

  // Temporary state lookup which is used during crash recovery of the topology state processors cache
  private val cacheDuringCrashRecovery =
    new AtomicReference[Option[(EffectiveTime, TopologyStateLookup)]](None)

  private def buildSnapshotFor(
      timestamp: CantonTimestamp,
      desiredTimestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): TopologySnapshotLoader = {

    def mkSnapshot(targetTimestamp: CantonTimestamp) = {
      val determineLookupToUse =
        (stateLookup.cacheCleanAsOf, cacheDuringCrashRecovery.get()) match {
          // use primary cache if possible
          case (Some(cleanAsOf), secondary)
              if cleanAsOf.value.immediateSuccessor >= targetTimestamp =>
            // if primary cache advanced beyond secondary, drop secondary
            if (secondary.exists(_._1 <= cleanAsOf)) {
              logger.info(
                s"Crash recovery on topology state completed, as cleanAsOf is at $cleanAsOf while grabbing snapshot at $targetTimestamp, dropping secondary cache which was at ${secondary
                    .map(_._1)}"
              )
              cacheDuringCrashRecovery.set(None)
            }
            Some(stateLookup)
          // otherwise, use secondary
          case (_, Some((useSecondaryUntil, secondary)))
              if useSecondaryUntil.value >= targetTimestamp =>
            Some(secondary)
          case (cleanAsOf, Some((useSecondaryUntil, _))) =>
            logger.error(
              s"Dirty read beyond known timestamp. Will serve directly and slow from db and pray (cleanAsOf=$cleanAsOf, target-ts=$targetTimestamp, last-known=$useSecondaryUntil)"
            )
            None
          case (cleanAsOf, None) =>
            logger.error(
              s"Dirty read before client initialization. Will serve directly and slow from db and pray (cleanAsOf=$cleanAsOf, target-ts=$targetTimestamp) "
            )
            None
        }
      determineLookupToUse match {
        case Some(lookup) =>
          new WriteThroughCacheTopologySnapshot(
            delegate.psid,
            lookup,
            store,
            packageDependencyResolver,
            targetTimestamp,
            loggerFactory,
          )
        case None => delegate.trySnapshot(targetTimestamp)
      }
    }
    val (knownUntil, lastChange) = delegate.knownUntilAndLatestChangeTimestamps
    ErrorUtil.requireState(knownUntil >= timestamp, s"known=$knownUntil, ts=$timestamp")

    // if the cache is up to date, return the cached snapshot
    val snapshot = cachedHeadState.get().filter { case (ts, _) => ts == lastChange } match {
      // if access in the future and we have a valid cached snapshot, so let's use it
      case Some((_, cached)) if timestamp >= lastChange => cached
      // access is in the future but we don't have a valid cached snapshot, let's create it
      case None if timestamp >= lastChange =>
        // create cached snapshot at last change. yep, this means we are adding a cache on top of a cache
        val caching =
          new CachingTopologySnapshot(
            mkSnapshot(lastChange),
            cachingConfigs,
            BatchingConfig(),
            loggerFactory,
            futureSupervisor,
          )
        // update it gracefully
        cachedHeadState.updateAndGet {
          // if the stored snapshot or another snapshot that was racily added is older, use this on
          case Some((ts, _)) if ts < lastChange => Some((lastChange, caching))
          // if there is none, update it
          case None => Some((lastChange, caching))
          // otherwise don't touch it
          case other => other
        }
        // we return the caching snapshot that was generated for requested timestamp, regardless of whether it
        // was actually stored in `cachedHeadState` or not. Another call for buildSnapshotFor might have won the race with a new timestamp,
        caching
      // access is in the past, just create a write through snapshot
      case _ => mkSnapshot(timestamp)
    }
    val validatingOrSnapshot =
      if (enableConsistencyChecks)
        new ValidatingTopologySnapshot(store, snapshot, packageDependencyResolver, loggerFactory)
      else snapshot
    // check if we need to wrap it into a forwarding snapshot
    if (snapshot.timestamp != desiredTimestamp)
      new ForwardingTopologySnapshotClient(
        desiredTimestamp,
        validatingOrSnapshot,
        loggerFactory,
      )
    else validatingOrSnapshot
  }

  /** Returns a snapshot with the state of [[latestTopologyChangeTimestamp]], but using
    * [[topologyKnownUntilTimestamp]] as the timestamp for the snapshot.
    *
    * If these timestamps are the same, the event most recently processed by the topology processor
    * was a topology transaction.
    */
  override def headSnapshot(implicit traceContext: TraceContext): TopologySnapshot = {
    val (knownUntil, lastChange) = delegate.knownUntilAndLatestChangeTimestamps
    buildSnapshotFor(lastChange, knownUntil)
  }

  override def hypotheticalSnapshot(timestamp: CantonTimestamp, desiredTimestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    waitForTimestampWithLogging(timestamp).map(_ => buildSnapshotFor(timestamp, desiredTimestamp))

  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshotLoader] =
    waitForTimestampWithLogging(timestamp).map(_ => buildSnapshotFor(timestamp, timestamp))

  private val maxTimestampCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    SequencedTime,
    Option[(SequencedTime, EffectiveTime)],
  ] = ScaffeineCache.buildTracedAsync[
    FutureUnlessShutdown,
    SequencedTime,
    Option[(SequencedTime, EffectiveTime)],
  ](
    cache = cachingConfigs.synchronizerClientMaxTimestamp.buildScaffeine(),
    loader = traceContext => delegate.awaitMaxTimestamp(_)(traceContext),
  )(logger, "maxTimestampCache")

  override def awaitMaxTimestamp(sequencedTime: SequencedTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    maxTimestampCache.get(sequencedTime)

  override def latestTopologyChangeTimestamp: CantonTimestamp =
    delegate.latestTopologyChangeTimestamp

  override def initialize(
      sequencerSnapshotTimestamp: Option[EffectiveTime],
      synchronizerUpgradeTime: Option[SequencedTime],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.initialize(sequencerSnapshotTimestamp, synchronizerUpgradeTime).map { _ =>
      cacheDuringCrashRecovery
        .set(Some((EffectiveTime(delegate.latestTopologyChangeTimestamp), stateLookup.makeCopy())))
      ()
    }

  override def staticSynchronizerParameters: StaticSynchronizerParameters =
    delegate.staticSynchronizerParameters

  override def synchronizerId: SynchronizerId = delegate.synchronizerId
  override def psid: PhysicalSynchronizerId = delegate.psid

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    delegate.snapshotAvailable(timestamp)

  override def awaitTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]] =
    delegate.awaitTimestamp(timestamp)

  override def awaitSequencedTimestamp(timestampInclusive: SequencedTime)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] = delegate.awaitSequencedTimestamp(timestampInclusive)

  override def approximateTimestamp: CantonTimestamp = delegate.approximateTimestamp

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshotLoader] = snapshot(approximateTimestamp)

  override def topologyKnownUntilTimestamp: CantonTimestamp = delegate.topologyKnownUntilTimestamp

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    delegate.scheduleAwait(
      currentSnapshotApproximation.flatMap(snapshot =>
        FutureUnlessShutdown.outcomeF(condition(snapshot))
      ),
      timeout,
    )

  override def awaitUS(
      condition: TopologySnapshot => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    delegate.scheduleAwait(currentSnapshotApproximation.flatMap(condition), timeout)

  override private[topology] def scheduleAwait(
      condition: => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  ) =
    delegate.scheduleAwait(condition, timeout)

  override def numPendingChanges: Int = delegate.numPendingChanges

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)

  override def updateHead(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
  )(implicit traceContext: TraceContext): Unit =
    delegate.updateHead(
      sequencedTimestamp,
      effectiveTimestamp,
      approximateTimestamp,
    )

  override def setSynchronizerTimeTracker(tracker: SynchronizerTimeTracker): Unit = {
    delegate.setSynchronizerTimeTracker(tracker)
    super.setSynchronizerTimeTracker(tracker)
  }

  override def close(): Unit = {
    LifeCycle.close(delegate)(logger)
    maxTimestampCache.invalidateAll()
    maxTimestampCache.cleanUp()
  }
}

object WriteThroughCacheSynchronizerTopologyClient {
  def create(
      clock: Clock,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      stateLookup: TopologyStateLookup,
      synchronizerUpgradeTime: Option[CantonTimestamp],
      packageDependencyResolver: PackageDependencyResolver,
      cachingConfigs: CachingConfigs,
      enableConsistencyChecks: Boolean,
      topologyConfig: TopologyConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(
      sequencerSnapshotTimestamp: Option[EffectiveTime] = None
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] = {
    val dbClient =
      new StoreBasedSynchronizerTopologyClient(
        clock,
        staticSynchronizerParameters,
        store,
        packageDependencyResolver,
        topologyConfig,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    val caching =
      new WriteThroughCacheSynchronizerTopologyClient(
        dbClient,
        stateLookup,
        store,
        packageDependencyResolver,
        cachingConfigs,
        enableConsistencyChecks || topologyConfig.enableTopologyStateCacheConsistencyChecks,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    caching
      .initialize(sequencerSnapshotTimestamp, synchronizerUpgradeTime.map(SequencedTime(_)))
      .map(_ => caching)
  }

}

/** Helper class to validate that what we serve from the store corresponds to what we serve from the
  * cache
  *
  * Just used when internal consistency checks are enabled.
  */
class ValidatingTopologySnapshot(
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    delegate: TopologySnapshotLoader,
    packageDependencyResolver: PackageDependencyResolver,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends TopologySnapshotLoader
    with NamedLogging {

  private val reference = new StoreBasedTopologySnapshot(
    delegate.timestamp,
    store,
    packageDependencyResolver,
    loggerFactory,
  )
  private val verified = new AtomicReference[Set[String]](Set.empty)

  private def verify[T](str: String)(
      ret: TopologySnapshotLoader => FutureUnlessShutdown[T]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[T] = if (
    verified.get().contains(str)
  ) ret(delegate)
  else {
    verified.updateAndGet(_ + str).discard
    val refF = ret(reference)
    val curF = ret(delegate)
    for {
      ref <- refF
      cur <- curF
    } yield {
      if (ref != cur) {
        logger.error(
          s"$str: mismatch between reference and current state at ${reference.timestamp}!\n$ref\n=======\n$cur"
        )
      }
      ref
    }
  }

  override def timestamp: CantonTimestamp = delegate.timestamp

  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    verify(s"allKeys $owner")(_.allKeys(owner))

  override def allKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    verify(s"allKeys $members")(_.allKeys(members))

  override def inspectKeys(filterOwner: String, filterOwnerType: Option[MemberCode], limit: Int)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    verify(s"inspectKeys $filterOwner $filterOwnerType $limit")(
      _.inspectKeys(filterOwner, filterOwnerType, limit)
    )

  override def signingKeysWithThreshold(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] =
    verify(s"signingKeysWithThreshold $party")(_.signingKeysWithThreshold(party))

  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] =
    verify("mediatorGroups")(_.mediatorGroups())

  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    verify("allMembers")(_.allMembers())

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    verify(s"isMemberKnown $member")(_.isMemberKnown(member))

  override def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    verify(s"areMembersKnown $members")(_.areMembersKnown(members))

  override def memberFirstKnownAt(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    verify(s"memberFirstKnownAt $member")(_.memberFirstKnownAt(member))

  override def loadParticipantStates(participants: Seq[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    verify(s"loadParticipantStates $participants")(_.loadParticipantStates(participants))

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PartyTopologySnapshotClient.PartyInfo] =
    verify(s"loadActiveParticipantsOf $party")(_.loadActiveParticipantsOf(party, participantStates))

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PartyId, PartyTopologySnapshotClient.PartyInfo]] =
    verify(s"loadBatchActiveParticipantsOf $parties")(
      _.loadBatchActiveParticipantsOf(parties, loadParticipantStates)
    )

  override def inspectKnownParties(filterParty: String, filterParticipant: String)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[PartyId]] =
    verify(s"inspectKnownParties $filterParty $filterParticipant")(
      _.inspectKnownParties(filterParty, filterParticipant)
    )

  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] =
    verify("sequencerGroup")(_.sequencerGroup())

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    verify("findDynamicSynchronizerParameters")(_.findDynamicSynchronizerParameters())

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    verify("findDynamicSequencingParameters")(_.findDynamicSequencingParameters())

  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] =
    verify("listDynamicSynchronizerParameterChanges")(_.listDynamicSynchronizerParametersChanges())

  override def synchronizerUpgradeOngoing()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] =
    verify("synchronizerUpgradeOngoing")(_.synchronizerUpgradeOngoing())

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    verify("sequencerConnectionSuccessors")(_.sequencerConnectionSuccessors())

  override private[client] def loadUnvettedPackagesOrDependenciesUsingLoader(
      participant: ParticipantId,
      packageId: PackageId,
      ledgerTime: CantonTimestamp,
      vettedPackagesLoader: VettedPackagesLoader,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[UnknownOrUnvettedPackages] =
    verify(s"loadUnvettedPackagesOrDependenciesUsingLoader $participant $packageId $ledgerTime")(
      _.loadUnvettedPackagesOrDependenciesUsingLoader(
        participant,
        packageId,
        ledgerTime,
        vettedPackagesLoader,
      )
    )

  override def loadVettedPackages(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PackageId, VettedPackage]] =
    verify(s"loadVettedPackages $participant")(_.loadVettedPackages(participant))

  override def loadVettedPackages(participants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[PackageId, VettedPackage]]] =
    verify(s"loadVettedPackages $participants")(_.loadVettedPackages(participants))
}
