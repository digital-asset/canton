// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.functorFilter._
import cats.syntax.list._
import cats.syntax.traverse._
import cats.syntax.functor._
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.{Clock, TimeAwaiter}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.DomainTopologyClient.Subscriber
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{DomainId, SequencerCounter}

import java.time.{Duration => JDuration}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success
import scala.util.control.NonFatal

trait TopologyAwaiter extends FlagCloseable {

  this: DomainTopologyClientWithInit =>

  protected def clock: Clock
  private val conditions = new AtomicReference[Seq[StateAwait]](Seq.empty)

  override protected def onClosed(): Unit = {
    super.onClosed()
    shutdownConditions()
  }

  private def shutdownConditions(): Unit = {
    conditions.updateAndGet { x =>
      x.foreach(_.promise.trySuccess(UnlessShutdown.AbortedDueToShutdown))
      Seq()
    }.discard
  }

  protected def checkAwaitingConditions()(implicit traceContext: TraceContext): Unit = {
    conditions
      .get()
      .foreach(stateAwait =>
        try { stateAwait.check() }
        catch {
          case NonFatal(e) =>
            logger.error("An exception occurred while checking awaiting conditions.", e)
            stateAwait.promise.tryFailure(e)
        }
      )
  }

  private class StateAwait(func: => Future[Boolean]) {
    val promise: Promise[UnlessShutdown[Boolean]] = Promise[UnlessShutdown[Boolean]]()
    promise.future.onComplete(_ => {
      val _ = conditions.updateAndGet(_.filterNot(_.promise.isCompleted))
    })

    def check(): Unit = {
      if (!promise.isCompleted) {
        // Ok to use onComplete as any exception will be propagated to the promise.
        func.onComplete {
          case Success(false) => // nothing to do, will retry later
          case res =>
            val _ = promise.tryComplete(res.map(UnlessShutdown.Outcome(_)))
        }
      }
    }
  }

  private[topology] def scheduleAwait(
      condition: => Future[Boolean],
      timeout: Duration,
  ): FutureUnlessShutdown[Boolean] = {
    val waiter = new StateAwait(condition)
    conditions.updateAndGet(_ :+ waiter)
    if (!isClosing) {
      if (timeout.isFinite) {
        clock.scheduleAfter(
          _ => {
            val _ = waiter.promise.trySuccess(UnlessShutdown.Outcome(false))
          },
          JDuration.ofMillis(timeout.toMillis),
        )
      }
      waiter.check()
    } else {
      // calling shutdownConditions() will ensure all added conditions are marked as aborted due to shutdown
      // ensure we don't have a race condition between isClosing and updating conditions
      shutdownConditions()
    }
    FutureUnlessShutdown(waiter.promise.future)
  }
}

abstract class BaseDomainTopologyClient
    extends DomainTopologyClientWithInit
    with TopologyAwaiter
    with TimeAwaiter {

  private val subscribers = new AtomicReference[Seq[DomainTopologyClient.Subscriber]](Seq())

  private val pendingChanges = new AtomicInteger(0)

  private case class HeadTimestamps(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
  ) {
    def update(
        newEffectiveTimestamp: EffectiveTime,
        newApproximateTimestamp: ApproximateTime,
    ): HeadTimestamps = {
      HeadTimestamps(
        effectiveTimestamp =
          EffectiveTime(CantonTimestamp.max(effectiveTimestamp.value, newEffectiveTimestamp.value)),
        approximateTimestamp = ApproximateTime(
          CantonTimestamp.max(approximateTimestamp.value, newApproximateTimestamp.value)
        ),
      )
    }
  }
  private val head = new AtomicReference[HeadTimestamps](
    HeadTimestamps(
      EffectiveTime(CantonTimestamp.MinValue),
      ApproximateTime(CantonTimestamp.MinValue),
    )
  )

  override def updateHead(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val curHead =
      head.updateAndGet(_.update(effectiveTimestamp, approximateTimestamp))
    // now notify the futures that wait for this update here. as the update is active at t+epsilon, (see most recent timestamp),
    // we'll need to notify accordingly
    notifyAwaitedFutures(curHead.effectiveTimestamp.value.immediateSuccessor)
    if (potentialTopologyChange)
      checkAwaitingConditions()
  }

  protected def currentKnownTime: CantonTimestamp = topologyKnownUntilTimestamp

  override def numPendingChanges: Int = pendingChanges.get()

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    // we update the head timestamp approximation with the current sequenced timestamp, right now
    // but don't check awaiting conditions until we informed our subscribers
    updateHead(
      effectiveTimestamp,
      ApproximateTime(sequencedTimestamp.value),
      potentialTopologyChange = false,
    )
    subscribers
      .get()
      .foreach(_.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions))
    // now, check awaiting conditions. needs to be done after notifying the observers, as otherwise the topology
    // dispatcher may subscribe before we start to notify the subscribers. as a result, the topology dispatcher would
    // be informed about transactions it has not dispatched, and therefore squeek
    checkAwaitingConditions()
    // and we schedule an update to the effective time in due time so that we start using the
    // right keys at the right time.
    if (effectiveTimestamp.value > sequencedTimestamp.value) {
      val deltaDuration = effectiveTimestamp.value - sequencedTimestamp.value
      pendingChanges.incrementAndGet()
      // schedule using after as we don't know the clock synchronisation level, but we know the relative time.
      clock.scheduleAfter(
        _ => {
          updateHead(
            effectiveTimestamp,
            ApproximateTime(effectiveTimestamp.value),
            potentialTopologyChange = true,
          )
          if (pendingChanges.decrementAndGet() == 0) {
            logger.debug(
              s"Effective at $effectiveTimestamp, there are no more pending topology changes (last were from $sequencedTimestamp)"
            )
          }
        },
        deltaDuration,
      )
    }
    FutureUnlessShutdown.unit
  }

  /** Subscribe to topology state change notifications */
  override def subscribe(subscriber: Subscriber): Unit =
    subscribers.updateAndGet(_ :+ subscriber).discard

  override def unsubscribe(subscriber: Subscriber): Unit =
    subscribers.updateAndGet(_.filter(_ != subscriber)).discard

  /** Returns whether a snapshot for the given timestamp is available. */
  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    topologyKnownUntilTimestamp >= timestamp

  override def topologyKnownUntilTimestamp: CantonTimestamp =
    head.get().effectiveTimestamp.value.immediateSuccessor

  /** returns the current approximate timestamp
    *
    * whenever we get an update, we do set the approximate timestamp first to the sequencer time
    * and schedule an update on the clock to advance the approximate time to the effective time
    * after the time difference elapsed.
    */
  override def approximateTimestamp: CantonTimestamp =
    head.get().approximateTimestamp.value.immediateSuccessor

  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] = if (waitForEffectiveTime)
    this.awaitKnownTimestamp(timestamp)
  else if (approximateTimestamp >= timestamp) None
  else {
    Some(
      // first, let's wait until we can determine the epsilon for the given timestamp
      for {
        snapshotAtTs <- awaitSnapshot(timestamp)
        parametersAtTs <- snapshotAtTs.findDynamicDomainParametersOrDefault()
        epsilonAtTs = parametersAtTs.topologyChangeDelay
        // then, wait for t+e
        _ <- awaitKnownTimestamp(timestamp.plus(epsilonAtTs.unwrap)).getOrElse(Future.unit)
      } yield ()
    )
  }

}

/** The domain topology client that reads data from a topology store
  *
  * @param domainId The domain-id corresponding to this store
  * @param store The store
  * @param initKeys The set of initial keys that should be mixed in in case we fetched an empty set of keys
  * @param useStateTxs Whether we use the state store or the transaction store. Generally, we use the state store
  *                    except in the authorized store
  */
class StoreBasedDomainTopologyClient(
    val clock: Clock,
    val domainId: DomainId,
    store: TopologyStore,
    initKeys: Map[KeyOwner, Seq[SigningPublicKey]],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    useStateTxs: Boolean = true,
)(implicit val executionContext: ExecutionContext)
    extends BaseDomainTopologyClient
    with NamedLogging {

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshot = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, topology known until =$topologyKnownUntilTimestamp",
    )
    new StoreBasedTopologySnapshot(
      timestamp,
      store,
      initKeys,
      useStateTxs = useStateTxs,
      packageDependencies,
      loggerFactory,
    )
  }

  /** returns the snapshot of the current member topology as of the most recently observed timestamp
    */
  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): StoreBasedTopologySnapshot =
    new StoreBasedTopologySnapshot(
      approximateTimestamp.immediateSuccessor,
      store,
      initKeys,
      useStateTxs = useStateTxs,
      packageDependencies,
      loggerFactory,
    )

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    scheduleAwait(condition(currentSnapshotApproximation), timeout)

  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParameters.WithValidity]] = store
    .inspect(
      stateStore = false,
      timeQuery = TimeQuery.Range(None, None),
      recentTimestampO = None,
      ops = Some(TopologyChangeOp.Replace),
      typ = Some(DomainTopologyTransactionType.DomainParameters),
      idFilter = "",
      namespaceOnly = false,
    )
    .map { storedTxs =>
      val domainParametersChanges = storedTxs.result
        .map(storedTx =>
          (storedTx.validFrom, storedTx.validUntil, storedTx.transaction.transaction.element)
        )
        .collect {
          case (
                validFrom,
                validUntil,
                DomainGovernanceElement(DomainParametersChange(_, domainParameters)),
              ) =>
            DynamicDomainParameters.WithValidity(
              validFrom,
              validUntil,
              domainParameters,
            )
        }

      if (domainParametersChanges.isEmpty)
        logger.warn("List of all domain parameters changes should not be empty.")

      domainParametersChanges
    }
}

object StoreBasedDomainTopologyClient {

  def NoPackageDependencies(implicit
      executionContext: ExecutionContext
  ): PackageId => EitherT[Future, PackageId, Set[PackageId]] = { _ =>
    EitherT.pure[Future, PackageId](Set())
  }
}

/** Topology snapshot loader
  *
  * @param timestamp the asOf timestamp to use
  * @param store the db store to use
  * @param initKeys any additional keys to use (for bootstrapping domains)
  * @param useStateTxs whether the queries should use the state or the tx store. state store means all tx are properly authorized
  * @param packageDependencies lookup function to determine the direct and indirect package dependencies
  */
class StoreBasedTopologySnapshot(
    val timestamp: CantonTimestamp,
    store: TopologyStore,
    initKeys: Map[KeyOwner, Seq[SigningPublicKey]],
    useStateTxs: Boolean,
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader
    with NamedLogging
    with NoTracing {

  private def findTransactions(
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp.Positive]] =
    if (useStateTxs)
      store
        .findStateTransactions(
          timestamp,
          asOfInclusive,
          includeSecondary,
          types,
          filterUid,
          filterNamespace,
        )
        .map(_.combine)
    else
      store
        .findPositiveTransactions(
          timestamp,
          asOfInclusive,
          includeSecondary,
          types,
          filterUid,
          filterNamespace,
        )
        .map(_.combine)

  // helper class used to fold a sequence of mappings, aggregating permissions and participant parties
  private case class PartyAggregation(
      work: Map[ParticipantId, (Option[ParticipantPermission], Option[ParticipantPermission])]
  ) {

    def addPartyToParticipantMapping(mapping: PartyToParticipant): PartyAggregation =
      update(mapping.participant, mapping.side, mapping.permission)

    private def update(
        participant: ParticipantId,
        side: RequestSide,
        permission: ParticipantPermission,
    ): PartyAggregation = {
      val (from, to) = work.getOrElse(participant, (None, None))

      def mix(cur: Option[ParticipantPermission]) =
        Some(ParticipantPermission.lowerOf(permission, cur.getOrElse(permission)))

      val updated = side match {
        case RequestSide.Both => (mix(from), mix(to))
        case RequestSide.To => (from, mix(to))
        case RequestSide.From => (mix(from), to)
      }
      copy(work = work.updated(participant, updated))
    }

    def addParticipantState(ps: ParticipantState): PartyAggregation = {
      if (ps.permission.isActive)
        update(ps.participant, ps.side, ps.permission)
      else
        this
    }

  }

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      fetchParticipantStates: Seq[ParticipantId] => Future[
        Map[ParticipantId, ParticipantAttributes]
      ],
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    loadBatchActiveParticipantsOf(Seq(party), fetchParticipantStates).map(
      _.getOrElse(party, Map.empty)
    )

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      fetchParticipantStates: Seq[ParticipantId] => Future[
        Map[ParticipantId, ParticipantAttributes]
      ],
  ): Future[Map[PartyId, Map[ParticipantId, ParticipantAttributes]]] = {
    def update(
        party: PartyId,
        mp: Map[PartyId, PartyAggregation],
        appender: PartyAggregation => PartyAggregation,
    ): Map[PartyId, PartyAggregation] = {
      mp + (party -> appender(mp.getOrElse(party, PartyAggregation(Map()))))
    }
    for {

      // get all party to participant mappings and also participant states for this uid (latter to mix in admin parties)
      transactions <- findTransactions(
        asOfInclusive = false,
        includeSecondary = false,
        types = Seq(
          DomainTopologyTransactionType.ParticipantState,
          DomainTopologyTransactionType.PartyToParticipant,
        ),
        filterUid = Some(parties.map(_.uid)),
        filterNamespace = None,
      ).map(_.toIdentityState)

      // aggregate the mappings, looking for matching request sides
      allAggregated = transactions.foldLeft(Map.empty[PartyId, PartyAggregation]) {
        case (acc, TopologyStateUpdateElement(_, pp: PartyToParticipant)) =>
          update(pp.party, acc, _.addPartyToParticipantMapping(pp))
        // aggregate participant states (for admin parties)
        case (acc, TopologyStateUpdateElement(_, ps: ParticipantState)) =>
          update(ps.participant.adminParty, acc, _.addParticipantState(ps))
        case (acc, _) => acc
      }
      // fetch the participant permissions on this domain
      participantStateMap <- fetchParticipantStates(
        allAggregated.values.flatMap(_.work.keys).toSeq.distinct
      )
    } yield {
      // cap the party to participant permission to the participant permission
      def capped(aggregated: PartyAggregation): Map[ParticipantId, ParticipantAttributes] = {
        aggregated.work
          .map { case (participantId, (from, to)) =>
            val participantState =
              participantStateMap.getOrElse(
                participantId,
                ParticipantAttributes(ParticipantPermission.Disabled, TrustLevel.Ordinary),
              )
            // using the lowest permission available
            val reducedPerm = ParticipantPermission.lowerOf(
              from.getOrElse(ParticipantPermission.Disabled),
              ParticipantPermission
                .lowerOf(
                  to.getOrElse(ParticipantPermission.Disabled),
                  participantState.permission,
                ),
            )
            (participantId, ParticipantAttributes(reducedPerm, participantState.trustLevel))
          }
          // filter out in-active
          .filter(_._2.permission.isActive)
      }
      allAggregated.fmap(v => capped(v))
    }
  }

  override def allKeys(owner: KeyOwner): Future[KeyCollection] =
    findTransactions(
      asOfInclusive = false,
      includeSecondary = false,
      types = Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
      filterUid = Some(Seq(owner.uid)),
      filterNamespace = None,
    ).map(_.toIdentityState)
      .map(_.collect {
        case TopologyStateUpdateElement(_, OwnerToKeyMapping(foundOwner, key))
            if foundOwner.code == owner.code =>
          key
      }.foldLeft(KeyCollection(Seq(), Seq()))((acc, key) => acc.addTo(key)))
      .map { collection =>
        // add initialisation keys if necessary
        if (collection.signingKeys.isEmpty) {
          initKeys
            .get(owner)
            .fold(collection)(_.foldLeft(collection)((acc, elem) => acc.addTo(elem)))
        } else {
          collection
        }
      }

  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    findTransactions(
      asOfInclusive = false,
      includeSecondary = false,
      types = Seq(DomainTopologyTransactionType.ParticipantState),
      filterUid = None,
      filterNamespace = None,
    ).map(_.toIdentityState)
      // TODO(i4930) this is quite inefficient
      .map(_.collect { case TopologyStateUpdateElement(_, ps: ParticipantState) =>
        ps.participant
      })
      .flatMap { all =>
        loadParticipantStates(all.distinct)
      }
      .map {
        _.map { case (k, v) =>
          (k, v.permission)
        }.toSeq
      }

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] = {
    def merge(
        current: (Option[ParticipantAttributes], Option[ParticipantAttributes]),
        ps: ParticipantState,
    ): (Option[ParticipantAttributes], Option[ParticipantAttributes]) = {
      val (from, to) = current
      val rel = ParticipantAttributes(ps.permission, ps.trustLevel)

      def mix(cur: Option[ParticipantAttributes]) = Some(cur.getOrElse(rel).merge(rel))

      ps.side match {
        case RequestSide.From => (mix(from), to)
        case RequestSide.To => (from, mix(to))
        case RequestSide.Both => (mix(from), mix(to))
      }
    }

    implicit val traceContext: TraceContext = TraceContext.todo
    if (participants.isEmpty) Future.successful(Map())
    else {
      findTransactions(
        asOfInclusive = false,
        includeSecondary = false,
        types = Seq(DomainTopologyTransactionType.ParticipantState),
        filterUid = Some(participants.map(_.uid)),
        filterNamespace = None,
      ).map(_.toIdentityState)
        .map { loaded =>
          loaded
            .foldLeft(
              Map
                .empty[
                  ParticipantId,
                  (Option[ParticipantAttributes], Option[ParticipantAttributes]),
                ]
            ) {
              case (acc, TopologyStateUpdateElement(_, ps: ParticipantState)) =>
                acc.updated(ps.participant, merge(acc.getOrElse(ps.participant, (None, None)), ps))
              case (acc, _) => acc
            }
            .mapFilter {
              case (Some(lft), Some(rght)) =>
                // merge permissions granted by participant vs domain
                // but take trust level only from "from" (domain side)
                Some(
                  ParticipantAttributes(
                    ParticipantPermission.lowerOf(lft.permission, rght.permission),
                    lft.trustLevel,
                  )
                )
              case (None, None) => None
              case (None, _) => None
              case (_, None) => None
            }
        }
    }
  }

  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] =
    loadParticipantStates(Seq(participantId)).map(_.get(participantId))

  override def findParticipantCertificate(
      participantId: ParticipantId
  )(implicit traceContext: TraceContext): Future[Option[LegalIdentityClaimEvidence.X509Cert]] = {
    import cats.implicits._
    findTransactions(
      asOfInclusive = false,
      includeSecondary = false,
      types = Seq(DomainTopologyTransactionType.SignedLegalIdentityClaim),
      filterUid = Some(Seq(participantId.uid)),
      filterNamespace = None,
    ).map(_.toIdentityState.reverse.collectFirstSome {
      case TopologyStateUpdateElement(_id, SignedLegalIdentityClaim(_, claimBytes, _signature)) =>
        val result = for {
          claim <- LegalIdentityClaim
            .fromByteString(claimBytes)
            .leftMap(err => s"Failed to parse legal identity claim proto: $err")

          certOpt = claim.evidence match {
            case cert: LegalIdentityClaimEvidence.X509Cert if claim.uid == participantId.uid =>
              Some(cert)
            case _ => None
          }
        } yield certOpt

        result.valueOr { err =>
          logger.error(s"Failed to inspect domain topology state for participant certificate: $err")
          None
        }

      case _ => None
    })
  }

  /** Returns a list of all known parties on this domain */
  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] = {
    store
      .inspect(
        stateStore = useStateTxs,
        timeQuery = TimeQuery.Snapshot(timestamp),
        recentTimestampO = None,
        ops = Some(TopologyChangeOp.Add),
        typ = Some(DomainTopologyTransactionType.OwnerToKeyMapping),
        idFilter = filterOwner,
        namespaceOnly = false,
      )
      .map { col =>
        col.toIdentityState
          .map(_.mapping)
          .collect {
            case OwnerToKeyMapping(owner, key)
                if owner.filterString.startsWith(filterOwner)
                  && filterOwnerType.forall(_ == owner.code) =>
              (owner, key)
          }
          .groupBy(_._1)
          .map { case (owner, keys) =>
            (
              owner,
              keys.foldLeft(KeyCollection.empty) { case (col, (_, publicKey)) =>
                col.addTo(publicKey)
              },
            )
          }
          .take(limit)
      }
  }

  /** Returns a list of all known parties on this domain */
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Map[PartyId, Map[ParticipantId, ParticipantAttributes]]] =
    findTransactions(
      asOfInclusive = false,
      includeSecondary = false,
      types = Seq(
        DomainTopologyTransactionType.PartyToParticipant,
        DomainTopologyTransactionType.ParticipantState,
      ),
      filterUid = None,
      filterNamespace = None,
    )
      .flatMap { col =>
        val parties = col.toIdentityState
          .collect {
            case TopologyStateUpdateElement(_, ParticipantState(_, _, participant, _, _)) =>
              participant.adminParty
            case TopologyStateUpdateElement(_, PartyToParticipant(_, party, _, _)) => party
          }
          .filter { party =>
            party.filterString.startsWith(filterParty)
          }
          .take(limit)
        parties
          .traverse { party =>
            activeParticipantsOf(party.toLf)
              .map(res => (party, res.filter(_._1.filterString.startsWith(filterParticipant))))
          } // FIXME(i7397): This will filter out disabled participants, breaking the limit clause.
          .map(_.toMap)
      }

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] = {

    val vettedET = EitherT.right[PackageId](
      findTransactions(
        asOfInclusive = false,
        includeSecondary = false,
        types = Seq(DomainTopologyTransactionType.PackageUse),
        filterUid = Some(Seq(participant.uid)),
        filterNamespace = None,
      ).map { res =>
        res.toIdentityState.flatMap {
          case TopologyStateUpdateElement(_, VettedPackages(_, packageIds)) => packageIds
          case _ => Seq()
        }.toSet
      }
    )

    val dependenciesET = packageDependencies(packageId)

    for {
      vetted <- vettedET
      // check that the main package is vetted
      res <-
        if (!vetted.contains(packageId))
          EitherT.rightT[Future, PackageId](Set(packageId)) // main package is not vetted
        else {
          // check which of the dependencies aren't vetted
          for {
            dependencies <- dependenciesET
          } yield dependencies -- vetted
        }
    } yield res

  }

  /** returns the list of currently known mediators */
  override def mediators(): Future[Seq[MediatorId]] = findTransactions(
    asOfInclusive = false,
    includeSecondary = false,
    types = Seq(DomainTopologyTransactionType.MediatorDomainState),
    filterUid = None,
    filterNamespace = None,
  ).map { res =>
    ArraySeq.from(
      res.toIdentityState
        .foldLeft(Map.empty[MediatorId, (Boolean, Boolean)]) {
          case (acc, TopologyStateUpdateElement(_, MediatorDomainState(side, _, mediator))) =>
            acc + (mediator -> RequestSide
              .accumulateSide(acc.getOrElse(mediator, (false, false)), side))
          case (acc, _) => acc
        }
        .filter { case (_, (lft, rght)) =>
          lft && rght
        }
        .keys
    )

  }

  override def findDynamicDomainParameters(implicit
      traceContext: TraceContext
  ): Future[Option[DynamicDomainParameters]] =
    findTransactions(
      asOfInclusive = false,
      includeSecondary = false,
      types = Seq(DomainTopologyTransactionType.DomainParameters),
      filterUid = None,
      filterNamespace = None,
    ).map { storedTxs =>
      // We sort the results to be able to pick the most recent one in case
      // several transactions are found.
      val domainParameters =
        StoredTopologyTransactions(storedTxs.result.sortBy(_.validFrom)).toIdentityState.collect {
          case DomainGovernanceElement(DomainParametersChange(_, domainParameters)) =>
            domainParameters
        }
      domainParameters.toNel.map { domainParametersNel =>
        if (domainParametersNel.size > 1)
          logger.warn(
            s"Expecting only one dynamic domain parameters, ${domainParametersNel.size} found. Considering the most recent one."
          )
        domainParametersNel.last
      }
    }

}
