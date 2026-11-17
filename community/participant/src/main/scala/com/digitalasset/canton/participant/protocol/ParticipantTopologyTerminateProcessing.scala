// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.foldable.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{
  CantonTimestamp,
  LogicalUpgradeTime,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.{SynchronizerUpdate, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  RunOnClosing,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.{
  OnboardingClearanceScheduler,
  PartyReplicationTriggers,
}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing.{
  EventInfo,
  relevantMappingsForEffectiveStateChanges,
}
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation.PendingOnboardingClearanceStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.synchronizer.PendingLsuOperation
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.PositiveStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.TopologyMapping
import com.digitalasset.canton.topology.{ConfiguredPhysicalSynchronizerId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SequencerCounter, topology}
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.*
import scala.util.{Failure, Success}

object ParticipantTopologyTerminateProcessing {

  /** Event with indication of whether the participant needs to initiate party replication */
  private final case class EventInfo(
      event: Update.TopologyTransactionEffective,
      onboardingLocallyHostedParty: Boolean,
      clearingOnboardingLocallyHostedParty: Boolean,
      revokingLocallyHostedParty: Boolean,
  )

  def relevantMappingsForEffectiveStateChanges(
      pv: ProtocolVersion
  ): NonEmpty[Seq[TopologyMapping.Code]] =
    if (pv >= ProtocolVersion.v36)
      NonEmpty(
        Seq,
        TopologyMapping.Code.PartyToParticipant,
        TopologyMapping.Code.SynchronizerTrustCertificate,
        TopologyMapping.Code.SynchronizerParametersState,
      )
    else
      NonEmpty(
        Seq,
        TopologyMapping.Code.PartyToParticipant,
        TopologyMapping.Code.SynchronizerTrustCertificate,
      )

}

class ParticipantTopologyTerminateProcessing(
    recordOrderPublisher: RecordOrderPublisher,
    ledgerApiIndexer: LedgerApiIndexer,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    initialRecordTime: CantonTimestamp,
    participantId: ParticipantId,
    pauseSynchronizerIndexingDuringPartyReplication: Boolean,
    synchronizerPredecessor: Option[SynchronizerPredecessor],
    pendingLsuOperationsStore: PendingLsuOperation.Store,
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    pendingOnboardingClearanceStore: PendingOnboardingClearanceStore,
    onboardingClearanceScheduler: OnboardingClearanceScheduler,
    partyReplicationTriggersO: Option[PartyReplicationTriggers],
    metrics: ParticipantMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  private val psid = store.storeId.psid

  private val mappingsForEffectiveStateChanges = relevantMappingsForEffectiveStateChanges(
    store.protocolVersion
  )

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    val shouldPublish =
      sequencedTime.value > initialRecordTime &&
        LogicalUpgradeTime.canProcessKnowingPredecessor(synchronizerPredecessor, sequencedTime)

    if (shouldPublish) {
      for {
        effectiveStateChanges <- store.findEffectiveStateChanges(
          fromEffectiveInclusive = effectiveTime.value,
          onlyAtEffective = true,
          filterTypes = Some(mappingsForEffectiveStateChanges),
        )
        _ = if (effectiveStateChanges.sizeIs > 1)
          logger.error(
            s"Invalid: findEffectiveStateChanges with onlyAtEffective = true should return only one EffectiveStateChange for effective time $effectiveTime, only the first one is taken into consideration."
          )
        events = effectiveStateChanges.headOption.flatMap(getNewEvents)

        _ <- events.traverse(processPartyHostingEvents)

        _ <- FutureUnlessShutdown.lift(
          events
            .map(scheduleEvent(effectiveTime, sequencedTime, sc, _))
            .getOrElse(UnlessShutdown.unit)
        )
      } yield ()
    } else {
      // invariant: initial record time < first processed record time, so we can safely omit ticking and publishing here
      // for crash recovery the scheduleMissingTopologyEventsAtInitialization method should be used
      logger.debug(
        s"Omit publishing and ticking during replay. Initial record time: $initialRecordTime, sequencer counter: $sc, sequenced time: $sequencedTime, effective time: $effectiveTime"
      )
      FutureUnlessShutdown.unit
    }
  }

  /** Processes party authorization events for the local participant to manage the onboarding (flag)
    * clearance lifecycle.
    *
    * When a party is onboarded, it records a pending operation and schedules a clearance task. When
    * the party is finally added (onboarding flag cleared), it removes the pending operation from
    * the store. When a party is revoked (offboarded), it removes any pending operation, also one
    * that `clear_party_onboarding_flag` records after the flag is cleared, so that a later import
    * of the party can record its own.
    */
  private def processPartyHostingEvents(eventInfo: EventInfo)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {

    val requiresProcessing = eventInfo.onboardingLocallyHostedParty ||
      eventInfo.clearingOnboardingLocallyHostedParty ||
      eventInfo.revokingLocallyHostedParty

    if (requiresProcessing) {
      // Pre-filter: We only care about PartyToParticipantAuthorization on the local participant
      val localAuthEvents = eventInfo.event.events.collect {
        case auth @ PartyToParticipantAuthorization(_, participant, _)
            if participant == participantId.toLf =>
          auth
      }

      MonadUtil.sequentialTraverse_[FutureUnlessShutdown, PartyToParticipantAuthorization, Unit](
        localAuthEvents
      ) { case PartyToParticipantAuthorization(lfParty, _, permission) =>
        // Parse PartyId exactly once per event
        PartyId.fromLfParty(lfParty) match {
          case Left(err) =>
            // permission.getClass.getSimpleName will print "Onboarding", "Added", or "Revoked$"
            ErrorUtil.invalidState(
              s"Failed to parse PartyId from $lfParty during '${permission.getClass.getSimpleName}' event: $err"
            )

          case Right(partyId) =>
            permission match {
              case Onboarding(_) =>
                val effectiveAt = EffectiveTime(eventInfo.event.effectiveTime)
                handleOnboardingEvent(partyId, effectiveAt)

              case Added(_) | Revoked =>
                // Group identical cleanup logic
                val action =
                  if (permission == Revoked) "revoked (offboarded) from"
                  else "successfully added to"
                logger.info(
                  s"Party $partyId was $action the local participant. Cleaning up any pending onboarding clearance operation."
                )
                pendingOnboardingClearanceStore.delete(
                  psid.logical,
                  OnboardingClearanceOperation.operationKey(partyId),
                  OnboardingClearanceOperation.operationName,
                )

              case _ =>
                // Ignore other permissions (e.g. ChangedTo)
                FutureUnlessShutdown.unit
            }
        }
      }
    } else {
      // When none of onboarding, clearing, or revoking locally hosted party flags are true
      FutureUnlessShutdown.unit
    }
  }

  private def handleOnboardingEvent(partyId: PartyId, effectiveAt: EffectiveTime)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(s"Party $partyId is onboarding on the local participant.")

    pendingOnboardingClearanceStore
      .get(
        psid.logical,
        OnboardingClearanceOperation.operationKey(partyId),
        OnboardingClearanceOperation.operationName,
      )
      .value
      .flatMap {
        case None =>
          // If no record is found, do nothing (no DB insertion, no clearance request)
          logger.info(
            s"No pending onboarding clearance operation found for party $partyId. Skipping clearance request."
          )
          FutureUnlessShutdown.unit

        // Record's existence guards against prematurely scheduling an onboarding flag clearance
        case Some(record) =>
          // If a record is found, update it only if onboardingEffectiveAt is undefined
          // (the record was persisted as part of the offline party ACS import)
          val persistOperationFus = record.operation.onboardingEffectiveAt match {
            case Some(existingTime) if existingTime == effectiveAt =>
              logger.debug(
                s"Clearance record for $partyId already has effective time $effectiveAt. Skipping DB update."
              )
              FutureUnlessShutdown.unit
            case _ =>
              // It was None (expected), OR it had a mismatched time (self-healing overwrite)
              val operation = OnboardingClearanceOperation(Some(effectiveAt))(
                OnboardingClearanceOperation.protocolVersionRepresentativeFor(store.protocolVersion)
              )
              pendingOnboardingClearanceStore.updateOperation(
                operation,
                psid.logical,
                OnboardingClearanceOperation.operationName,
                OnboardingClearanceOperation.operationKey(partyId),
              )
          }

          // After ensuring the update is persisted (if it was needed), request clearance
          persistOperationFus.map { _ =>
            onboardingClearanceScheduler.requestClearanceInBackground(partyId, effectiveAt)
          }
      }
  }

  override def notifyUpgradeAnnouncement(
      successor: SynchronizerSuccessor
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Unit = {
    logger.info(
      s"Node is notified about the upgrade of $psid to ${successor.psid} scheduled at ${successor.upgradeTime}"
    )

    recordOrderPublisher.setSuccessor(Some(successor))

    metrics.setLsuStatus(ParticipantMetrics.LsuStatus.LsuAnnounced, successor.psid)

    // Note: We do not need to explicitly delete the old PendingLsuOperation
    // on receiving multiple Replaces. PendingLsuOperation uses an empty string ("")
    // for its operationKey, so the store will safely overwrite the
    // existing operation for this synchronizer.
  }

  override def notifyUpgradeCancellation(successor: SynchronizerSuccessor)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(
      s"Node is notified about the cancellation of upgrade"
    )

    recordOrderPublisher.setSuccessor(None)

    metrics.resetLsuStatus(successor.psid)

    for {
      _ <- pendingLsuOperationsStore.delete(
        psid,
        PendingLsuOperation.operationKey,
        PendingLsuOperation.operationName,
      )
      validPsidO = successor.psid.parseAsPhysical.toOption
      successorConnectionO = validPsidO.flatMap(validPsid =>
        synchronizerConnectionConfigStore.get(validPsid).toOption
      )
      _ <- successorConnectionO.traverse_(connection =>
        synchronizerConnectionConfigStore
          .setStatus(
            alias = connection.config.synchronizerAlias,
            configuredPsid = ConfiguredPhysicalSynchronizerId(validPsidO),
            status = SynchronizerConnectionConfigStore.Inactive,
          )
          .valueOr(err =>
            ErrorUtil.invalidState(
              s"Failed to set cancelled LSU synchronizer connection config status to Inactive for ${successor.psid}: $err"
            )
          )
      )
    } yield ()
  }

  private def scheduleEvent(
      effectiveTime: EffectiveTime,
      sequencedTime: SequencedTime,
      sc: SequencerCounter,
      eventInfo: EventInfo,
  )(implicit traceContext: TraceContext): UnlessShutdown[Unit] = {
    val flushPartyReplicationPublications =
      partyReplicationTriggersO match {
        case Some(partyReplicationTriggers)
            if eventInfo.clearingOnboardingLocallyHostedParty && !pauseSynchronizerIndexingDuringPartyReplication =>
          val onboardingParties = NonEmpty
            .from(eventInfo.event.events.collect[LfPartyId] {
              case PartyToParticipantAuthorization(party, _, AuthorizationEvent.Added(_)) => party
            })
            .getOrElse(ErrorUtil.invalidState("Expect at least one added party"))
          logger.info(
            s"Flushing activation changes before clearing onboarding flag at $sequencedTime with $effectiveTime on behalf of $onboardingParties with $eventInfo"
          )
          partyReplicationTriggers.flushContractActivationChangesToIndexer(
            onboardingParties,
            store.storeId.psid.logical,
            effectiveTime,
          )(_)

        case Some(_) | None =>
          (_: SynchronizerUpdate => FutureUnlessShutdown[Unit]) => FutureUnlessShutdown.unit
      }

    (for {
      _ <- EitherT(
        recordOrderPublisher.scheduleFloatingEventPublication(
          timestamp = effectiveTime.value,
          eventFactory = _ => Some(eventInfo.event),
          onScheduled = () => FutureUnlessShutdown.unit,
          publishBefore = flushPartyReplicationPublications,
        )
      )
      _ <- EitherT(
        if (
          pauseSynchronizerIndexingDuringPartyReplication && eventInfo.onboardingLocallyHostedParty
        )
          recordOrderPublisher.scheduleEventBuffering(effectiveTime.value)
        else
          UnlessShutdown.Outcome(Right(()))
      )
    } yield ()).value.map {
      case Right(()) =>
        logger.debug(
          s"Scheduled topology event publication with sequencer counter: $sc, sequenced time: $sequencedTime, effective time: $effectiveTime"
        )

      case Left(invalidTime) =>
        // invariant: sequencedTime <= effectiveTime
        // at this point we did not tick sequencedTime yet
        ErrorUtil.invalidState(
          s"Cannot schedule topology event as record time is already at $invalidTime (publication with sequencer counter: $sc, sequenced time: $sequencedTime, effective time: $effectiveTime)"
        )
    }
  }

  /** Scheduling missing events at synchronizer initialization.
    *
    * This process takes care of both scheduling missing events at crash recovery, and scheduling
    * missing events at joining a synchronizer the first time (bootstrapping).
    *
    * @param topologyEventPublishedOnInitialRecordTime
    *   There could be at most one topology event per record-time, this information helps to make
    *   precise scheduling at recovery.
    * @param traceContextForSequencedEvent
    *   Helper function to look up TraceContext for sequenced timestamps.
    */
  def scheduleMissingTopologyEventsAtInitialization(
      topologyEventPublishedOnInitialRecordTime: Boolean,
      traceContextForSequencedEvent: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]],
      parallelism: PositiveInt,
      flagCloseable: FlagCloseable,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(
      s"Fetching effective state changes after initial record time $initialRecordTime at synchronizer-startup for topology event recovery."
    )
    val initialEffectiveTime = EffectiveTime(initialRecordTime)
    for {
      trustCertificateEffectiveTime <- store
        .findFirstTrustCertificateForParticipant(participantId)
        .map {
          case Some(trustCertTx) => trustCertTx.validFrom
          case None =>
            ErrorUtil.invalidState(
              "Missing Trust Certificate topology transaction at initialization."
            )
        }
      _ <-
        if (initialEffectiveTime <= trustCertificateEffectiveTime) {
          streamedSynchronizerOnboardingRecovery(
            trustCertificateEffectiveTime = trustCertificateEffectiveTime,
            parallelism = parallelism,
            flagCloseable = flagCloseable,
          ).flatMap(_ =>
            protocolProcessingRecovery(
              fromEffectiveInclusive = trustCertificateEffectiveTime.immediateSuccessor,
              trustCertificateEffectiveTime = trustCertificateEffectiveTime,
              // always publish as preceded by onboarding recovery - events fromEffectiveInclusive = trustCertificateEffectiveTime.immediateSuccessor must be unseen
              topologyEventPublishedOnInitialRecordTime = false,
              traceContextForSequencedEvent = traceContextForSequencedEvent,
              parallelism = parallelism,
            )
          )
        } else {
          protocolProcessingRecovery(
            fromEffectiveInclusive = initialEffectiveTime,
            trustCertificateEffectiveTime = trustCertificateEffectiveTime,
            topologyEventPublishedOnInitialRecordTime = topologyEventPublishedOnInitialRecordTime,
            traceContextForSequencedEvent = traceContextForSequencedEvent,
            parallelism = parallelism,
          )
        }
    } yield ()
  }

  private def streamedSynchronizerOnboardingRecovery(
      trustCertificateEffectiveTime: EffectiveTime,
      parallelism: PositiveInt,
      flagCloseable: FlagCloseable,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(
      s"Start streamed topology recovery for synchronizer onboarding from record time $initialRecordTime to record time $trustCertificateEffectiveTime"
    )
    val (ks, doneF) = store
      .findEffectiveStateChangesAtTopologyInit(
        // invariant: the first effective time is bigger than Genesis
        fromExclusive = EffectiveTime(initialRecordTime),
        toInclusive = trustCertificateEffectiveTime,
        filterTypes = Some(mappingsForEffectiveStateChanges),
      )
      .groupedAdjacentBy(_.sequenced)
      .mapAsync(parallelism.value)(toTopologyUpdateForOnboardingTransactions)
      .mapConcat(identity)
      .statefulMapAsync(System.nanoTime()) { case (previousLogTime, topologyUpdate) =>
        val now = System.nanoTime()
        val newLogTime = if ((now - previousLogTime).nanos >= 10.seconds) {
          logger.info(
            s"Streaming topology recovery for synchronizer onboarding from:$initialRecordTime current:${topologyUpdate.recordTime} to:$trustCertificateEffectiveTime"
          )
          now
        } else {
          previousLogTime
        }
        ledgerApiIndexer
          .enqueue(topologyUpdate)
          .map(newLogTime -> _)
          .failOnShutdownToAbortException("enqueue topology update to the indexer")
      }
      // the kill switch is triggered with abort, so it is placed at the end of the pipeline,
      // to reflect that there is no intention to orderly "drain" the source.
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    val killSwitchHandle = flagCloseable.runOnOrAfterClose(new RunOnClosing {
      override def name: String = "pull-streaming-onboarding-killswitch-on-close"
      override def done: Boolean = false
      override def run()(implicit traceContext: TraceContext): Unit =
        UnlessShutdown.failOnShutdownToAbortException(Success(AbortedDueToShutdown), name) match {
          case Failure(exception) => ks.abort(exception)
          case Success(_) => ErrorUtil.invalidArgument("Unable to extract AbortException")
        }
    })

    FutureUnlessShutdown
      // recover the AbortedDueToShutdown exception thrown in findEffectiveStateChangesAtTopologyInit
      .recoverFromAbortException(doneF)
      .map(_ => ())
      .thereafter { _ =>
        killSwitchHandle.cancel().discard
      }
  }

  private def toTopologyUpdateForOnboardingTransactions(
      txBatch: Seq[PositiveStoredTopologyTransaction]
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Option[Update.TopologyTransactionEffective]] =
    Future {
      for {
        effectiveTime <- txBatch.lastOption.map(_.validFrom)
        sequencedTime <- txBatch.lastOption.map(_.sequenced)
        eventInfo <- getNewEvents(
          EffectiveStateChange(
            effectiveTime = effectiveTime,
            sequencedTime = sequencedTime,
            before = StoredTopologyTransactions.empty,
            after = StoredTopologyTransactions(txBatch),
          )
        )
      } yield eventInfo.event
    }

  private def protocolProcessingRecovery(
      fromEffectiveInclusive: EffectiveTime,
      trustCertificateEffectiveTime: EffectiveTime,
      topologyEventPublishedOnInitialRecordTime: Boolean,
      traceContextForSequencedEvent: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]],
      parallelism: PositiveInt,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    for {
      outstandingEffectiveChanges <- store.findEffectiveStateChanges(
        fromEffectiveInclusive = fromEffectiveInclusive.value,
        onlyAtEffective = false,
        filterTypes = Some(mappingsForEffectiveStateChanges),
      )
      eventFromEffectiveChangeWithInitializationTraceContext =
        (effectiveChange: EffectiveStateChange) =>
          getNewEvents(effectiveChange)
            .map(eventInfo => eventInfo.event -> effectiveChange.sequencedTime.value)
      eventsFromProcessingWithoutTraceContext = outstandingEffectiveChanges.view
        .filter(
          // all sequenced above fromEffectiveInclusive (if any) will be re-processed
          _.sequencedTime.value <= fromEffectiveInclusive.value
        )
        .flatMap(eventFromEffectiveChangeWithInitializationTraceContext)
        .toVector
      _ = logger.info(
        s"Found ${eventsFromProcessingWithoutTraceContext.size} outstanding events from topology processing to process."
      )
      _ = logger.info(
        s"Fetching trace-contexts for ${eventsFromProcessingWithoutTraceContext.size} topology-updates for topology event recovery."
      )
      eventsFromProcessing <- MonadUtil.parTraverseWithLimit(parallelism)(
        eventsFromProcessingWithoutTraceContext
      ) { case (event, sequencedTime) =>
        traceContextForSequencedEvent(sequencedTime).map {
          case Some(sourceEventTraceContext) =>
            event.copy(traceContext = sourceEventTraceContext) -> sequencedTime

          case None =>
            // for the events related to onboarding it is normal not to have the event in the SequencedEventStore
            if (sequencedTime > trustCertificateEffectiveTime.value) {
              logger.warn(
                s"Cannot find trace context for sequenced time: $sequencedTime, using initialization trace context to schedule this event."
              )
            }
            event -> sequencedTime
        }
      }
    } yield {
      logger.info(
        s"Scheduling ${eventsFromProcessing.size} topology events at synchronizer-startup."
      )
      eventsFromProcessing.foreach(
        scheduleEventAtRecovery(topologyEventPublishedOnInitialRecordTime)
      )
    }

  private def scheduleEventAtRecovery(
      topologyEventPublishedOnInitialRecordTime: Boolean
  ): ((Update.TopologyTransactionEffective, CantonTimestamp)) => Unit = {
    case (event, sequencedTime) =>
      implicit val traceContext: TraceContext = event.traceContext
      val effectiveTime = event.effectiveTime
      recordOrderPublisher
        .scheduleFloatingEventPublication(
          timestamp = effectiveTime,
          eventFactory = _ => Some(event),
        )
        .foreach {
          case Right(()) =>
            logger.debug(
              s"Scheduled topology event publication at initialization, with sequenced time: $sequencedTime, effective time: $effectiveTime"
            )

          case Left(`initialRecordTime`) =>
            if (topologyEventPublishedOnInitialRecordTime) {
              logger.debug(
                s"Omit scheduling topology event publication at initialization: a topology event is already published at initial record time: $initialRecordTime. (sequenced time: $sequencedTime, effective time: $effectiveTime)"
              )
            } else {
              recordOrderPublisher.scheduleFloatingEventPublicationImmediately {
                actualEffectiveTime =>
                  assert(actualEffectiveTime == initialRecordTime)
                  Some(event)
              }.discard
              logger.debug(
                s"Scheduled topology event publication immediately at initialization, as record time is already the effective time, and no topology event was published at this time. (sequenced time: $sequencedTime, effective time: $effectiveTime)"
              )
            }

          case Left(invalidTime) =>
            // invariant: sequencedTime <= effectiveTime
            // at this point we did not tick sequencedTime yet
            ErrorUtil.invalidState(
              s"Cannot schedule topology event as record time is already at $invalidTime (publication with sequenced time: $sequencedTime, effective time: $effectiveTime)"
            )
        }
  }

  private def getNewEvents(effectiveStateChange: EffectiveStateChange)(implicit
      traceContext: TraceContext
  ): Option[EventInfo] =
    TopologyTransactionDiff(
      psid = psid,
      oldRelevantState = effectiveStateChange.before.signedTransactions,
      currentRelevantState = effectiveStateChange.after.signedTransactions,
      localParticipantId = participantId,
    ).map {
      case TopologyTransactionDiff(
            events,
            updateId,
            onboardingLocalParty,
            clearingOnboardingLocalParty,
            revokingLocalParty,
            genericTopologyEvents,
          ) =>
        EventInfo(
          Update.TopologyTransactionEffective(
            updateId = updateId,
            events = events,
            synchronizerId = psid.logical,
            effectiveTime = effectiveStateChange.effectiveTime.value,
            genericTopologyEvents = genericTopologyEvents,
            traceContext = traceContext,
          ),
          onboardingLocallyHostedParty = onboardingLocalParty,
          clearingOnboardingLocallyHostedParty = clearingOnboardingLocalParty,
          revokingLocallyHostedParty = revokingLocalParty,
        )
    }
}
