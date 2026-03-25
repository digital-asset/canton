// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.implicits.toTraverseOps
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  LogicalUpgradeTime,
  SynchronizerPredecessor,
  SynchronizerSuccessor,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  Onboarding,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.OnboardingClearanceScheduler
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing.EventInfo
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation.PendingOnboardingClearanceStore
import com.digitalasset.canton.participant.synchronizer.PendingHandshakeWithLsuSuccessor
import com.digitalasset.canton.participant.synchronizer.PendingHandshakeWithLsuSuccessor.PendingHandshakesWithSuccessorsStore
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyMapping
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, MonadUtil}
import com.digitalasset.canton.{SequencerCounter, topology}
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

object ParticipantTopologyTerminateProcessing {

  /** Event with indication of whether the participant needs to initiate party replication */
  private final case class EventInfo(
      event: Update.TopologyTransactionEffective,
      onboardingLocallyHostedParty: Boolean,
      clearingOnboardingLocallyHostedParty: Boolean,
  )
}

class ParticipantTopologyTerminateProcessing(
    recordOrderPublisher: RecordOrderPublisher,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    initialRecordTime: CantonTimestamp,
    participantId: ParticipantId,
    pauseSynchronizerIndexingDuringPartyReplication: Boolean,
    synchronizerPredecessor: Option[SynchronizerPredecessor],
    pendingHandshakesWithSuccessorsStore: PendingHandshakesWithSuccessorsStore,
    pendingOnboardingClearanceStore: PendingOnboardingClearanceStore,
    onboardingClearanceScheduler: OnboardingClearanceScheduler,
    retrieveAndStoreMissingSequencerIds: TraceContext => EitherT[
      FutureUnlessShutdown,
      String,
      Unit,
    ],
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  private val psid = store.storeId.psid

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
          filterTypes = Some(
            Seq(
              TopologyMapping.Code.PartyToParticipant,
              TopologyMapping.Code.SynchronizerTrustCertificate,
            )
          ),
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
    * the store.
    */
  private def processPartyHostingEvents(eventInfo: EventInfo)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    if (eventInfo.onboardingLocallyHostedParty || eventInfo.clearingOnboardingLocallyHostedParty) {
      MonadUtil.sequentialTraverse_[FutureUnlessShutdown, TopologyEvent, Unit](
        eventInfo.event.events
      ) {
        // Onboarding: Persist the clearance intent and trigger the scheduler
        case PartyToParticipantAuthorization(lfParty, participant, Onboarding(_permission))
            if participant == participantId.toLf =>
          val effectiveAt = EffectiveTime(eventInfo.event.effectiveTime)

          PartyId
            .fromLfParty(lfParty)
            .fold(
              err => {
                logger.error(
                  s"Failed to parse PartyId from $lfParty during 'Onboarding' event. Reason: $err"
                )
                FutureUnlessShutdown.unit
              },
              partyId => {
                logger.info(
                  s"Party $partyId is onboarding on the local participant."
                )

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

                    // Record's existence guards against prematurely scheduling of an onboarding flag clearance
                    case Some(record) =>
                      // If a record is found, update it only if onboardingEffectiveAt is undefined
                      // (the record was persisted as part of the offline party ACS import)
                      val persistOperationFus =
                        if (record.operation.onboardingEffectiveAt.isDefined) {
                          logger.warn(
                            s"Unexpected: An effective PTP mapping with set onboarding flag (validFrom=${record.operation.onboardingEffectiveAt}) " +
                              s"together with a pending onboarding clearance operation record for party $partyId."
                          )
                          FutureUnlessShutdown.unit // Already tracked with the effective time
                        } else {
                          val operation =
                            OnboardingClearanceOperation(Some(effectiveAt))(
                              OnboardingClearanceOperation
                                .protocolVersionRepresentativeFor(store.protocolVersion)
                            )
                          pendingOnboardingClearanceStore
                            .updateOperation(
                              operation,
                              psid.logical,
                              OnboardingClearanceOperation.operationName,
                              OnboardingClearanceOperation.operationKey(partyId),
                            )
                        }

                      // After ensuring the update is persisted (if it was needed), request clearance
                      persistOperationFus.flatMap { _ =>
                        val onboardingEffectiveAt =
                          record.operation.onboardingEffectiveAt.getOrElse(effectiveAt)

                        onboardingClearanceScheduler
                          .requestClearance(
                            partyId,
                            onboardingEffectiveAt,
                            maxInitialRetries = NonNegativeInt.one,
                          )
                          .fold(
                            error =>
                              logger.warn(
                                s"Failed to request onboarding clearance for party $partyId: $error. " +
                                  "Please recover by manually calling the clear party onboarding flag endpoint."
                              ),
                            _ => (), // Successfully requested the onboarding flag clearance
                          )
                      }
                  }
              },
            )

        // Added: The onboarding flag has been successfully cleared
        case PartyToParticipantAuthorization(lfParty, participant, Added(_permission))
            if participant == participantId.toLf =>
          PartyId
            .fromLfParty(lfParty)
            .fold(
              err => {
                logger.error(
                  s"Failed to parse PartyId from $lfParty during 'Added' event. Reason: $err"
                )
                FutureUnlessShutdown.unit
              },
              partyId => {
                logger.info(
                  s"Party $partyId was successfully added to the local participant. Removing pending onboarding clearance operation."
                )
                pendingOnboardingClearanceStore.delete(
                  psid.logical,
                  OnboardingClearanceOperation.operationKey(partyId),
                  OnboardingClearanceOperation.operationName,
                )
              },
            )

        case _ =>
          // Ignore other events (Revoked, ChangedTo, or events for other parties/participants)
          FutureUnlessShutdown.unit
      }
    } else {
      // When neither onboardingLocallyHostedParty nor clearingOnboardingLocallyHostedParty is true
      FutureUnlessShutdown.unit
    }
  }

  override def notifyUpgradeAnnouncement(
      successor: SynchronizerSuccessor
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Unit = {
    logger.info(
      s"Node is notified about the upgrade of $psid to ${successor.psid} scheduled at ${successor.upgradeTime}"
    )

    recordOrderPublisher.setSuccessor(Some(successor))

    EitherTUtil.doNotAwaitUS(
      retrieveAndStoreMissingSequencerIds(traceContext),
      s"retrieve and store missing sequencer ids for $psid",
      failLevel = Level.WARN,
    )
  }

  override def notifyUpgradeCancellation()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.info(
      s"Node is notified about the cancellation of upgrade"
    )

    recordOrderPublisher.setSuccessor(None)

    pendingHandshakesWithSuccessorsStore.delete(
      psid,
      PendingHandshakeWithLsuSuccessor.operationKey,
      PendingHandshakeWithLsuSuccessor.operationName,
    )
  }

  private def scheduleEvent(
      effectiveTime: EffectiveTime,
      sequencedTime: SequencedTime,
      sc: SequencerCounter,
      eventInfo: EventInfo,
  )(implicit traceContext: TraceContext): UnlessShutdown[Unit] =
    (for {
      _ <- EitherT(
        recordOrderPublisher.scheduleFloatingEventPublication(
          timestamp = effectiveTime.value,
          eventFactory = _ => Some(eventInfo.event),
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
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] = {
    logger.info(
      s"Fetching effective state changes after initial record time $initialRecordTime at synchronizer-startup for topology event recovery."
    )
    for {
      trustCertificateEffectiveTime <- store
        .findFirstTrustCertificateForParticipant(participantId)
        .map {
          case Some(trustCertTx) => trustCertTx.validFrom.value
          case None =>
            ErrorUtil.invalidState(
              "Missing Trust Certificate topology transaction at initialization."
            )
        }
      outstandingEffectiveChanges <- store.findEffectiveStateChanges(
        fromEffectiveInclusive = initialRecordTime,
        onlyAtEffective = false,
        filterTypes = Some(
          Seq(
            TopologyMapping.Code.PartyToParticipant,
            TopologyMapping.Code.SynchronizerTrustCertificate,
          )
        ),
      )
      eventFromEffectiveChangeWithInitializationTraceContext =
        (effectiveChange: EffectiveStateChange) =>
          getNewEvents(effectiveChange)
            .map(eventInfo => eventInfo.event -> effectiveChange.sequencedTime.value)
      eventsFromBootstrapping = outstandingEffectiveChanges.view
        .filter(change =>
          // changes coming from bootstrapping (not a mistake, topology state initialization is happening for this bound - topology transaction will be emitted to new member which are sequenced after the trustCertificateEffectiveTime)
          change.sequencedTime.value <= trustCertificateEffectiveTime &&
            LogicalUpgradeTime
              .canProcessKnowingPredecessor(synchronizerPredecessor, change.sequencedTime)
        )
        .flatMap(eventFromEffectiveChangeWithInitializationTraceContext)
        .toVector
      eventsFromProcessingWithoutTraceContext = outstandingEffectiveChanges.view
        .filter(
          // changes coming from topology processing
          _.sequencedTime.value > trustCertificateEffectiveTime
        )
        .filter(
          // all sequenced above initialRecordTime (if any) will be re-processed
          _.sequencedTime.value <= initialRecordTime
        )
        .flatMap(eventFromEffectiveChangeWithInitializationTraceContext)
        .toVector
      _ = logger.info(
        s"Found ${eventsFromBootstrapping.size} outstanding events from synchronizer bootstrapping and ${eventsFromProcessingWithoutTraceContext.size} outstanding events from topology processing to process."
      )
      _ = logger.info(
        s"Fetching trace-contexts for ${eventsFromProcessingWithoutTraceContext.size} topology-updates for topology event recovery."
      )
      eventsFromProcessing <- MonadUtil.parTraverseWithLimit(parallelism)(
        eventsFromProcessingWithoutTraceContext
      ) { case (event, sequencedTime) =>
        traceContextForSequencedEvent(sequencedTime).map {
          case Some(sourceEventTraceContext) =>
            event.copy()(traceContext = sourceEventTraceContext) -> sequencedTime

          case None =>
            logger.warn(
              s"Cannot find trace context for sequenced time: $sequencedTime, using initialization trace context to schedule this event."
            )
            event -> sequencedTime
        }
      }
      allEvents = eventsFromBootstrapping ++ eventsFromProcessing
    } yield {
      logger.info(
        s"Scheduling ${allEvents.size} topology events at synchronizer-startup."
      )
      allEvents.foreach(
        scheduleEventAtRecovery(topologyEventPublishedOnInitialRecordTime)
      )
    }
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
          ) =>
        EventInfo(
          Update.TopologyTransactionEffective(
            updateId = updateId,
            events = events,
            synchronizerId = psid.logical,
            effectiveTime = effectiveStateChange.effectiveTime.value,
          ),
          onboardingLocallyHostedParty = onboardingLocalParty,
          clearingOnboardingLocallyHostedParty = clearingOnboardingLocalParty,
        )
    }
}
