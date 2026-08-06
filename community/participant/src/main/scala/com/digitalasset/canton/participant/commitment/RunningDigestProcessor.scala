// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService.AcsUpdateContainer
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  GenericTopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  InternalIndexService,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.*
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  CheckpointType,
  allCheckpointsFilter,
}
import com.digitalasset.canton.protocol.{DynamicSynchronizerParameters, LfContractId}
import com.digitalasset.canton.time.RefinedDuration
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.{
  SynchronizerParametersState,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.signalling.Notification
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/** Builds the pipeline for processing events that trigger a change in the ACS commitment, namely
  *   - contract activations/deactivations
  *   - party onboarding to or offboarding from this or a remote participant
  */
// TODO(#33084): expose health status and metrics
class RunningDigestProcessor(
    thisParticipant: ParticipantId,
    override val synchronizerId: SynchronizerId,
    acsCommitmentConfig: AcsCommitmentConfig,
    digestAccumulator: DigestAccumulator,
    protected override val acsDigestStore: AcsDigestStore,
    tickSignaller: TickSignaller,
    indexService: InternalIndexService,
    getTopologySnapshot: Traced[CantonTimestamp] => FutureUnlessShutdown[TopologySnapshot],
    enableAdditionalConsistencyChecks: Boolean,
    periodWriter: AcsCommitmentPeriodWriter,
    override private[canton] val metrics: CommitmentMetrics,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    mat: Materializer,
) extends NamedLogging
    with BaseDigestProcessor {

  private val thisLfParticipant = thisParticipant.toLf

  override def isReinitializingProcessor: Boolean = false

  /** Inserts a checkpointing fence into the processing pipeline in the following scenarios:
    *   - after a topology event with the same time as the event
    *   - before an AcsUpdate with the predecessor timestamp of the AcsChange, if a checkpoint
    *     boundary has been crossed
    */
  def checkpointing(
      startingRecordTimeO: Option[CantonTimestamp],
      // intentionally not implicit to not accidentally be used instead of the update's TraceContext
      traceContext: TraceContext,
  ): Flow[Checkpointing_Input, Checkpointing_Output, NotUsed] = {
    val mainCheckpointingFlow = Flow[Checkpointing_Input]
      .statefulMapAsyncUSAndDrain(
        (
          // numEventsSinceLastCheckpointFence
          0,
          // previously processed record time
          startingRecordTimeO.getOrElse(CantonTimestamp.MinValue),
        )
      ) {
        case (
              (numEventsSinceLastCheckpoint, previousRecordTime),
              context @ ProcessingContext(timepoint, event),
            ) =>
          implicit val traceContext: TraceContext = context.traceContext
          val recordTime = timepoint.recordTime
          for {
            topologySnapshot <- getTopologySnapshot(Traced(recordTime))
            dynamicParameters <- getDynamicSynchronizerParametersOrFail(topologySnapshot)
          } yield {
            // first determine whether the event should be emitted at all, and whether it triggers a checkpoint
            val (eventToEmit, postEventCheckpoint) = event match {
              case InternalIndexService.AcsUpdate.AcsChangeUpdate(_) =>
                (Some(context.withValue(NotCheckpointFence(topologySnapshot, event))), None)

              case InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(
                    partyTopologyEvents,
                    newSynchronizerParamsO,
                  ) =>
                // only propagate the ACS update if there is a party hosting change
                val (partyHostingChangeEvent, partyHostingChangeCheckpoint) = Option
                  .when(partyTopologyEvents.nonEmpty)(
                    (NotCheckpointFence(topologySnapshot, event), CheckpointType.PartyHostingChange)
                  )
                  .unzip

                val tickIntervalChangeCheckpoint =
                  newSynchronizerParamsO.flatMap(hasTickIntervalChanged(_, dynamicParameters))

                val postEventCheckpoint = tickIntervalChangeCheckpoint
                  .orElse(partyHostingChangeCheckpoint)
                  .map(checkpointType => context.withValue(CheckpointFence(checkpointType)))

                (partyHostingChangeEvent.map(context.withValue), postEventCheckpoint)

              case InternalIndexService.AcsUpdate.AcsCommitment(_) =>
                // the running digest processor does not do anything in particular with AcsCommitment messages
                (None, None)

              case InternalIndexService.AcsUpdate.OffsetCheckpoint =>
                // the running digest processor does not do anything in particular with OffsetCheckpoint messages
                (None, None)
            }

            // determine whether the event crossed a reconciliation interval boundary
            val crossedReconciliationIntervalBoundary =
              determineCheckpointAtReconciliationBoundary(
                timepoint = timepoint,
                previouslyProcessedRecordTime = previousRecordTime,
                dynamicParameters,
              ).map { case (checkpointTimepoint, checkpointType) =>
                ProcessingContext(checkpointTimepoint, CheckpointFence(checkpointType))
              }

            // determine whether the event is the event that reaches the limit of maxNumUpdatesBetweenCheckpoints
            val checkpointByNumProcessedEvents: Option[ProcessingContext[CheckpointFence]] =
              Option.when(
                numEventsSinceLastCheckpoint + 1 == acsCommitmentConfig.maxNumUpdatesBetweenCheckpoints.unwrap
              )(context.withValue(CheckpointFence(CheckpointType.MaxEventsWithoutCheckpoint)))

            // determine the next `numEventsSinceLastCheckpoint` and the output elements to emit
            val (updatedNumEventsSinceLastCheckpoint, result) = (
              crossedReconciliationIntervalBoundary,
              postEventCheckpoint,
              checkpointByNumProcessedEvents,
            ) match {
              case (None, None, None) =>
                // the input event itself did not produce a checkpoint
                // => increase the counter and emit all events (which should really be just the event itself
                (numEventsSinceLastCheckpoint + 1, eventToEmit.toList)

              case (tickBoundaryCP, postEventCP @ Some(_), _) =>
                // the input event produced a post-event checkpoint on its own
                // => reset the counter to 0
                // => emit the tick boundary checkpoint, in case an interval tick was crossed
                // => do not emit a MaxEventsWithoutCheckpoint checkpoint
                (0, tickBoundaryCP.toList ++ eventToEmit ++ postEventCP)

              case (tickBoundaryCP @ Some(_), None, _) =>
                // the input event itself did not produce a checkpoint, but it crossed a tick interval boundary
                // => set the counter to 1, because the tick interval checkpoint is emitted before the event
                // => do not emit a checkpoint of type MaxEventsWithoutCheckpoint
                (1, tickBoundaryCP.toList ++ eventToEmit)

              case (None, None, maxNumEventsCheckpointFence @ Some(_)) =>
                // the input event itself did not produce a checkpoint and not tick interval boundary was crossed,
                // but it triggers a MaxEventsWithoutCheckpoint
                // => reset the counter to 0, because the checkpoint is emitted AFTER the event
                (0, eventToEmit.toList ++ maxNumEventsCheckpointFence)
            }

            (updatedNumEventsSinceLastCheckpoint, recordTime) -> result
          }
      }(NamedLoggingContext(loggerFactory, traceContext))
      .mapConcat(identity)
      .map(updateMetric(metrics.runningDigestProcessor.latestCheckpointedRecordTime, _))

    if (enableAdditionalConsistencyChecks) {
      validateCheckpointConsistency(mainCheckpointingFlow)
    } else {
      mainCheckpointingFlow
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def validateCheckpointConsistency(
      mainCheckpointingFlow: Flow[Checkpointing_Input, Checkpointing_Output, NotUsed]
  ): Flow[Checkpointing_Input, Checkpointing_Output, NotUsed] = {
    noTracingLogger.debug("wiring up checkpoint consistency checks")
    mainCheckpointingFlow.statefulMapConcat { () =>
      var mostRecentCheckpoint: Option[(Timepoint, CheckpointType)] = None

      currentEvent => {
        implicit val tc = currentEvent.traceContext
        currentEvent.value match {
          case CheckpointFence(cpType) =>
            mostRecentCheckpoint.foreach { case (lastCheckpoint, lastCheckpointType) =>
              // tick checkpoints must not be overwritten
              ErrorUtil.requireState(
                !(lastCheckpointType.isTickCheckpoint && lastCheckpoint.offset == currentEvent.offset),
                s"Previous checkpoint $lastCheckpoint at $lastCheckpoint must not be overwritten by $cpType at the same offset ${currentEvent.offset}.",
              )
              // both offset and record time must increase monotonically
              ErrorUtil.requireState(
                currentEvent.offset >= lastCheckpoint.offset && currentEvent.recordTime >= lastCheckpoint.recordTime,
                s"The previous checkpoint was observed at ${lastCheckpoint.tupled}, and new checkpoint at ${currentEvent.timepoint.tupled} seems to go back in time ",
              )
            }
            mostRecentCheckpoint = Some((currentEvent.timepoint, cpType))
            Seq(currentEvent)
          case _ => Seq(currentEvent)
        }
      }
    }
  }

  /** Enriches the incoming events (acs change or topology change) with the data that is needed to
    * determine which digests need to be loaded and updated during a later stages of the pipeline.
    */
  def classification: Flow[Classifcation_Input, Classification_Output, NotUsed] =
    Flow[Classifcation_Input]
      .flatMap { context =>
        context.traverse[Source[*, NotUsed], CheckpointFenceOr[Classification]] {
          // propagate checkpoint fences
          case fence: CheckpointFence => Source.single(fence: CheckpointFenceOr[Classification])
          case other @ NotCheckpointFence(topoSnapshot, value) =>
            implicit val traceContext: TraceContext = context.traceContext
            value match {
              case InternalIndexService.AcsUpdate.AcsChangeUpdate(acsChange) =>
                // determine which digests need to be changed for acs changes:
                // for each activation/deactivation, update the digest for all stakeholders with the locally hosted parties.
                // the returned classification also contains the information about the counterparticipants that need to be updated.
                determineRequiredDigestChangesFromAcsChange(topoSnapshot, acsChange)
                  .map(update => other.withValue(update: Classification))
              case InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(events, _) =>
                // determine the digests that need to be changed for topology changes
                //
                // given n topology events at the same record time, when processing the i-th topology event (where i <= n),
                // all effects of the previously processed topology events 1 <= j < i must be applied to the party to participant
                // topology state, so that the classification correctly calculates the required digest updates.
                Source(events)
                  .statefulMapAsyncUSAndDrain(
                    // start with the noop change
                    TopologyChangeTracker.empty
                  ) { (changeTracker, topoEvent) =>
                    // determine the digests that need to be changed for adding or removing a party from this participant.
                    topoEvent match {
                      case ptp @ PartyToParticipantAuthorization(
                            _,
                            `thisLfParticipant`,
                            (Added(_) | Revoked),
                          ) =>
                        FutureUnlessShutdown.pure(
                          determineClassificationForLocalTopologyChange(
                            context.timepoint.offset,
                            topoSnapshot,
                            ptp,
                            changeTracker,
                          )
                        )

                      // determine the digests that need to be changed for remote topology changes
                      case ptp: PartyToParticipantAuthorization
                          if ptp.participant != thisLfParticipant =>
                        val (updatedChangeTracker, classification) =
                          classificationForTopologyChange(ptp, changeTracker)
                        FutureUnlessShutdown.pure(
                          (
                            updatedChangeTracker,
                            classification
                              .map(Source.single)
                              .getOrElse(Source.empty[Classification]),
                          )
                        )

                      // in all other cases, do nothing and return the unmodified change tracker
                      case _ =>
                        FutureUnlessShutdown.pure(changeTracker -> Source.empty[Classification])
                    }
                  }
                  .flatten
                  .map(classification => other.withValue(classification))
              case InternalIndexService.AcsUpdate.AcsCommitment(_) =>
                // ignore incoming acs commitments for now
                Source.empty
              case InternalIndexService.AcsUpdate.OffsetCheckpoint =>
                // ignore incoming offset checkpoints for now
                Source.empty
            }
        }
      }
      .map(updateMetric(metrics.runningDigestProcessor.latestClassifiedRecordTime, _))

  /** Determines the classification for a topology event and register the corresponding change in
    * the topology change tracker.
    */
  private def classificationForTopologyChange(
      topologyEvent: PartyToParticipantAuthorization,
      changeTracker: TopologyChangeTracker,
  ): (TopologyChangeTracker, Option[Classification]) = {
    val PartyToParticipantAuthorization(party, participant, authorizationEvent) = topologyEvent
    authorizationEvent match {
      case AuthorizationEvent.Onboarding(_) =>
        changeTracker ->
          Option(PartyOnboardingToParticipant(party, participant))

      case AuthorizationEvent.Added(_) =>
        changeTracker.addPartyToParticipant(party, participant) ->
          Option(
            PartyAddedToParticipant(
              party,
              participant,
            )
          )

      case AuthorizationEvent.Revoked =>
        changeTracker.removePartyFromParticipant(party, participant) ->
          Option(
            PartyRemovedFromParticipant(
              party,
              participant,
            )
          )

      case ChangedTo(_) =>
        changeTracker -> Option.empty
    }
  }

  /** Determines the required digests that need to be updated by:
    *   1. loading the ACS of `partyAffectedByTopologyChange` to find all counterparties
    *   1. loading the ACS for batches of counterparties, discarding contracts that are not shared
    *      with `partyAffectedByTopologyChange`
    *   1. streaming `AcsUpdate`s to update the digests of the counterparties with the respective
    *      hashes of (cid, rc, counterparty, partyAffectedByTopologyChange).
    *
    * The reason for streaming the `AcsUpdate`s for batches of counterparties is to limit the number
    * of digests that need to be held in memory at any given point in time. At the end of processing
    * all possible updates to a counterparty's digest, this digest is now in a consistent state at
    * the respective record time and can be persisted to the database.
    *
    * In case of a party being added to the participant, the emitted AcsUpdates do not contain this
    * participant as a hosting participant of the party, and therefore the participant's own digest
    * doesn't get updated on the fly with the AcsChanges. Only after updating all the party digests
    * do we update the participant's own digest by adding the party's digest directly to the
    * participant's digest (just like a normal remote topology change). Doing it this way is more
    * efficient, as illustrated by the following:
    *
    * This intended implementation performs the following sequence of digest update operations:
    *   1. forall counterparties of party, do:
    *      - `digest(party) += acsUpdates`
    *   1. `digest(participant) += digest(party)`
    *
    * We could do it the other way around, but that would result in more digest update operations:
    *   1. `digest(participant) += digest(party)`
    *   1. forall counterparties of party, do:
    *      - `digest(party) += acsUpdates`
    *      - `digest(participant) += acsUpdates`
    *
    * Conversely, the offboarding of a party is done in reverse order, for efficiency's sake
    *   1. `digest(participant) -= digest(party)`
    *   1. forall counterparties of party, do:
    *      - `digest(party) -= acsUpdates`
    */
  private def determineClassificationForLocalTopologyChange(
      offset: Offset,
      topologySnapshot: TopologySnapshot,
      ptp: PartyToParticipantAuthorization,
      changeTracker: TopologyChangeTracker,
  )(implicit
      traceContext: TraceContext
  ): (TopologyChangeTracker, Source[Classification, NotUsed]) = {
    val isPartyBeingAdded = ptp.authorizationEvent match {
      case _: AuthorizationEvent.Added => true
      case AuthorizationEvent.Revoked => false
      case _: AuthorizationEvent.Onboarding | _: AuthorizationEvent.ChangedTo =>
        ErrorUtil.invalidArgument(s"Unexpected authorization level at $offset: $ptp")
    }

    ErrorUtil.requireArgument(
      ptp.participant == thisLfParticipant,
      s"Unexpected topology change for non-local participant at $offset: $ptp",
    )

    val partyAffectedByTopologyChange = ptp.party

    // determine the change for the topology change tracker and how the local participant's digest needs to be updated
    val (updatedTracker, topologyChangeForThisLfParticipant) =
      classificationForTopologyChange(ptp, changeTracker)

    val acsUpdates = indexService
      // load the ACS of the party to determine the counterparties that need to have their digest updated
      .counterParties(synchronizerId, offset, Some(partyAffectedByTopologyChange))
      .grouped(acsCommitmentConfig.counterpartyBatchSize.unwrap)
      .flatMapConcat { counterparties =>
        // for a group of counterparties, load the acs that is shared with the locally onboarded party
        // and emit the corresponding classification
        indexService
          .acs(
            synchronizerId,
            offset,
            counterparties.toSet,
            Set(partyAffectedByTopologyChange),
          )
          .mapAsyncAndDrainUS(1) { activeContractOfCounterparty =>
            val stakeholdersOfContract = activeContractOfCounterparty.stakeholders

            for {
              partyToParticipant <- getOnboardedParticipantsOfParties(
                topologySnapshot,
                stakeholdersOfContract,
              )
                // see scaladoc of this method as to why we don't apply the updated topology changes for added parties,
                // but we do for removed parties.
                .map(
                  if (isPartyBeingAdded) changeTracker.applyPendingTopologyChanges
                  else updatedTracker.applyPendingTopologyChanges
                )
            } yield {
              // emit the classification update for all stakeholders of the current stakeholder batch
              // of the contract and their respective hosting participants.
              val stakeholdersToHostingParticipants = stakeholdersOfContract.view
                .filter(counterparties.contains)
                .map(sh => sh -> partyToParticipant.getOrElse(sh, Set.empty))
                .toMap

              Seq(
                AcsUpdate(
                  stakeholdersToHostingParticipants,
                  // only emit the onboarded party as local party. Other locally hosted stakeholders will have already
                  // been processed by other events (e.g. an AcsChange or their own party onboarding event).
                  Seq(partyAffectedByTopologyChange),
                  activeContractOfCounterparty.contractId,
                  activeContractOfCounterparty.reassignmentCounter,
                  isActivation = isPartyBeingAdded,
                )
              )
            }
          }
          .mapConcat(identity)
      }

    (
      updatedTracker,
      // please see the scaladoc of this method as to why the topology change is emitted after the AcsUpdates for added parties,
      // but before the AcsUpdates for removed parties.
      if (isPartyBeingAdded) acsUpdates.concat(Source(topologyChangeForThisLfParticipant.toList))
      else Source(topologyChangeForThisLfParticipant.toList).concat(acsUpdates),
    )
  }

  private def determineRequiredDigestChangesFromAcsChange(
      topologySnapshot: TopologySnapshot,
      acsChange: AcsChange,
  )(implicit traceContext: TraceContext): Source[AcsUpdate, NotUsed] = {
    val allStakeholders = acsChange.activations.values.flatMap(_.stakeholders) ++
      acsChange.deactivations.values.flatMap(_.stakeholders)

    val futureSource = for {
      partyToParticipants <- getOnboardedParticipantsOfParties(
        topologySnapshot,
        allStakeholders.toSet,
      )
    } yield {
      val changes = toAcsChange(acsChange.activations, partyToParticipants, isActivation = true) ++
        toAcsChange(acsChange.deactivations, partyToParticipants, isActivation = false)
      Source(changes)
    }
    PekkoUtil.futureSourceUS(futureSource)
  }

  private def toAcsChange(
      change: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
      partyToParticipants: Map[LfPartyId, Set[LedgerParticipantId]],
      isActivation: Boolean,
  ): immutable.Iterable[AcsUpdate] =
    change.flatMap {
      case (
            cid,
            ContractStakeholdersAndReassignmentCounter(stakeholders, reassignmentCounter),
          ) =>
        val locallyHostedStakeholders =
          stakeholders
            .filter(sh => partyToParticipants.getOrElse(sh, Set.empty).contains(thisLfParticipant))
            .toSeq
        // if the change does not affect a locally hosted party, which could be the case if a party hasn't been fully onboarded yet,
        // simply ignore the change. Once the party onboarding has completed, the corresponding topology change will trigger the appropriate digest updates.
        if (locallyHostedStakeholders.isEmpty) Seq.empty
        else {
          val stakeholdersToHostingParticipants = stakeholders.view
            .map(sh => sh -> partyToParticipants.getOrElse(sh, Set.empty))
            .toMap
          Seq(
            AcsUpdate(
              // update the digest for these stakeholders and their respective hosting participants
              stakeholdersToHostingParticipants,
              // with all locally hosted parties
              locallyHostedStakeholders,
              // for this contract
              cid,
              // and reassignment counter
              reassignmentCounter,
              // with an additive or negative change
              isActivation = isActivation,
            )
          )
        }
    }

  def pipeline(
      startingRecordTimeO: Option[CantonTimestamp],
      priorReconciliationTick: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Flow[Checkpointing_Input, DigestAccumulator_Output, NotUsed] =
    Flow[Checkpointing_Input].async
      .via(checkpointing(startingRecordTimeO, traceContext))
      .async
      .via(classification)
      .async
      .via(digestAccumulator.flow())
      .statefulMapAsyncUSAndDrain(priorReconciliationTick.getOrElse(CantonTimestamp.MinValue))(
        writeOutstandingAndCheckpoint
      )
      .map { cp =>
        if (cp.checkpointType.isTickCheckpoint)
          tickSignaller.notify(Notification.All, cp.offsetInclusive)
        cp
      }

  private def updateMetric[A](
      gauge: Gauge[Long],
      elem: ProcessingContext[A],
  ): ProcessingContext[A] = {
    gauge.updateValue(elem.recordTime.toMicros)
    elem
  }

  private def updateMetric[A](
      gauge: Gauge[Long],
      acsUpdate: AcsUpdateContainer,
  ): AcsUpdateContainer = {
    gauge.updateValue(acsUpdate.synchronizerTime.toMicros)
    acsUpdate
  }

  override protected def startPipelineInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] =
    for {
      latestCheckpointO <- acsDigestStore.latestCheckpointUpTo(
        Offset.MaxValue,
        allCheckpointsFilter,
      )
      latestReconciliatioCheckpointO <- acsDigestStore.latestReconciliationCheckpoint()
      startingOffsetO = latestCheckpointO.map(_.offset)
      startingRecordTimeO = latestCheckpointO.map(_.recordTime)
      _ <- startingOffsetO.traverse { startingOffset =>
        logger.info(
          s"Deleting ACS digest data after latest checkpoint $latestCheckpointO before starting the processing pipeline"
        )
        acsDigestStore.deleteAfter(startingOffset)
      }
    } yield {
      logger.info(s"Starting ACS digest processor from latest checkpoint $latestCheckpointO.")
      val graph = indexService
        .acsUpdates(synchronizerId, startingOffsetO)
        .map(updateMetric(metrics.runningDigestProcessor.latestAcsUpdate, _))
        .viaMat(KillSwitches.single)(Keep.right)
        .map { update =>
          val timepoint = Timepoint(update.offset)(update.synchronizerTime)
          ProcessingContext(timepoint, update.acsUpdate)(update.traceContext)
        }
        .via(pipeline(startingRecordTimeO, latestReconciliatioCheckpointO.map(_.recordTime)))
        .toMat(
          Sink.foreach(cp =>
            logger.debug(
              s"An ACS digest checkpoint ${cp.checkpointType} was written at recordTime=${cp.recordTimeInclusive}, offset=${cp.offsetInclusive}"
            )
          )
        )(Keep.both)

      val (ks, doneF) = PekkoUtil.runSupervised(graph, this.toString)
      (ks, doneF.void)
    }

  private def writeOutstandingAndCheckpoint(
      priorReconciliationTick: CantonTimestamp,
      cp: CheckpointToBeWritten,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(CantonTimestamp, DigestAccumulator_Output)] =
    for {
      isReconciliationTick <- periodWriter.writeOutstandingAtTick(
        cp,
        priorReconciliationTick,
        acsCommitmentConfig.periodStore.writerPageSize,
      )
      _ <- writeCheckpoint(cp)
    } yield {
      if (cp.checkpointType.isTickCheckpoint) {
        metrics.tickWatermark.updateValue(cp.recordTimeInclusive.toMicros)
      }

      val reconciliationTick =
        if (isReconciliationTick) cp.recordTimeInclusive else priorReconciliationTick
      reconciliationTick -> cp.toCheckpointWritten
    }

  /** Helper method to calculate the most recent reconciliation/affirmation interval tick up to and
    * including the given record time.
    */
  private def mostRecentIntervalTickUpToInclusive(
      recordTime: CantonTimestamp,
      interval: RefinedDuration,
  ): CantonTimestamp =
    CantonTimestamp.assertFromLong(
      recordTime.toMicros - (recordTime.toMicros % interval.toScala.toMicros)
    )

  /** A checkpoint is required if the reconciliation interval boundary is after or at the previously
    * processed timestamp and before the currently processed timestamp.
    */
  private def determineCheckpointAtReconciliationBoundary(
      timepoint: Timepoint,
      previouslyProcessedRecordTime: CantonTimestamp,
      dynamicParameters: DynamicSynchronizerParameters,
  ): Option[(Timepoint, CheckpointType)] = {
    val boundary =
      mostRecentIntervalTickUpToInclusive(
        timepoint.recordTime,
        dynamicParameters.reconciliationInterval,
      )
    if (previouslyProcessedRecordTime <= boundary && boundary < timepoint.recordTime)
      timepoint.offset.decrement.map(offsetPredecessor =>
        (Timepoint(offsetPredecessor)(boundary), CheckpointType.ReconciliationIntervalBoundary)
      )
    else None

  }

  /** Gets the dynamic synchronizer parameters from the topology snapshot of fails with an
    * exception. This should never really happen.
    */
  private def getDynamicSynchronizerParametersOrFail(topologySnapshot: TopologySnapshot)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DynamicSynchronizerParameters] =
    topologySnapshot
      .findDynamicSynchronizerParameters()
      .flatMap(
        _.fold(
          err => FutureUnlessShutdown.failed(new IllegalStateException(err)),
          params => FutureUnlessShutdown.pure(params.parameters),
        )
      )

  /** Determines whether a tick interval has changed and returns the corresponding
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType]].
    * @param event
    *   the
    *   [[com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.GenericTopologyEvent]]
    *   containing the synchronizer parameters topology transaction
    * @param currentSynchronizerParams
    *   the currently effective dynamic synchronizer parameters
    * @return
    *   - [[com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReconciliationIntervalBoundary]]
    *     if the reconciliation interval has changed
    *   - [[com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.AffirmationIntervalBoundary]]
    *     if the reconciliation interval has changed
    *   - `None` if no interval has changed
    */
  private def hasTickIntervalChanged(
      event: GenericTopologyEvent.SynchronizerParametersState,
      currentSynchronizerParams: DynamicSynchronizerParameters,
  )(implicit traceContext: TraceContext): Option[CheckpointType] =
    TopologyTransaction.fromTrustedByteString(event.payload) match {
      case Left(error) => ErrorUtil.invalidArgument(error.message)
      case Right(topoTx) =>
        topoTx.selectMapping[SynchronizerParametersState] match {
          case Some(syncParamState) =>
            // TODO(#33084): add check for affirmation interval, once it has been introduced
            Option.when(
              currentSynchronizerParams.reconciliationInterval != syncParamState.mapping.parameters.reconciliationInterval
            )(CheckpointType.ReconciliationIntervalBoundary)
          case None =>
            ErrorUtil.invalidArgument(
              s"SynchronizerParametersState did not contain a the expected mapping type. Actual: $topoTx"
            )
        }
    }

  override def toString: String = s"RunningDigestProcessor($synchronizerId)"
}
