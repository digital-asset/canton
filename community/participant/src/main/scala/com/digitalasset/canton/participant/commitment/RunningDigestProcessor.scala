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
import com.digitalasset.canton.participant.commitment.RunningDigestProcessorImpl.CheckpointingState
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.{
  TickListener,
  TickSignaller,
}
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReceivedCommitmentCheckpoint
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  CheckpointType,
  allCheckpointsFilter,
}
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfigOverrides
import com.digitalasset.canton.protocol.{DynamicSynchronizerParameters, LfContractId}
import com.digitalasset.canton.time.RefinedDuration
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  SynchronizerParametersState,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.signalling.Notification
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId}
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/** Builds the pipeline for processing events that trigger a change in the ACS commitment, namely
  *   - contract activations/deactivations
  *   - party onboarding to or offboarding from this or a remote participant
  */
class RunningDigestProcessorImpl(
    thisParticipant: ParticipantId,
    override val synchronizerId: SynchronizerId,
    acsCommitmentConfig: AcsCommitmentConfig,
    digestAccumulator: DigestAccumulator,
    protected override val acsDigestStore: AcsDigestStore,
    tickSignaller: TickSignaller,
    indexService: InternalIndexService,
    digestProcessorTopologyLookup: DigestProcessorTopologyLookup,
    enableAdditionalConsistencyChecks: Boolean,
    periodWriter: AcsCommitmentPeriodWriter,
    override private[canton] val metrics: CommitmentMetrics,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    mat: Materializer,
) extends NamedLogging
    with RunningDigestProcessor {

  private val thisLfParticipant = thisParticipant.toLf

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
        CheckpointingState(
          numEventsSinceLastCheckpoint = 0,
          previousRecordTime = startingRecordTimeO.getOrElse(CantonTimestamp.MinValue),
          // The pipeline always starts at a checkpoint. So we never have to emit a checkpoint
          // before the first event.
          previousEventCheckpoint = None,
          previousTopologyClient = None,
        )
      ) { (state, context) =>
        implicit val traceContext: TraceContext = context.traceContext
        val CheckpointingState(
          numEventsSinceLastCheckpoint,
          previousRecordTime,
          _,
          previousTopologyClient,
        ) = state
        val ProcessingContext(timepoint, event) = context
        val recordTime = timepoint.recordTime
        for {
          topologyClient <- digestProcessorTopologyLookup.topologyClientForRunningDigestProcessor(
            synchronizerId,
            recordTime,
            previousTopologyClient,
          )
          // technically we should call something like `tryGetSnapshot`, because we know that the topology for the record time is known,
          // otherwise the indexer wouldn't have produced an event with that record time.
          // TODO(#33084): handle the topology client being shut down between acquiring it and calling awaitSnapshot
          topologySnapshot <- topologyClient.awaitSnapshot(recordTime)
          dynamicParameters <- getDynamicSynchronizerParametersOrFail(topologySnapshot)
        } yield {
          // first determine whether the event should be emitted at all, and whether it triggers a checkpoint
          val (eventToEmit, postEventTopologyCheckpoint) = event match {
            case InternalIndexService.AcsUpdate.AcsChangeUpdate(_) =>
              (Some(context.withValue(NotCheckpointFence(topologySnapshot, event))), None)

            case InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(
                  partyTopologyEvents,
                  newSynchronizerParamsO,
                ) =>
              // only propagate the ACS update if there is a party hosting change
              val (partyHostingChangeEvent, partyHostingChangeCheckpoint) = Option
                .when(partyTopologyEvents.nonEmpty)(
                  (
                    NotCheckpointFence(topologySnapshot, event),
                    CheckpointType.PartyHostingChange,
                  )
                )
                .unzip

              val tickIntervalChangeCheckpoint =
                newSynchronizerParamsO.flatMap(hasTickIntervalChanged(_, dynamicParameters))

              val postEventCheckpoint = tickIntervalChangeCheckpoint
                .orElse(partyHostingChangeCheckpoint)
                .map(checkpointType => context.withValue(CheckpointFence(checkpointType)))

              (partyHostingChangeEvent.map(context.withValue), postEventCheckpoint)

            case InternalIndexService.AcsUpdate.AcsCommitment(_) =>
              // the running digest processor persists a checkpoint for received commitments
              // to ensure that they can quickly be matched.
              (None, Some(context.withValue(CheckpointFence(ReceivedCommitmentCheckpoint))))

            case InternalIndexService.AcsUpdate.OffsetCheckpoint =>
              // Don't do anything with checkpoints other than them triggering the checkpoint of the previous event
              // In particular, do not emit a checkpoint because multiple checkpoints can be received for the same offset
              // and we'd like to avoid overwriting checkpoints for the same offset.
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

          val preEventCheckpoint = crossedReconciliationIntervalBoundary
            .orElse(state.previousEventCheckpoint)

          // determine whether the event is the event that reaches the limit of maxNumUpdatesBetweenCheckpoints
          @inline def checkpointByNumProcessedEvents: Option[ProcessingContext[CheckpointFence]] =
            Option.when(
              numEventsSinceLastCheckpoint + 1 == acsCommitmentConfig.maxNumUpdatesBetweenCheckpoints.unwrap
            )(context.withValue(CheckpointFence(CheckpointType.MaxEventsWithoutCheckpoint)))

          val postEventCheckpoint =
            postEventTopologyCheckpoint.orElse(checkpointByNumProcessedEvents)

          // determine the next `numEventsSinceLastCheckpoint` and the output elements to emit
          val updatedNumEventsSinceLastCheckpoint = preEventCheckpoint match {
            case Some(_) => 1
            case None => numEventsSinceLastCheckpoint + 1
          }
          val result = preEventCheckpoint.toList ++ eventToEmit.toList
          val newState =
            CheckpointingState(
              updatedNumEventsSinceLastCheckpoint,
              recordTime,
              postEventCheckpoint,
              Some(topologyClient),
            )
          newState -> result
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
                s"Previous checkpoint $lastCheckpointType at $lastCheckpoint must not be overwritten by $cpType at ${currentEvent.timepoint}.",
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
      .mapAsync(acsCommitmentConfig.classificationParallelism.unwrap) { context =>
        Future(context.traverse[Source[*, NotUsed], CheckpointFenceOr[Classification]] {
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
        })
      }
      .flatten
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

    // Reset the metrics for the local party change processing
    metrics.runningDigestProcessor.localPartyChangeContractChanges.updateValue(0)
    metrics.runningDigestProcessor.localPartyChangeCounterparties.updateValue(0)

    val configOverrides = ActiveContractsServiceStreamsConfigOverrides(
      maxParallelActiveIdQueries = acsCommitmentConfig.maxParallelActiveIdQueries.unwrap,
      maxParallelPayloadCreateQueries = acsCommitmentConfig.maxParallelPayloadCreateQueries.unwrap,
    )
    val acsUpdates =
      // load the ACS of the party to determine the counterparties that need to have their digest updated
      DigestProcessor
        .counterPartiesWithRetries(
          indexService,
          synchronizerId = synchronizerId,
          activeAt = offset,
          party = Some(partyAffectedByTopologyChange),
          configOverrides = configOverrides,
        )
        .grouped(acsCommitmentConfig.counterpartyBatchSize.unwrap)
        .mapAsync(acsCommitmentConfig.acsFetchParallelism.unwrap) { counterparties =>
          metrics.runningDigestProcessor.localPartyChangeCounterparties.updateValue(
            _ + counterparties.size
          )
          val counterpartiesSet = counterparties.toSet

          // for a group of counterparties, load the acs that is shared with the locally onboarded party
          // and emit the corresponding classification
          Future(
            DigestProcessor
              .acsWithRetries(
                indexService,
                synchronizerId,
                offset,
                counterpartiesSet,
                Set(partyAffectedByTopologyChange),
                configOverrides = configOverrides,
              )
              .grouped(acsCommitmentConfig.contractChangeClassificationBatchSize.unwrap)
              .map(counterpartiesSet -> _)
          )
        }
        .flatten
        .mapAsyncAndDrainUS(
          acsCommitmentConfig.contractChangeClassificationParallelism.unwrap
        ) { case (counterpartiesSet, activeContractsOfCounterparties) =>
          metrics.runningDigestProcessor.localPartyChangeContractChanges.updateValue(
            _ + activeContractsOfCounterparties.size
          )
          val stakeholdersOfContracts =
            activeContractsOfCounterparties.iterator.flatMap(_.stakeholders).toSet

          for {
            partyToParticipant <- getOnboardedParticipantsOfParties(
              topologySnapshot,
              stakeholdersOfContracts,
            )
              // see scaladoc of this method as to why we don't apply the updated topology changes for added parties,
              // but we do for removed parties.
              .map(
                if (isPartyBeingAdded) changeTracker.applyPendingTopologyChanges
                else updatedTracker.applyPendingTopologyChanges
              )
          } yield {
            val contractChanges =
              activeContractsOfCounterparties.iterator.map { activeContractOfCounterparty =>
                // emit the classification update for all stakeholders of the current stakeholder batch
                // of the contract and their respective hosting participants.
                val counterpartiesStakeholders =
                  activeContractOfCounterparty.stakeholders.iterator
                    .filter(counterpartiesSet.contains)
                    .toSet

                ContractChange(
                  counterpartiesStakeholders,
                  // only emit the on-/offboarded party as local party. Other locally hosted stakeholders will have already
                  // been processed by other events (e.g. an AcsChange or their own party onboarding event).
                  Seq(partyAffectedByTopologyChange),
                  activeContractOfCounterparty.contractId,
                  activeContractOfCounterparty.reassignmentCounter,
                  isActivation = isPartyBeingAdded,
                )
              }.toSeq

            val counterpartiesToParticipant = activeContractsOfCounterparties.iterator
              .flatMap(_.stakeholders)
              .distinct
              .filter(counterpartiesSet)
              .map(party => party -> partyToParticipant.getOrElse(party, Set.empty))
              .toMap

            ContractChangeBatch.create(
              counterpartiesToParticipant,
              contractChanges,
              enableAdditionalConsistencyChecks,
            )
          }
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
  )(implicit traceContext: TraceContext): Source[ContractChangeBatch, NotUsed] = {
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
      Source.fromIterator(() =>
        changes
          .grouped(acsCommitmentConfig.contractChangeClassificationBatchSize.unwrap)
          .map { contractChanges =>
            val partyHostingsForBatch = contractChanges.iterator
              .flatMap(_.stakeholders)
              .distinct
              .map(party => party -> partyToParticipants.getOrElse(party, Set.empty))
              .toMap
            ContractChangeBatch.create(
              partyHostingsForBatch,
              contractChanges,
              enableAdditionalConsistencyChecks,
            )
          }
      )
    }
    PekkoUtil.futureSourceUS(futureSource)
  }

  private def toAcsChange(
      change: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
      partyToParticipants: Map[LfPartyId, Set[LedgerParticipantId]],
      isActivation: Boolean,
  ): immutable.Iterable[ContractChange] =
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
          Seq(
            ContractChange(
              stakeholders,
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
  ): Flow[Checkpointing_Input, DigestAccumulator_Output, NotUsed] = {
    val bufferSize = acsCommitmentConfig.digestPipelineBufferSize.unwrap
    metrics.bufferDigestPipelineSize.updateValue(bufferSize.toLong)
    Flow[Checkpointing_Input].async
      .buffered(metrics.bufferDigestPipelineCheckpointing, bufferSize)
      .via(checkpointing(startingRecordTimeO, traceContext))
      .async
      .buffered(metrics.bufferDigestPipelineBeforeClassification, bufferSize)
      .via(classification)
      .async
      .buffered(metrics.bufferDigestPipelineBeforeAccumulation, bufferSize)
      .via(digestAccumulator.flow())
      .buffered(metrics.bufferDigestPipelineBeforeOutstanding, bufferSize)
      .statefulMapAsyncUSAndDrain(priorReconciliationTick.getOrElse(CantonTimestamp.MinValue))(
        writeOutstandingAndCheckpoint
      )
      .map { cp =>
        if (cp.checkpointType.isTickCheckpoint)
          tickSignaller.notify(Notification.All, cp.offsetInclusive)
        else if (cp.checkpointType == CheckpointType.ReceivedCommitmentCheckpoint)
          tickSignaller.notify(
            Notification
              .Keys(NonEmpty(Set, TickListener.TicksAndReceivedCommitmentCheckpointsListener)),
            cp.offsetInclusive,
          )
        cp
      }
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
      val graph = DigestProcessor
        .acsUpdatesWithRetries(indexService, synchronizerId, startingOffsetO)
        // we ignore acs updates at topology initialization time, because the topology snapshot is empty, and we cannot do
        // any meaningful topology inspection.
        .dropWhile(_.synchronizerTime <= SignedTopologyTransaction.InitialTopologySequencingTime)
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

  /** Gets the dynamic synchronizer parameters from the topology snapshot. If no parameters are
    * found, this fails with an exception, but this should never really happen.
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

object RunningDigestProcessorImpl {

  private final case class CheckpointingState(
      numEventsSinceLastCheckpoint: Int,
      previousRecordTime: CantonTimestamp,
      previousEventCheckpoint: Option[ProcessingContext[CheckpointFence]],
      previousTopologyClient: Option[SynchronizerTopologyClient],
  )
}
