// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  InternalIndexService,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.*
import com.digitalasset.canton.participant.config.AcsDigestTracingMode
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/** Builds the pipeline for processing events that trigger a change in the ACS commitment, namely
  *   - contract activations/deactivations
  *   - party onboarding to or offboarding from this or a remote participant
  * @param maxNumUpdatesBetweenCheckpoints
  *   a checkpoint is generated after this many events since the last checkpoint
  * @param counterpartyBatchSize
  *   how many counterparties get their digest updated at a time in case of a local party
  *   onboarding. With the assumption that a party may have a lot of counterparties, but each
  *   counterparty is only hosted on a small number of participants, this parameter essentially
  *   limits how many digests are loaded into memory: `numDigestsInMemory = counterPartyBatchSize *
  *   hostingParticipantsOfCounterparties`
  */
class RunningDigestProcessor(
    thisParticipant: ParticipantId,
    synchronizerId: SynchronizerId,
    maxNumUpdatesBetweenCheckpoints: PositiveInt,
    indexService: InternalIndexService,
    getTopologySnapshot: CantonTimestamp => FutureUnlessShutdown[TopologySnapshot],
    acsDigestStore: AcsDigestStore,
    stringInterning: StringInterning,
    hashOps: HashOps,
    counterpartyBatchSize: PositiveInt,
    tracingMode: AcsDigestTracingMode,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext,
    tc: TraceContext,
) extends NamedLogging {

  private val thisLfParticipant = thisParticipant.toLf

  /** A checkpoint is required if the reconciliation interval boundary is after or at the previously
    * processed timestamp and before the currently processed timestamp.
    */
  private def determineCheckpointAtReconciliationBoundary(
      recordTime: CantonTimestamp,
      previouslyProcessedRecordTime: CantonTimestamp,
      topologySnapshot: TopologySnapshot,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    topologySnapshot.findDynamicSynchronizerParameters().map {
      case Right(params) =>
        val boundary =
          CantonTimestamp.assertFromLong(
            recordTime.toMicros - (recordTime.toMicros % params.parameters.reconciliationInterval.toScala.toMicros)
          )
        Option.when(previouslyProcessedRecordTime <= boundary && boundary < recordTime)(boundary)
      case Left(error) => ErrorUtil.invalidState(error)
    }

  /** Inserts a checkpointing fence into the processing pipeline in the following scenarios:
    *   - after a topology event with the same time as the event
    *   - before an AcsUpdate with the predecessor timestamp of the AcsChange, if a checkpoint
    *     boundary has been crossed
    */
  def checkpointing: Flow[Checkpointing_Input, Checkpointing_Output, NotUsed] =
    Flow[Checkpointing_Input]
      .statefulMapAsyncUSAndDrain(
        (
          // numEventsSinceLastCheckpointFence
          0,
          // TODO(#33084) use proper value from crash recovery
          // previously processed record time
          CantonTimestamp.MinValue,
        )
      ) {
        case (
              (numEventsSinceLastCheckpoint, previousRecordTime),
              ProcessingContext(recordTime, offset, event),
            ) =>
          for {
            topologySnapshot <- getTopologySnapshot(recordTime)
            crossedReconciliationIntervalBoundary <- determineCheckpointAtReconciliationBoundary(
              recordTime = recordTime,
              previouslyProcessedRecordTime = previousRecordTime,
              topologySnapshot,
            )
          } yield {
            val (updatedNumEventsSinceLastCheckpoint, result) = event match {
              case InternalIndexService.AcsUpdate.AcsChangeUpdate(_) =>
                // emit a checkpoint at the record time's predecessor after maxNumUpdatesBetweenCheckpoints events have been emitted
                @inline def checkpointByNumProcessedEvents = Option.when(
                  (numEventsSinceLastCheckpoint >= maxNumUpdatesBetweenCheckpoints.unwrap && previousRecordTime != recordTime)
                )(recordTime.immediatePredecessor)
                // emit a checkpoint fence after crossing a reconciliation interval boundary
                val maybeFence = crossedReconciliationIntervalBoundary
                  .orElse(checkpointByNumProcessedEvents)
                  .zip(offset.decrement)
                  .map { case (checkpointRecordTime, checkpointOffset) =>
                    ProcessingContext(
                      recordTime = checkpointRecordTime,
                      checkpointOffset,
                      CheckpointFence,
                    )
                  }

                val acsEvent = List(
                  ProcessingContext(
                    recordTime,
                    offset,
                    NotCheckpointFence(topologySnapshot, event),
                  )
                )
                maybeFence
                  // if a checkpoint fence is about to be emitted, it is emitted BEFORE the AcsChange,
                  // so we reset the counter to 1 instead of 0, to correctly count the AcsChange emitted
                  // after the checkpoint fence
                  .map(fence => (1, fence :: acsEvent))
                  // otherwise increase the counter and emit just the AcsChange
                  .getOrElse((numEventsSinceLastCheckpoint + 1, acsEvent))

              case InternalIndexService.AcsUpdate.EffectivePartyToParticipantMappings(_) =>
                // add a checkpoint fence after the topology event
                val events =
                  Seq(
                    NotCheckpointFence(topologySnapshot, event),
                    CheckpointFence,
                  )
                    .map(
                      ProcessingContext(recordTime, offset, _)
                    )
                // the checkpoint is emitted AFTER the topology change, so we reset the counter to 0
                (0, events)

              case InternalIndexService.AcsUpdate.AcsCommitment(_) =>
                (numEventsSinceLastCheckpoint, Seq.empty)
            }

            (updatedNumEventsSinceLastCheckpoint, recordTime) -> result
          }
      }
      .mapConcat(identity)

  /** Enriches the incoming events (acs change or topology change) with the data that is needed to
    * determine which digests need to be loaded and updated during a later stages of the pipeline.
    */
  def classification: Flow[Classifcation_Input, Classification_Output, NotUsed] =
    Flow[Classifcation_Input]
      .flatMap {
        // propagate checkpoint fences
        case ProcessingContext(recordTime, offset, CheckpointFence) =>
          Source.single(
            ProcessingContext[CheckpointFenceOr[Classification]](
              recordTime,
              offset,
              CheckpointFence,
            )
          )

        // determine which digests need to be changed for acs changes:
        // for each activation/deactivation, update the digest for all stakeholders with the locally hosted parties.
        // the returned classification also contains the information about the counterparticipants that need to be updated.
        case ProcessingContext(
              recordTime,
              offset,
              NotCheckpointFence(
                topoSnapshot,
                InternalIndexService.AcsUpdate.AcsChangeUpdate(acsChange),
              ),
            ) =>
          determineRequiredDigestChangesFromAcsChange(topoSnapshot, acsChange)
            .map(update =>
              ProcessingContext(recordTime, offset, NotCheckpointFence(topoSnapshot, update))
            )

        // determine the digests that need to be changed for topology changes
        case ProcessingContext(
              recordTime,
              offset,
              NotCheckpointFence(
                topologySnapshot,
                InternalIndexService.AcsUpdate.EffectivePartyToParticipantMappings(events),
              ),
            ) =>
          // given n topology events at the same record time, when processing the i-th topology event (where i <= n),
          // all effects of the previously processed topology events 1 <= j < i must be applied to the party to participant
          // topology state, so that the classification correctly calculates the required digest updates.
          Source(events)
            .statefulMapAsyncUSAndDrain(
              // start with the noop change
              TopologyChangeTracker.empty
            ) {
              // determine the digests that need to be changed for adding or removing a party from this participant.
              case (
                    changeTracker,
                    ptp @ PartyToParticipantAuthorization(
                      _,
                      `thisLfParticipant`,
                      (Added(_) | Revoked),
                    ),
                  ) =>
                FutureUnlessShutdown.pure(
                  determineClassificationForLocalTopologyChange(
                    offset,
                    topologySnapshot,
                    ptp,
                    changeTracker,
                  )
                )

              // determine the digests that need to be changed for remote topology changes
              case (changeTracker, ptp: PartyToParticipantAuthorization)
                  if ptp.participant != thisLfParticipant =>
                val (updatedChangeTracker, classification) =
                  classificationForTopologyChange(ptp, changeTracker)
                FutureUnlessShutdown.pure(
                  (
                    updatedChangeTracker,
                    classification.map(Source.single).getOrElse(Source.empty[Classification]),
                  )
                )

              // in all other cases, do nothing and return the unmodified change tracker
              case (changeTracker, _) =>
                FutureUnlessShutdown.pure(changeTracker -> Source.empty[Classification])
            }
            .flatten
            .map(classification =>
              ProcessingContext[CheckpointFenceOr[Classification]](
                recordTime,
                offset,
                NotCheckpointFence(
                  topologySnapshot,
                  classification,
                ),
              )
            )

        // ignore incoming acs commitements for now
        case ProcessingContext(
              _,
              _,
              NotCheckpointFence(_, InternalIndexService.AcsUpdate.AcsCommitment(_)),
            ) =>
          Source.empty
      }

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

  def reinitializationAcsUpdates(
      recordTime: CantonTimestamp,
      offset: Offset,
      topologySnapshot: TopologySnapshot,
  ): Source[ProcessingContext[CheckpointFenceOr[AcsUpdate]], NotUsed] = {
    val acsUpdates = indexService
      .counterParties(synchronizerId, offset, party = None)
      .grouped(counterpartyBatchSize.unwrap)
      .flatMap { counterparties =>
        val counterpartiesSet = counterparties.toSet

        indexService
          .acs(synchronizerId, offset, counterpartiesSet, Set.empty)
          .mapAsyncAndDrainUS(1) { activeContractOfCounterparty =>
            val stakeholdersOfContract = activeContractOfCounterparty.stakeholders

            for {
              partyToParticipant <- getOnboardedParticipantsOfParties(
                topologySnapshot,
                stakeholdersOfContract,
              )
            } yield {
              val stakeholdersToHostingParticipants = stakeholdersOfContract.view
                .filter(counterpartiesSet.contains)
                .map { sh =>
                  sh -> partyToParticipant
                    .getOrElse(sh, Set.empty)
                }
                .toMap

              val thisParticipantStakeholders = partyToParticipant.collect {
                case (party, hostingParticipants)
                    if hostingParticipants.contains(thisLfParticipant) =>
                  party
              }

              val acsUpdate = AcsUpdate(
                stakeholdersToHostingParticipants,
                thisParticipantStakeholders.toSeq,
                activeContractOfCounterparty.contractId,
                activeContractOfCounterparty.reassignmentCounter,
                isActivation = true,
              )

              ProcessingContext(recordTime, offset, NotCheckpointFence(topologySnapshot, acsUpdate))
            }
          }

      }

    acsUpdates.concat(Source.single(ProcessingContext(recordTime, offset, CheckpointFence)))
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
      .grouped(counterpartyBatchSize.unwrap)
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
  ): Source[AcsUpdate, NotUsed] = {
    val allStakeholders = acsChange.activations.values.flatMap(_.stakeholders) ++
      acsChange.deactivations.values.flatMap(_.stakeholders)

    val futureSource = for {
      partyToParticipants <- getOnboardedParticipantsOfParties(
        topologySnapshot,
        allStakeholders.toSet,
      )
    } yield {
      def toAcsChange(
          change: Map[LfContractId, ContractStakeholdersAndReassignmentCounter],
          isActivation: Boolean,
      ): immutable.Iterable[AcsUpdate] =
        change.flatMap {
          case (
                cid,
                ContractStakeholdersAndReassignmentCounter(stakeholders, reassignmentCounter),
              ) =>
            val locallyHostedStakeholders =
              stakeholders
                .filter(sh =>
                  partyToParticipants.getOrElse(sh, Set.empty).contains(thisLfParticipant)
                )
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

      val changes = toAcsChange(acsChange.activations, isActivation = true) ++
        toAcsChange(acsChange.deactivations, isActivation = false)
      Source(changes)
    }
    PekkoUtil.futureSourceUS(futureSource)
  }

  /** Returns a mapping between parties and the participants to which they are fully onboarded (i.e.
    * onboarding flag is false).
    * @param topologySnapshot
    *   the topology snapshot for looking up the party hosting information
    * @param parties
    *   the parties for which to load the hosting participants
    * @return
    */
  private def getOnboardedParticipantsOfParties(
      topologySnapshot: TopologySnapshot,
      parties: Set[LfPartyId],
  ): FutureUnlessShutdown[Map[LfPartyId, Set[LedgerParticipantId]]] =
    topologySnapshot
      .activeParticipantsOfPartiesWithInfo(parties.toSeq)
      .map { ptp =>
        val onboardingCompleted = ptp.view
          .mapValues(info =>
            info.participants.view.collect {
              case (pid, attr) if !attr.onboarding => pid.toLf
            }.toSet
          )
          .toMap
        onboardingCompleted
      }

  private val digestAccumulator = new SequentialDigestAccumulator(
    thisLfParticipant,
    acsDigestStore,
    stringInterning,
    hashOps,
    tracingMode,
    loggerFactory,
  )
  def inMemoryDigestAccumulator: Flow[DigestAccumulator_Input, DigestAccumulator_Output, NotUsed] =
    Flow[DigestAccumulator_Input]
      .mapAsyncAndDrainUS(1)(digestAccumulator.process)
      .collect { case Some(checkpointWritten) => checkpointWritten }

  def pipeline: Flow[Checkpointing_Input, DigestAccumulator_Output, NotUsed] =
    Flow[Checkpointing_Input].async
      .via(checkpointing)
      .async
      .via(classification)
      .async
      .via(inMemoryDigestAccumulator)

  def run()(implicit mat: Materializer) = for {
    latestCheckpointO <- acsDigestStore.latestCheckpointUpTo(Offset.MaxValue)
    startingOffsetO = latestCheckpointO
      .map { case (offset, _) => offset }
    _ = PekkoUtil.runSupervised(
      indexService
        .acsUpdates(synchronizerId, startingOffsetO)
        .map(update =>
          ProcessingContext(
            update.synchronizerTime,
            update.offset,
            update.acsUpdate,
          )
        )
        .via(pipeline)
        .to(
          Sink.foreach(cp =>
            logger.debug(
              s"An ACS digest checkpoint was written at ${cp.recordTimeInclusive} ${cp.offsetInclusive}"
            )
          )
        ),
      s"AcsDigestProcessor($synchronizerId)",
    )
  } yield ()
}

object RunningDigestProcessor {

  type Checkpointing_Input = ProcessingContext[InternalIndexService.AcsUpdate]
  type Checkpointing_Output = ProcessingContext[CheckpointFenceOr[InternalIndexService.AcsUpdate]]

  type Classifcation_Input = Checkpointing_Output
  type Classification_Output = ProcessingContext[CheckpointFenceOr[Classification]]

  type DigestAccumulator_Input = Classification_Output
  type DigestAccumulator_Output = CheckpointWritten

  /** Holds some data that we thread through the pipeline for a given input event.
    */
  final case class ProcessingContext[+T](
      recordTime: CantonTimestamp,
      offset: Offset,
      value: T,
  ) {
    def map[U](f: T => U): ProcessingContext[U] = copy(recordTime, offset, f(value))
  }

  /** Data type to represent a checkpointing fence or some event to be processed. While this is
    * isomorphic to Either[Unit, A], the explicit type adds clarity.
    */
  sealed trait CheckpointFenceOr[+A] extends Product with Serializable {
    def map[B](f: A => B): CheckpointFenceOr[B] = this match {
      case CheckpointFence => CheckpointFence
      case NotCheckpointFence(topologySnapshot, value) =>
        NotCheckpointFence(topologySnapshot, f(value))
    }

    @VisibleForTesting
    private[commitment] def tryValue: A = this match {
      case CheckpointFence => throw new NoSuchElementException("CheckpointFence")
      case NotCheckpointFence(_, value) => value
    }

    @VisibleForTesting
    private[commitment] def toOption: Option[A] =
      this match {
        case CheckpointFence => None
        case NotCheckpointFence(_, value) => Some(value)
      }
  }
  case object CheckpointFence extends CheckpointFenceOr[Nothing]
  final case class NotCheckpointFence[+A](topologySnapshot: TopologySnapshot, value: A)
      extends CheckpointFenceOr[A]
      with PrettyPrinting {
    override protected def pretty: Pretty[NotCheckpointFence.this.type] =
      prettyOfClass(
        unnamedParam(c => prettyOfString[A](_.toString).treeOf(c.value))
      )
  }

  /** The output of classification describes which digests need to be updated
    */
  sealed trait Classification extends Product with Serializable

  /** Defines which digests (party and participant) need to be updated with the hash of the
    * contract.
    * @param stakeholders
    *   the parties and affected participants for which the digest needs to be updated with the hash
    *   of the contract and the locally hosted stakeholders.
    * @param locallyHostedStakeholders
    *   the stakeholders of the contract that are hosted by the processing participant. This
    *   collection does not contain duplicates.
    */
  final case class AcsUpdate(
      stakeholders: Map[LfPartyId, Set[LedgerParticipantId]],
      locallyHostedStakeholders: Seq[LfPartyId],
      cid: LfContractId,
      rc: ReassignmentCounter,
      isActivation: Boolean,
  ) extends Classification

  /** When a party is being onboarded to a participant.
    */
  final case class PartyOnboardingToParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  /** When a party has been added to a participant.
    */
  final case class PartyAddedToParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  /** When a party has been removed from a participant.
    */
  final case class PartyRemovedFromParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  /** When a checkpoint has been written, meaning that all digests up to record time and offset
    * (both inclusive) have been persisted.
    */
  final case class CheckpointWritten(recordTimeInclusive: CantonTimestamp, offsetInclusive: Offset)

  /** Tracks changes to the hosting relationship per party.
    */
  class TopologyChangeTracker(
      private val deltas: Map[LfPartyId, Set[LedgerParticipantId] => Set[LedgerParticipantId]]
  ) {

    /** Returns a function that updates a party to participants map by adding the association from
      * the given party to the given participant.
      */
    def addPartyToParticipant(
        party: LfPartyId,
        participant: LedgerParticipantId,
    ): TopologyChangeTracker =
      new TopologyChangeTracker(
        deltas.updatedWith(party)(
          _.map(_.andThen(s => s + participant)).orElse(Some(s => s + participant))
        )
      )

    /** Returns a function that updates a party to participants map by removing the association from
      * the given party to the given participant.
      */
    def removePartyFromParticipant(
        party: LfPartyId,
        participant: LedgerParticipantId,
    ): TopologyChangeTracker = new TopologyChangeTracker(
      deltas.updatedWith(party)(
        _.map(_.andThen(s => s - participant)).orElse(Some(s => s - participant))
      )
    )

    def applyPendingTopologyChanges(
        map: Map[LfPartyId, Set[LedgerParticipantId]]
    ): Map[LfPartyId, Set[LedgerParticipantId]] =
      if (deltas.isEmpty) map
      else {
        map.view.map { case kv @ (party, participants) =>
          deltas.get(party).map(f => (party, f(participants))).getOrElse(kv)
        }.toMap
      }
  }

  object TopologyChangeTracker {
    val empty: TopologyChangeTracker = new TopologyChangeTracker(Map.empty)
  }
}
