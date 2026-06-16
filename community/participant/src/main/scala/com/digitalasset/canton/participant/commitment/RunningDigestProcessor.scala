// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
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
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.AcsLookup.AcsActiveContract
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.*
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

/** Interface that describes what data the RunningDigestProcessor needs from the indexer.
  */
trait AcsLookup {

  /** Returns a stream of active contracts as of the given
    * [[com.digitalasset.canton.participant.event.RecordTime]], which have at least one party in
    * `anyOf` as stakeholder and, if set, also at least one party in `anyOf2` as stakeholder. If
    * `anyOf2` is not specified, it does not act as a filter.
    */
  def contractMetadata(
      anyOf: Set[LfPartyId],
      anyOf2: Set[LfPartyId],
      synchronizerId: SynchronizerId,
      asOfInclusive: RecordTime,
  )(implicit traceContext: TraceContext): Source[AcsActiveContract, NotUsed]
}

object AcsLookup {
  final case class AcsActiveContract(
      cid: LfContractId,
      reassignmentCounter: ReassignmentCounter,
      stakeholders: Set[LfPartyId],
  )

  /** Example on how the AcsLookup interface could be implemented with `InternalIndexService` as the
    * backing service.
    */
  def fromInternalIndexedService(
      svc: InternalIndexService
  ): AcsLookup = new AcsLookup {
    override def contractMetadata(
        anyOf: Set[LfPartyId],
        anyOf2: Set[LfPartyId],
        synchronizerId: SynchronizerId,
        asOfInclusive: RecordTime,
    )(implicit
        traceContext: TraceContext
    ): Source[AcsActiveContract, NotUsed] =
      svc
        // TODO(#33084) convert RecordTime to Offset
        // TODO(#33084) make sure `anyOf` is not too large
        .activeContracts(anyOf, None)
        .mapConcat { ac =>
          val stakeholders =
            (ac.getActiveContract.getCreatedEvent.signatories.view ++ ac.getActiveContract.getCreatedEvent.observers.view)
              .map(LfPartyId.assertFromString)
              .toSet

          val matchesPartyRequirements =
            anyOf2.isEmpty || stakeholders.exists(anyOf2)
          val matchesSynchronizer =
            ac.getActiveContract.synchronizerId == synchronizerId.toProtoPrimitive

          Option.when(matchesPartyRequirements && matchesSynchronizer)(
            AcsActiveContract(
              LfContractId.assertFromString(ac.getActiveContract.getCreatedEvent.contractId),
              ReassignmentCounter(ac.getActiveContract.reassignmentCounter),
              stakeholders,
            )
          )
        }
  }
}

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
    acsLookup: AcsLookup,
    getTopologySnapshot: CantonTimestamp => FutureUnlessShutdown[TopologySnapshot],
    counterpartyBatchSize: PositiveInt,
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
      rt: RecordTime,
      previouslyProcessedRecordTime: RecordTime,
      topologySnapshot: TopologySnapshot,
  ): FutureUnlessShutdown[Option[RecordTime]] =
    topologySnapshot.findDynamicSynchronizerParameters().flatMap {
      case Right(params) =>
        val boundary = RecordTime(
          CantonTimestamp.assertFromLong(
            rt.timestamp.toMicros - (rt.timestamp.toMicros % params.parameters.reconciliationInterval.toScala.toMicros)
          ),
          tieBreaker = 0L,
        )
        FutureUnlessShutdown
          .pure(Option.when(previouslyProcessedRecordTime <= boundary && boundary < rt)(boundary))
      case Left(error) => FutureUnlessShutdown.failed(new IllegalStateException(error))
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
          0, // numEventsSinceLastCheckpointFence
          // TODO(#33084) use proper value from crash recovery
          RecordTime.MinValue, // previously processed record time
        )
      ) {
        case (
              (numEventsSinceLastCheckpoint, previousRecordTime),
              ProcessingContext(recordTime, event),
            ) =>
          for {
            topologySnapshot <- getTopologySnapshot(recordTime.timestamp)
            crossedReconciliationIntervalBoundary <- determineCheckpointAtReconciliationBoundary(
              rt = recordTime,
              previouslyProcessedRecordTime = previousRecordTime,
              topologySnapshot,
            )
          } yield {
            val (updatedNumEventsSinceLastCheckpoint, result) = event match {
              case AcsChangeInputEvent(acsChange) =>
                // emit a checkpoint at the record time's predecessor after maxNumUpdatesBetweenCheckpoints events have been emitted
                lazy val checkpointByNumProcessedEvents = Option.when(
                  (numEventsSinceLastCheckpoint >= maxNumUpdatesBetweenCheckpoints.unwrap && previousRecordTime != recordTime)
                )(immediatePredecessor(recordTime))
                // emit a checkpoint fence after crossing a reconciliation interval boundary
                val maybeFence = crossedReconciliationIntervalBoundary
                  .orElse(checkpointByNumProcessedEvents)
                  .map(rt => ProcessingContext(recordTime = rt, CheckpointFence))

                val acsEvent = List(
                  ProcessingContext(
                    recordTime,
                    NotCheckpointFence(topologySnapshot, AcsChangeInputEvent(acsChange)),
                  )
                )
                maybeFence
                  // if a checkpoint fence is about to be emitted, it is emitted BEFORE the AcsChange,
                  // so we reset the counter to 1 instead of 0, to correctly count the AcsChange emitted
                  // after the checkpoint fence
                  .map(fence => (1, fence :: acsEvent))
                  // otherwise increase the counter and emit just the AcsChange
                  .getOrElse((numEventsSinceLastCheckpoint + 1, acsEvent))

              case TopologyInputEvent(topologyEvent) =>
                // add a checkpoint fence after the topology event
                val events =
                  Seq(
                    NotCheckpointFence(topologySnapshot, TopologyInputEvent(topologyEvent)),
                    CheckpointFence,
                  )
                    .map(
                      ProcessingContext(recordTime, _)
                    )
                // the checkpoint is emitted AFTER the topology change, so we reset the counter to 0
                (0, events)
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
        case ProcessingContext(recordTime, CheckpointFence) =>
          Source.single(
            ProcessingContext[CheckpointFenceOr[Classification]](
              recordTime,
              CheckpointFence,
            )
          )

        // determine which digests need to be changed for acs changes:
        // for each activation/deactivation, update the digest for all stakeholders with the locally hosted parties.
        // the returned classification also contains the information about the counterparticipants that need to be updated.
        case ProcessingContext(
              recordTime,
              NotCheckpointFence(topoSnapshot, AcsChangeInputEvent(acsChange)),
            ) =>
          determineRequiredDigestChangesFromAcsChange(recordTime, topoSnapshot, acsChange)

        // determine the digests that need to be changed for topology changes
        case ProcessingContext(
              recordTime,
              NotCheckpointFence(
                topologySnapshot,
                TopologyInputEvent(topologyTransactionEffective),
              ),
            ) =>
          // given n topology events at the same record time, when processing the i-th topology event (where i <= n),
          // all effects of the previously processed topology events 1 <= j < i must be applied to the party to participant
          // topology state, so that the classification correctly calculates the required digest updates.
          Source(topologyTransactionEffective.events)
            .statefulMapAsyncUSAndDrain(
              // start with the noop change
              TopologyChangeTracker.empty
            ) {
              // determine the digests that need to be changed for adding a party to thisParticipant
              case (
                    changeTracker,
                    PartyToParticipantAuthorization(party, `thisLfParticipant`, Added(_)),
                  ) =>
                val updatedTracker = changeTracker.addPartyToParticipant(party, thisLfParticipant)

                determineClassificationForLocalTopologyChange(
                  recordTime,
                  topologySnapshot,
                  party,
                  // the fact that `party` was onboarded to `thisParticipant` is not yet reflected in the topology state itself.
                  // but this participant's digest must be updated as well, therefore we need to register this topology change BEFORE
                  // processing the event
                  updatedTracker.applyPendingTopologyChanges,
                  isActivation = true,
                ).map(source => (updatedTracker, source))

              // determine the digests that need to be changed for removing a party from thisParticipant
              case (
                    changeTracker,
                    PartyToParticipantAuthorization(party, `thisLfParticipant`, Revoked),
                  ) =>
                determineClassificationForLocalTopologyChange(
                  recordTime,
                  topologySnapshot,
                  party,
                  // we want to update thisParticipant's digest with the removal of the corresponding contract hashes.
                  // therefore we must apply the offboarding of the party only AFTER processing the event, so that the resulting
                  // AcsUpdate still includes thisParticipant.
                  changeTracker.applyPendingTopologyChanges,
                  isActivation = false,
                ).map { source =>
                  val updatedTracker =
                    changeTracker.removePartyFromParticipant(party, thisLfParticipant)
                  (updatedTracker, source)
                }

              // determine the digests that need to be changed for remote topology changes
              case (changeTracker, ptp: PartyToParticipantAuthorization)
                  if ptp.participant != thisLfParticipant =>
                val (updatedChangeTracker, classification) =
                  classificationForRemotePartyAuthorization(ptp, changeTracker)
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
                NotCheckpointFence(
                  topologySnapshot,
                  classification,
                ),
              )
            )
      }

  /** Determines the classification for a remote topology event and register the corresponding
    * change in the topology change tracker.
    */
  private def classificationForRemotePartyAuthorization(
      topologyEvent: PartyToParticipantAuthorization,
      changeTracker: TopologyChangeTracker,
  ): (TopologyChangeTracker, Option[Classification]) = {
    val PartyToParticipantAuthorization(party, participant, authorizationEvent) = topologyEvent
    authorizationEvent match {
      case AuthorizationEvent.Onboarding(_) =>
        changeTracker ->
          Option(PartyOnboardingToRemoteParticipant(party, participant))

      case AuthorizationEvent.Added(_) =>
        changeTracker.addPartyToParticipant(party, participant) ->
          Option(
            PartyAddedToRemoteParticipant(
              party,
              participant,
            )
          )

      case AuthorizationEvent.Revoked =>
        changeTracker.removePartyFromParticipant(party, participant) ->
          Option(
            PartyRemovedFromRemoteParticipant(
              party,
              participant,
            )
          )

      case ChangedTo(_) =>
        changeTracker -> Option.empty
    }
  }

  def reinitializationAcsUpdates(
      recordTime: RecordTime,
      topologySnapshot: TopologySnapshot,
  ): Source[ProcessingContext[CheckpointFenceOr[AcsUpdate]], NotUsed] = {
    val parties: Source[LfPartyId, NotUsed] =
      acsLookup
        .contractMetadata(Set.empty, Set.empty, synchronizerId, recordTime)
        .statefulMapConcat(extractPartiesFromContracts())

    val groupedParties: Source[Seq[LfPartyId], NotUsed] =
      parties.grouped(counterpartyBatchSize.unwrap)

    val acsUpdates: Source[ProcessingContext[CheckpointFenceOr[AcsUpdate]], NotUsed] =
      groupedParties
        .flatMap { counterparties =>
          val counterpartiesSet = counterparties.toSet

          acsLookup
            .contractMetadata(counterparties.toSet, Set.empty, synchronizerId, recordTime)
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
                  activeContractOfCounterparty.cid,
                  activeContractOfCounterparty.reassignmentCounter,
                  isActivation = true,
                )

                ProcessingContext(recordTime, NotCheckpointFence(topologySnapshot, acsUpdate))
              }
            }

        }

    acsUpdates.concat(Source.single(ProcessingContext(recordTime, CheckpointFence)))
  }

  private def extractPartiesFromContracts(): () => AcsActiveContract => Set[LfPartyId] = { () =>
    // keep track of the already processed counterparties
    // TODO(#33084) it would be better to use interned party ids instead of the full party strings
    val alreadyVisitedCounterParties = mutable.Set.empty[LfPartyId]

    activeContract => {
      val notYetProcessedCounterparties =
        activeContract.stakeholders.filter(!alreadyVisitedCounterParties.contains(_))

      if (notYetProcessedCounterparties.nonEmpty)
        alreadyVisitedCounterParties.addAll(notYetProcessedCounterparties)

      // emit the counterparties that haven't been processed yet
      notYetProcessedCounterparties
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
    */
  private def determineClassificationForLocalTopologyChange(
      recordTime: RecordTime,
      topologySnapshot: TopologySnapshot,
      partyAffectedByTopologyChange: LfPartyId,
      applyTopologyChange: ApplyTopologyChange,
      isActivation: Boolean,
  ): FutureUnlessShutdown[Source[Classification, NotUsed]] = {
    val acsUpdates = acsLookup
      // load the ACS of the party to determine the counterparties that need to have their digest updated
      .contractMetadata(
        anyOf = Set(partyAffectedByTopologyChange),
        anyOf2 = Set.empty,
        synchronizerId,
        recordTime,
      )
      .statefulMapConcat(extractPartiesFromContracts())
      .grouped(counterpartyBatchSize.unwrap)
      .flatMapConcat { counterparties =>
        // for a group of counterparties, load the acs that is shared with the locally onboarded party
        // and emit the corresponding classification
        acsLookup
          .contractMetadata(
            anyOf = counterparties.toSet,
            anyOf2 = Set(partyAffectedByTopologyChange),
            synchronizerId,
            recordTime,
          )
          .mapAsyncAndDrainUS(1) { activeContractOfCounterparty =>
            val stakeholdersOfContract = activeContractOfCounterparty.stakeholders

            for {
              partyToParticipant <- getOnboardedParticipantsOfParties(
                topologySnapshot,
                stakeholdersOfContract,
              )
                // immediately apply the topology changes from the same record time,
                // so that there is only the final mapping in scope
                .map(applyTopologyChange)
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
                  activeContractOfCounterparty.cid,
                  activeContractOfCounterparty.reassignmentCounter,
                  isActivation = isActivation,
                )
              )
            }
          }
          .mapConcat(identity)
      }
    FutureUnlessShutdown.pure(acsUpdates)
  }

  private def determineRequiredDigestChangesFromAcsChange(
      recordTime: RecordTime,
      topologySnapshot: TopologySnapshot,
      acsChange: AcsChange,
  ): Source[ProcessingContext[CheckpointFenceOr[Classification]], NotUsed] = {
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
      ): immutable.Iterable[ProcessingContext[CheckpointFenceOr[Classification]]] =
        change.map {
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
            val stakeholdersToHostingParticipants = stakeholders.view
              .map(sh => sh -> partyToParticipants.getOrElse(sh, Set.empty))
              .toMap
            ProcessingContext(
              recordTime,
              NotCheckpointFence(
                topologySnapshot,
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
                ),
              ),
            )
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

  def inMemoryDigestAccumulator: Flow[DigestAccumulator_Input, DigestAccumulator_Output, NotUsed] =
    ???

  def pipeline: Flow[Checkpointing_Input, DigestAccumulator_Output, NotUsed] =
    Flow[Checkpointing_Input].async
      .via(checkpointing)
      .async
      .via(classification)
      .async
      .via(inMemoryDigestAccumulator)
}

object RunningDigestProcessor {

  sealed trait InputEvent extends Product with Serializable
  object InputEvent {
    def apply(event: Update.TopologyTransactionEffective): InputEvent = TopologyInputEvent(event)
    def apply(acsChange: AcsChange): InputEvent = AcsChangeInputEvent(acsChange)
  }
  final case class TopologyInputEvent(event: Update.TopologyTransactionEffective) extends InputEvent
  final case class AcsChangeInputEvent(acsChange: AcsChange) extends InputEvent

  type Checkpointing_Input = ProcessingContext[InputEvent]
  type Checkpointing_Output = ProcessingContext[CheckpointFenceOr[InputEvent]]

  type Classifcation_Input = Checkpointing_Output
  type Classification_Output = ProcessingContext[CheckpointFenceOr[Classification]]

  type DigestAccumulator_Input = Classification_Output
  type DigestAccumulator_Output = CheckpointWritten

  /** Holds some data that we thread through the pipeline for a given input event.
    */
  final case class ProcessingContext[+T](
      recordTime: RecordTime,
      value: T,
  ) {
    def map[U](f: T => U): ProcessingContext[U] = copy(recordTime, f(value))
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

  /** When a party is being onboarded to a remote participant.
    */
  final case class PartyOnboardingToRemoteParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  /** When the party onboarding to a remote participant has completed.
    */
  final case class PartyAddedToRemoteParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  /** For a party offboarding from a remote participant, the `party`'s digest must be subtracted
    * from the `participant`'s digest.
    */
  final case class PartyRemovedFromRemoteParticipant(
      party: LfPartyId,
      participant: LedgerParticipantId,
  ) extends Classification

  /** Signals the record time for which a checkpoint has been written.
    */
  final case class CheckpointWritten(upToInclusive: RecordTime)

  /** Represents updates to a map from parties to a set of participants.
    */
  private type ApplyTopologyChange =
    Map[LfPartyId, Set[LedgerParticipantId]] => Map[LfPartyId, Set[LedgerParticipantId]]

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

  def immediatePredecessor(rt: RecordTime): RecordTime =
    if (rt.tieBreaker == RecordTime.lowestTiebreaker)
      RecordTime(rt.timestamp.immediatePredecessor, RecordTime.highestTiebreaker)
    else (RecordTime(rt.timestamp, rt.tieBreaker - 1))

}
