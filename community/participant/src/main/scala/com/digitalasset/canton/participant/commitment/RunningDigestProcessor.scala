// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Onboarding,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  TopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  InternalIndexService,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.AcsLookup.AcsActiveContract
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.*
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext

/** Interface that describes what data the RunningDigestProcessor needs from the indexer.
  */
trait AcsLookup {

  /** Returns a stream of active contracts as of the given
    * [[com.digitalasset.canton.participant.event.RecordTime]] for which at least 1 of the given
    * parties is a stakeholder.
    */
  def activeContracts(
      parties: Set[LfPartyId],
      asOfInclusive: RecordTime,
  )(implicit traceContext: TraceContext): Source[AcsActiveContract, NotUsed]
}

object AcsLookup {
  final case class AcsActiveContract(
      cid: LfContractId,
      reassignmentCounter: ReassignmentCounter,
      stakeholders: Seq[LfPartyId],
  )

  /** Example on how the AcsLookup interface could be implemented with `InternalIndexService` as the
    * backing service.
    */
  def fromInternalIndexedService(
      svc: InternalIndexService
  ): AcsLookup = new AcsLookup {
    override def activeContracts(parties: Set[LfPartyId], asOfInclusive: RecordTime)(implicit
        traceContext: TraceContext
    ): Source[AcsActiveContract, NotUsed] =
      svc
        // TODO(#33084) convert RecordTime to Offset
        // TODO(#33084) make sure `parties` is not too large
        .activeContracts(parties, None)
        .map { ac =>
          AcsActiveContract(
            LfContractId.assertFromString(ac.getActiveContract.getCreatedEvent.contractId),
            ReassignmentCounter(ac.getActiveContract.reassignmentCounter),
            (ac.getActiveContract.getCreatedEvent.signatories.view ++ ac.getActiveContract.getCreatedEvent.observers.view)
              .map(LfPartyId.assertFromString)
              .toSeq,
          )
        }
  }
}

/** Builds the pipeline for processing events that trigger a change in the ACS commitment, namely
  *   - contract activations/deactivations
  *   - party onboarding to or offboarding from this or a remote participant
  */
class RunningDigestProcessor(
    thisParticipant: ParticipantId,
    acsLookup: AcsLookup,
    counterpartyBatchSize: PositiveInt,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext,
    tc: TraceContext,
) extends NamedLogging {

  private val thisLfParticipant = thisParticipant.toLf

  /** Determines whether to insert a checkpointing fence into the processing pipeline
    */
  def checkpointing: Flow[Checkpointing_Input, Checkpointing_Output, NotUsed] = ???

  /** Enriches the incoming events (acs change or topology change) with the data that is needed to
    * determine which digests need to be loaded and updated during a later stages of the pipeline.
    */
  def classification: Flow[Classifcation_Input, Classification_Output, NotUsed] =
    Flow[Classifcation_Input]
      .mapAsyncAndDrainUS(1 /* make configurable */ ) {
        // propagate checkpoint fences
        case ProcessingContext(recordTime, CheckpointFence) =>
          FutureUnlessShutdown.pure(
            Source.single(
              ProcessingContext[CheckpointFenceOr[Classification]](
                recordTime,
                CheckpointFence,
              )
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

        // determine the digests that need to be changed for remote topology changes
        case ProcessingContext(
              recordTime,
              NotCheckpointFence(
                topologySnapshot,
                TopologyInputEvent(
                  PartyToParticipantAuthorization(
                    party,
                    participant,
                    authorizationEvent,
                  )
                ),
              ),
            ) if participant != thisLfParticipant =>
          val classification = authorizationEvent match {
            case Onboarding(_) =>
              Option(
                PartyOnboardingToRemoteParticipant(party, participant)
              )

            case AuthorizationEvent.Added(_) =>
              Option(
                PartyAddedToRemoteParticipant(
                  party,
                  participant,
                )
              )

            case AuthorizationEvent.Revoked =>
              Option(
                PartyRemovedFromRemoteParticipant(
                  party,
                  participant,
                )
              )

            case ChangedTo(_) =>
              Option.empty
          }

          FutureUnlessShutdown.pure(
            Source(
              classification
                .map(classification =>
                  ProcessingContext[CheckpointFenceOr[Classification]](
                    recordTime,
                    NotCheckpointFence(
                      topologySnapshot,
                      classification,
                    ),
                  )
                )
                .toList
            )
          )

        // determine the digests that need to be changed for local topology changes
        case ProcessingContext(
              recordTime,
              NotCheckpointFence(
                topologySnapshot,
                TopologyInputEvent(
                  PartyToParticipantAuthorization(party, `thisLfParticipant`, Added(_))
                ),
              ),
            ) =>
          determineRequiredDigestUpdatesFromLocalTopologyChange(
            recordTime,
            topologySnapshot,
            party,
          )

        case _ =>
          FutureUnlessShutdown.pure(Source.empty)
      }
      .flatten

  /** Determines the required digests that need to be updated by:
    *   1. loading the ACS of `locallyOnboardedParty` to find all counterparties
    *   1. loading the ACS for batches of counterparties, discarding contracts that are not shared
    *      with `locallyOnboardedParty`
    *   1. streaming `AcsUpdate`s to update the digests of the counterparties with the respective
    *      hashes of (cid, rc, counterparty, locallyHostedParty).
    *
    * The reason for streaming the `AcsUpdate`s for batches of counterparties is to limit the number
    * of digests that need to be held in memory at any given point in time. At the end of processing
    * all possible updates to a counterparty's digest, this digest is now in a consistent state at
    * the respective record time and can be persisted to the database.
    */
  private def determineRequiredDigestUpdatesFromLocalTopologyChange(
      recordTime: RecordTime,
      topologySnapshot: TopologySnapshot,
      locallyOnboardedParty: LfPartyId,
  ): FutureUnlessShutdown[Source[ProcessingContext[NotCheckpointFence[AcsUpdate]], NotUsed]] = {
    val acsUpdates = acsLookup
      // load the ACS of the party to determine the counterparties that need to have their digest updated
      .activeContracts(Set(locallyOnboardedParty), recordTime)
      .statefulMapConcat { () =>
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
      .grouped(counterpartyBatchSize.unwrap)
      .flatMapConcat { counterparties =>
        val counterpartiesSet = counterparties.toSet

        // for a group of counterparties, load the acs
        // and emit the corresponding classification
        acsLookup
          .activeContracts(counterparties.toSet, recordTime)
          .mapAsyncAndDrainUS(1) { activeContractOfCounterparty =>
            val stakeholdersOfContract = activeContractOfCounterparty.stakeholders

            // TODO(#33084) this check should happen as part of `activeContracts` or its replacement
            if (!stakeholdersOfContract.contains(locallyOnboardedParty)) {
              // if `locallyOnboardedParty` is not a stakeholder of `activeContractOfCounterparty`,
              // then there is nothing to do, as this contract of the counterparty is not shared with the local party
              FutureUnlessShutdown.pure(Seq.empty[AcsUpdate])
            } else {
              for {
                partyToParticipant <- getOnboardedParticipantsOfParties(
                  topologySnapshot,
                  stakeholdersOfContract,
                )
              } yield {
                // emit the classification update for all stakeholders of the current stakeholder batch
                // of the contract and their respective hosting participants.
                val stakeholdersToHostingParticipants = stakeholdersOfContract.view
                  .filter(counterpartiesSet.contains)
                  .map(sh => sh -> partyToParticipant.getOrElse(sh, Set.empty).toSeq)
                  .toMap

                Seq(
                  AcsUpdate(
                    stakeholdersToHostingParticipants,
                    // only emit the onboarded party as local party. Other locally hosted stakeholders will have already
                    // been processed by other events (e.g. an AcsChange or their own party onboarding event).
                    Seq(locallyOnboardedParty),
                    activeContractOfCounterparty.cid,
                    activeContractOfCounterparty.reassignmentCounter,
                    isActivation = true,
                  )
                )
              }
            }
          }
          .mapConcat(
            _.map(acsUpdate =>
              ProcessingContext(recordTime, NotCheckpointFence(topologySnapshot, acsUpdate))
            )
          )
      }
    FutureUnlessShutdown.pure(acsUpdates)
  }

  private def determineRequiredDigestChangesFromAcsChange(
      recordTime: RecordTime,
      topologySnapshot: TopologySnapshot,
      acsChange: AcsChange,
  ): FutureUnlessShutdown[Source[ProcessingContext[CheckpointFenceOr[Classification]], NotUsed]] = {
    val allStakeholders = acsChange.activations.values.flatMap(_.stakeholders) ++
      acsChange.deactivations.values.flatMap(_.stakeholders)

    for {
      partyToParticipants <- getOnboardedParticipantsOfParties(
        topologySnapshot,
        allStakeholders.toSeq.distinct,
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
              .map(sh => sh -> partyToParticipants.getOrElse(sh, Set.empty).toSeq)
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
  }

  private def getOnboardedParticipantsOfParties(
      topologySnapshot: TopologySnapshot,
      parties: Seq[LfPartyId],
  ): FutureUnlessShutdown[Map[LfPartyId, Set[LedgerParticipantId]]] =
    topologySnapshot
      .activeParticipantsOfPartiesWithInfo(parties)
      .map { ptp =>
        ptp.view
          .mapValues(info =>
            info.participants.view.collect {
              case (pid, attr) if !attr.onboarding => pid.toLf
            }.toSet
          )
          .toMap

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
    def apply(event: TopologyEvent): InputEvent = TopologyInputEvent(event)
    def apply(acsChange: AcsChange): InputEvent = AcsChangeInputEvent(acsChange)
  }
  final case class TopologyInputEvent(event: TopologyEvent) extends InputEvent
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
    *   the parties and their respective hosting participants for which their digest needs to be
    *   updated with the hash of the contract and the locally hosted stakeholders.
    * @param locallyHostedStakeholders
    *   the stakeholders of the contract that are hosted by the processing participant. This
    *   collection does not contain duplicates.
    */
  final case class AcsUpdate(
      stakeholders: Map[LfPartyId, Seq[LedgerParticipantId]],
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

}
