// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.runner

import cats.syntax.either.*
import cats.syntax.flatMap.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.commands.DisclosedContract as ProtoDisclosedContract
import com.daml.ledger.api.v2.transaction.Transaction as ProtoTransaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.ast.Implicits.*
import com.digitalasset.canton.testing.modelbased.conversions.ConcreteToCommands
import com.digitalasset.canton.testing.modelbased.conversions.ConcreteToCommands.*
import com.digitalasset.canton.testing.modelbased.projections.ToProjection.{
  ContractIdReverseMapping,
  PartyIdReverseMapping,
}
import com.digitalasset.canton.testing.modelbased.projections.{Projections, ToProjection}
import com.digitalasset.canton.testing.modelbased.runner.InterpretationErrors.{
  InterpreterError,
  SubmitFailure,
}
import com.digitalasset.canton.testing.modelbased.universal.java.universal
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Party, PartyKind, PhysicalSynchronizerId}
import com.digitalasset.daml.lf.data.Ref

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.OptionConverters.*

/** An interpreter that executes scenarios against a real Canton instance.
  *
  * Unlike [[com.digitalasset.canton.testing.modelbased.runner.ReferenceInterpreter]] which runs
  * against an IDE ledger, this interpreter submits commands to actual Canton participants and
  * collects transaction trees to compute per-party, per-participant projections.
  *
  * Create instances using [[CantonInterpreter.initializeAndUpload]].
  */
final class CantonInterpreter private (
    participants: IndexedSeq[ParticipantReference],
    synchronizerId: PhysicalSynchronizerId,
    allocateParties: CantonInterpreter.AllocateParties,
) {

  import CantonInterpreter.*

  /** Checks the cancellation function and returns `Left` if cancellation has been requested. */
  private def checkCancelled(cancelled: () => Boolean): Either[InterpreterError, Unit] =
    Either.cond(!cancelled(), (), SubmitFailure("Cancelled"))

  // -- Command execution --

  /** Executes a single [[Concrete.Commands]] against a Canton participant.
    *
    * @return
    *   the new contract ID mappings and updated disclosure store produced by this submission
    */
  private def runCommands(
      partyIdMapping: CantonPartyIdMapping,
      contractIdMapping: ContractIdMapping,
      disclosureStore: DisclosureStore,
      commands: Concrete.Commands,
  ): Either[InterpreterError, (ContractIdMapping, DisclosureStore)] =
    for {
      protoCommands <- commands.commands
        .traverse(cmd =>
          concreteToCommands.actionToProtoCommand(partyIdMapping, contractIdMapping, cmd.action)
        )
        .left
        .map[InterpreterError](SubmitFailure(_))
      result <- {
        val disclosures = commands.disclosures.toSeq.map(
          disclosureStore.fetchDisclosure(contractIdMapping, _)
        )

        val participant = participants(commands.participantId)
        val actAs =
          commands.actAs.toSeq.map(partyIdMapping)
        // Submit with LEDGER_EFFECTS shape to get exercise results and created event blobs
        Either
          .catchOnly[CommandFailure](
            participant.ledger_api.commands.submit(
              actAs = actAs,
              commands = protoCommands,
              readAs = Seq.empty,
              disclosedContracts = disclosures,
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
              includeCreatedEventBlob = true,
            )
          )
          .left
          .map[InterpreterError](e => SubmitFailure(e.getMessage))
          .map { protoTx =>
            val updatedDisclosureStore = disclosureStore.recordFromTransaction(protoTx)
            val javaTx =
              javaapi.data.Transaction.fromProto(ProtoTransaction.toJavaProto(protoTx))
            (
              // In the case of a Create node, commandResultsToContractIdMapping only extracts the singleton mapping for
              // the created contract, so we need to combine it with the existing mapping to preserve previously mapped
              // contract IDs. In the case of an Exercise, this union is redundant but harmless.
              contractIdMapping ++ ContractIdMappings.commandResultsToContractIdMapping(
                commands.commands.map(_.action),
                javaTx,
              ),
              updatedDisclosureStore,
            )
          }
      }
    } yield result

  /** Executes a list of [[Concrete.Commands]], threading contract ID mapping and disclosure store
    * through.
    *
    * @return
    *   the accumulated contract ID mapping after all submissions
    */
  private def runCommandsList(
      partyIdMapping: CantonPartyIdMapping,
      contractIdMapping: ContractIdMapping,
      commandsList: List[Concrete.Commands],
      cancelled: () => Boolean,
  ): Either[InterpreterError, ContractIdMapping] =
    commandsList
      .foldLeftM((contractIdMapping, DisclosureStore())) {
        case ((previousContractIdMapping, previousDisclosureStore), commands) =>
          // We check the cancellation flag between each runCommands to enable cooperative shutdown.
          checkCancelled(cancelled) >> runCommands(
            partyIdMapping,
            previousContractIdMapping,
            previousDisclosureStore,
            commands,
          )
      }
      .map(_._1)

  /** For each party, for each participant that hosts it, fetch the projection. We check the
    * cancellation flag between each fetch to enable cooperative shutdown.
    */
  private def fetchProjections(
      partyToParticipants: Map[Concrete.PartyId, Set[Concrete.Participant]],
      partyIdMapping: CantonPartyIdMapping,
      contractIdMapping: ContractIdMapping,
      numLedgerSteps: Int,
      cancelled: () => Boolean,
  ): Either[InterpreterError, Map[
    Projections.PartyId,
    Map[Concrete.ParticipantId, Projections.Projection],
  ]] = {
    val reversePartyIds = partyIdMapping.view.mapValues(_.partyId.toLf).toMap.map(_.swap)
    val reverseContractIds = contractIdMapping.map { case (k, v) => v.contractId -> k }
    partyToParticipants.toList
      .traverse { case (partyId, partyParticipants) =>
        val party = partyIdMapping(partyId).partyId
        partyParticipants.toList
          .traverse { p =>
            checkCancelled(cancelled).map { _ =>
              p.participantId -> fetchProjection(
                participants(p.participantId),
                party,
                reversePartyIds,
                reverseContractIds,
                numLedgerSteps,
              )
            }
          }
          .map(entries => partyId -> entries.toMap)
      }
      .map(_.toMap)
  }

  /** Run a full scenario against Canton participants and compute per-party, per-participant
    * projections.
    *
    * This method:
    *   - Allocates parties on participants using the provided `allocateParties` function
    *   - Translates and submits each
    *     [[com.digitalasset.canton.testing.modelbased.ast.Concrete.Commands]] against the correct
    *     participant
    *   - Fetches transaction trees from each participant for each party
    *   - Computes per-party, per-participant projections using
    *     [[com.digitalasset.canton.testing.modelbased.projections.ToProjection]]
    *
    * @param scenario
    *   the scenario to execute
    * @param cancelled
    *   a cancellation function: when it returns `true`, the interpreter stops between RPC calls and
    *   returns a `Left("Cancelled")` error.
    * @return
    *   either an error message, or a map from party ID to (participant ID to projection)
    */
  def runAndProject(
      scenario: Concrete.Scenario,
      cancelled: () => Boolean = () => false,
  )(implicit partyKind: PartyKind): Either[
    String,
    Map[Projections.PartyId, Map[Concrete.ParticipantId, Projections.Projection]],
  ] = {

    val partySuffix = uniqueRunId.incrementAndGet()
    def party(pid: Concrete.PartyId) = s"p$pid-$partySuffix"

    val partyToParticipants: Map[Concrete.PartyId, Set[Concrete.Participant]] =
      scenario.topology.groupedByPartyId

    // Each party is "owned" by the first participant that hosts it
    val partyOwnership: Seq[(String, com.digitalasset.canton.topology.ParticipantId)] =
      partyToParticipants.toSeq.map { case (partyId, parts) =>
        party(partyId) -> participants(
          parts.headOption
            .getOrElse(throw new IllegalArgumentException("party without participant"))
            .participantId
        ).id
      }

    val participantPermission = partyKind match {
      case PartyKind.Local => ParticipantPermission.Submission
      case _: PartyKind.External => ParticipantPermission.Confirmation
    }

    // Build target topology: each party should be hosted on all participants listed in the topology
    val targetTopology: Map[String, Map[
      PhysicalSynchronizerId,
      (PositiveInt, Set[(com.digitalasset.canton.topology.ParticipantId, ParticipantPermission)]),
    ]] =
      partyToParticipants.map { case (partyId, partyParticipants) =>
        val hostingParticipants =
          partyParticipants
            .map[(com.digitalasset.canton.topology.ParticipantId, ParticipantPermission)](p =>
              participants(p.participantId).id -> participantPermission
            )
        party(partyId) -> Map(synchronizerId -> (PositiveInt.one, hostingParticipants))
      }

    // Allocate parties according to the two mappings computed above
    val allocatedParties: Seq[Party] =
      allocateParties(participants.toSet, partyOwnership, targetTopology)

    // Build mapping from abstract party IDs to allocated Canton party IDs
    val partyIdMapping: CantonPartyIdMapping = partyToParticipants.keys.toSeq
      .zip(allocatedParties)
      .map { case (abstractId, cantonParty) =>
        abstractId -> cantonParty
      }
      .toMap

    // Run all commands
    val result = for {
      contractIdMapping <- runCommandsList(
        partyIdMapping = partyIdMapping,
        contractIdMapping = Map.empty,
        commandsList = scenario.ledger,
        cancelled = cancelled,
      )
      projections <- fetchProjections(
        partyToParticipants = partyToParticipants,
        partyIdMapping = partyIdMapping,
        contractIdMapping = contractIdMapping,
        numLedgerSteps = scenario.ledger.size,
        cancelled = cancelled,
      )
    } yield projections

    result.left.map(_.pretty)
  }
}

object CantonInterpreter {

  // -- Type aliases --

  /** For a given party on a given synchronizer: confirmation threshold and participant permission
    * per participant.
    */
  type PartyHostingState =
    (
        PositiveInt,
        Set[(com.digitalasset.canton.topology.ParticipantId, ParticipantPermission)],
    )

  /** A function that allocates parties on participants, returning their allocated
    * [[com.digitalasset.canton.topology.Party]]s.
    *
    * This is typically backed by `com.digitalasset.canton.integration.util.PartiesAllocator.apply`
    * but is injected as a function to avoid a dependency on `community-app`.
    */
  type AllocateParties = (
      Set[ParticipantReference],
      Seq[(String, com.digitalasset.canton.topology.ParticipantId)],
      Map[String, Map[PhysicalSynchronizerId, PartyHostingState]],
  ) => Seq[Party]

  // -- Converters --

  private val concreteToCommands = new ConcreteToCommands(
    Ref.PackageId.assertFromString(universal.Universal.PACKAGE_ID)
  )

  // -- Disclosure fetching --

  /** Tracks created event blobs seen during submission, keyed by concrete contract ID. */
  private final case class DisclosureStore(
      disclosures: Map[String, ProtoDisclosedContract] = Map.empty
  ) {

    /** Record all created events with blobs from a proto transaction. */
    def recordFromTransaction(transaction: ProtoTransaction): DisclosureStore = {
      val newBlobs = transaction.events.foldLeft(disclosures) { (acc, event) =>
        event.event.created.fold(acc) { created =>
          if (!created.createdEventBlob.isEmpty)
            acc.updated(
              created.contractId,
              ProtoDisclosedContract(
                templateId = created.templateId,
                contractId = created.contractId,
                createdEventBlob = created.createdEventBlob,
                synchronizerId = transaction.synchronizerId,
              ),
            )
          else acc
        }
      }
      DisclosureStore(newBlobs)
    }

    /** Retrieves the disclosure of an abstract contract ID. */
    def fetchDisclosure(
        contractIdMapping: ContractIdMapping,
        abstractContractId: Concrete.ContractId,
    ): ProtoDisclosedContract = {
      val concreteContractId = contractIdMapping(abstractContractId).contractId.coid
      disclosures.getOrElse(
        concreteContractId,
        throw new IllegalArgumentException(
          s"No disclosure found for contract $abstractContractId (concrete: $concreteContractId). " +
            s"Available disclosures: ${disclosures.keys.mkString(", ")}"
        ),
      )
    }
  }

  // -- Projection retrieval --

  /** Fetches transaction trees from a participant for a given party and convert to projections. */
  private def fetchProjection(
      participant: ParticipantReference,
      party: com.digitalasset.canton.topology.Party,
      reversePartyIds: PartyIdReverseMapping,
      reverseContractIds: ContractIdReverseMapping,
      numTransactions: Int,
  ): Projections.Projection =
    if (numTransactions == 0) Nil
    else {
      val javaTxs: Seq[javaapi.data.GetUpdatesResponse] =
        participant.ledger_api.javaapi.updates.transactions(
          partyIds = Set(party),
          completeAfter = PositiveInt.tryCreate(numTransactions),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )

      val transactions: List[javaapi.data.Transaction] =
        javaTxs.flatMap(_.getTransaction.toScala).toList

      ToProjection.convertFromCantonProjection(
        reversePartyIds,
        reverseContractIds,
        transactions,
      )
    }

  // -- Entry point --

  /** A global counter to generate unique party IDs across multiple runs. */
  private val uniqueRunId = new AtomicInteger(0)

  /** Initializes a [[CantonInterpreter]] by uploading the universal DAR to all participants.
    *
    * The DAR is uploaded once during initialization. The returned interpreter can then be used to
    * run multiple scenarios without re-uploading.
    *
    * @param participants
    *   the Canton participants, indexed by abstract participant ID
    * @param synchronizerId
    *   the synchronizer to which all participants are connected
    * @param allocateParties
    *   a function that allocates parties on participants
    * @return
    *   a [[CantonInterpreter]] ready to run scenarios
    */
  def initializeAndUpload(
      participants: IndexedSeq[ParticipantReference],
      synchronizerId: PhysicalSynchronizerId,
      allocateParties: AllocateParties,
  ): CantonInterpreter = {
    val universalDarUrl = getClass.getClassLoader.getResource("universal.dar")
    require(universalDarUrl != null, s"universal.dar not found on the classpath")
    participants.foreach(_.dars.upload(universalDarUrl.getFile).discard)
    new CantonInterpreter(participants, synchronizerId, allocateParties)
  }
}
