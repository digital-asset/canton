// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.runner

import cats.implicits.toTraverseOps
import cats.instances.all.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.conversions.ConcreteToCommands
import com.digitalasset.canton.testing.modelbased.conversions.ConcreteToCommands.*
import com.digitalasset.canton.testing.modelbased.projections.{Projections, ToProjection}
import com.digitalasset.canton.testing.modelbased.runner.InterpretationErrors.{
  InterpreterError,
  SubmitFailure,
  TranslationError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.IdeLedgerBridge
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.script.IdeLedger
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  NextGenContractStateMachine as ContractStateMachine,
}

import java.io.File
import scala.annotation.tailrec

object ReferenceInterpreter {

  /** The IDE ledger state with the current submission seed. */
  private final case class LedgerState(
      ledger: IdeLedger,
      seed: com.digitalasset.daml.lf.crypto.Hash,
  ) {
    def advance(newLedger: IdeLedger): LedgerState =
      LedgerState(newLedger, IdeLedgerBridge.nextSeed(seed))
  }

  /** Creates a ReferenceInterpreter by loading the universal DAR from the classpath. */
  def apply(
      loggerFactory: NamedLoggerFactory,
      csmMode: ContractStateMachine.Mode = ContractStateMachine.Mode.NUCK,
  ): ReferenceInterpreter = {
    val url = getClass.getClassLoader.getResource("universal.dar")
    require(url != null, s"universal.dar not found on the classpath")
    new ReferenceInterpreter(new File(url.toURI), csmMode, loggerFactory)
  }
}

/** A reference interpreter that executes scenarios against the IDE ledger.
  *
  * On initialization, loads the universal DAR and compiles it. The main entry point is
  * [[runAndProject]].
  */
class ReferenceInterpreter(
    darFile: File,
    csmMode: ContractStateMachine.Mode,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  import ReferenceInterpreter.*

  // -- DAR loading and compilation --

  private val dar = DarDecoder.assertReadArchiveFromFile(darFile)
  private val universalPackageId: Ref.PackageId = dar.main._1
  private val compiledPackagesHandle: IdeLedgerBridge.CompiledPackagesHandle =
    IdeLedgerBridge.buildCompiledPackages(dar.all.toMap, enableLfDev = true)

  // -- Converters --

  private val concreteToCommands = new ConcreteToCommands(universalPackageId)

  // -- Ledger state --

  private val initialLedgerState: LedgerState = LedgerState(
    IdeLedger.initialLedger(Time.Timestamp.Epoch, csmMode),
    com.digitalasset.daml.lf.crypto.Hash.hashPrivateKey("ReferenceInterpreter"),
  )

  // -- Low-level submission --

  /** Submits a batch of API commands against the IDE ledger. */
  private def submit(
      state: LedgerState,
      committers: Set[Ref.Party],
      readAs: Set[Ref.Party],
      commands: ImmArray[ApiCommand],
      disclosures: Iterable[FatContractInstance],
  )(implicit traceContext: TraceContext): Either[String, (LedgerState, IdeLedger.CommitResult)] =
    IdeLedgerBridge.submitApiCommands(
      handle = compiledPackagesHandle,
      ledger = state.ledger,
      committers = committers,
      readAs = readAs,
      apiCommands = commands,
      seed = state.seed,
      disclosures = disclosures,
    ) match {
      case IdeLedgerBridge.SubmitSuccess(newLedger, commitResult) =>
        Right((state.advance(newLedger), commitResult))
      case IdeLedgerBridge.SubmitError(error) =>
        Left(error)
    }

  // -- Disclosure fetching --

  /** Looks up a contract on the IDE ledger and return it as a [[FatContractInstance]] for
    * disclosure.
    */
  private def fetchDisclosure(
      ledger: IdeLedger,
      partyIdMapping: PartyIdMapping,
      contractIdMapping: ContractIdMapping,
      abstractContractId: Concrete.ContractId,
  ): FatContractInstance = {
    val concreteContractId = contractIdMapping(abstractContractId).contractId
    ledger.lookupGlobalContract(
      actAs = Set.empty,
      readAs = partyIdMapping.values.toSet,
      effectiveAt = ledger.currentTime,
      coid = concreteContractId,
    ) match {
      case IdeLedger.LookupOk(contract) => contract
      case other =>
        throw new IllegalArgumentException(
          s"Failed to fetch disclosure for contract $abstractContractId (concrete: $concreteContractId): $other"
        )
    }
  }

  // -- Command execution --

  /** Executes a single [[com.digitalasset.canton.ledger.api.Commands]] against the IDE ledger.
    *
    * @return
    *   the new contract ID mappings produced by this submission
    */
  private def runCommands(
      state: LedgerState,
      partyIdMapping: PartyIdMapping,
      contractIdMapping: ContractIdMapping,
      commands: Concrete.Commands,
  )(implicit
      traceContext: TraceContext
  ): Either[InterpreterError, (LedgerState, ContractIdMapping)] =
    for {
      apiCommands <- commands.commands
        .traverse(cmd =>
          concreteToCommands.actionToApiCommand(partyIdMapping, contractIdMapping, cmd.action)
        )
        .left
        .map(TranslationError(_))
      disclosures = commands.disclosures.map(
        fetchDisclosure(state.ledger, partyIdMapping, contractIdMapping, _)
      )
      result <- submit(
        state,
        committers = commands.actAs.map(partyIdMapping),
        readAs = Set.empty,
        commands = ImmArray.from(apiCommands),
        disclosures = disclosures,
      ).left.map[InterpreterError](SubmitFailure(_))
    } yield {
      val (newState, commitResult) = result
      val actions = commands.commands.map(_.action)
      (
        newState,
        // In the case of a Create node, commandResultsToContractIdMapping only extracts the singleton mapping for the
        // created contract, so we need to combine it with the existing mapping to preserve previously mapped contract
        // IDs. In the case of an Exercise, this union is redundant but harmless.
        contractIdMapping ++ ContractIdMappings.commandResultsToContractIdMapping(
          actions,
          commitResult,
        ),
      )
    }

  /** Executes a list of [[com.digitalasset.canton.ledger.api.Commands]], threading [[LedgerState]]
    * and contract ID mapping through.
    *
    * @return
    *   the final ledger state and accumulated contract ID mapping after all submissions
    */
  @tailrec
  private def runCommandsList(
      state: LedgerState,
      partyIdMapping: PartyIdMapping,
      contractIdMapping: ContractIdMapping,
      commandsList: List[Concrete.Commands],
  )(implicit
      traceContext: TraceContext
  ): Either[InterpreterError, (LedgerState, ContractIdMapping)] =
    commandsList match {
      case Nil => Right((state, contractIdMapping))
      case cmds :: rest =>
        runCommands(state, partyIdMapping, contractIdMapping, cmds) match {
          case Left(err) => Left(err)
          case Right((newState, newMapping)) =>
            runCommandsList(
              newState,
              partyIdMapping,
              newMapping,
              rest,
            )
        }
    }

  // -- Entry point --

  /** Run a full scenario and compute per-party projections.
    *
    * This method:
    *   - Allocates a new IDE ledger
    *   - "Allocates" parties as simple strings ("p0", "p1", ...)
    *   - Translates and submits each
    *     [[com.digitalasset.canton.testing.modelbased.ast.Concrete.Commands]] against the IDE
    *     ledger
    *   - Computes per-party projections using
    *     [[com.digitalasset.canton.testing.modelbased.projections.ToProjection]]
    *
    * @param scenario
    *   the scenario to execute
    * @return
    *   either an error message, or a map from party ID to projection
    */
  def runAndProject(
      scenario: Concrete.Scenario
  )(implicit
      traceContext: TraceContext
  ): Either[String, Map[Projections.PartyId, Projections.Projection]] =
    try {
      val allPartyIds: Set[Concrete.PartyId] =
        scenario.topology.flatMap(_.parties).toSet
      val partyIdMapping: PartyIdMapping =
        allPartyIds.map(pid => pid -> Ref.Party.assertFromString(s"p$pid")).toMap
      runCommandsList(
        state = initialLedgerState,
        partyIdMapping = partyIdMapping,
        contractIdMapping = Map.empty,
        commandsList = scenario.ledger,
      ) match {
        case Left(err) => Left(err.pretty)
        case Right((finalState, contractIdMapping)) =>
          val reversePartyIds: ToProjection.PartyIdReverseMapping =
            partyIdMapping.map(_.swap)
          val reverseContractIds: ToProjection.ContractIdReverseMapping =
            contractIdMapping.map { case (k, v) => v.contractId -> k }

          Right(allPartyIds.map { partyId =>
            val party = partyIdMapping(partyId)
            partyId -> ToProjection.projectFromIdeLedger(
              reversePartyIds,
              reverseContractIds,
              finalState.ledger,
              party,
            )
          }.toMap)
      }
    } catch {
      case ex: Throwable =>
        Left(s"Unexpected exception during interpretation: ${ex.getMessage} ($ex)")
    }
}
