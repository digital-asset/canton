// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.api.PackageReference.*
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SyncService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{FullReference, PackageName, Party}
import com.digitalasset.daml.lf.engine.Blinding

import scala.concurrent.ExecutionContext

// TODO(#25385): Consider introducing performance observability metrics
//               due to the high computational complexity of the algorithm
/** Command executor that uses the '''topology-aware package selection''' algorithm for computing
  * the package preference set to-be-used by the Daml Engine in command interpretation.
  *
  * =Topology-Aware Package Selection (TAPS)=
  *
  * The topology-aware package selection (abbreviated further as '''TAPS''') algorithm is a
  * heuristic that computes a package-name-to-package-ID map to be used by the Daml Engine in
  * command interpretation for resolving the packages that contracts must be interpreted with in for
  * the purpose of implementing up/downgrading.
  *
  * TAPS uses approximations of topology requirements as input, because the exact requirements (e.g.
  * which informees, on which synchronizers, require which packages) can only be precisely derived
  * from the Daml transaction resulted from command interpretation.
  *
  * The situations in which the Engine needs to resolve package names to package IDs during command
  * interpretation for up/downgrading are:
  *
  *   - A top-level command is submitted for a contract with a template-id specified using the
  *     package-name reference format (see [[com.daml.ledger.api.v2.value.Identifier]])
  *
  *   - An exercise-by-interface or fetch-by-interface in an action node in the interpreted
  *     transaction
  *
  * The algorithm depends on the following key definitions:
  *
  *   - '''Party-level vetted package''': A package that is vetted by every participant hosting a
  *     specific party. Also referred to as a consistently-vetted package for a party.
  *
  *   - '''Submitter interest in a package-name''': A submitter is interested in a package-name if
  *     it has vetted any package ID pertaining to that package name.
  *
  *   - '''Informee interest in a package-name''': An informee, other than a submitter, is
  *     interested in a package-name if it requires the package-name in the transaction, as
  *     interpreted from the previous pass.
  *
  *   - '''Commonly-vetted package''': A package that is vetted by all parties interested in the
  *     package lineage and whose direct and transitive dependencies are commonly-vetted.
  *
  * Since each package ID has a unique package name, the package-name-to-package ID resolution can
  * simply be represented by its image: a set of package IDs whose package names are all different.
  * We refer to this set as the '''package preference set'''
  *
  * TAPS chooses the synchronizer during package preference set computation, as package vetting and
  * party hosting are topology information tied to specific synchronizers. While the synchronizer
  * choice remains hidden from the Engine, the provided package preference set ensures compliance
  * with topology constraints of the selected synchronizer.
  *
  * As part of command execution, there are several TAPS attempts:
  *   - An initial - approximation - pass that uses information derived only from the submitted
  *     command for computing the package preference set.
  *   - Subsequent - refinement - passes that use the information derived from the interpreted
  *     transaction obtained from the previous passes to compute a more accurate package preference
  *     set.
  *
  * Each TAPS pass generally performs the following steps:
  *
  *   1. '''Define Input Topology Requirements''' - find the topology constraints that the
  *      interpreted Daml transaction should satisfy when evaluating submission candidate
  *      synchronizers:
  *      a. Expected transaction submitters: which parties should have submission rights on the
  *         preparing participant
  *      a. Expected transaction informees: which parties should have at least observation rights on
  *         some participant
  *      a. Root-node package-names vetting requirements: the package-names of the command's root
  *         nodes for which there must exist commonly-vetted package IDs for the expected informees
  *         of these nodes.
  *
  *   1. '''Compute Per-Synchronizer Package Preference Set''': Based on the Input Topology
  *      transaction informees requirement, a package preference set is computed for each
  *      synchronizer connected to the preparing participant. This set contains the
  *      highest-versioned ''commonly-vetted'' package ID for each package name the expected
  *      transaction's informees are interested in. '''Note''': Only synchronizers with valid
  *      package candidates in the package preference set for all the command's root package-names
  *      are admissible and used for further TAPS processing.
  *
  *   1. '''Process Package Preference Set''': The per-synchronizer package preference set is then
  *      processed into the package preference set used for interpretation (which differs between
  *      Initial Pass and Subsequent Passes).
  *
  *   1. '''Interpret Commands''': The Daml Engine interprets the submitted commands using the
  *      computed package preference set (see [[com.digitalasset.daml.lf.engine.Engine.submit]]
  *
  *   1. '''Route Transaction''': If interpretation is successful, synchronizer routing searches for
  *      a suitable synchronizer that satisfies the interpreted transaction's topology constraints
  *      (see
  *      [[com.digitalasset.canton.ledger.participant.state.SyncService.selectRoutingSynchronizer]]).
  *      If found, the transaction is routed for protocol synchronization.
  *
  * The two types of passes of the algorithm differ as follows:
  *
  * ==Initial Pass==
  *
  * The input topology requirements are derived from the submitter party, which is conventionally
  * the first party of `Commands.act_as`. This party is considered the submitter and sole informee
  * of the transaction. The package-name requirements are derived from the package-names of all of
  * the submitted command’s root nodes, leading to an input topology requirement modelled as
  * submitter-party -> Set[command_root_nodes_packages]. The resulting per-synchronizer preference
  * set is merged into a single package preference set by selecting the highest-versioned package
  * across all admissible synchronizers for the submitter's vetted package names. If synchronizer
  * routing in this pass doesn't yield a valid synchronizer, the algorithm proceeds to the next
  * pass.
  *
  * ==Subsequent Passes==
  *
  * In this pass, the topology requirements are derived from the Daml transactions obtained during
  * the command interpretation from the previous passes, referred to below as the '''draft
  * transactions'''. These new requirements stipulate that every informee of the previous draft
  * transactions has expressed interest in specific packages. As such their vetting state will
  * constrain the selection of those packages. This ensures that TAPS converges after a finite
  * number of passes. The package preference set for interpretation in this pass is derived from the
  * per-synchronizer package preference sets by selecting the one associated with the highest-ranked
  * admissible synchronizer (see
  * [[com.digitalasset.canton.ledger.participant.state.SyncService.computeHighestRankedSynchronizerFromAdmissible]]).
  * If the resulting Daml transaction after interpretation in this pass cannot be routed to a valid
  * synchronizer, the algorithm loop with this transaction as the new draft transaction for the next
  * pass.
  *
  * The algorithm terminates successfully as soon as the interpreted transaction can be routed to a
  * valid synchronizer.
  *
  * The algorithm fails if:
  *   - Interpretation fails.
  *   - A pass fails to make progress (the set of required packages is not growing).
  *   - The number of passes has reached the configured maximum (default: 3).
  *
  * '''Note''': When `Commands.package_id_selection_preference` is specified, it acts as a
  * restriction in the package preference set computation for both passes. If this restriction
  * cannot be honored, command submission fails.
  */
private[execution] class TopologyAwareCommandExecutor(
    syncService: SyncService,
    commandInterpreter: CommandInterpreter,
    maxPassesDefault: PositiveInt,
    maxPassesLimit: PositiveInt,
    metrics: LedgerApiServerMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends NamedLogging
    with CommandExecutor {

  override def execute(
      commands: Commands,
      submissionSeed: Hash,
      routingSynchronizerState: RoutingSynchronizerState,
      forExternallySigned: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult] = {
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot
    val packageIndex = packageMetadataSnapshot.packageIdVersionMap

    val maxNumberOfPass = commands.tapsMaxPasses match {
      case None => maxPassesDefault
      case Some(commandMaxPasses) =>
        if (commandMaxPasses > maxPassesLimit) {
          logger.info(
            s"Requested max TAPS passes $commandMaxPasses in `max_taps_passes` of the submitted command exceeds the participant-configured limit $maxPassesLimit. Using the participant-configured limit."
          )
          maxPassesLimit
        } else commandMaxPasses
    }
    val rootLevelPackageNames = apiCommandsRootPackageNames(commands)

    val tapsExecutionFactory = new TapsCommandExecutionFactory(
      commands = commands,
      commandInterpreter = commandInterpreter,
      forExternallySigned = forExternallySigned,
      loggerFactory = loggerFactory,
      packageMetadataSnapshot = packageMetadataSnapshot,
      rootLevelPackageNames = rootLevelPackageNames,
      routingSynchronizerState = routingSynchronizerState,
      submissionSeed = submissionSeed,
      syncService = syncService,
      metrics = metrics,
    )
    def firstPass(passInput: PassInput): FutureUnlessShutdown[TapsResult] =
      tapsExecutionFactory.executePass(
        tapsPassDescription = "TAPS pass 1",
        requiredSubmitters = passInput.requiredSubmitters,
        partyPackageRequirements = passInput.partyPackageRequirements,
        computePackagePreferenceSet = perSynchronizerPreferenceSet =>
          FutureUnlessShutdown.pure(
            (
              perSynchronizerPreferenceSet.values.flatten
                .map(_.unsafeToPackageReference(packageIndex))
                .groupBy(_.packageName)
                .valuesIterator
                .map(
                  _.maxOption
                    .getOrElse(sys.error("Unexpected empty references set after groupBy"))
                    .pkgId
                )
                .toSet,
              perSynchronizerPreferenceSet.keySet.max1.protocolVersion,
            )
          ),
      )

    def loop(
        previousInput: PassInput,
        passResult: TapsResult,
        passNumber: PositiveInt,
    ): FutureUnlessShutdown[(TapsResult, PositiveInt)] =
      passResult match {
        case routingFailed: TapsResult.RoutingFailed if passNumber >= maxNumberOfPass =>
          logger.info(
            s"Phase 1 [TAPS pass $passNumber]: Stopping after routing failure because it reached the maximum number of passes."
          )
          FutureUnlessShutdown.pure((routingFailed, passNumber))
        case routingFailed @ TapsResult.RoutingFailed(interpretationResult, _) =>
          val nextInput = buildNextInput(previousInput, interpretationResult, packageIndex)
          if (nextInput.hasMorePackageRequirementsThan(previousInput)) {
            val nextPassNumber = passNumber + PositiveInt.one
            tapsExecutionFactory
              .executePass(
                tapsPassDescription = s"TAPS pass $nextPassNumber",
                requiredSubmitters = nextInput.requiredSubmitters,
                partyPackageRequirements = nextInput.partyPackageRequirements,
                computePackagePreferenceSet = perSynchronizerPreferenceSet =>
                  syncService
                    .computeHighestRankedSynchronizerFromAdmissible(
                      submitterInfo = interpretationResult.submitterInfo,
                      transaction = interpretationResult.transaction,
                      transactionMeta = interpretationResult.transactionMeta,
                      admissibleSynchronizers = perSynchronizerPreferenceSet.keySet,
                      disclosedContractIds =
                        interpretationResult.processedDisclosedContracts.map(_.contractId).toList,
                      routingSynchronizerState = routingSynchronizerState,
                    )
                    .leftSemiflatMap(err => FutureUnlessShutdown.failed(err.asGrpcError))
                    .merge
                    .map(highestRankedSync =>
                      (
                        checked(perSynchronizerPreferenceSet(highestRankedSync)),
                        highestRankedSync.protocolVersion,
                      )
                    ),
              )
              .flatMap(nextResult => loop(nextInput, nextResult, nextPassNumber))
          } else {
            logger.info(
              s"Phase 1 [TAPS pass $passNumber]: Stopping after routing failure because no new package requirements were found. TAPS cannot make further progress."
            )
            FutureUnlessShutdown.pure((routingFailed, passNumber))
          }
        case successOrFailure => FutureUnlessShutdown.pure((successOrFailure, passNumber))
      }

    val requiredSubmitter =
      commands.actAs.headOption.getOrElse(sys.error("act_as must be non-empty"))
    val initialInput = PassInput(
      requiredSubmitters = Set(requiredSubmitter),
      partyPackageRequirements = Map(requiredSubmitter -> rootLevelPackageNames),
    )
    val result = for {
      firstResult <- firstPass(initialInput)
      (finalResult, passNumber) <- loop(initialInput, firstResult, PositiveInt.one)
    } yield {
      val metricsContext = finalResult match {
        case _: TapsResult.Succeeded => MetricsContext("status" -> "success")
        case _: TapsResult.InterpretationFailed =>
          MetricsContext("status" -> "interpretation_failed")
        case _: TapsResult.RoutingFailed => MetricsContext("status" -> "routing_failed")
      }
      metrics.commands.tapsPasses.update(passNumber.value)(metricsContext)
      finalResult.toSubmissionResult
    }
    EitherT(result)
  }

  private def buildNextInput(
      previousInput: PassInput,
      interpretation: CommandInterpretationResult,
      packageIndex: Map[LfPackageId, (PackageName, LfPackageVersion)],
  ): PassInput =
    // Merge with previous requirements to avoid oscillating between two different failing
    // requirement sets. Narrowing down the number of package candidates ensures we reach either
    // a valid configuration or a terminal error state.
    previousInput
      .addPartyPackageRequirements(
        Blinding.partyPackages(interpretation.transaction).view.mapValues { pkgIds =>
          pkgIds.map(
            // It is fine to use unsafe here since the package must have been indexed on the participant
            // if it appeared in the draft transaction.
            _.unsafeToPackageReference(packageIndex).packageName
          )
        }
      )
      .withRequiredSubmitters(
        interpretation.transaction.rootNodes.iterator.flatMap(_.requiredAuthorizers).toSet
      )

  private case class PassInput(
      requiredSubmitters: Set[Party],
      partyPackageRequirements: Map[LfPartyId, Set[LfPackageName]],
  ) {
    def withRequiredSubmitters(newRequiredSubmitters: Set[Party]): PassInput =
      copy(requiredSubmitters = newRequiredSubmitters)

    def addPartyPackageRequirements(
        newPartyPackageRequirements: Iterable[(LfPartyId, Set[LfPackageName])]
    ): PassInput =
      copy(
        partyPackageRequirements = newPartyPackageRequirements
          .foldLeft(partyPackageRequirements) { case (acc, (k, v)) =>
            acc.updated(k, acc.getOrElse(k, Set.empty) ++ v)
          }
      )

    def hasMorePackageRequirementsThan(other: PassInput): Boolean =
      partyPackageRequirements.exists { case (party, packages) =>
        (packages -- other.partyPackageRequirements.getOrElse(party, Set.empty)).nonEmpty
      }
  }

  // Note: This method also collect package-names for interfaces in case of exercise-by-interface commands
  //       even though interfaces are not upgradeable. However, this is fine
  //       since the package selection algorithm merely should still be able to find a commonly-vetted package
  //       for the package-names used, even though this preference is ignored by the Engine
  private def apiCommandsRootPackageNames(commands: Commands): Set[PackageName] =
    commands.commands.commands.iterator.collect {
      case ApiCommand.Create(FullReference(LfPackageRef.Name(pkgName), _), _) =>
        pkgName
      case ApiCommand.Exercise(FullReference(LfPackageRef.Name(pkgName), _), _, _, _) =>
        pkgName
      case ApiCommand.ExerciseByKey(FullReference(LfPackageRef.Name(pkgName), _), _, _, _) =>
        pkgName
      case ApiCommand.CreateAndExercise(FullReference(LfPackageRef.Name(pkgName), _), _, _, _) =>
        pkgName
    }.toSet
}
