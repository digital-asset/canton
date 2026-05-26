// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import cats.implicits.{catsSyntaxAlternativeSeparate, toFoldableOps}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.PackageReference
import com.digitalasset.canton.ledger.api.PackageReference.*
import com.digitalasset.canton.ledger.api.validation.GetPreferredPackagesRequestValidator.PackageVettingRequirements
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound.PackageNamesNotFound
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.platform.PackagePreferenceBackend.{
  Candidate,
  PackageFilter,
  SupportedPackagesFilter,
}
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.PackageSignature

import scala.collection.immutable.SortedSet
import scala.collection.{MapView, mutable}
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

class PackagePreferenceBackend(
    clock: Clock,
    adminParty: Party,
    syncService: SyncService,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Computes the preferred package versions for the provided package vetting requirements.
    *
    * In detail, the method outputs the most preferred package for each package-name specified in
    * the package vetting requirements. The algorithm proceeds as follows:
    *
    *   1. The input package vetting requirements for each party are extended to incorporate
    *      requirements derived from other parties: for each party, and for each of the party's
    *      required package-names, the vetted packages with that package-name are resolved, and the
    *      package-names of their transitive dependencies are added to the party's requirements if
    *      those package-names are required by any other party.
    *
    *   1. For each required package-name, the set of candidate package-ids is computed as the
    *      intersection of the vetted package-ids across all parties that require that package-name
    *      (after extension).
    *
    *   1. Any candidate whose transitive dependencies include a package-id pertaining to a required
    *      (by any party) package-name that is not in the corresponding intersection is discarded.
    *
    *   1. The highest-versioned remaining candidate for each originally requested package-name is
    *      selected.
    *
    * Note:
    *   - For brevity, we refer here to a party vetting a package if all its hosting participants
    *     have vetted the package.
    *
    * @param packageVettingRequirements
    *   The package vetting requirements for which the package preferences should be computed.
    * @param packageFilter
    *   Filters which package IDs are eligible for consideration in the preference computation for
    *   each package-name specified in the provided requirements.
    * @param synchronizerId
    *   If provided, only this synchronizer's vetting topology state is considered in the
    *   computation. Otherwise, the highest package version from all the connected synchronizers is
    *   returned.
    * @param vettingValidAt
    *   If provided, used to compute the package vetting state at this timestamp.
    * @return
    *   if a solution exists, the best package preference coupled with the synchronizer id that it
    *   pertains to.
    */
  def getPreferredPackages(
      packageVettingRequirements: PackageVettingRequirements,
      packageFilter: PackageFilter,
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, (Seq[PackageReference], PhysicalSynchronizerId)]] = {
    val packageMetadataSnapshot = syncService.getPackageMetadataSnapshot

    for {
      routingSynchronizerState <- syncService.getRoutingSynchronizerState
      _ <- ensurePackageNamesKnown(packageVettingRequirements, packageMetadataSnapshot)
      packageMapForRequest <- syncService.computePartyVettingMap(
        submitters = Set.empty,
        informees = packageVettingRequirements.allParties,
        vettingValidityTimestamp = vettingValidAt.getOrElse(clock.now),
        prescribedSynchronizer = synchronizerId,
        routingSynchronizerState = routingSynchronizerState,
      )
    } yield {
      val partyToRequiredPackages = MapsUtil.transpose(packageVettingRequirements.value)
      val allCandidates = packageVettingRequirements.allPackageNames
        .flatMap(packageMetadataSnapshot.packageNameMap(_).allPackageIdsForName)

      // Mapping from a candidate package-id to the package names of its transitive dependencies
      // that are also candidates
      val candidateToRequiredDependencyNames: Map[PackageId, Set[PackageName]] =
        packageMetadataSnapshot
          .allDependencySetsRecursively(allCandidates)
          .view
          .mapValues(deps => deps.filter(allCandidates))
          .mapValues(deps => deps.map(packageMetadataSnapshot.packageIdVersionMap(_)._1))
          .toMap
      val synchronizerCandidates = packageMapForRequest.map { case (syncId, partiesVettingState) =>
        val extendedRequirements = partyToRequiredPackages.map {
          case (partyId, requiredPackageNames) =>
            // For each party, extend its required package names to include
            // the package names of the recursive dependencies of package names required by other parties
            val vettedPackagesForParty = partiesVettingState(partyId)
            val extendedRequiredPackageNames =
              requiredPackageNames.foldLeft(requiredPackageNames) { (packageNames, packageName) =>
                packageNames ++ packageMetadataSnapshot
                  .packageNameMap(packageName)
                  .allPackageIdsForName
                  // Only consider required package-name dependencies of vetted packages
                  .filter(vettedPackagesForParty.contains)
                  .flatMap(candidateToRequiredDependencyNames)
              }
            partyId -> extendedRequiredPackageNames
        }
        val candidates = PackagePreferenceBackend.computePerSynchronizerPackageCandidates(
          partiesVettingState = partiesVettingState,
          packageMetadataSnapshot = packageMetadataSnapshot,
          packageFilter = packageFilter,
          requirements = extendedRequirements,
          synchronizerProtocolVersion = syncId.protocolVersion,
        )
        syncId -> PackagePreferenceBackend.selectRequestedPackages(
          candidates,
          packageVettingRequirements.allPackageNames,
        )
      }
      findValidCandidate(synchronizerCandidates)
    }
  }

  private def findValidCandidate(
      synchronizerCandidates: Map[PhysicalSynchronizerId, Candidate[Set[PackageReference]]]
  )(implicit
      traceContext: TraceContext
  ): Either[String, (Seq[PackageReference], PhysicalSynchronizerId)] = {
    val (discardedCandidates, validCandidates) = synchronizerCandidates.view
      .map { case (sync, candidateE) =>
        candidateE.left.map(sync -> _).map(sync -> _)
      }
      .toList
      .separate

    validCandidates
      .maxByOption(_._1)(
        // TODO(#25385): Order by the package version with the package precedence set by the order of the vetting requirements
        // Follow the pattern used for SynchronizerRank ordering,
        // where lexicographic order picks the most preferred synchronizer by id
        implicitly[Ordering[PhysicalSynchronizerId]].reverse
      )
      .map { case (syncId, packageRefs) =>
        // Valid candidate found
        // Log discarded candidates and return the package references and synchronizer id of the valid candidate
        if (discardedCandidates.nonEmpty) {
          logger.debug(show"Discarded synchronizers: $discardedCandidates")
        }
        packageRefs.toSeq -> syncId
      }
      .toRight(
        show"No synchronizer satisfies the vetting requirements. Discarded synchronizers: $discardedCandidates"
      )
  }

  def getPreferredPackageVersionForParticipant(
      packageName: PackageName,
      supportedPackageIds: Set[PackageId],
      supportedPackageIdsDescription: String,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, PackageId]] =
    getPreferredPackages(
      packageVettingRequirements = PackageVettingRequirements(
        value = Map(packageName -> Set(adminParty))
      ),
      packageFilter = SupportedPackagesFilter(
        Map(packageName -> supportedPackageIds),
        supportedPackageIdsDescription,
      ),
      synchronizerId = None,
      vettingValidAt = Some(CantonTimestamp.MaxValue),
    )
      .map(_.map {
        case (Seq(pkgRef), _) => pkgRef.pkgId
        case (invalidSeq, syncId) =>
          throw new RuntimeException(
            s"Expected exactly one package reference for package name $packageName and $syncId, but got $invalidSeq. This is likely a programming error. Please contact support"
          )
      })

  private def ensurePackageNamesKnown(
      packageVettingRequirements: PackageVettingRequirements,
      packageMetadataSnapshot: PackageMetadata,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val requestPackageNames = packageVettingRequirements.allPackageNames
    val knownPackageNames = packageMetadataSnapshot.packageNameMap.keySet
    val unknownPackageNames = requestPackageNames.diff(knownPackageNames)

    if (unknownPackageNames.isEmpty) FutureUnlessShutdown.unit
    else
      FutureUnlessShutdown.failed(
        PackageNamesNotFound.Reject(unknownPackageNames).asGrpcError
      )
  }
}

object PackagePreferenceBackend extends HasLoggerName {
  type SortedPreferences = NonEmpty[SortedSet[PackageReference]] // most preferred last
  // A candidate refers to a value T that:
  //   - wraps the value in a Right if it is valid for package preferences computation OR
  //   - wraps the value's discarded reason in a Left
  type Candidate[T] = Either[String, T]

  sealed trait PackageFilter extends Product with Serializable with PrettyPrinting {
    def apply(packageName: PackageName, packageId: PackageId): Boolean
  }

  case object AllowAllPackageIds extends PackageFilter {
    def apply(packageName: PackageName, packageId: PackageId): Boolean = true

    override protected def pretty: Pretty[AllowAllPackageIds.this.type] =
      Pretty.prettyOfString(_ => "All package-ids supported")
  }

  final case class SupportedPackagesFilter(
      supportedPackagesPerPackageName: Map[PackageName, Set[PackageId]],
      restrictionDescription: String,
  ) extends PackageFilter {
    def apply(packageName: PackageName, packageId: PackageId): Boolean =
      supportedPackagesPerPackageName.get(packageName).forall(_.contains(packageId))

    override protected def pretty: Pretty[SupportedPackagesFilter] =
      Pretty.prettyOfString(c =>
        show"${c.restrictionDescription.singleQuoted}=$supportedPackagesPerPackageName"
      )
  }

  def computePerSynchronizerPackageCandidates(
      partiesVettingState: Map[Party, Set[PackageId]],
      packageMetadataSnapshot: PackageMetadata,
      requirements: Map[Party, Set[PackageName]],
      packageFilter: PackageFilter,
      synchronizerProtocolVersion: ProtocolVersion,
  )(implicit
      loggingContext: NamedLoggingContext
  ): MapView[PackageName, Candidate[SortedPreferences]] =
    partiesVettingState.view
      // Resolve to full package references
      .mapValues(groupAndSortPackageLineages(packageMetadataSnapshot.packageIdVersionMap, _))
      .toMap
      .pipe((candidates: Map[Party, Map[PackageName, SortedPreferences]]) =>
        // Mark candidates for which there is no vetted package satisfying a vetting requirement (party <-> package-name)
        documentUnsatisfiedPackageNameRequirements(candidates, requirements)
      )
      .pipe { (candidates: Map[Party, Map[PackageName, Candidate[SortedPreferences]]]) =>
        // At this point we are reducing the party dimension:
        // - for required package names, we intersect the sets of package-ids of the requiring parties.
        // - for other package names, we sum the sets of available package-ids of all parties.
        computePartyPackageCandidatesIntersection(requirements, candidates)
      }
      // Preserve only the candidate package-ids that are commonly-vetted and all their dependencies are commonly-vetted
      .pipe(
        preserveDeeplyVetted(
          packageMetadataSnapshot,
          _,
          requirements.values.flatten.toSet,
          synchronizerProtocolVersion,
        )
      )
      // Apply package-id filter restriction to the candidate package-ids
      // Note: the filter can discard packages that are dependencies of other candidates
      .pipe(filterPackages(packageFilter, _))

  // Preserve for each package-name all the package-ids that are vetted and all their dependencies are vetted
  private def preserveDeeplyVetted(
      packageMetadataSnapshot: PackageMetadata,
      candidatesForPackageName: Map[PackageName, Candidate[SortedPreferences]],
      requiredPackageNames: Set[PackageName],
      protocolVersion: ProtocolVersion,
  ): MapView[PackageName, Candidate[SortedPreferences]] = {
    val packageIndex: Map[PackageId, (PackageName, PackageVersion)] =
      packageMetadataSnapshot.packageIdVersionMap
    val dependencyGraph: Map[PackageId, Set[PackageId]] =
      packageMetadataSnapshot.packages.view.mapValues(_.directDeps).toMap

    val allVettedPackages = candidatesForPackageName.view.values
      .collect { case Right(vettedCandidates) =>
        vettedCandidates.view.map(_.pkgId)
      }
      .flatten
      .toSet

    val getPackageAst: PackageId => PackageSignature = pkgId =>
      packageMetadataSnapshot.packages.getOrElse(
        pkgId,
        throw new NoSuchElementException(
          s"Package with id $pkgId not found in the package metadata snapshot"
        ),
      )

    if (protocolVersion <= ProtocolVersion.v34)
      preserveDeeplyVettedPV34(
        candidatesForPackageName,
        dependencyGraph,
        allVettedPackages,
        packageIndex,
        getPackageAst,
      )
    else
      preserveDeeplyRequiredVetted(
        requiredPackageNames,
        candidatesForPackageName,
        dependencyGraph,
        allVettedPackages,
        packageIndex,
        getPackageAst,
      )
  }

  // Discard packages that have an unvetted direct or transitive dependency package-id
  // pertaining to a required package-name
  private def preserveDeeplyRequiredVetted(
      requiredPackageNames: Set[PackageName],
      candidatesForPackageName: Map[PackageName, Candidate[SortedPreferences]],
      dependencyGraph: Map[PackageId, Set[PackageId]],
      allVettedPackages: Set[PackageId],
      packageIndex: Map[PackageId, (PackageName, PackageVersion)],
      getPackageAst: PackageId => PackageSignature,
  ): MapView[PackageName, Candidate[NonEmpty[SortedSet[PackageReference]]]] = {
    val deepVettingCache: mutable.HashMap[PackageId, Either[PackageId, Unit]] =
      mutable.HashMap.empty

    // Note: Keeping it simple without tailrec since the dependency graph depth should be limited
    def checkRequiredVettingRecursively(pkgId: PackageId): Either[PackageId, Unit] =
      // Note: This re-entrant call to `mutable.HashMap.getOrElseUpdate` is safe
      //       since the call is single-threaded and the Daml package dependency graph
      //       is acyclic (a key cannot be re-visited along a recursive call-stack)
      deepVettingCache.getOrElseUpdate(
        pkgId, {
          val pkg = getPackageAst(pkgId)
          if (allVettedPackages(pkgId) || !requiredPackageNames(pkg.pkgName)) {
            val dependencies = dependencyGraph(pkgId)
            MonadUtil.sequentialTraverse_(dependencies.toSeq)(checkRequiredVettingRecursively)
          } else Left(pkgId)
        },
      )

    candidatesForPackageName.view
      .mapValues(
        _.flatMap { candidates =>
          val (packagesWithUnvettedRequiredDeps, validCandidates) = candidates.view
            .map(pkgRef =>
              checkRequiredVettingRecursively(pkgRef.pkgId).left
                .map(pkgRef.pkgId -> _)
                .map(_ => pkgRef)
            )
            .toList
            .separate

          def showPkg(pkgId: PackageId): String =
            pkgId.toPackageReference(packageIndex).map(_.show).getOrElse(pkgId.show)

          lazy val packageRefsWithUnvettedDepsForError = packagesWithUnvettedRequiredDeps.map {
            case (pkg, unvettedDep) => s"${showPkg(pkg)} -> ${showPkg(unvettedDep)}"
          }

          NonEmpty
            .from(validCandidates.to(SortedSet))
            .toRight(
              show"Packages with required dependencies not vetted by all interested parties: $packageRefsWithUnvettedDepsForError"
            )
        }
      )
  }

  // TODO(#25385): Legacy behavior, only supported for backwards compatibility with Protocol Version 34.
  //               Remove once PV34 support is dropped.
  private def preserveDeeplyVettedPV34(
      candidatesForPackageName: Map[PackageName, Candidate[SortedPreferences]],
      dependencyGraph: Map[PackageId, Set[PackageId]],
      allVettedPackages: Set[PackageId],
      packageIndex: Map[PackageId, (PackageName, PackageVersion)],
      getPackageAst: PackageId => Ast.PackageSignature,
  ): MapView[PackageName, Candidate[NonEmpty[SortedSet[PackageReference]]]] = {
    val deepVettingCache: mutable.Map[PackageId, Either[PackageId, Unit]] =
      mutable.Map.empty

    // Note: Keeping it simple without tailrec since the dependency graph depth should be limited
    def isDeeplyVetted(pkgId: PackageId): Either[PackageId, Unit] = {
      val pkg = getPackageAst(pkgId)
      if (
        // If a package is vetted or it is not a schema package, we continue with checking its dependencies.
        // We ignore unvetted non-schema packages to support
        // disjoint versions across informees (e.g. Daml stdlib packages)
        allVettedPackages(pkgId) || !isSchemaPackage(pkg)
      ) {
        val dependencies = dependencyGraph(pkgId)

        dependencies.foldLeft(Right(()): Either[PackageId, Unit]) {
          case (Right(()), dep) => deepVettingCache.getOrElseUpdate(dep, isDeeplyVetted(dep))
          case (left, _) => left
        }
      } else {
        // If the schema package is not vetted, return it as an error
        Left(pkgId)
      }
    }

    candidatesForPackageName.view
      .mapValues(
        _.flatMap { candidates =>
          val (packagesWithUnvettedDeps, candidatesWithVettedDeps) = candidates.view
            .map(pkgRef =>
              isDeeplyVetted(pkgRef.pkgId).left.map(pkgRef.pkgId -> _).map(_ => pkgRef)
            )
            .toList
            .separate

          val lazyPackageRefsWithUnvettedDepsForError = packagesWithUnvettedDeps.view
            .map { case (pkg, unvettedDep) =>
              s"${pkg.toPackageReference(packageIndex).map(_.show).getOrElse(pkg.show)} -> ${unvettedDep.toPackageReference(packageIndex).map(_.show).getOrElse(pkg.show)}"
            }

          NonEmpty
            .from(candidatesWithVettedDeps.to(SortedSet))
            .toRight(
              show"Packages with required dependencies not vetted by all interested parties: ${lazyPackageRefsWithUnvettedDepsForError.toList}"
            )
        }
      )
  }

  private def filterPackages(
      packageFilter: PackageFilter,
      candidatePackagesForName: MapView[PackageName, Candidate[SortedPreferences]],
  ): MapView[PackageName, Candidate[SortedPreferences]] =
    candidatePackagesForName.view
      .mapValues(_.flatMap { packageRefs =>
        NonEmpty
          .from(packageRefs.forgetNE.filter(ref => packageFilter(ref.packageName, ref.pkgId)))
          .toRight(
            // TODO(#25385): Improve error message by making it explicit that these packages are not vetted by the requested parties
            show"No vetted package candidate satisfies the package-id filter $packageFilter.\nCandidates: ${packageRefs
                .map(_.pkgId)}"
          )
      })

  private def groupAndSortPackageLineages(
      packageIdVersionMap: Map[PackageId, (PackageName, PackageVersion)],
      packageIds: Set[PackageId],
  )(implicit loggingContext: NamedLoggingContext): Map[PackageName, SortedPreferences] =
    packageIds.view
      .flatMap { pkgId =>
        val pkgRef = pkgId.toPackageReference(packageIdVersionMap)
        if (pkgRef.isEmpty)
          loggingContext.trace(
            show"Discarding package ID $pkgId as it doesn't exist in the participant's package store."
          )
        pkgRef
      }
      .groupBy(_.packageName)
      .view
      .map { case (pkgName, pkgRefs) =>
        pkgName ->
          NonEmpty
            .from(SortedSet.from(pkgRefs))
            // The groupBy in this chain ensures non-empty pkgRefs
            .getOrElse(
              sys.error(
                "Empty package references. This is likely a programming error. Please contact support"
              )
            )
      }
      .toMap

  private def computePartyPackageCandidatesIntersection(
      requirements: Map[Party, Set[PackageName]],
      candidatesPerParty: Map[Party, Map[PackageName, Candidate[SortedPreferences]]],
  ): Map[PackageName, Candidate[SortedPreferences]] = {
    val requiredNames = requirements.values.flatten.toSet
    candidatesPerParty.view
      .flatMap { case (party, pkgNameCandidates) => pkgNameCandidates.view.map(party -> _) }
      .foldLeft(Map.empty[PackageName, Candidate[SortedPreferences]]) {
        case (acc, (party, (pkgName, newCandidates))) =>
          if (requiredNames.contains(pkgName)) {
            if (requirements.get(party).exists(_.contains(pkgName))) {
              // The current package name is required by the current party: we compute the
              // intersection of candidates.
              acc.updatedWith(pkgName) {
                case None => Some(newCandidates)
                case Some(existingCandidates) =>
                  Some(
                    for {
                      existingPkgRefs <- existingCandidates
                      pkgRefs <- newCandidates
                      newPkgRefs <- NonEmpty
                        .from(existingPkgRefs.forgetNE.intersect(pkgRefs))
                        .toRight(
                          show"""|No package candidates for '$pkgName' after considering candidates for party $party.
                                 |Current candidates: $existingPkgRefs.
                                 |Candidates for party $party: $pkgRefs""".stripMargin
                        )
                    } yield newPkgRefs
                  )
              }
            } else acc
          } else {
            // This package is not a known requirement. It won't be used to compute GetPreferredPackages.
            // However TAPS may need it during interpretation. We retain all candidates to maximize
            // the options. If routing fails, the package will appear as requirement of the next TAPS pass.
            acc.updatedWith(pkgName) {
              case None => Some(newCandidates)
              case Some(existingCandidates) =>
                (existingCandidates, newCandidates) match {
                  case (Right(existingCandidates), Right(newCandidates)) =>
                    NonEmpty.from(existingCandidates.forgetNE ++ newCandidates).map(Right(_))
                  case (existingCandidates, newCandidates) =>
                    Some(existingCandidates.orElse(newCandidates))
                }
            }
          }
      }
  }

  private def documentUnsatisfiedPackageNameRequirements(
      candidatesPerParty: Map[Party, Map[PackageName, SortedPreferences]],
      requirementsPerParty: Map[Party, Set[PackageName]],
  ): Map[Party, Map[PackageName, Candidate[SortedPreferences]]] =
    requirementsPerParty.map { case (party, pkgNameReqs) =>
      lazy val noPartyBackfillMsg =
        show"No package is consistently by all hosting participants of $party."

      val backfillForMissingPartyCandidates = pkgNameReqs.view
        .map(_ -> Left(noPartyBackfillMsg))
        .toMap

      val candidatesForParty = candidatesPerParty
        .get(party)
        .map(_.view.mapValues(Right(_)).toMap)
        .getOrElse(backfillForMissingPartyCandidates)

      val packagesWithNoCandidates = pkgNameReqs.diff(candidatesForParty.keySet)
      val pkgNameWithNoVettedCandidates = packagesWithNoCandidates.view
        .map(pkgName =>
          pkgName -> Left(
            show"No package with package-name '$pkgName' is consistently vetted by all hosting participants of party $party."
          )
        )
        .toMap

      // If all the required package-names are present in the candidates for the party, all good.
      // If there are required package-names that are not present in the party's candidates,
      // back-fill the candidates for the party with an error reason for each missing package-name
      party -> (candidatesForParty ++ pkgNameWithNoVettedCandidates)
    }

  // Select the highest version package for each requested package-name
  // or discard the preference set if there is a requirement not satisfied
  private def selectRequestedPackages(
      candidates: MapView[PackageName, Candidate[SortedPreferences]],
      requiredPackageNames: Set[PackageName],
  ): Candidate[Set[PackageReference]] =
    requiredPackageNames.toList
      .foldM(Set.empty[PackageReference]) { case (acc, requestedPackageName) =>
        for {
          preferencesForNameE <- candidates
            .get(requestedPackageName)
            .toRight(
              show"No package is consistently vetted by all hosting participants of the requested parties for package-name '$requestedPackageName'"
            )
          preferencesForName <- preferencesForNameE
        } yield acc + preferencesForName.last1
      }

  private def isSchemaPackage(pkg: Ast.PackageSignature): Boolean =
    pkg.modules.exists { case (_, module) =>
      module.interfaces.nonEmpty || module.templates.nonEmpty
    }
}
