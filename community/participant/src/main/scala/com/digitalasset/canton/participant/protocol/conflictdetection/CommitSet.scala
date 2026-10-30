// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import com.digitalasset.canton.data.{ContractReassignment, ContractsReassignmentBatch}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet.{
  ArchivalCommit,
  AssignmentCommit,
  CreationCommit,
  UnassignmentCommit,
}
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  GenContractInstance,
  HostedOnboardingParties,
  LfContractId,
  ReassignmentId,
  RequestId,
}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SetsUtil.requireDisjoint
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.digitalasset.nonempty.NonEmpty

/** Describes the effect of a confirmation request on the active contracts, contract keys, and
  * reassignments. Transient contracts appear the following two sets:
  *   - The union of [[creations]] and [[assignments]]
  *   - The union of [[archivals]] or [[unassignments]]
  *
  * @param archivals
  *   The contracts to be archived, along with their stakeholders. Must not contain contracts in
  *   [[unassignments]].
  * @param creations
  *   The contracts to be created.
  * @param unassignments
  *   The contracts to be unassigned, along with their target synchronizers and stakeholders. Must
  *   not contain contracts in [[archivals]].
  * @param assignments
  *   The contracts to be assigned, along with their reassignment IDs.
  * @param reassignments
  *   The reassignment Ids to be completed in the reassignment store.
  * @throws java.lang.IllegalArgumentException
  *   if `unassignments` overlap with `archivals` or `creations` overlaps with `assignments`.
  */
final case class CommitSet(
    archivals: Map[LfContractId, ArchivalCommit],
    creations: Map[LfContractId, CreationCommit],
    unassignments: Map[LfContractId, UnassignmentCommit],
    assignments: Map[LfContractId, AssignmentCommit],
    reassignments: Seq[ReassignmentId],
    hostedOnboardingPartiesO: Option[HostedOnboardingParties],
) extends PrettyPrintingFromCompanion {
  requireDisjoint(unassignments.keySet -> "unassignments", archivals.keySet -> "archivals")
  requireDisjoint(assignments.keySet -> "assignments", creations.keySet -> "creations")

  def excludeForPartyOnboarding(stakeholders: Set[LfPartyId]): Boolean =
    CommitSet.excludeForPartyOnboarding(hostedOnboardingPartiesO, stakeholders)

  override def prettyCompanion: PrettyPrintingCompanion[CommitSet] =
    CommitSet
}

object CommitSet extends PrettyPrintingCompanion[CommitSet] {

  override protected val pretty: Pretty[CommitSet] = prettyOfClass(
    paramIfNonEmpty("archivals", _.archivals),
    paramIfNonEmpty("creations", _.creations),
    paramIfNonEmpty("unassignments", _.unassignments),
    paramIfNonEmpty("assignments", _.assignments),
    paramIfNonEmpty("reassignments", _.reassignments),
    paramIfDefined("onboarding", _.hostedOnboardingPartiesO),
  )

  val empty: CommitSet = CommitSet(Map.empty, Map.empty, Map.empty, Map.empty, Nil, None)

  // Exclude contracts if there are onboarding hosted parties and all hosted contract stakeholders are onboarding.
  // This filter excludes activeness changes whose hosted stakeholders are all onboarding parties and in such cases
  // helps ensure that "fully" hosted non-stakeholder informees witness the corresponding change as a ledger effect
  // rather than an ACS delta. AcsChangeFactory.contractActivenessChanged(cid) returning false causes the indexer to
  // treat corresponding exercises and creates as witnessed events.
  def excludeForPartyOnboarding(
      hostedOnboardingPartiesO: Option[HostedOnboardingParties],
      stakeholders: Set[LfPartyId],
  ): Boolean =
    hostedOnboardingPartiesO.fold(false)(
      _.hostedPartiesIfAllOnboarding(stakeholders).nonEmpty
    )

  /** Contract creation reference information needed for persistence and updating of in-memory state
    * when the associated transaction commits.
    */
  final case class CreationCommit(
      contractMetadata: ContractMetadata,
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[CreationCommit] = CreationCommit
  }

  object CreationCommit extends PrettyPrintingCompanion[CreationCommit] {
    override protected val pretty: Pretty[CreationCommit] = prettyOfClass(
      param("contractMetadata", _.contractMetadata),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }
  final case class UnassignmentCommit(
      targetSynchronizerId: Target[SynchronizerId],
      stakeholders: Set[LfPartyId],
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[UnassignmentCommit] = UnassignmentCommit
  }

  object UnassignmentCommit extends PrettyPrintingCompanion[UnassignmentCommit] {
    override protected val pretty: Pretty[UnassignmentCommit] = prettyOfClass(
      param("targetSynchronizerId", _.targetSynchronizerId),
      paramIfNonEmpty("stakeholders", _.stakeholders),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }
  final case class AssignmentCommit(
      sourceSynchronizerId: Source[SynchronizerId],
      reassignmentId: ReassignmentId,
      contractMetadata: ContractMetadata,
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[AssignmentCommit] = AssignmentCommit
  }

  object AssignmentCommit extends PrettyPrintingCompanion[AssignmentCommit] {
    override protected val pretty: Pretty[AssignmentCommit] = prettyOfClass(
      param("source", _.sourceSynchronizerId),
      param("reassignmentId", _.reassignmentId),
      param("contractMetadata", _.contractMetadata),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }

  /** Contract archival reference information needed for persistence and updating of in-memory state
    * when the associated transaction commits.
    *
    * @param stakeholders
    *   all contract stakeholders (i.e. not only locally hosted stakeholders)
    */
  final case class ArchivalCommit(
      stakeholders: Set[LfPartyId]
  ) extends PrettyPrintingFromCompanion {

    override def prettyCompanion: PrettyPrintingCompanion[ArchivalCommit] = ArchivalCommit
  }

  object ArchivalCommit extends PrettyPrintingCompanion[ArchivalCommit] {
    override protected val pretty: Pretty[ArchivalCommit] = prettyOfClass(
      param("stakeholders", _.stakeholders)
    )
  }

  def createForTransaction(
      activenessResult: ActivenessResult,
      requestId: RequestId,
      consumedInputsOfHostedParties: Map[LfContractId, Set[LfPartyId]],
      transient: Map[LfContractId, Set[LfPartyId]],
      createdContracts: Map[LfContractId, GenContractInstance],
      crashAfterFailedValidation: Boolean,
      hostedOnboardingPartiesO: Option[HostedOnboardingParties],
      activationsDeactivationsOrderO: Option[Map[LfContractId, Seq[ChangeType]]],
  )(implicit loggingContext: ErrorLoggingContext): CommitSet = {
    alarmIfActivenessCheckFailed(
      activenessResult.isSuccessful,
      activenessResult,
      requestId,
      requestDescription = "Request",
      crashAfterFailedValidation,
    )

    // invariant checks only when `activationsDeactivationsOrderO` is not None, otherwise model conformance
    // checks failed
    activationsDeactivationsOrderO.foreach { operations =>
      // Model conformance filters out the nodes whose hosted informees are all onboarding, so their
      // contracts may be missing from `activationsDeactivationsOrderO`. These contracts are excluded
      // from the checks, as AcsChangeSupport.fromCommitSet excludes them from the ACS change anyway.
      createdContracts.foreach { case (cid, contract) =>
        val opsO = operations.get(cid)

        if (
          !excludeForPartyOnboarding(hostedOnboardingPartiesO, contract.metadata.stakeholders) &&
          !opsO.exists(_.contains(ChangeType.Activation))
        ) {
          loggingContext.warn(
            s"Created contract $cid has no Activation in its operation history. " +
              s"Operations: $opsO"
          )
        }
      }

      consumedInputsOfHostedParties.foreach { case (cid, stakeholders) =>
        val opsO = operations.get(cid)

        if (
          !excludeForPartyOnboarding(hostedOnboardingPartiesO, stakeholders) &&
          !opsO.flatMap(_.lastOption).contains(ChangeType.Deactivation)
        ) {
          loggingContext.warn(
            s"Archived contract $cid does not have Deactivation as its last operation. " +
              s"Operations: $opsO"
          )
        }
      }

      transient.foreach { case (cid, stakeholders) =>
        if (!excludeForPartyOnboarding(hostedOnboardingPartiesO, stakeholders))
          operations.get(cid) match {
            case Some(ops)
                if ops.contains(ChangeType.Activation) &&
                  ops.contains(ChangeType.Deactivation) =>
              ()

            case opsO =>
              loggingContext.warn(
                s"Transient contract $cid does not have both an Activation and a " +
                  s"Deactivation. Operations: $opsO"
              )
          }
      }
    }

    val archivals = (consumedInputsOfHostedParties ++ transient)
      .filter { case (cid, _) =>
        activationsDeactivationsOrderO.flatMap(_.get(cid)).flatMap(_.lastOption) match {
          // the last state change for that cid wins, regardless of whether it was preceded by
          // activations / deactivations for that cid in the same transaction
          case Some(ChangeType.Deactivation) => true
          case Some(ChangeType.Activation) => false
          // the order is unknown because model conformance failed: keep the archival
          case None => true
        }
      }
      .fmap(CommitSet.ArchivalCommit(_))
    val reassignmentCounter = ReassignmentCounter.Genesis

    // All created contracts are kept, also when the last state change is a deactivation:
    val creations =
      createdContracts.fmap(c => CommitSet.CreationCommit(c.metadata, reassignmentCounter))

    CommitSet(
      archivals = archivals,
      creations = creations,
      unassignments = Map.empty,
      assignments = Map.empty,
      reassignments = Nil,
      hostedOnboardingPartiesO = hostedOnboardingPartiesO,
    )
  }

  /** Creates the commit set of an unassignment request.
    *
    * @param contracts
    *   The contracts to be unassigned, along with their reassignment counters.
    * @param targetSynchronizer
    *   The synchronizer the contracts are reassigned to.
    * @param stakeholders
    *   The stakeholders of the contracts.
    */
  def createForUnassignment(
      contracts: ContractsReassignmentBatch,
      targetSynchronizer: Target[PhysicalSynchronizerId],
      stakeholders: Set[LfPartyId],
  ): CommitSet = CommitSet(
    archivals = Map.empty,
    creations = Map.empty,
    assignments = Map.empty,
    unassignments = (contracts.contractIdCounters
      .map { case (contractId, reassignmentCounter) =>
        (
          contractId,
          CommitSet.UnassignmentCommit(
            targetSynchronizer.map(_.logical),
            stakeholders,
            reassignmentCounter,
          ),
        )
      })
      .toMap
      .forgetNE,
    reassignments = Nil,
    hostedOnboardingPartiesO = None,
  )

  /** Creates the commit set of an assignment request.
    *
    * @param reassignmentId
    *   The id of the reassignment the contracts are assigned by.
    * @param assignments
    *   The contracts to be assigned, along with their reassignment counters.
    * @param sourceSynchronizerId
    *   The synchronizer the contracts are reassigned from.
    * @param completeReassignmentInStore
    *   Whether the reassignment is to be completed in the reassignment store, which is the case on
    *   reassigning participants only.
    */
  def createForAssignment(
      reassignmentId: ReassignmentId,
      assignments: NonEmpty[Seq[ContractReassignment]],
      sourceSynchronizerId: Source[SynchronizerId],
      completeReassignmentInStore: Boolean,
  ): CommitSet =
    CommitSet(
      archivals = Map.empty,
      creations = Map.empty,
      unassignments = Map.empty,
      reassignments = if (completeReassignmentInStore) Seq(reassignmentId) else Nil,
      assignments = assignments
        .map(reassign =>
          reassign.contract.contractId -> CommitSet.AssignmentCommit(
            sourceSynchronizerId,
            reassignmentId,
            reassign.contract.metadata,
            reassign.counter,
          )
        )
        .toMap
        .forgetNE,
      hostedOnboardingPartiesO = None,
    )

  private[protocol] def alarmIfActivenessCheckFailed(
      activenessResultIsSuccessful: Boolean,
      activenessResult: ActivenessResult,
      requestId: RequestId,
      requestDescription: String,
      crashAfterFailedValidation: Boolean,
  )(implicit loggingContext: ErrorLoggingContext): Unit =
    if (!activenessResultIsSuccessful) {
      val message = s"$requestDescription $requestId with failed activeness check is approved."
      SyncServiceAlarm.Warn(message).report()
      loggingContext.info(s"Failed activeness result for request $requestId is $activenessResult")
      if (crashAfterFailedValidation) {
        throw new RuntimeException(message)
      }
    }
}
