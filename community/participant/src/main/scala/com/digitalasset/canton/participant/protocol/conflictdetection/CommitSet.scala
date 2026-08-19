// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import com.digitalasset.canton.data.{ContractReassignment, ContractsReassignmentBatch}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet.*
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
) extends PrettyPrinting {
  requireDisjoint(unassignments.keySet -> "unassignments", archivals.keySet -> "archivals")
  requireDisjoint(assignments.keySet -> "assignments", creations.keySet -> "creations")

  override protected def pretty: Pretty[CommitSet] = prettyOfClass(
    paramIfNonEmpty("archivals", _.archivals),
    paramIfNonEmpty("creations", _.creations),
    paramIfNonEmpty("unassignments", _.unassignments),
    paramIfNonEmpty("assignments", _.assignments),
    paramIfNonEmpty("reassignments", _.reassignments),
    paramIfDefined("onboarding", _.hostedOnboardingPartiesO),
  )
}

object CommitSet {

  val empty: CommitSet = CommitSet(Map.empty, Map.empty, Map.empty, Map.empty, Nil, None)

  /** Contract creation reference information needed for persistence and updating of in-memory state
    * when the associated transaction commits.
    */
  final case class CreationCommit(
      contractMetadata: ContractMetadata,
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[CreationCommit] = prettyOfClass(
      param("contractMetadata", _.contractMetadata),
      param("reassignmentCounter", _.reassignmentCounter),
    )
  }
  final case class UnassignmentCommit(
      targetSynchronizerId: Target[SynchronizerId],
      stakeholders: Set[LfPartyId],
      reassignmentCounter: ReassignmentCounter,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[UnassignmentCommit] = prettyOfClass(
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
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[AssignmentCommit] = prettyOfClass(
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
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[ArchivalCommit] = prettyOfClass(
      param("stakeholders", _.stakeholders)
    )
  }

  def createForTransaction(
      activenessResult: ActivenessResult,
      requestId: RequestId,
      consumedInputsOfHostedParties: Map[LfContractId, Set[LfPartyId]],
      transient: Map[LfContractId, Set[LfPartyId]],
      createdContracts: Map[LfContractId, GenContractInstance],
      commitAfterFailedActivenessCheck: Boolean,
      hostedOnboardingPartiesO: Option[HostedOnboardingParties],
  )(implicit loggingContext: ErrorLoggingContext): CommitSet = {
    if (!activenessResult.isSuccessful) {
      SyncServiceAlarm
        .Warn(s"Request $requestId with failed activeness check is approved.")
        .report()
      loggingContext.info(s"Failed activeness result for request $requestId is $activenessResult")
      if (!commitAfterFailedActivenessCheck) {
        throw new RuntimeException(s"Request $requestId with failed activeness check is approved.")
      }
    }

    val archivals = (consumedInputsOfHostedParties ++ transient).map {
      case (cid, hostedStakeholders) =>
        cid -> CommitSet.ArchivalCommit(hostedStakeholders)
    }
    val reassignmentCounter = ReassignmentCounter.Genesis
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
}
