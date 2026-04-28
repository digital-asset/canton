// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.participant.store.PersistedContractInstance
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value

sealed trait Reassignment {
  def templateId: Ref.Identifier
  def packageName: Ref.PackageName
  def stakeholders: Set[Ref.Party]
  def nodeId: Int
}

object Reassignment {

  final case class Batch private (
      reassignments: NonEmpty[Seq[Reassignment]]
  ) extends Iterable[Reassignment] {
    def iterator = reassignments.iterator
  }

  object Batch {
    def apply(first: Reassignment, rest: Reassignment*): Batch =
      new Batch(NonEmpty(Seq, first, rest*))

    def apply(reassignments: NonEmpty[Seq[Reassignment]]): Batch =
      apply(reassignments.head1, reassignments.tail1*)
  }

  /** Represent the update of unassigning a contract from a synchronizer.
    *
    * @param contractId
    *   Contract ID of the underlying contract.
    * @param templateId
    *   Template ID of the underlying contract.
    * @param packageName
    *   Package name of the underlying contract's template.
    * @param stakeholders
    *   Stakeholders of the underlying contract.
    * @param assignmentExclusivity
    *   Before this time (measured on the target synchronizer), only the submitter of the
    *   unassignment can initiate the assignment. Defined for reassigning participants.
    * @param reassignmentCounter
    *   The reassignment counter of the underlying contract.
    * @param nodeId
    *   The node ID of the unassign node.
    */
  final case class Unassign(
      contractId: Value.ContractId,
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      stakeholders: Set[Ref.Party],
      assignmentExclusivity: Option[Timestamp],
      reassignmentCounter: Long,
      nodeId: Int,
  ) extends Reassignment {}

  /** Represents the update of assigning a contract to a synchronizer.
    *
    * @param reassignmentCounter
    *   The reassignment counter of the underlying contract.
    * @param nodeId
    *   The node ID of the create node.
    * @param persistedContractInstance
    *   The persisted contract
    */
  final case class Assign(
      reassignmentCounter: Long,
      nodeId: Int,
      persistedContractInstance: PersistedContractInstance,
  ) extends Reassignment {
    def createNode: Node.Create = persistedContractInstance.inst.toCreateNode
    def ledgerEffectiveTime: Timestamp = persistedContractInstance.inst.createdAt.time
    def contractAuthenticationData: Bytes = persistedContractInstance.inst.authenticationData
    def templateId: Ref.Identifier = persistedContractInstance.inst.templateId
    def packageName: Ref.PackageName = persistedContractInstance.inst.packageName
    def stakeholders: Set[Ref.Party] = persistedContractInstance.inst.stakeholders
    def internalContractId: Long = persistedContractInstance.internalContractId
  }
}

/** The common information for all reassignments. Except from the hosted and reassigning
  * stakeholders, all fields are the same for reassign and assign updates, which belong to the same
  * reassignment.
  *
  * @param sourceSynchronizer
  *   The synchronizer ID from which the contract is unassigned.
  * @param targetSynchronizer
  *   The synchronizer ID to which the contract is assigned.
  * @param submitter
  *   Submitter of the command, unless the operation is performed offline.
  * @param reassignmentId
  *   The ID of the unassign event. This should be used for the assign command.
  * @param isReassigningParticipant
  *   Whether the participant is reassigning for the reassignment.
  */
final case class ReassignmentInfo(
    sourceSynchronizer: Source[SynchronizerId],
    targetSynchronizer: Target[SynchronizerId],
    submitter: Option[Ref.Party],
    reassignmentId: ReassignmentId,
    isReassigningParticipant: Boolean,
)
