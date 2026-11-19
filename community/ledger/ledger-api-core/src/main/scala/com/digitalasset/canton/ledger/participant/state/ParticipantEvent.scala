// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId
import com.digitalasset.canton.participant.store.PersistedContractInstance
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.GlobalKeyWithMaintainers
import com.digitalasset.daml.lf.value.Value

sealed trait ParticipantEvent extends Product with Serializable {
  def templateId: Ref.Identifier
  def packageName: Ref.PackageName
  def stakeholders: Set[Ref.Party]
  def reassignmentCounter: Long
}

object ParticipantEvent {
  final case class Added(
      nodeId: Int,
      contractInstance: PersistedContractInstance,
      representativePackageId: RepresentativePackageId,
      reassignmentCounter: Long,
  ) extends ParticipantEvent {
    override def templateId: Ref.Identifier = contractInstance.inst.templateId
    override def packageName: Ref.PackageName = contractInstance.inst.packageName
    override def stakeholders: Set[Ref.Party] = contractInstance.inst.stakeholders
  }

  final case class Removed(
      contractId: Value.ContractId,
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      stakeholders: Set[Ref.Party],
      nodeId: Int,
      reassignmentCounter: Long,
      keyOpt: Option[GlobalKeyWithMaintainers],
      signatories: Option[Set[Ref.Party]],
  ) extends ParticipantEvent
}
