// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.ast

import com.microsoft.z3.*

object Symbolic {

  // ledgers

  type ParticipantIdSort = IntSort
  type ContractIdSort = IntSort
  type KeyIdSort = IntSort
  type PackageIdSort = IntSort
  type PartySort = IntSort
  type PartySetSort = SetSort[PartySort]
  type PackageIdSetSort = SetSort[PackageIdSort]

  type PartySet = ArrayExpr[PartySort, BoolSort]
  type ContractIdList = SeqExpr[ContractIdSort]
  type PackageIdSet = ArrayExpr[PackageIdSort, BoolSort]
  type PartyId = IntExpr
  type ContractId = IntExpr
  type KeyId = IntExpr
  type PackageId = IntExpr

  type ParticipantId = IntExpr

  // length must be <= elements.length
  final case class BoundedContractIdList(
      elements: IndexedSeq[ContractId],
      length: IntExpr,
  )

  sealed trait ExerciseKind
  case object Consuming extends ExerciseKind
  case object NonConsuming extends ExerciseKind

  sealed trait Action
  final case class Create(
      contractId: ContractId,
      signatories: PartySet,
      observers: PartySet,
  ) extends Action
  final case class CreateWithKey(
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
      signatories: PartySet,
      observers: PartySet,
  ) extends Action
  final case class Exercise(
      kind: ExerciseKind,
      contractId: ContractId,
      controllers: PartySet,
      choiceObservers: PartySet,
      subTransaction: Transaction,
  ) extends Action
  final case class ExerciseByKey(
      kind: ExerciseKind,
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
      controllers: PartySet,
      choiceObservers: PartySet,
      subTransaction: Transaction,
  ) extends Action
  final case class Fetch(
      contractId: ContractId
  ) extends Action
  final case class FetchByKey(
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
  ) extends Action
  final case class LookupByKey(
      contractId: Option[ContractId],
      keyId: KeyId,
      maintainers: PartySet,
  ) extends Action
  final case class QueryByKey(
      contractIds: BoundedContractIdList,
      keyId: KeyId,
      maintainers: PartySet,
      exhaustive: Boolean,
  ) extends Action
  final case class Rollback(
      subTransaction: Transaction
  ) extends Action

  type Transaction = List[Action]

  final case class Command(
      packageId: Option[PackageId],
      action: Action,
  )

  final case class Commands(
      participantId: ParticipantId,
      actAs: PartySet,
      disclosures: BoundedContractIdList,
      commands: List[Command],
  )

  type Ledger = List[Commands]

  // topologies

  final case class Participant(
      participantId: ParticipantId,
      packages: PackageIdSet,
      parties: PartySet,
  )

  type Topology = Seq[Participant]

  // tying all together

  final case class Scenario(topology: Topology, ledger: Ledger)
}
