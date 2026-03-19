// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.ast

object Concrete {

  // ledgers

  type PartySet = Set[PartyId]
  type ContractIdList = List[ContractId]
  type PackageIdSet = Set[PackageId]
  type PartyId = Int
  type ContractId = Int
  type KeyId = Int
  type PackageId = Int

  type ParticipantId = Int

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
  final case class LookupByKey(
      contractId: Option[ContractId],
      keyId: KeyId,
      maintainers: PartySet,
  ) extends Action
  final case class QueryByKey(
      contractIds: ContractIdList,
      keyId: KeyId,
      maintainers: PartySet,
      exhaustive: Boolean,
  ) extends Action
  final case class FetchByKey(
      contractId: ContractId,
      keyId: KeyId,
      maintainers: PartySet,
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
      // disclosures are ordered because their order matters for key resolution
      disclosures: ContractIdList,
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
