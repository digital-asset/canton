// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.conversions

import com.digitalasset.canton.testing.modelbased.ast.{Concrete as Conc, Symbolic as Sym}
import com.microsoft.z3.enumerations.Z3_lbool
import com.microsoft.z3.{Context, IntNum, Model}

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class SymbolicToConcrete(numPackages: Int, numParties: Int, ctx: Context, model: Model) {

  def toConcrete(scenario: Sym.Scenario): Conc.Scenario =
    Conc.Scenario(scenario.topology.map(toConcrete), toConcrete(scenario.ledger))

  private def evalParticipantId(pid: Sym.ParticipantId): Conc.ParticipantId =
    model.evaluate(pid, false).asInstanceOf[IntNum].getInt

  private def evalContractId(cid: Sym.ContractId): Conc.ContractId =
    model.evaluate(cid, false).asInstanceOf[IntNum].getInt

  private def evalKeyId(cid: Sym.KeyId): Conc.KeyId =
    model.evaluate(cid, true).asInstanceOf[IntNum].getInt

  private def evalPackageId(pid: Sym.PackageId): Conc.PackageId =
    model.evaluate(pid, false).asInstanceOf[IntNum].getInt

  private def evalPartySet(set: Sym.PartySet): Conc.PartySet =
    (1 to numParties).toSet.filter { i =>
      model
        .evaluate(ctx.mkSelect(set, ctx.mkInt(i)), false)
        .getBoolValue == Z3_lbool.Z3_L_TRUE
    }

  private def evalBoundedContractIdList(list: Sym.BoundedContractIdList): Conc.ContractIdList = {
    val len = model.evaluate(list.length, false).asInstanceOf[IntNum].getInt
    (0 until len).toList.map { i =>
      model.evaluate(list.elements(i), false).asInstanceOf[IntNum].getInt
    }
  }

  private def evalPackageIdSet(set: Sym.PackageIdSet): Conc.PackageIdSet =
    (0 until numPackages).toSet.filter { i =>
      model
        .evaluate(ctx.mkSelect(set, ctx.mkInt(i)), false)
        .getBoolValue == Z3_lbool.Z3_L_TRUE
    }

  private def toConcrete(commands: Sym.Commands): Conc.Commands =
    Conc.Commands(
      evalParticipantId(commands.participantId),
      evalPartySet(commands.actAs),
      evalBoundedContractIdList(commands.disclosures),
      commands.commands.map(toConcrete),
    )

  private def toConcrete(command: Sym.Command): Conc.Command =
    Conc.Command(command.packageId.map(evalPackageId), toConcrete(command.action))

  private def toConcrete(kind: Sym.ExerciseKind): Conc.ExerciseKind =
    kind match {
      case Sym.Consuming => Conc.Consuming
      case Sym.NonConsuming => Conc.NonConsuming
    }

  private def toConcrete(action: Sym.Action): Conc.Action = action match {
    case Sym.Create(contractId, signatories, observers) =>
      Conc.Create(
        evalContractId(contractId),
        evalPartySet(signatories),
        evalPartySet(observers),
      )
    case Sym.CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
      Conc.CreateWithKey(
        evalContractId(contractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
        evalPartySet(signatories),
        evalPartySet(observers),
      )
    case Sym.Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
      Conc.Exercise(
        toConcrete(kind),
        evalContractId(contractId),
        evalPartySet(controllers),
        evalPartySet(choiceObservers),
        subTransaction.map(toConcrete),
      )
    case Sym.ExerciseByKey(
          kind,
          contractId,
          keyId,
          maintainers,
          controllers,
          choiceObservers,
          subTransaction,
        ) =>
      Conc.ExerciseByKey(
        toConcrete(kind),
        evalContractId(contractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
        evalPartySet(controllers),
        evalPartySet(choiceObservers),
        subTransaction.map(toConcrete),
      )
    case Sym.Fetch(contractId) =>
      Conc.Fetch(evalContractId(contractId))
    case Sym.FetchByKey(contractId, keyId, maintainers) =>
      Conc.FetchByKey(
        evalContractId(contractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
      )
    case Sym.LookupByKey(contractId, keyId, maintainers) =>
      Conc.LookupByKey(
        contractId.map(evalContractId),
        evalKeyId(keyId),
        evalPartySet(maintainers),
      )
    case Sym.QueryByKey(contractIds, keyId, maintainers, exhaustive) =>
      Conc.QueryByKey(
        evalBoundedContractIdList(contractIds),
        evalKeyId(keyId),
        evalPartySet(maintainers),
        exhaustive,
      )
    case Sym.Rollback(subTransaction) =>
      Conc.Rollback(subTransaction.map(toConcrete))
  }

  private def toConcrete(ledger: Sym.Ledger): Conc.Ledger =
    ledger.map(toConcrete)

  private def toConcrete(participant: Sym.Participant): Conc.Participant =
    Conc.Participant(
      evalParticipantId(participant.participantId),
      evalPackageIdSet(participant.packages),
      evalPartySet(participant.parties),
    )
}
