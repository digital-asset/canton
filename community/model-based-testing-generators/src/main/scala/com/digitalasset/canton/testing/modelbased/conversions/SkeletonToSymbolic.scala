// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.conversions

import com.digitalasset.canton.testing.modelbased.ast.Implicits.*
import com.digitalasset.canton.testing.modelbased.ast.{Skeleton as Skel, Symbolic as Sym}
import com.microsoft.z3.{Context, IntExpr}

import scala.annotation.nowarn

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object SkeletonToSymbolic {
  def toSymbolic(ctx: Context, skeleton: Skel.Scenario): Sym.Scenario =
    new Converter(ctx, skeleton).toSymbolic()

  private class Converter(ctx: Context, scenario: Skel.Scenario) {

    private val maxDisclosures: Int = scenario.ledger.numContracts
    private val maxQueryByKeyResults: Int = scenario.ledger.numContracts

    private val partySetSort: Sym.PartySetSort = ctx.mkSetSort(ctx.mkIntSort())
    private val packageIdSetSort: Sym.PackageIdSetSort = ctx.mkSetSort(ctx.mkIntSort())
    private val contractIdSort: Sym.ContractIdSort = ctx.mkIntSort()
    private val participantIdSort: Sym.ParticipantIdSort = ctx.mkIntSort()

    private def mkFreshParticipantId(): Sym.ParticipantId =
      ctx.mkFreshConst("p", participantIdSort).asInstanceOf[Sym.ContractId]

    private def mkFreshContractId(): Sym.ContractId =
      ctx.mkFreshConst("c", contractIdSort).asInstanceOf[Sym.ContractId]

    private def mkFreshkeyId(): Sym.KeyId =
      ctx.mkFreshConst("ki", contractIdSort).asInstanceOf[Sym.KeyId]

    private def mkFreshPartySet(name: String): Sym.PartySet =
      ctx.mkFreshConst(name, partySetSort).asInstanceOf[Sym.PartySet]

    private def mkFreshBoundedContractIdList(
        name: String,
        maxLength: Int,
    ): Sym.BoundedContractIdList =
      Sym.BoundedContractIdList(
        elements = (0 until maxLength).map(i =>
          ctx.mkFreshConst(s"${name}_$i", contractIdSort).asInstanceOf[Sym.ContractId]
        ),
        length = ctx.mkFreshConst(s"${name}_len", ctx.mkIntSort()).asInstanceOf[IntExpr],
      )

    private def mkFreshPackageIdSet(name: String): Sym.PackageIdSet =
      ctx.mkFreshConst(name, packageIdSetSort).asInstanceOf[Sym.PackageIdSet]

    private def mkFreshPackageId(): Sym.PackageId =
      ctx.mkFreshConst("pkg", contractIdSort).asInstanceOf[Sym.PackageId]

    private def toSymbolic(commands: Skel.Commands): Sym.Commands =
      Sym.Commands(
        mkFreshParticipantId(),
        mkFreshPartySet("a"),
        mkFreshBoundedContractIdList("d", maxDisclosures),
        commands.commands.map(toSymbolic),
      )

    private def toSymbolic(command: Skel.Command): Sym.Command =
      Sym.Command(
        Option.when(command.explicitPackageId)(mkFreshPackageId()),
        toSymbolic(command.action),
      )

    private def toSymbolic(kind: Skel.ExerciseKind): Sym.ExerciseKind =
      kind match {
        case Skel.Consuming => Sym.Consuming
        case Skel.NonConsuming => Sym.NonConsuming
      }

    private def toSymbolic(action: Skel.Action): Sym.Action = action match {
      case Skel.Create() =>
        Sym.Create(
          mkFreshContractId(),
          mkFreshPartySet("s"),
          mkFreshPartySet("o"),
        )
      case Skel.CreateWithKey() =>
        Sym.CreateWithKey(
          mkFreshContractId(),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
          mkFreshPartySet("s"),
          mkFreshPartySet("o"),
        )
      case Skel.Exercise(kind, subTransaction) =>
        Sym.Exercise(
          toSymbolic(kind),
          mkFreshContractId(),
          mkFreshPartySet("k"),
          mkFreshPartySet("q"),
          subTransaction.map(toSymbolic),
        )
      case Skel.ExerciseByKey(kind, subTransaction) =>
        Sym.ExerciseByKey(
          toSymbolic(kind),
          mkFreshContractId(),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
          mkFreshPartySet("k"),
          mkFreshPartySet("q"),
          subTransaction.map(toSymbolic),
        )
      case Skel.Fetch() =>
        Sym.Fetch(mkFreshContractId())
      case Skel.FetchByKey() =>
        Sym.FetchByKey(
          mkFreshContractId(),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
        )
      case Skel.LookupByKey(successful) =>
        Sym.LookupByKey(
          if (successful) Some(mkFreshContractId()) else None,
          mkFreshkeyId(),
          mkFreshPartySet("m"),
        )
      case Skel.QueryByKey(exhaustive) =>
        Sym.QueryByKey(
          mkFreshBoundedContractIdList("qbk", maxQueryByKeyResults),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
          exhaustive,
        )
      case Skel.Rollback(subTransaction) =>
        Sym.Rollback(subTransaction.map(toSymbolic))
    }

    def toSymbolic(ledger: Skel.Ledger): Sym.Ledger =
      ledger.map(toSymbolic)

    @nowarn("cat=unused")
    def toSymbolic(participant: Skel.Participant): Sym.Participant =
      Sym.Participant(mkFreshParticipantId(), mkFreshPackageIdSet("pkgs"), mkFreshPartySet("ps"))

    def toSymbolic(topology: Skel.Topology): Sym.Topology =
      topology.map(toSymbolic)

    def toSymbolic(): Sym.Scenario =
      Sym.Scenario(toSymbolic(scenario.topology), toSymbolic(scenario.ledger))
  }
}
