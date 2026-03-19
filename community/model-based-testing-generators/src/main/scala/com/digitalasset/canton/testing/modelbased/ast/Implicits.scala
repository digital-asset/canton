// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.ast

import com.digitalasset.canton.testing.modelbased.ast.Concrete.{Participant, PartyId}

object Implicits {

  implicit class RichTopology(topology: Concrete.Topology) {
    def groupedByPartyId: Map[PartyId, Set[Participant]] =
      topology
        .flatMap(participant => participant.parties.map(party => party -> Set(participant)))
        .groupMapReduce(_._1)(_._2)(_ ++ _)
  }

  implicit class RichSkeletonLedger(ledger: Skeleton.Ledger) {
    def numRollbacks: Int = {
      def numRollbacksAction(action: Skeleton.Action): Int = action match {
        case rb: Skeleton.Rollback =>
          1 + rb.subTransaction.map(numRollbacksAction).sum
        case exe: Skeleton.Exercise =>
          exe.subTransaction.map(numRollbacksAction).sum
        case exe: Skeleton.ExerciseByKey =>
          exe.subTransaction.map(numRollbacksAction).sum
        case _: Skeleton.Create =>
          0
        case _: Skeleton.CreateWithKey =>
          0
        case _: Skeleton.Fetch =>
          0
        case _: Skeleton.FetchByKey =>
          0
        case _: Skeleton.LookupByKey =>
          0
        case _: Skeleton.QueryByKey =>
          0
      }

      def numRollbacksCommand(command: Skeleton.Command): Int =
        numRollbacksAction(command.action)

      def numRollbacksCommands(commands: Skeleton.Commands): Int =
        commands.commands.map(numRollbacksCommand).sum

      ledger.map(numRollbacksCommands).sum
    }

    def numNonExhaustiveQueryByKey: Int = {
      def numNonExhaustiveQueryByKeyAction(action: Skeleton.Action): Int = action match {
        case qbk: Skeleton.QueryByKey =>
          if (qbk.exhaustive) 0 else 1
        case rb: Skeleton.Rollback =>
          rb.subTransaction.map(numNonExhaustiveQueryByKeyAction).sum
        case exe: Skeleton.Exercise =>
          exe.subTransaction.map(numNonExhaustiveQueryByKeyAction).sum
        case exe: Skeleton.ExerciseByKey =>
          exe.subTransaction.map(numNonExhaustiveQueryByKeyAction).sum
        case _: Skeleton.Create =>
          0
        case _: Skeleton.CreateWithKey =>
          0
        case _: Skeleton.Fetch =>
          0
        case _: Skeleton.FetchByKey =>
          0
        case _: Skeleton.LookupByKey =>
          0
      }

      def numNonExhaustiveQueryByKeyCommand(command: Skeleton.Command): Int =
        numNonExhaustiveQueryByKeyAction(command.action)

      def numNonExhaustiveQueryByKeyCommands(commands: Skeleton.Commands): Int =
        commands.commands.map(numNonExhaustiveQueryByKeyCommand).sum

      ledger.map(numNonExhaustiveQueryByKeyCommands).sum
    }

    def numContracts: Int = {
      def numActionContracts(action: Skeleton.Action): Int = action match {
        case _: Skeleton.Create =>
          1
        case _: Skeleton.CreateWithKey =>
          1
        case exe: Skeleton.Exercise =>
          exe.subTransaction.map(numActionContracts).sum
        case exe: Skeleton.ExerciseByKey =>
          exe.subTransaction.map(numActionContracts).sum
        case _: Skeleton.Fetch =>
          0
        case _: Skeleton.FetchByKey =>
          0
        case _: Skeleton.LookupByKey =>
          0
        case _: Skeleton.QueryByKey =>
          0
        case rb: Skeleton.Rollback =>
          rb.subTransaction.map(numActionContracts).sum
      }

      def numCommandContracts(command: Skeleton.Command): Int =
        numActionContracts(command.action)

      def numCommandsContracts(commands: Skeleton.Commands): Int =
        commands.commands.map(numCommandContracts).sum

      ledger.map(numCommandsContracts).sum
    }
  }

  implicit class RichSymbolicLedger(ledger: Symbolic.Ledger) {
    def numContracts: Int = {
      def numActionContracts(action: Symbolic.Action): Int = action match {
        case _: Symbolic.Create =>
          1
        case _: Symbolic.CreateWithKey =>
          1
        case exe: Symbolic.Exercise =>
          exe.subTransaction.map(numActionContracts).sum
        case exe: Symbolic.ExerciseByKey =>
          exe.subTransaction.map(numActionContracts).sum
        case _: Symbolic.Fetch =>
          0
        case _: Symbolic.FetchByKey =>
          0
        case _: Symbolic.LookupByKey =>
          0
        case _: Symbolic.QueryByKey =>
          0
        case rb: Symbolic.Rollback =>
          rb.subTransaction.map(numActionContracts).sum
      }

      def numCommandContracts(command: Symbolic.Command): Int =
        numActionContracts(command.action)

      def numCommandsContracts(commands: Symbolic.Commands): Int =
        commands.commands.map(numCommandContracts).sum

      ledger.map(numCommandsContracts).sum
    }
  }

  implicit class RichCommands(commands: Concrete.Commands) {
    def actions: List[Concrete.Action] = commands.commands.map(_.action)
  }
}
