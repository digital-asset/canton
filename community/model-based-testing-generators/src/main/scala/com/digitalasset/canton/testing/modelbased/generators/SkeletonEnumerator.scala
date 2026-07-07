// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.generators

import cats.Applicative
import cats.syntax.all.*
import com.digitalasset.canton.testing.modelbased.ast.Skeleton.*
import com.digitalasset.canton.testing.modelbased.genlib.Spaces.Space
import com.digitalasset.canton.testing.modelbased.genlib.Spaces.Space as S
import com.digitalasset.canton.testing.modelbased.genlib.Spaces.Space.Instances.*

class SkeletonEnumerator(
    contractKeys: Boolean,
    readOnlyRollbacks: Boolean,
) {

  // Commands := Commands Parties [TopLevelAction]
  // TopLevelAction := Create | Exercise EKind Action*
  // Action := Create | Exercise EKind Action* | Fetch | QueryByKey | Rollback ActionUnderRollback+
  // EKind := Consuming | NonConsuming
  //
  // For PV=dev:
  //   ActionUnderRollback := Exercise NonConsuming ActionUnderRollback* | Fetch | QueryByKey
  // For other PVs:
  //   ActionUnderRollback := Create | Exercise EActionUnderRollback* | Fetch | QueryByKey
  //
  // The ByKey variants are only available in PV=dev

  val AS: Applicative[Space] = implicitly

  def when[A](condition: Boolean)(s: => Space[A]): Space[A] =
    if (condition) s
    else Space.empty[A]

  lazy val bools: Space[Boolean] = S.singleton(true) + S.singleton(false)

  def listsOf[A](s: Space[A]): Space[List[A]] = {
    lazy val res: Space[List[A]] = S.pay(S.singleton(List.empty[A]) + AS.map2(s, res)(_ :: _))
    res
  }

  def nonEmptyListOf[A](s: Space[A]): Space[List[A]] =
    S.pay(AS.map2(s, listsOf(s))(_ :: _))

  lazy val exerciceKinds: Space[ExerciseKind] =
    S.singleton[ExerciseKind](Consuming) + S.singleton[ExerciseKind](NonConsuming)

  lazy val creates: Space[Action] = S.singleton(Create())

  lazy val createsWithKey: Space[Action] = when(contractKeys) {
    S.singleton(CreateWithKey())
  }

  lazy val exercises: Space[Action] =
    AS.map2(
      exerciceKinds,
      listsOf(actions),
    )(Exercise.apply)

  lazy val exercisesByKey: Space[Action] = when(contractKeys) {
    AS.map2(
      exerciceKinds,
      listsOf(actions),
    )(ExerciseByKey.apply)
  }

  lazy val nonEffectfulExercises: Space[Action] =
    AS.map(
      listsOf(nonEffectfulActions)
    )(Exercise(NonConsuming, _))

  lazy val nonEffectfulExercisesByKey: Space[Action] =
    when(contractKeys) {
      AS.map(
        listsOf(nonEffectfulActions)
      )(ExerciseByKey(NonConsuming, _))
    }

  lazy val fetches: Space[Action] =
    S.singleton(Fetch())

  lazy val fetchesByKey: Space[Action] = when(contractKeys) {
    S.singleton(FetchByKey())
  }

  lazy val queriesByKey: Space[Action] =
    when(contractKeys) {
      bools.map(QueryByKey.apply)
    }

  lazy val rollbacks: Space[Action] =
    AS.map(nonEmptyListOf(if (readOnlyRollbacks) nonEffectfulActions else actions))(Rollback.apply)

  lazy val actions: Space[Action] =
    S.pay(
      creates + createsWithKey + exercises + exercisesByKey + fetches + fetchesByKey + queriesByKey + rollbacks
    )

  lazy val nonEffectfulActions: Space[Action] =
    S.pay(
      nonEffectfulExercises + nonEffectfulExercisesByKey + fetches + fetchesByKey + queriesByKey + rollbacks
    )

  lazy val topLevelActions: Space[Action] =
    S.pay(creates + createsWithKey + exercises + exercisesByKey)

  lazy val command: Space[Command] =
    AS.map2(bools, topLevelActions)(Command.apply)

  def commands(singletonCommands: Boolean): Space[Commands] =
    if (singletonCommands) AS.map(command)(c => Commands(List(c)))
    else AS.map(nonEmptyListOf(command))(Commands.apply)

  def ledgers(numCommands: Option[Int] = None, singletonCommands: Boolean): Space[Ledger] =
    numCommands match {
      case Some(n) => List.fill(n)(commands(singletonCommands)).sequence
      case None => listsOf(commands(singletonCommands))
    }

  def scenarios(
      numParticipants: Int,
      numCommands: Option[Int] = None,
      singletonCommands: Boolean = false,
  ): Space[Scenario] = {
    val topology = Seq.fill(numParticipants)(Participant())
    ledgers(numCommands, singletonCommands).map(Scenario(topology, _))
  }
}
