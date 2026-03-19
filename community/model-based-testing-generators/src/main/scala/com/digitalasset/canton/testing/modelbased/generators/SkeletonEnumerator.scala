// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.generators

import cats.Applicative
import cats.syntax.all.*
import com.digitalasset.canton.testing.modelbased.ast.Skeleton.*
import com.digitalasset.canton.testing.modelbased.genlib.Spaces.Space
import com.digitalasset.canton.testing.modelbased.genlib.Spaces.Space as S
import com.digitalasset.canton.testing.modelbased.genlib.Spaces.Space.Instances.*
import com.digitalasset.daml.lf.language.LanguageVersion

class SkeletonEnumerator(
    languageVersion: LanguageVersion,
    readOnlyRollbacks: Boolean,
    generateQueryByKey: Boolean = false,
) {

  // Commands := Commands Parties [TopLevelAction]
  // TopLevelAction := Create | Exercise EKind Action*
  // Action := Create | Exercise EKind Action* | Fetch | Lookup | QueryByKey | Rollback ActionUnderRollback+
  // EKind := Consuming | NonConsuming
  //
  // For PV=dev:
  //   ActionUnderRollback := Exercise NonConsuming ActionUnderRollback* | Fetch | Lookup | QueryByKey
  // For other PVs:
  //   ActionUnderRollback := Create | Exercise EActionUnderRollback* | Fetch | Lookup | QueryByKey
  //
  // The ByKey variants are only available in PV=dev

  val AS: Applicative[Space] = implicitly

  def when[A](condition: Boolean)(s: => Space[A]): Space[A] =
    if (condition) s
    else Space.empty[A]

  def withFeature[A](feature: LanguageVersion.Feature)(s: Space[A]): Space[A] =
    when(feature.versionRange.contains(languageVersion))(s)

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

  lazy val createsWithKey: Space[Action] = withFeature(LanguageVersion.featureContractKeys) {
    S.singleton(CreateWithKey())
  }

  lazy val exercises: Space[Action] =
    AS.map2(
      exerciceKinds,
      listsOf(actions),
    )(Exercise.apply)

  lazy val exercisesByKey: Space[Action] = withFeature(LanguageVersion.featureContractKeys) {
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
    withFeature(LanguageVersion.featureContractKeys) {
      AS.map(
        listsOf(nonEffectfulActions)
      )(ExerciseByKey(NonConsuming, _))
    }

  lazy val fetches: Space[Action] =
    S.singleton(Fetch())

  lazy val fetchesByKey: Space[Action] = withFeature(LanguageVersion.featureContractKeys) {
    S.singleton(FetchByKey())
  }

  lazy val lookupsByKey: Space[Action] = withFeature(LanguageVersion.featureContractKeys) {
    bools.map(LookupByKey.apply)
  }

  lazy val queriesByKey: Space[Action] =
    when(generateQueryByKey) {
      withFeature(LanguageVersion.featureContractKeys) {
        bools.map(QueryByKey.apply)
      }
    }

  lazy val rollbacks: Space[Action] =
    AS.map(nonEmptyListOf(if (readOnlyRollbacks) nonEffectfulActions else actions))(Rollback.apply)

  lazy val actions: Space[Action] =
    S.pay(
      creates + createsWithKey + exercises + exercisesByKey + fetches + fetchesByKey + lookupsByKey + queriesByKey + rollbacks
    )

  lazy val nonEffectfulActions: Space[Action] =
    S.pay(
      nonEffectfulExercises + nonEffectfulExercisesByKey + fetches + fetchesByKey + lookupsByKey + queriesByKey + rollbacks
    )

  lazy val topLevelActions: Space[Action] =
    S.pay(creates + createsWithKey + exercises + exercisesByKey)

  lazy val command: Space[Command] =
    AS.map2(bools, topLevelActions)(Command.apply)

  lazy val commands: Space[Commands] =
    AS.map(nonEmptyListOf(command))(Commands.apply)

  lazy val ledgers: Space[Ledger] =
    listsOf(commands)

  def scenarios(numParticipants: Int): Space[Scenario] = {
    val topology = Seq.fill(numParticipants)(Participant())
    ledgers.map(Scenario(topology, _))
  }

  def scenarios(numParticipants: Int, numCommands: Int): Space[Scenario] = {
    val topology = Seq.fill(numParticipants)(Participant())
    List.fill(numCommands)(commands).sequence.map(Scenario(topology, _))
  }
}
