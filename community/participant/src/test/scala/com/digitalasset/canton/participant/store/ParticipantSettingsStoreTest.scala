// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option._
import cats.syntax.foldable._
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.store.ParticipantSettingsStore.Settings
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import monocle.Lens
import monocle.macros.GenLens

import scala.concurrent.Future

trait ParticipantSettingsStoreTest
    extends BaseTestWordSpec
    with HasExecutionContext // because we want to test concurrent insertions
    {

  lazy val resourceLimits0: ResourceLimits = ResourceLimits.noLimit
  lazy val resourceLimits1: ResourceLimits =
    ResourceLimits(Some(NonNegativeInt.tryCreate(42)), None)
  lazy val resourceLimits2: ResourceLimits =
    ResourceLimits(Some(NonNegativeInt.tryCreate(84)), None)
  lazy val resourceLimits3: ResourceLimits =
    ResourceLimits(Some(NonNegativeInt.tryCreate(42)), Some(NonNegativeInt.tryCreate(22)))
  lazy val resourceLimits4: ResourceLimits =
    ResourceLimits(None, Some(NonNegativeInt.tryCreate(22)))

  lazy val maxDedupTime = NonNegativeFiniteDuration.ofMicros(123456789L)

  def participantSettingsStore(mk: () => ParticipantSettingsStore): Unit = {
    "resource limits" should {
      "support insert / delete / update" in {
        val store = mk()
        (for {
          _ <- store.refreshCache()
          settings0 = store.settings

          _ <- store.writeResourceLimits(resourceLimits1)
          settings1 = store.settings

          _ <- store.writeResourceLimits(resourceLimits2)
          settings2 = store.settings

          _ <- store.writeResourceLimits(resourceLimits3)
          settings3 = store.settings

          _ <- store.writeResourceLimits(resourceLimits4)
          settings4 = store.settings
        } yield {
          settings0 shouldBe Settings(resourceLimits = resourceLimits0)
          settings1 shouldBe Settings(resourceLimits = resourceLimits1)
          settings2 shouldBe Settings(resourceLimits = resourceLimits2)
          settings3 shouldBe Settings(resourceLimits = resourceLimits3)
          settings4 shouldBe Settings(resourceLimits = resourceLimits4)
        }).futureValue
      }
    }

    def singleInsertion[A](first: A, other: A)(
        insert: (ParticipantSettingsStore, A) => Future[Unit],
        lens: Lens[Settings, Option[A]],
    ): Unit = {

      "support a single insertion" in {
        val store = mk()
        (for {
          _ <- store.refreshCache()
          settings0 = store.settings

          _ <- insert(store, first)
          settings1 = store.settings

          _ <- insert(store, first)
          settings2 = store.settings

          _ <- insert(store, other)
          settings3 = store.settings
        } yield {
          val emptyS = Settings()
          val firstS = lens.replace(first.some).apply(emptyS)
          settings0 shouldBe emptyS
          settings1 shouldBe firstS
          settings2 shouldBe firstS
          settings3 shouldBe firstS
        }).futureValue
      }

      "not affect resource limits" in {
        val store = mk()
        (for {
          _ <- store.writeResourceLimits(resourceLimits = resourceLimits1)
          _ <- insert(store, first)
          settings1 = store.settings
          _ <- store.writeResourceLimits(resourceLimits4)
          settings2 = store.settings
        } yield {
          val setter = lens.replace(first.some)
          settings1 shouldBe setter.apply(Settings(resourceLimits = resourceLimits1))
          settings2 shouldBe setter.apply(Settings(resourceLimits = resourceLimits4))
        }).futureValue
      }
    }

    "max deduplication time" should {
      behave like singleInsertion(maxDedupTime, NonNegativeFiniteDuration.Zero)(
        _.insertMaxDeduplicationTime(_),
        GenLens[Settings](_.maxDeduplicationTime),
      )
    }

    "unique contract keys" should {
      behave like singleInsertion(false, true)(
        _.insertUniqueContractKeysMode(_),
        GenLens[Settings](_.uniqueContractKeys),
      )
    }

    "eventually reach a consistent cache after concurrent updates" in {
      val store = mk()
      store.refreshCache().futureValue

      (1 until 10)
        .map(NonNegativeInt.tryCreate)
        .toList
        .traverse_(i => store.writeResourceLimits(ResourceLimits(Some(i), None)))
        .futureValue

      val cachedValue = store.settings

      store.refreshCache().futureValue
      store.settings shouldBe cachedValue
    }

    "fail if a value is queried before refreshing the cache" in {
      val store = mk()
      an[IllegalStateException] should be thrownBy { store.settings }
    }
  }
}
