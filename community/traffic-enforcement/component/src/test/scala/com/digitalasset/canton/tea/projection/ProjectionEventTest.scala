// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.tea.AccountId
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class ProjectionEventTest extends AnyWordSpec with BaseTest {

  private val alice = AccountId.tryCreate("alice")
  private val bob = AccountId.tryCreate("bob")
  private val delta = TrafficDelta.creditBalanceDelta(10L)

  private def idsOf(event: ProjectionEvent.TopUps): Seq[(AccountDelta, String)] =
    event.deltasWithEventIds("p").map { case (d, id) => d -> id.unwrap }

  private def topUps(first: (Int, AccountDelta), rest: (Int, AccountDelta)*) =
    ProjectionEvent.TopUps(42L, CantonTimestamp.Epoch, NonEmpty(Map, first, rest*))

  "ProjectionEvent.TopUps.deltasWithEventIds" should {
    "append the key to the offset" in {
      val a = AccountDelta(alice, delta)
      val b = AccountDelta(bob, delta)
      idsOf(topUps(3 -> a, 7 -> b)) should contain theSameElementsAs Seq(
        a -> "p-42-3",
        b -> "p-42-7",
      )
    }

    "include the key even for a single entry" in {
      val a = AccountDelta(alice, delta)
      idsOf(topUps(5 -> a)) shouldBe Seq(a -> "p-42-5")
    }

    "give distinct ids to deltas for the same account" in {
      val a = AccountDelta(alice, delta)
      idsOf(topUps(1 -> a, 2 -> a)).map(_._2) should contain theSameElementsAs Seq(
        "p-42-1",
        "p-42-2",
      )
    }
  }
}
