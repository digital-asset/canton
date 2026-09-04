// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import com.digitalasset.canton.BaseTest.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ledger.participant.state.SynchronizerRank
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId, Stakeholders}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class SynchronizerRankTest extends AnyWordSpec with Matchers {

  private def synchronizer(alias: String): PhysicalSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryCreate(alias, DefaultTestIdentities.namespace)
  ).toPhysical

  private val acme = synchronizer("acme")
  private val da = synchronizer("da")
  private val repair = synchronizer("repair")

  private val submitter = LfPartyId.assertFromString("submitter::default")
  private val alice = LfPartyId.assertFromString("alice::default")
  private val bob = LfPartyId.assertFromString("bob::default")

  private val stakeholdersA =
    Stakeholders.withSignatoriesAndObservers(Set(submitter), Set(alice))
  private val stakeholdersB =
    Stakeholders.withSignatoriesAndObservers(Set(submitter), Set(bob))

  private def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(-1, i)

  private def rank(
      target: PhysicalSynchronizerId,
      priority: Int,
      batches: (PhysicalSynchronizerId, Stakeholders, Set[LfContractId])*
  ): SynchronizerRank =
    SynchronizerRank(
      batches.map { case (source, stakeholders, contractIds) =>
        (submitter, source, stakeholders) -> contractIds
      }.toMap,
      priority,
      target,
    )

  "SynchronizerRank ordering" should {
    "prefer fewer batches over fewer contracts" in {
      val oneBatchThreeContracts =
        rank(acme, priority = 0, (repair, stakeholdersA, Set(cid(1), cid(2), cid(3))))
      val twoBatchesTwoContracts = rank(
        repair,
        priority = 0,
        (acme, stakeholdersA, Set(cid(1))),
        (acme, stakeholdersB, Set(cid(2))),
      )

      Seq(twoBatchesTwoContracts, oneBatchThreeContracts).min shouldBe oneBatchThreeContracts
    }

    "prefer fewer contracts when the batch count is equal" in {
      val oneContract = rank(acme, priority = 0, (repair, stakeholdersA, Set(cid(1))))
      val twoContracts = rank(repair, priority = 0, (acme, stakeholdersA, Set(cid(1), cid(2))))

      Seq(twoContracts, oneContract).min shouldBe oneContract
    }

    "prefer the higher priority synchronizer even when it costs more batches" in {
      val higherPriority = rank(
        acme,
        priority = 1,
        (repair, stakeholdersA, Set(cid(1))),
        (da, stakeholdersB, Set(cid(2))),
      )
      val lowerPriorityNoReassignment = rank(repair, priority = 0)

      Seq(lowerPriorityNoReassignment, higherPriority).min shouldBe higherPriority
    }

    "prefer a synchronizer that needs no reassignment at all" in {
      val none = rank(acme, priority = 0)
      val oneBatch = rank(repair, priority = 0, (acme, stakeholdersA, Set(cid(1))))

      Seq(oneBatch, none).min shouldBe none
    }
  }
}
