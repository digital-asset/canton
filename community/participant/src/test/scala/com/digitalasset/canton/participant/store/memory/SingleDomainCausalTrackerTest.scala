// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Semigroup
import cats.implicits._
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.EventPerPartyCausalState
import com.digitalasset.canton.participant.protocol._
import com.digitalasset.canton.participant.store.SingleDomainCausalDependencyStore.CausalityWriteFinished
import com.digitalasset.canton.participant.store.memory.SingleDomainCausalTrackerTest._
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{
  BaseTest,
  DomainId,
  HasExecutionContext,
  LfPartyId,
  RepeatableTestSuiteTest,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class SingleDomainCausalTrackerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with RepeatableTestSuiteTest {

  lazy val participant: ParticipantId = ParticipantId("p1-mydomain")

  lazy val domain1: DomainId = DomainId.tryFromString("domain1::namespace")
  lazy val domain2: DomainId = DomainId.tryFromString("domain2::namespace")
  lazy val domain3: DomainId = DomainId.tryFromString("domain3::namespace")
  lazy val domain4: DomainId = DomainId.tryFromString("domain4::namespace")

  lazy val alice: LfPartyId = LfPartyId.assertFromString("alice")
  lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")

  "in a single domain topology, events" should {

    "have no causal dependencies" in {
      val gco = createGco
      val sut =
        new SingleDomainCausalTracker(
          gco,
          new InMemorySingleDomainCausalDependencyStore(domain1, loggerFactory),
          loggerFactory,
        )

      val updates = List(
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch, domain1, 0),
        TransactionUpdate(Set(bob), CantonTimestamp.Epoch.plusSeconds(1), domain1, 1),
      )

      val events = runUpdates(sut, updates)

      val expectedEvents = updates.map { u =>
        EventPerPartyCausalState((u.domain), u.ts, u.rc)(
          Map.empty,
          CausalityWriteFinished(Future.unit),
        )
      }

      events shouldBe expectedEvents
    }

  }

  "in a simple-two domain topology, events" should {
    "maintain causal per-party dependencies across domains" in {
      val gco = createGco
      val sut =
        new SingleDomainCausalTracker(
          gco,
          new InMemorySingleDomainCausalDependencyStore(domain2, loggerFactory),
          loggerFactory,
        )

      val id =
        TransferId(originDomain = domain1, requestTimestamp = CantonTimestamp.Epoch.minusSeconds(1))
      val outTime = CantonTimestamp.Epoch.plusSeconds(5)

      val updates: List[CausalityUpdate] = List[CausalityUpdate](
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch, domain2, 0),
        TransferInUpdate(
          Set(alice),
          CantonTimestamp.Epoch.plusSeconds(1),
          domain2,
          1,
          id,
        ),
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch.plusSeconds(2), domain2, 2),
      )

      val cm =
        CausalityMessage(
          domain2,
          id,
          VectorClock(domain1, localTs = outTime, alice, Map(domain1 -> outTime)),
        )
      sut.registerCausalityMessages(List(cm))

      val events = runUpdates(sut, updates)

      val expectedEvents = updates
        .foldLeft(List.empty[EventPerPartyCausalState]) { case (eventsList, update) =>
          val delta: Map[DomainId, CantonTimestamp] = update match {
            case TransactionUpdate(parties, ts, domain, rc) => Map.empty
            // Causal dependencies should be introduced by the transfer in
            case TransferInUpdate(parties, ts, domain, rc, transferId) =>
              Map(transferId.originDomain -> outTime)
            case _ => fail()
          }
          // For this test, the next event clock is the previous event clock plus any extra causal dependencies
          val map = eventsList.headOption.map(c => c.waitOn).getOrElse(Map.empty) ++ delta
          val next =
            EventPerPartyCausalState((update.domain), update.ts, update.rc)(
              Map(alice -> map),
              CausalityWriteFinished(Future.unit),
            )

          next :: eventsList
        }
        .reverse

      events shouldBe expectedEvents
    }

    "register causality information on transfer out" in {
      val gco = createGco
      val sut =
        new SingleDomainCausalTracker(
          gco,
          new InMemorySingleDomainCausalDependencyStore(domain2, loggerFactory),
          loggerFactory,
        )

      val outTime = CantonTimestamp.Epoch.plusSeconds(1)
      val id = TransferId(originDomain = domain2, requestTimestamp = outTime)

      val updates: List[CausalityUpdate] = List[CausalityUpdate](
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch, domain2, 0),
        TransferOutUpdate(
          Set(alice),
          outTime,
          id,
          1,
        ),
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch.plusSeconds(2), domain2, 2),
      )

      val events = runUpdates(sut, updates)
      val expectedEvents =
        updates.map(u =>
          EventPerPartyCausalState((u.domain), u.ts, u.rc)(
            Map.empty,
            CausalityWriteFinished(Future.unit),
          )
        )

      events shouldBe expectedEvents

      val transferInfo =
        sut.globalCausalOrderer.domainCausalityStore.inspectTransferStoreForTesting(id).value
      transferInfo shouldBe Map(
        alice -> VectorClock(domain2, outTime, alice, clock = Map(domain2 -> outTime))
      )
    }
  }

  "in more complex cases, events" should {

    "save only per-party causal state" in {
      val gco = createGco
      val sut =
        new SingleDomainCausalTracker(
          gco,
          new InMemorySingleDomainCausalDependencyStore(domain4, loggerFactory),
          loggerFactory,
        )

      // First transfer from d1 to d4
      val outTime = CantonTimestamp.Epoch.plusSeconds(5)
      val transfer1ID = TransferId(originDomain = domain1, requestTimestamp = outTime)

      // Second transfer out from d4
      val outTime2 = CantonTimestamp.Epoch.plusSeconds(10)
      val transfer2ID = TransferId(originDomain = domain4, requestTimestamp = outTime2)

      // Represents a transfer in to domain d4 followed by a transfer out
      val updates: List[CausalityUpdate] = List[CausalityUpdate](
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch, domain4, 0),
        TransferInUpdate(
          Set(alice, bob),
          CantonTimestamp.Epoch.plusSeconds(1),
          domain4,
          1,
          transfer1ID,
        ),
        TransactionUpdate(Set(alice), CantonTimestamp.Epoch.plusSeconds(2), domain4, 2),
        TransactionUpdate(Set(bob), CantonTimestamp.Epoch.plusSeconds(3), domain4, 3),
        TransferOutUpdate(
          Set(alice, bob),
          outTime,
          transfer2ID,
          4,
        ),
      )

      // Causality messages for the transfer in
      val aliceD3Time = CantonTimestamp.Epoch.plusSeconds(10)
      val cmAlice =
        CausalityMessage(
          domain4,
          transfer1ID,
          VectorClock(
            domain1,
            localTs = outTime,
            alice,
            Map(domain1 -> outTime, domain3 -> aliceD3Time),
          ),
        )

      val bobD2Time = CantonTimestamp.Epoch.plusSeconds(20)
      val cmBob =
        CausalityMessage(
          domain4,
          transfer1ID,
          VectorClock(
            domain1,
            localTs = outTime,
            bob,
            Map(domain1 -> outTime, domain2 -> bobD2Time),
          ),
        )

      sut.registerCausalityMessages(List(cmBob))
      sut.registerCausalityMessages(List(cmAlice))

      val events = runUpdates(sut, updates)

      val expectedEvents = updates
        .foldLeft(
          (
            List.empty[EventPerPartyCausalState],
            Map.empty[LfPartyId, Map[DomainId, CantonTimestamp]],
          )
        ) { case ((events, perPartyCausalDependencies), causalityUpdate) =>
          val delta: Map[LfPartyId, Map[DomainId, CantonTimestamp]] = causalityUpdate match {

            case TransactionUpdate(parties, ts, domain, rc) =>
              parties.toList.map(p => p -> Map(domain -> ts)).toMap

            case TransferInUpdate(parties, ts, domain, rc, transferId) =>
              Map(
                alice -> Map(domain1 -> outTime, domain3 -> aliceD3Time, domain -> ts),
                bob -> Map(domain1 -> outTime, domain2 -> bobD2Time, domain -> ts),
              )

            case TransferOutUpdate(parties, ts, transferId, rc) =>
              (parties.toList.map(p => p -> Map(transferId.originDomain -> ts)).toMap)

            case _ =>
              fail()
          }

          val updatedCausalDependencies: Map[LfPartyId, Map[DomainId, CantonTimestamp]] =
            delta.foldLeft(perPartyCausalDependencies) { case (acc, (k, v)) =>
              acc.get(k) match {
                case None => acc + (k -> v)
                case Some(value) => acc + (k -> merge(v, value))
              }
            }

          val next =
            EventPerPartyCausalState(
              (causalityUpdate.domain),
              causalityUpdate.ts,
              causalityUpdate.rc,
            )(updatedCausalDependencies, CausalityWriteFinished(Future.unit))

          val nextList = next :: events
          (nextList, updatedCausalDependencies)
        }
        ._1
        .reverse

      forAll(events.zip(expectedEvents)) { case (seen, expected) =>
        seen shouldBe expected
      }

      val saved = sut.globalCausalOrderer.domainCausalityStore
        .inspectTransferStoreForTesting(transfer2ID)
        .value

      // Check that the state propagated with the transfer-out does not confuse causal dependencies between different parties
      saved.get(alice).value.clock.keySet shouldBe Set(domain1, domain3, domain4)
      saved.get(bob).value.clock.keySet shouldBe Set(domain1, domain2, domain4)
    }

  }

  def runUpdates(
      sut: SingleDomainCausalTracker,
      updates: List[CausalityUpdate],
  ): List[EventPerPartyCausalState] = {

    def step(
        acc: List[EventPerPartyCausalState],
        e: CausalityUpdate,
    ): Future[List[EventPerPartyCausalState]] = {
      sut.registerCausalityUpdate(e).map(c => c :: acc)
    }

    MonadUtil
      .foldLeftM(initialState = List.empty[EventPerPartyCausalState], xs = updates)(step)
      .futureValue
      .reverse
  }

  def createGco: GlobalCausalOrderer = {
    new GlobalCausalOrderer(
      participant,
      _ => true,
      DefaultProcessingTimeouts.testing,
      new InMemoryMultiDomainCausalityStore(loggerFactory),
      loggerFactory,
    )
  }
}

object SingleDomainCausalTrackerTest {

  // Combine two maps, taking the highest canton timestamp where there is a duplicate
  def merge(
      m1: Map[DomainId, CantonTimestamp],
      m2: Map[DomainId, CantonTimestamp],
  ): Map[DomainId, CantonTimestamp] = {
    implicit val maxTimestampSemigroup: Semigroup[CantonTimestamp] =
      (x: CantonTimestamp, y: CantonTimestamp) => x.max(y)

    m1.combine(m2)
  }

}
