// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.db.DbEventLogTestResources
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.PublicPackageUploadRejected
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LedgerSubmissionId,
  LfPartyId,
  RequestCounter,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

trait CausalityStoresTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  case class TestedStores(
      multiDomainCausalityStore: MultiDomainCausalityStore,
      singleDomainCausalDependencyStore: SingleDomainCausalDependencyStore,
  )

  lazy val namespace = "causality-stores-test"
  lazy val writeToDomain: DomainId = DomainId.tryFromString(s"domain1::$namespace")
  lazy val domain2: DomainId = DomainId.tryFromString(s"domain2::$namespace")
  lazy val domain3: DomainId = DomainId.tryFromString(s"domain3::$namespace")
  lazy val domain4: DomainId = DomainId.tryFromString(s"domain4::$namespace")

  lazy val indexedStringStore: InMemoryIndexedStringStore =
    DbEventLogTestResources.dbCausalityStoresTestIndexedStringStore

  def persistEvents(events: Seq[(EventLogId, TimestampedEvent, Boolean)]): Future[Unit]

  def causalityStores(mk: () => Future[TestedStores], persistence: Boolean): Unit = {
    s"the ${SingleDomainCausalDependencyStore.getClass} and ${MultiDomainCausalityStore.getClass} work together... " must {
      val alice: LfPartyId = LfPartyId.assertFromString("alice")
      val bob: LfPartyId = LfPartyId.assertFromString("bob")
      val charlie: LfPartyId = LfPartyId.assertFromString("charlie")

      val txOutTs = CantonTimestamp.Epoch
      val txOutId = TransferId(writeToDomain, txOutTs)
      val txOut2Ts = txOutTs.plusSeconds(1)
      val txOut2Id = TransferId(writeToDomain, txOut2Ts)

      if (persistence) s"looking up state at a transfer-out" should {

        "be able to look-up past transfer-outs from storage" in {
          val testedStores = mk().futureValue
          val store = testedStores.singleDomainCausalDependencyStore
          val globalStore = testedStores.multiDomainCausalityStore

          val () = store.initialize(None).futureValue

          val deps =
            Map(alice -> Map(writeToDomain -> txOutTs), bob -> Map(writeToDomain -> txOutTs))
          val causalityWrite =
            store.updateStateAndStore(RequestCounter(0), txOutTs, deps, Some(txOutId))

          val () = causalityWrite.futureValue.finished.finished.futureValue

          val seen = globalStore.awaitTransferOutRegistered(txOutId, Set(alice, bob)).futureValue

          seen shouldBe deps.map { case (id, value) =>
            id -> VectorClock(txOutId.sourceDomain, txOutTs, id, value)
          }
        }

        "be able to look-up past causality messages from storage" in {
          val testedStores = mk().futureValue
          val store = testedStores.singleDomainCausalDependencyStore
          val globalStoore = testedStores.multiDomainCausalityStore

          val () = store.initialize(None).futureValue

          val clock =
            VectorClock(txOutId.sourceDomain, txOutTs, alice, Map(writeToDomain -> txOutTs))
          val cmsg = CausalityMessage(txOutId.sourceDomain, testedProtocolVersion, txOutId, clock)
          val written = globalStoore.registerCausalityMessages(List(cmsg))

          val () = written.futureValue

          val seen = globalStoore.awaitTransferOutRegistered(txOutId, Set(alice)).futureValue

          seen shouldBe Map(alice -> clock)
        }

      }

      s"synchronize" should {

        "wait until transfer-out state is registered" in {
          val testedStores = mk().futureValue
          val singleDomainCausalDependencyStore = testedStores.singleDomainCausalDependencyStore
          val multiDomainCausalityStore = testedStores.multiDomainCausalityStore

          def transferOutBlocks(transferId: TransferId, rc: RequestCounter) = {
            val ts = transferId.requestTimestamp

            val seen =
              multiDomainCausalityStore.awaitTransferOutRegistered(transferId, Set(alice, bob))

            always() {
              seen.isCompleted shouldBe false
            }

            val dependencies =
              Map(alice -> Map(writeToDomain -> ts), bob -> Map(writeToDomain -> ts))

            val clocks = dependencies.map { case (id, value) =>
              id -> VectorClock(transferId.sourceDomain, ts, id, value)
            }

            singleDomainCausalDependencyStore
              .updateStateAndStore(rc, ts, dependencies, Some(transferId))
              .futureValue
              .finished
              .finished
              .futureValue

            multiDomainCausalityStore.registerTransferOut(transferId, clocks.values.toSet)

            seen.futureValue.keySet shouldBe Set(alice, bob)
          }

          val () = singleDomainCausalDependencyStore.initialize(None).futureValue

          transferOutBlocks(transferId = txOutId, RequestCounter(0))
          transferOutBlocks(transferId = txOut2Id, RequestCounter(1))
        }
      }

      s"maintain the per-domain, per-party causal state" should {
        val aliceBobRequestTs = txOutTs
        val aliceBobRc = RequestCounter(0)

        val charlieRequestTs = aliceBobRequestTs.plusSeconds(1)
        val charlieRc = RequestCounter(1)
        val charlieDepTs = aliceBobRequestTs.plusSeconds(2)

        val aliceBobState =
          Map(
            alice -> Map(writeToDomain -> aliceBobRequestTs),
            bob -> Map(writeToDomain -> aliceBobRequestTs),
          )

        val charlieState =
          Map(charlie -> Map(writeToDomain -> charlieRequestTs, domain2 -> charlieDepTs))

        def writeState(store: SingleDomainCausalDependencyStore): Unit = {
          val causalityWrite =
            store.updateStateAndStore(aliceBobRc, aliceBobRequestTs, aliceBobState, None)
          val () = causalityWrite.futureValue.finished.finished.futureValue

          val causalityWrite2 =
            store.updateStateAndStore(charlieRc, charlieRequestTs, charlieState, Some(txOutId))
          val () = causalityWrite2.futureValue.finished.finished.futureValue
        }

        "track at run-time" in {
          val testedStores = mk().futureValue
          val store = testedStores.singleDomainCausalDependencyStore

          val () = store.initialize(None).futureValue
          writeState(store)
          val state = store.snapshotStateForTesting

          state shouldBe (aliceBobState ++ charlieState)
        }

        if (persistence) "initialise the store with stored state" in {
          // Write some state to the db
          val testedStores = mk().futureValue
          val store = testedStores.singleDomainCausalDependencyStore

          val () = store.initialize(None).futureValue
          writeState(store)

          // Create a new store, to be initialized with the state that already exists in the db
          val testedStores2 = mk().futureValue
          val freshStore = testedStores2.singleDomainCausalDependencyStore
          freshStore.initialize(Some(charlieRc)).futureValue

          val stateAfterRestart = freshStore.snapshotStateForTesting

          stateAfterRestart shouldBe (aliceBobState ++ charlieState)
        }

      }

      s"track the largest timestamp published per-domain" should {
        s"track at run-time" in {
          val state = mk().futureValue
          val mdcs = state.multiDomainCausalityStore

          val ts = txOutTs.plusSeconds(1)

          mdcs.highestSeenOn(writeToDomain) shouldBe None

          mdcs.registerSeen(writeToDomain, ts)

          mdcs.highestSeenOn(writeToDomain) shouldBe Some(ts)
        }

        if (persistence) "initialise the per-domain highest timestamps upon startup" in {

          def tsOfRc(requestCounter: RequestCounter): CantonTimestamp =
            txOutTs.plusSeconds(requestCounter.v)

          def timestampedEvent(
              requestCounter: RequestCounter
          ): TimestampedEvent =
            TimestampedEvent(
              PublicPackageUploadRejected(
                LedgerSubmissionId.assertFromString(requestCounter.toString),
                tsOfRc(requestCounter).toLf,
                s"rejectionReason(${this.getClass})",
              ): LedgerSyncEvent,
              requestCounter.asLocalOffset,
              None,
            )

          val publishedEventOrder = Seq(
            writeToDomain -> 1,
            writeToDomain -> 2,
            domain2 -> 4,
            domain2 -> 3,
            domain3 -> 5,
          )

          val unpublishedEvents = Seq(writeToDomain -> 6, domain4 -> 7)

          val events = (publishedEventOrder.map { case (id, rc) =>
            (id, timestampedEvent(RequestCounter(rc)), true)
          } ++ unpublishedEvents
            .map { case (id, rc) =>
              (id, timestampedEvent(RequestCounter(rc)), false)
            })
            .map { case (id, x, y) =>
              val eventLogId: EventLogId =
                EventLogId.forDomain(indexedStringStore)(id).futureValue
              (eventLogId, x, y)
            }

          val () = persistEvents(events).futureValue

          val state = mk().futureValue
          val globalStore = state.multiDomainCausalityStore

          val highestPerDomain: Map[DomainId, CantonTimestamp] =
            publishedEventOrder
              .groupBy { case (domainId, _) => domainId }
              .map { case (domainId, events) =>
                val highestEvent = events.map { case (_, rc) => rc }.maxOption.value
                val domainStr = domainId
                val ts = tsOfRc(RequestCounter(highestEvent.toLong))
                domainStr -> ts
              }

          globalStore.highestSeen.toMap shouldBe highestPerDomain
        }
      }
    }
  }
}
