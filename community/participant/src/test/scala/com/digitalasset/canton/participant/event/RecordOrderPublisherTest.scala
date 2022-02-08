// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.data.EitherT
import cats.implicits._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.TaskSchedulerTest.MockTaskSchedulerMetrics
import com.digitalasset.canton.participant.event.RecordOrderPublisher.{
  PendingEventPublish,
  PendingPublish,
  PendingTransferPublish,
}
import com.digitalasset.canton.participant.event.RecordOrderPublisherTest.Fixture
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.EventClock
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.protocol._
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.MultiDomainEventLog
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.memory._
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  DefaultDamlValues,
  DomainId,
  HasExecutionContext,
  LfPartyId,
}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class RecordOrderPublisherTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "the RecordOrderPublisher event recovery" should {

    def createSut(domain: DomainId): Fixture = {
      val eventLogId = DomainEventLogId(IndexedDomain.tryCreate(domain, 1))
      val eventLog = new InMemorySingleDimensionEventLog(eventLogId, loggerFactory)
      val lookup = new InMemoryMultiDomainCausalityStore(loggerFactory)
      val gco =
        new GlobalCausalOrderer(participant, _ => true, ProcessingTimeout(), lookup, loggerFactory)
      val causalStore = new InMemorySingleDomainCausalDependencyStore(domain, loggerFactory)
      val singleDomainCausalTracker =
        new SingleDomainCausalTracker(gco, causalStore, loggerFactory)

      val multiDomainEventLog = mock[MultiDomainEventLog]
      when(multiDomainEventLog.publish(any[PublicationData])).thenReturn(Future.unit)

      val ist = mock[InFlightSubmissionTracker]
      when(ist.observeTimestamp(any[DomainId], any[CantonTimestamp])).thenReturn(
        EitherT
          .pure[Future, InFlightSubmissionTracker.UnknownDomain](()): EitherT[
          Future,
          InFlightSubmissionTracker.UnknownDomain,
          Unit,
        ]
      )
      when(ist.recoverDomain(any[DomainId], any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(Future.unit)
      when(multiDomainEventLog.publish(any[PublicationData])).thenReturn(Future.unit)
      val sut = new RecordOrderPublisher(
        domain,
        0L,
        CantonTimestamp.MinValue,
        eventLog,
        multiDomainEventLog,
        singleDomainCausalTracker,
        ist,
        new MockTaskSchedulerMetrics(),
        ProcessingTimeout(),
        true,
        loggerFactory,
      )
      Fixture(multiDomainEventLog, sut, singleDomainCausalTracker, gco)
    }

    lazy val domain1: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain1::test"))
    lazy val domain2: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain2::test"))
    lazy val participant: ParticipantId = DefaultTestIdentities.participant1
    lazy val eventLogId: DomainEventLogId = DomainEventLogId(IndexedDomain.tryCreate(domain1, 1))
    lazy val eventLog2Id: DomainEventLogId = DomainEventLogId(IndexedDomain.tryCreate(domain2, 2))

    val alice = LfPartyId.assertFromString("alice")

    val domain1Ts1 = CantonTimestamp.MinValue
    val domain1Ts2 = CantonTimestamp.MinValue.plusSeconds(2L)
    val id = TransferId(domain1, domain1Ts1)

    def timestampedEvent(timestamp: CantonTimestamp) =
      TimestampedEvent(DefaultDamlValues.dummyStateUpdate(timestamp), 1L, None)

    "publish events to the multi-domain causality store" in {
      val fixture = createSut(domain1)
      val sut = fixture.rop
      val tse = timestampedEvent(domain1Ts2)

      val recover1 = PendingTransferPublish(
        0L,
        (TransferOutUpdate(Set(alice), id.requestTimestamp, id, 0L)),
        id.requestTimestamp,
        eventLogId,
      )

      val recover2 = PendingEventPublish(
        Some(TransactionUpdate(Set(alice), tse.timestamp, domain1, tse.localOffset)),
        tse,
        tse.timestamp,
        eventLogId,
      )

      val () = sut.scheduleRecoveries(List[PendingPublish](recover1, recover2))
      val () = sut.flush().futureValue

      // Wait for the multi-domain causality store to asynchronously register the publish of the recovered events
      eventually() {
        fixture.gco.domainCausalityStore.highestSeen.toMap shouldBe Map(domain1 -> recover2.ts)
      }
    }

    "block events missing causal dependencies" in {

      val fixture = createSut(domain2)
      val sut = fixture.rop
      val gco = fixture.gco

      val domain2Ts1 = CantonTimestamp.MinValue.plusSeconds(1L)
      val domain2Ts2 = CantonTimestamp.MinValue.plusSeconds(3L)

      val tse = timestampedEvent(domain2Ts1)
      val publishTx = PendingEventPublish(
        Some(TransactionUpdate(Set(alice), tse.timestamp, domain2, tse.localOffset)),
        tse,
        tse.timestamp,
        eventLog2Id,
      )

      val publishTransferIn =
        PendingTransferPublish(
          1L,
          TransferInUpdate(Set(alice), domain2Ts2, domain2, 1L, id),
          domain2Ts2,
          eventLog2Id,
        )

      val recover1 = publishTx
      val recover2 = publishTransferIn

      val () = sut.scheduleRecoveries(List[PendingPublish](recover1, recover2))

      // The flush should time out, as the recovery task can't be completed yet
      a[TestFailedException] shouldBe thrownBy { sut.flush().futureValue }

      eventually() {
        gco.domainCausalityStore.highestSeen shouldBe TrieMap {
          domain2 -> tse.timestamp
        }
      }

      // Unblock the transfer-in
      gco
        .registerCausalityMessages(
          List(
            CausalityMessage(
              domain2,
              id,
              VectorClock(
                id.originDomain,
                id.requestTimestamp,
                alice,
                Map(id.originDomain -> id.requestTimestamp),
              ),
            )
          )
        )
        .futureValue
      gco.registerPublished(EventClock(id.originDomain, id.requestTimestamp, 0L)(Map.empty))

      // The flush should now complete
      sut.flush().futureValue

      eventually() {
        gco.domainCausalityStore.highestSeen shouldBe TrieMap(
          domain1 -> id.requestTimestamp,
          domain2 -> publishTransferIn.ts,
        )
      }
    }
  }

}

object RecordOrderPublisherTest {
  case class Fixture(
      mdel: MultiDomainEventLog,
      rop: RecordOrderPublisher,
      sdct: SingleDomainCausalTracker,
      gco: GlobalCausalOrderer,
  )
}
