// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.Eval
import cats.data.EitherT
import cats.implicits.*
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
import com.digitalasset.canton.participant.protocol.*
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.MultiDomainEventLog
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.{DefaultLedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  ParticipantId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class RecordOrderPublisherTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "the RecordOrderPublisher event recovery" should {

    def createSut(domain: DomainId): Fixture = {
      val eventLogId = DomainEventLogId(IndexedDomain.tryCreate(domain, 1))
      val eventLog =
        new InMemorySingleDimensionEventLog(eventLogId, loggerFactory)
      val lookup = new InMemoryMultiDomainCausalityStore(loggerFactory)
      val gco =
        new GlobalCausalOrderer(participant, _ => true, ProcessingTimeout(), lookup, loggerFactory)
      val causalStore = new InMemorySingleDomainCausalDependencyStore(domain, loggerFactory)
      val singleDomainCausalTracker =
        new SingleDomainCausalTracker(gco, causalStore, loggerFactory)

      val multiDomainEventLog = mock[MultiDomainEventLog]
      when(multiDomainEventLog.publish(any[PublicationData])).thenReturn(Future.unit)

      val ist = mock[InFlightSubmissionTracker]
      when(ist.observeTimestamp(any[DomainId], any[CantonTimestamp])(any[TraceContext])).thenReturn(
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
        SequencerCounter(0),
        CantonTimestamp.MinValue,
        eventLog,
        Eval.now(multiDomainEventLog),
        singleDomainCausalTracker,
        ist,
        new MockTaskSchedulerMetrics(),
        ProcessingTimeout(),
        true,
        loggerFactory,
        futureSupervisor,
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
      TimestampedEvent(DefaultLedgerSyncEvent.dummyStateUpdate(timestamp), 1L, None)

    "publish events to the multi-domain causality store" in {
      val fixture = createSut(domain1)
      val sut = fixture.rop
      val tse = timestampedEvent(domain1Ts2)

      val recover1 = PendingTransferPublish(
        RequestCounter(0),
        (TransferOutUpdate(
          Set(alice),
          id.requestTimestamp,
          id,
          RequestCounter(0),
          SourceProtocolVersion(testedProtocolVersion),
        )),
        id.requestTimestamp,
        eventLogId,
      )

      val recover2 = PendingEventPublish(
        Some(
          TransactionUpdate(
            Set(alice),
            tse.timestamp,
            domain1,
            RequestCounter(tse.localOffset),
            testedProtocolVersion,
          )
        ),
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
        Some(
          TransactionUpdate(
            Set(alice),
            tse.timestamp,
            domain2,
            RequestCounter(tse.localOffset),
            testedProtocolVersion,
          )
        ),
        tse,
        tse.timestamp,
        eventLog2Id,
      )

      val publishTransferIn =
        PendingTransferPublish(
          RequestCounter(1),
          TransferInUpdate(
            Set(alice),
            domain2Ts2,
            domain2,
            RequestCounter(1),
            id,
            TargetProtocolVersion(testedProtocolVersion),
          ),
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
              testedProtocolVersion,
              id,
              VectorClock(
                id.sourceDomain,
                id.requestTimestamp,
                alice,
                Map(id.sourceDomain -> id.requestTimestamp),
              ),
            )
          )
        )
        .futureValue
      gco.registerPublished(EventClock(id.sourceDomain, id.requestTimestamp, 0L)(Map.empty))

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
  final case class Fixture(
      mdel: MultiDomainEventLog,
      rop: RecordOrderPublisher,
      sdct: SingleDomainCausalTracker,
      gco: GlobalCausalOrderer,
  )
}
