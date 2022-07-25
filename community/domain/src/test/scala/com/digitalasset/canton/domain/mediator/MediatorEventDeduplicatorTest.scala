// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.TestVerdictSender.Result
import com.digitalasset.canton.domain.mediator.store.MediatorDeduplicationStore.DeduplicationData
import com.digitalasset.canton.domain.mediator.store.{
  InMemoryMediatorDeduplicationStore,
  MediatorDeduplicationStore,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject.MaliciousSubmitter.NonUniqueRequestUuid
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{RequestId, TransferId}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology.DefaultTestIdentities._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.Assertion

import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

class MediatorEventDeduplicatorTest extends BaseTestWordSpec with HasExecutionContext {

  val requestTime: CantonTimestamp = CantonTimestamp.Epoch
  val requestTime2: CantonTimestamp = requestTime.plusSeconds(1)
  val deduplicationTimeout: Duration = Duration.ofSeconds(10)
  val decisionTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(100)

  val maxDelayMillis: Int = 10

  def mkDeduplicator()
      : (MediatorEventDeduplicator, TestVerdictSender, MediatorDeduplicationStore) = {
    val store: MediatorDeduplicationStore = new InMemoryMediatorDeduplicationStore(loggerFactory)
    store.initialize(CantonTimestamp.MinValue).futureValue

    val verdictSender = new TestVerdictSender

    val deduplicator = new DefaultMediatorEventDeduplicator(
      store,
      verdictSender,
      _ => delayed(deduplicationTimeout),
      _ => delayed(decisionTime),
      loggerFactory,
    )
    (deduplicator, verdictSender, store)
  }

  def delayed[A](value: A): Future[A] = {
    val duration = Random.nextInt(maxDelayMillis + 1)
    DelayUtil.delay(duration.millis).map(_ => value)
  }

  lazy val uuids: Seq[UUID] = List(
    "51f3ffff-9248-453b-807b-91dd7ed23298",
    "c0175d4a-def2-481e-a979-ae9d335b5d35",
    "b9f66e2a-4867-465e-b51f-c727f2d0a18f",
  ).map(UUID.fromString)

  lazy val request: Seq[OpenEnvelope[MediatorRequest]] = uuids.map(mkMediatorRequest)

  def requests(is: Int*): Seq[OpenEnvelope[MediatorRequest]] = is.map(request)

  def deduplicationData(iAndTime: (Int, CantonTimestamp)*): Set[DeduplicationData] = iAndTime.map {
    case (i, requestTime) =>
      DeduplicationData(uuids(i), requestTime, requestTime plus deduplicationTimeout)
  }.toSet

  def deduplicationData(requestTime: CantonTimestamp, is: Int*): Set[DeduplicationData] =
    deduplicationData(is.map(_ -> requestTime): _*)

  def mkMediatorRequest(uuid: UUID): OpenEnvelope[MediatorRequest] = {
    import Pretty._

    val mediatorRequest = mock[MediatorRequest]
    when(mediatorRequest.requestUuid).thenReturn(uuid)
    when(mediatorRequest.pretty).thenReturn(
      prettyOfClass[MediatorRequest](param("uuid", _.requestUuid))
    )

    mkDefaultOpenEnvelope(mediatorRequest)
  }

  def mkDefaultOpenEnvelope[A <: ProtocolMessage](protocolMessage: A): OpenEnvelope[A] =
    OpenEnvelope(protocolMessage, Recipients.cc(mediator), testedProtocolVersion)

  lazy val response: DefaultOpenEnvelope = {
    val message =
      SignedProtocolMessage(
        mock[MediatorResponse],
        SymbolicCrypto.emptySignature,
        testedProtocolVersion,
      )
    mkDefaultOpenEnvelope(message)
  }

  lazy val causalityEnvelope: DefaultOpenEnvelope = {
    val message = CausalityMessage(
      domainId,
      testedProtocolVersion,
      TransferId(domainId, requestTime),
      VectorClock(domainId, requestTime, party1.toLf, Map.empty),
    )
    mkDefaultOpenEnvelope(message)
  }

  def assertNextSentVerdict(
      verdictSender: TestVerdictSender,
      envelope: OpenEnvelope[MediatorRequest],
      requestTime: CantonTimestamp = this.requestTime,
      expireAfter: CantonTimestamp = this.requestTime.plus(deduplicationTimeout),
  ): Assertion = {
    val request = envelope.protocolMessage

    verdictSender.sentResultsQueue.poll(0, TimeUnit.SECONDS) shouldBe Result(
      RequestId(requestTime),
      decisionTime,
      Some(request),
      Some(
        NonUniqueRequestUuid.Reject(
          s"The request uuid (${request.requestUuid}) must not be used until $expireAfter."
        )
      ),
      None,
    )
  }

  "The event deduplicator" should {
    "accept events with unique uuids" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1, 2)).futureValue
      uniqueEvents shouldBe requests(0, 1, 2)

      store.allData() shouldBe deduplicationData(requestTime, 0, 1, 2)

      storeF.futureValue
      verdictSender.sentResults shouldBe empty
    }

    "accept non-requests" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val envelopes = Seq(response, causalityEnvelope)

      val (uniqueEvents, storeF) = deduplicator.rejectDuplicates(requestTime, envelopes).futureValue
      uniqueEvents shouldBe envelopes

      store.allData() shouldBe empty

      storeF.futureValue
      verdictSender.sentResults shouldBe empty
    }

    "reject duplicates in same batch" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF) = loggerFactory.assertLogs(
        deduplicator.rejectDuplicates(requestTime, requests(0, 1, 0)).futureValue,
        entry => {
          entry.shouldBeCantonErrorCode(NonUniqueRequestUuid)
          entry.warningMessage should include(
            s"The request uuid (${uuids(0)}) must not be used until ${requestTime.plus(deduplicationTimeout)}."
          )
        },
      )
      uniqueEvents shouldBe requests(0, 1)

      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF.futureValue
      assertNextSentVerdict(verdictSender, request(0))
      verdictSender.sentResults shouldBe empty
    }

    "reject duplicates across batches" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      // populate the store
      val (uniqueEvents, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1)).futureValue
      uniqueEvents shouldBe requests(0, 1)
      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF1.futureValue
      verdictSender.sentResults shouldBe empty

      // submit same event with same requestTime
      // This should not occur in production, as the sequencer creates unique timestamps and
      // the deduplication state is cleaned up during initialization.
      val (uniqueEvents2, storeF2) = loggerFactory.assertLogs(
        deduplicator.rejectDuplicates(requestTime, requests(0)).futureValue,
        _.shouldBeCantonErrorCode(NonUniqueRequestUuid),
      )
      uniqueEvents2 shouldBe Seq.empty

      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF2.futureValue
      assertNextSentVerdict(verdictSender, request(0))
      verdictSender.sentResults shouldBe empty

      // submit same event with increased requestTime
      val (uniqueEvents3, storeF3) = loggerFactory.assertLogs(
        deduplicator.rejectDuplicates(requestTime2, requests(0)).futureValue,
        _.shouldBeCantonErrorCode(NonUniqueRequestUuid),
      )
      uniqueEvents3 shouldBe Seq.empty

      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF3.futureValue
      assertNextSentVerdict(verdictSender, request(0), requestTime2)
      verdictSender.sentResults shouldBe empty
    }

    "filter out duplicate requests" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1)).futureValue
      uniqueEvents shouldBe requests(0, 1)
      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF1.futureValue
      verdictSender.sentResults shouldBe empty

      val (uniqueEvents2, storeF2) = loggerFactory.assertLogs(
        deduplicator
          .rejectDuplicates(
            requestTime2,
            Seq(
              response,
              request(0),
              request(2),
              request(0),
              response,
              request(1),
              causalityEnvelope,
            ),
          )
          .futureValue,
        _.shouldBeCantonErrorCode(NonUniqueRequestUuid),
        _.shouldBeCantonErrorCode(NonUniqueRequestUuid),
        _.shouldBeCantonErrorCode(NonUniqueRequestUuid),
      )
      uniqueEvents2 shouldBe Seq(response, request(2), response, causalityEnvelope)
      store
        .allData() shouldBe deduplicationData(0 -> requestTime, 1 -> requestTime, 2 -> requestTime2)

      storeF2.futureValue

      assertNextSentVerdict(verdictSender, request(0), requestTime2)
      assertNextSentVerdict(verdictSender, request(0), requestTime2)
      assertNextSentVerdict(verdictSender, request(1), requestTime2)
      verdictSender.sentResults shouldBe empty
    }

    "allow for reusing uuids after expiration time" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      val (uniqueEvents, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0, 1)).futureValue
      uniqueEvents shouldBe requests(0, 1)
      store.allData() shouldBe deduplicationData(requestTime, 0, 1)

      storeF1.futureValue
      verdictSender.sentResults shouldBe empty

      val expireAfter = requestTime.plus(deduplicationTimeout).immediateSuccessor
      val (uniqueEvents2, storeF2) = deduplicator
        .rejectDuplicates(
          expireAfter,
          requests(0),
        )
        .futureValue
      uniqueEvents2 shouldBe requests(0)
      store.allData() shouldBe deduplicationData(
        0 -> requestTime,
        1 -> requestTime,
        0 -> expireAfter,
      )

      storeF2.futureValue
      verdictSender.sentResults shouldBe empty
    }

    "handle concurrent requests in the right order" in {
      val (deduplicator, verdictSender, store) = mkDeduplicator()

      forAll(request.indices) { i =>
        val (uniqueEvents1, storeF1) =
          deduplicator.rejectDuplicates(requestTime, requests(i)).futureValue
        val (uniqueEvents2, storeF2) = loggerFactory.suppressWarningsAndErrors(
          deduplicator.rejectDuplicates(requestTime2, requests(i)).futureValue
        )

        uniqueEvents1 shouldBe requests(i)
        uniqueEvents2 shouldBe empty

        store.findUuid(uuids(i), requestTime) shouldBe deduplicationData(requestTime, i)

        storeF1.futureValue
        storeF2.futureValue
        assertNextSentVerdict(verdictSender, request(i), requestTime = requestTime2)
      }
    }

    "correctly propagate completion of asynchronous actions" in {
      val deduplicator = mkHangingDeduplicator()

      val (uniqueEvents1, storeF1) =
        deduplicator.rejectDuplicates(requestTime, requests(0)).futureValue

      val (uniqueEvents2, storeF2) = loggerFactory.suppressWarningsAndErrors(
        deduplicator.rejectDuplicates(requestTime, requests(0)).futureValue
      )

      uniqueEvents1 shouldBe requests(0)
      uniqueEvents2 shouldBe empty

      always(durationOfSuccess = 1.second) {
        storeF1 should not be Symbol("completed")
        storeF2 should not be Symbol("completed")
      }
    }
  }

  def mkHangingDeduplicator(): MediatorEventDeduplicator = {
    val store = new MediatorDeduplicationStore {
      override protected def loggerFactory: NamedLoggerFactory =
        MediatorEventDeduplicatorTest.this.loggerFactory

      override protected def doInitialize(deleteFromInclusive: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): Future[Unit] = Future.unit

      override protected def persist(data: DeduplicationData)(implicit
          traceContext: TraceContext
      ): Future[Unit] = Future.never

      override protected def prunePersistentData(upToInclusive: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): Future[Unit] = Future.unit
    }
    store.initialize(CantonTimestamp.MinValue).futureValue

    val verdictSender = new VerdictSender {
      override def sendResult(
          requestId: RequestId,
          request: MediatorRequest,
          verdict: Verdict,
          decisionTime: CantonTimestamp,
      )(implicit traceContext: TraceContext): Future[Unit] =
        Future.never

      override def sendResultBatch(
          requestId: RequestId,
          batch: Batch[DefaultOpenEnvelope],
          decisionTime: CantonTimestamp,
      )(implicit traceContext: TraceContext): Future[Unit] =
        Future.never
    }

    new DefaultMediatorEventDeduplicator(
      store,
      verdictSender,
      _ => Future.successful(deduplicationTimeout),
      _ => Future.successful(decisionTime),
      loggerFactory,
    )
  }
}
