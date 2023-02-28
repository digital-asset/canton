// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.WrappedPendingRequestData
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{MediatorId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, RequestCounter, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class Phase37SynchronizerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private def mk(initRc: RequestCounter = RequestCounter(0)): Phase37Synchronizer =
    new Phase37Synchronizer(initRc, loggerFactory, FutureSupervisor.Noop)

  private val requestId1 = RequestId(CantonTimestamp.ofEpochSecond(1))
  private val requestId2 = RequestId(CantonTimestamp.ofEpochSecond(2))

  private val requestType = TestPendingRequestDataType

  private val maxDecisionTime = NonNegativeFiniteDuration.ofSeconds(60)
  private val requestId1DecisionTime = requestId1.unwrap.add(maxDecisionTime.unwrap)
  private val requestId2DecisionTime = requestId2.unwrap.add(maxDecisionTime.unwrap)

  private def pendingRequestDataFor(
      i: Long
  ): WrappedPendingRequestData[TestPendingRequestData] =
    WrappedPendingRequestData(
      TestPendingRequestData(
        RequestCounter(i),
        SequencerCounter(i),
        Set.empty,
        MediatorId(UniqueIdentifier.tryCreate("another", "mediator")),
      )
    )

  "return after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )
    p37s
      .awaitConfirmed[requestType.PendingRequestData](requestId1)
      .futureValue
      .value shouldBe pendingRequestData
  }

  "return after reaching confirmed (for request timeout)" in {
    val p37s = mk()

    p37s.markTimeout(RequestCounter(0), requestId1, requestId1DecisionTime)
    p37s
      .awaitConfirmed[requestType.PendingRequestData](requestId1)
      .futureValue shouldBe None
  }

  "return only after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    assert(!f.isCompleted)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId2DecisionTime,
      pendingRequestData,
    )
    f.futureValue.value shouldBe pendingRequestData
  }

  "return only after reaching confirmed (for request timeout)" in {
    val p37s = mk()

    val f = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    assert(!f.isCompleted)

    p37s.markTimeout(RequestCounter(0), requestId1, requestId1DecisionTime)
    f.futureValue shouldBe None
  }

  "return value only once after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f1 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    assert(!f1.isCompleted)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )

    val f2 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)

    f1.futureValue.value shouldBe pendingRequestData
    f2.futureValue shouldBe None
  }

  "return even if earlier requests have not reached confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(1)

    val f1 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    assert(!f1.isCompleted)
    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )
    f1.futureValue.value shouldBe pendingRequestData
  }

  "deal with several calls for the same request" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f1 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    val f2 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)

    assert(!f1.isCompleted)
    assert(!f2.isCompleted)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )

    val f3 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )
    val f4 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)

    f1.futureValue.value shouldBe pendingRequestData
    forAll(Seq(f2, f3, f4))(fut => fut.futureValue shouldBe None)
  }

  "advance head and lower bound" in {
    val p37s = mk()
    val pendingRequestData0 = pendingRequestDataFor(0)
    val pendingRequestData1 = pendingRequestDataFor(1)

    val f1 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId1)
    val f2 = p37s.awaitConfirmed[requestType.PendingRequestData](requestId2)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData0,
    )
    p37s.head shouldBe RequestCounter(1)
    p37s.lowerBound shouldBe requestId1.unwrap

    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId2,
      requestId2DecisionTime,
      pendingRequestData1,
    )
    p37s.head shouldBe RequestCounter(2)
    p37s.lowerBound shouldBe requestId2.unwrap

    f1.futureValue.value shouldBe pendingRequestData0
    f2.futureValue.value shouldBe pendingRequestData1
  }

  "support non-zero start counter" in {
    val rc = RequestCounter.MaxValue - 1
    val p37s = mk(rc)
    val pendingRequestData = pendingRequestDataFor(rc.v)

    p37s.markConfirmed(requestType)(rc, requestId2, requestId2DecisionTime, pendingRequestData)
    p37s
      .awaitConfirmed[requestType.PendingRequestData](requestId2)
      .futureValue
      .value shouldBe pendingRequestData
  }

  "complain about multiple IDs for the same request" in {
    val p37s = mk()
    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId1,
      requestId1DecisionTime,
      pendingRequestDataFor(1),
    )
    assertThrows[IllegalArgumentException](
      p37s.markConfirmed(requestType)(
        RequestCounter(1),
        requestId2,
        requestId2DecisionTime,
        pendingRequestDataFor(1),
      )
    )
  }

  "complain about different request data" in {
    val tests = Seq(
      // Mismatch for confirmed request
      (Some(pendingRequestDataFor(1)), Some(pendingRequestDataFor(2))),
      // Timeout and then confirmed
      (None, Some(pendingRequestDataFor(2))),
      // Confirmed and then timeout
      (Some(pendingRequestDataFor(1)), None),
    )

    forAll(tests) { case (pendingRequestData1, pendingRequestData2) =>
      val p37s = mk()

      def updateState(data: Option[WrappedPendingRequestData[TestPendingRequestData]]): Unit =
        data match {
          case Some(pendingRequestData) =>
            p37s.markConfirmed(requestType)(
              RequestCounter(1),
              requestId1,
              requestId1DecisionTime,
              pendingRequestData,
            )

          case None => p37s.markTimeout(RequestCounter(1), requestId1, requestId1DecisionTime)
        }

      updateState(pendingRequestData1)

      loggerFactory.assertThrowsAndLogs[IllegalStateException](
        updateState(pendingRequestData2),
        err => {
          val ex = err.throwable.value.getMessage

          def toString(requestDataO: Option[WrappedPendingRequestData[TestPendingRequestData]]) =
            requestDataO.fold("timeout")(_.toString)

          ex should include(toString(pendingRequestData1))
          ex should include(toString(pendingRequestData2))
        },
      )
    }
  }

  "skip requests" in {
    val p37s = mk()
    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId1,
      requestId1DecisionTime,
      pendingRequestDataFor(1),
    )
    p37s.skipRequestCounter(RequestCounter(2), requestId1)
    p37s.head shouldBe RequestCounter(0)
    p37s.skipRequestCounter(RequestCounter(0), requestId1)
    p37s.head shouldBe RequestCounter(3)
    p37s.lowerBound shouldBe requestId1.unwrap
  }

  "complain about skipping and confirming the same request" in {
    val p37s = mk()
    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId1,
      requestId1DecisionTime,
      pendingRequestDataFor(1),
    )
    a[IllegalArgumentException] shouldBe thrownBy(
      p37s.skipRequestCounter(RequestCounter(1), requestId1)
    )
    p37s.skipRequestCounter(RequestCounter(2), requestId1)
    a[IllegalArgumentException] shouldBe thrownBy(
      p37s.markConfirmed(requestType)(
        RequestCounter(2),
        requestId2,
        requestId2DecisionTime,
        pendingRequestDataFor(2),
      )
    )
  }

  "complain about using the maximum request counter" in {
    val p37s = mk()
    val rc = RequestCounter.MaxValue
    assertThrows[IllegalArgumentException](
      p37s.markConfirmed(requestType)(
        rc,
        requestId1,
        requestId1DecisionTime,
        pendingRequestDataFor(rc.v),
      )
    )
  }

  "no valid confirms" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f1 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(false),
      )
    val f2 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(false),
      )
    val f3 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(false),
      )

    assert(!f1.isCompleted)
    assert(!f2.isCompleted)
    assert(!f3.isCompleted)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )

    f1.futureValue shouldBe None
    f2.futureValue shouldBe None
    f3.futureValue shouldBe None
  }

  "deal with several calls for the same request with different filters" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f1 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(true),
      )
    val f2 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(false),
      )
    val f3 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(true),
      )

    assert(!f1.isCompleted)
    assert(!f2.isCompleted)
    assert(!f3.isCompleted)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )

    val f4 = p37s
      .awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(true),
      )
    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData,
    )
    val f5 = p37s
      .awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(false),
      )

    f1.futureValue.value shouldBe pendingRequestData
    forAll(Seq(f2, f3, f4, f5))(fut => fut.futureValue shouldBe None)
  }

  "a valid confirm even if the bound is updated in the meantime" in {
    val p37s = mk()
    val pendingRequestData0 = pendingRequestDataFor(0)
    val pendingRequestData1 = pendingRequestDataFor(1)

    val f1 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId1,
        _ => Future.successful(true),
      )
    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData0,
    )

    val f2 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId2,
        _ => Future.successful(false),
      )
    val f3 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId2,
        _ => Future.successful(false),
      )

    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId2,
      requestId2DecisionTime,
      pendingRequestData1,
    )

    val f4 =
      p37s.awaitConfirmed[requestType.PendingRequestData](
        requestId2,
        _ => Future.successful(true),
      )

    f1.futureValue.value shouldBe pendingRequestData0
    f2.futureValue shouldBe None
    f3.futureValue shouldBe None
    f4.futureValue.value shouldBe pendingRequestData1
  }

  "memory is cleaned if a request is valid and completed" in {
    val p37s = mk()
    val pendingRequestData0 = pendingRequestDataFor(0)

    p37s.markConfirmed(requestType)(
      RequestCounter(0),
      requestId1,
      requestId1DecisionTime,
      pendingRequestData0,
    )
    p37s
      .awaitConfirmed[requestType.PendingRequestData](requestId1)
      .futureValue
      .value shouldBe pendingRequestData0

    p37s.markConfirmed(requestType)(
      RequestCounter(1),
      requestId2,
      requestId2DecisionTime,
      pendingRequestData0,
    )

    p37s.memoryIsCleaned(requestId1) shouldBe true
    p37s.memoryIsCleaned(requestId2) shouldBe false
  }

}
