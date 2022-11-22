// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.WrappedPendingRequestData
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.{
  BaseTest,
  DiscardOps,
  HasExecutionContext,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.wordspec.AnyWordSpec

class Phase37SynchronizerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private def mk(initRc: RequestCounter = RequestCounter(0)): Phase37Synchronizer =
    new Phase37Synchronizer(initRc, loggerFactory)

  private val requestId1 = RequestId(CantonTimestamp.ofEpochSecond(1))
  private val requestId2 = RequestId(CantonTimestamp.ofEpochSecond(2))

  private val requestType = TestPendingRequestDataType

  private def pendingRequestDataFor(
      i: Long
  ): WrappedPendingRequestData[TestPendingRequestData] =
    WrappedPendingRequestData(
      TestPendingRequestData(RequestCounter(i), SequencerCounter(i), Set.empty)
    )

  "return after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    p37s.markConfirmed(requestType)(RequestCounter(0), requestId1, pendingRequestData)
    p37s.awaitConfirmed(requestId1, requestType).futureValue.value shouldBe pendingRequestData
  }

  "return after reaching confirmed (for request timeout)" in {
    val p37s = mk()

    p37s.markTimeout(RequestCounter(0), requestId1)
    p37s.awaitConfirmed(requestId1, requestType).futureValue shouldBe None
  }

  "return only after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f = p37s.awaitConfirmed(requestId1, requestType)
    assert(!f.isCompleted)

    p37s.markConfirmed(requestType)(RequestCounter(0), requestId1, pendingRequestData)
    f.futureValue.value shouldBe pendingRequestData
  }

  "return only after reaching confirmed (for request timeout)" in {
    val p37s = mk()

    val f = p37s.awaitConfirmed(requestId1, requestType)
    assert(!f.isCompleted)

    p37s.markTimeout(RequestCounter(0), requestId1)
    f.futureValue shouldBe None
  }

  "return value only once after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val f1 = p37s.awaitConfirmed(requestId1, requestType)
    assert(!f1.isCompleted)

    p37s.markConfirmed(requestType)(RequestCounter(0), requestId1, pendingRequestData)
    f1.futureValue.value shouldBe pendingRequestData

    val f2 = p37s.awaitConfirmed(requestId1, requestType)
    assert(f2.isCompleted)
    f2.futureValue shouldBe None
  }

  "return even if earlier requests have not reached confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(1)

    val f1 = p37s.awaitConfirmed(requestId1, requestType)
    assert(!f1.isCompleted)
    p37s.markConfirmed(requestType)(RequestCounter(1), requestId1, pendingRequestData)
    f1.futureValue.value shouldBe pendingRequestData
  }

  "deal with several calls for the same request" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(3)

    val f1 = p37s.awaitConfirmed(requestId1, requestType)
    val f2 = p37s.awaitConfirmed(requestId1, requestType)

    // First call will get the request data when available
    assert(!f1.isCompleted)

    // Subsequent calls directly returns None
    assert(f2.isCompleted)
    f2.futureValue shouldBe None

    p37s.markConfirmed(requestType)(RequestCounter(3), requestId1, pendingRequestData)
    f1.futureValue.value shouldBe pendingRequestData

    p37s.awaitConfirmed(requestId1, requestType).futureValue shouldBe None
    p37s.markConfirmed(requestType)(RequestCounter(3), requestId1, pendingRequestData)
    p37s.awaitConfirmed(requestId1, requestType).futureValue shouldBe None
  }

  "advance head and lower bound" in {
    val p37s = mk()
    val pendingRequestData0 = pendingRequestDataFor(0)
    val pendingRequestData1 = pendingRequestDataFor(1)

    val f1 = p37s.awaitConfirmed(requestId1, requestType)
    p37s.awaitConfirmed(requestId2, requestType).discard

    p37s.markConfirmed(requestType)(RequestCounter(0), requestId1, pendingRequestData0)
    p37s.head shouldBe RequestCounter(1)
    p37s.lowerBound shouldBe requestId1.unwrap
    f1.futureValue.value shouldBe pendingRequestData0

    p37s.markConfirmed(requestType)(RequestCounter(1), requestId2, pendingRequestData1)
    p37s.head shouldBe RequestCounter(2)
    p37s.lowerBound shouldBe requestId2.unwrap
  }

  "support non-zero start counter" in {
    val rc = RequestCounter.MaxValue - 1
    val p37s = mk(rc)
    val pendingRequestData = pendingRequestDataFor(rc.v)

    p37s.markConfirmed(requestType)(rc, requestId2, pendingRequestData)
    p37s.awaitConfirmed(requestId2, requestType).futureValue.value shouldBe pendingRequestData
  }

  "complain about multiple IDs for the same request" in {
    val p37s = mk()
    p37s.markConfirmed(requestType)(RequestCounter(1), requestId1, pendingRequestDataFor(1))
    assertThrows[IllegalArgumentException](
      p37s.markConfirmed(requestType)(RequestCounter(1), requestId2, pendingRequestDataFor(1))
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

      def updateState(data: Option[WrappedPendingRequestData[TestPendingRequestData]]) =
        data match {
          case Some(pendingRequestData) =>
            p37s.markConfirmed(requestType)(RequestCounter(1), requestId1, pendingRequestData)

          case None => p37s.markTimeout(RequestCounter(1), requestId1)
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
    p37s.markConfirmed(requestType)(RequestCounter(1), requestId1, pendingRequestDataFor(1))
    p37s.skipRequestCounter(RequestCounter(2))
    p37s.head shouldBe RequestCounter(0)
    p37s.skipRequestCounter(RequestCounter(0))
    p37s.head shouldBe RequestCounter(3)
    p37s.lowerBound shouldBe requestId1.unwrap
  }

  "complain about skipping and confirming the same request" in {
    val p37s = mk()
    p37s.markConfirmed(requestType)(RequestCounter(1), requestId1, pendingRequestDataFor(1))
    a[IllegalArgumentException] shouldBe thrownBy(p37s.skipRequestCounter(RequestCounter(1)))
    p37s.skipRequestCounter(RequestCounter(2))
    a[IllegalArgumentException] shouldBe thrownBy(
      p37s.markConfirmed(requestType)(RequestCounter(2), requestId2, pendingRequestDataFor(2))
    )
  }

  "complain about using the maximum request counter" in {
    val p37s = mk()
    val rc = RequestCounter.MaxValue
    assertThrows[IllegalArgumentException](
      p37s.markConfirmed(requestType)(rc, requestId1, pendingRequestDataFor(rc.v))
    )
  }
}
