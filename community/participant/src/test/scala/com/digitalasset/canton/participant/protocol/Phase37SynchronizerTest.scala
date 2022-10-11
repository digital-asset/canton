// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.{BaseTest, DiscardOps, HasExecutionContext, RequestCounter}
import org.scalatest.wordspec.AnyWordSpec

class Phase37SynchronizerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  def mk(initRc: RequestCounter = RequestCounter(0)): Phase37Synchronizer =
    new Phase37Synchronizer(initRc, loggerFactory)

  val requestId1 = RequestId(CantonTimestamp.ofEpochSecond(1))
  val requestId2 = RequestId(CantonTimestamp.ofEpochSecond(2))

  "return after reaching confirmed" in {
    val p37s = mk()

    p37s.markConfirmed(RequestCounter(0), requestId1)
    p37s.awaitConfirmed(requestId1).futureValue
  }

  "return only after reaching confirmed" in {
    val p37s = mk()

    val f = p37s.awaitConfirmed(requestId1)
    assert(!f.isCompleted)
    p37s.markConfirmed(RequestCounter(0), requestId1)
    f.futureValue
  }

  "return even if earlier requests have not reached confirmed" in {
    val p37s = mk()

    val f1 = p37s.awaitConfirmed(requestId1)
    assert(!f1.isCompleted)
    p37s.markConfirmed(RequestCounter(1), requestId1)
    f1.futureValue
  }

  "deal with several calls for the same request" in {
    val p37s = mk()

    val f1 = p37s.awaitConfirmed(requestId1)
    val f2 = p37s.awaitConfirmed(requestId1)
    assert(!f1.isCompleted)
    assert(!f2.isCompleted)
    p37s.markConfirmed(RequestCounter(3), requestId1)
    f1.futureValue
    f2.futureValue
    p37s.awaitConfirmed(requestId1).futureValue
    p37s.markConfirmed(RequestCounter(3), requestId1)
    p37s.awaitConfirmed(requestId1).futureValue
  }

  "advance head and lower bound" in {
    val p37s = mk()

    val f1 = p37s.awaitConfirmed(requestId1)
    p37s.awaitConfirmed(requestId2).discard

    p37s.markConfirmed(RequestCounter(0), requestId1)
    p37s.head shouldBe RequestCounter(1)
    p37s.lowerBound shouldBe requestId1.unwrap
    f1.futureValue

    p37s.markConfirmed(RequestCounter(1), requestId2)
    p37s.head shouldBe RequestCounter(2)
    p37s.lowerBound shouldBe requestId2.unwrap
  }

  "support non-zero start counter" in {
    val p37s = mk(RequestCounter.MaxValue - 1)
    p37s.markConfirmed(RequestCounter.MaxValue - 1, requestId2)
    p37s.awaitConfirmed(requestId2).futureValue
  }

  "complain about multiple IDs for the same request" in {
    val p37s = mk()
    p37s.markConfirmed(RequestCounter(1), requestId1)
    assertThrows[IllegalArgumentException](p37s.markConfirmed(RequestCounter(1), requestId2))
  }

  "skip requests" in {
    val p37s = mk()
    p37s.markConfirmed(RequestCounter(1), requestId1)
    p37s.skipRequestCounter(RequestCounter(2))
    p37s.head shouldBe RequestCounter(0)
    p37s.skipRequestCounter(RequestCounter(0))
    p37s.head shouldBe RequestCounter(3)
    p37s.lowerBound shouldBe requestId1.unwrap
  }

  "complain about skipping and confirming the same request" in {
    val p37s = mk()
    p37s.markConfirmed(RequestCounter(1), requestId1)
    a[IllegalArgumentException] shouldBe thrownBy(p37s.skipRequestCounter(RequestCounter(1)))
    p37s.skipRequestCounter(RequestCounter(2))
    a[IllegalArgumentException] shouldBe thrownBy(p37s.markConfirmed(RequestCounter(2), requestId2))
  }

  "complain about using the maximum request counter" in {
    val p37s = mk()
    assertThrows[IllegalArgumentException](p37s.markConfirmed(RequestCounter.MaxValue, requestId1))
  }
}
