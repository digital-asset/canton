// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.funspec.PathAnyFunSpec

trait RequestCounterAllocatorTest extends PathAnyFunSpec with BaseTest {

  def requestCounterAllocator(
      mk: (RequestCounter, SequencerCounter) => RequestCounterAllocator
  ): Unit = {

    describe("when created starting from 0") {
      val rca = mk(0L, 0L)

      describe("skip sequencer counters below the clean replay prehead") {
        forEvery(Seq(Long.MinValue, -1L)) { sc =>
          rca.allocateFor(sc) shouldBe None
        }
      }

      describe("allocation should start at 0") {
        rca.allocateFor(0L) shouldBe Some(0)
      }

      describe("allocation should be consecutive") {
        val scs =
          Table(
            ("sequencer counter", "expected request counter"),
            (0L, 0L),
            (1L, 1L),
            (4L, 2L),
            (8L, 3L),
            (10L, 4L),
          )

        forEvery(scs) { (sc, expRc) =>
          rca.allocateFor(sc) shouldBe Some(expRc)
        }

        describe("allocation is idempotent for consecutive calls") {
          rca.allocateFor(10L) shouldBe Some(4L)
        }

        describe(
          "allocation should fail if a sequence counter comes twice with an intermediate call"
        ) {
          loggerFactory.assertInternalError[IllegalStateException](
            rca.allocateFor(8L),
            _.getMessage shouldBe "Cannot allocate request counter for confirmation request with counter 8 because a lower request counter has already been allocated to 10",
          )
        }

        describe("allocation should fail if a sequence counter comes too late") {
          loggerFactory.assertInternalError[IllegalStateException](
            rca.allocateFor(9L),
            _.getMessage shouldBe "Cannot allocate request counter for confirmation request with counter 9 because a lower request counter has already been allocated to 10",
          )
        }
      }

      describe(s"allocation should succeed for ${Long.MaxValue - 1}") {
        rca.allocateFor(Long.MaxValue - 1) shouldBe Some(0L)

        it("and then fail because of an invalid sequence counter") {
          loggerFactory.assertInternalError[IllegalArgumentException](
            rca.allocateFor(Long.MaxValue),
            _.getMessage shouldBe "Sequencer counter 9223372036854775807 cannot be used.",
          )
        }
      }
    }

    describe(s"when created with ${RequestCounter.MaxValue - 1}") {
      val rca = mk(RequestCounter.MaxValue - 1, 0L)
      it("should allocate only one request counter and then fail") {
        rca.allocateFor(0L) shouldBe Some(RequestCounter.MaxValue - 1)
        loggerFactory.assertInternalError[IllegalStateException](
          rca.allocateFor(1L),
          _.getMessage shouldBe "No more request counters can be allocated because the request counters have reached 9223372036854775806.",
        )
      }
    }

    describe(s"when created with non-zero clean replay sequencer counter") {
      val cleanReplaySc = 100L
      val rca = mk(0L, cleanReplaySc)

      describe("skip allocations below") {
        forEvery(Seq(Long.MinValue, 0L, 99L)) { sc =>
          rca.allocateFor(sc) shouldBe None
        }
      }
    }
  }
}

class RequestCounterAllocatorImplTest extends RequestCounterAllocatorTest {

  describe("RequestCounterAllocatorImplTest") {
    behave like requestCounterAllocator { (initRc, cleanReplaySc) =>
      new RequestCounterAllocatorImpl(initRc, cleanReplaySc, loggerFactory)
    }

    assertThrows[IllegalArgumentException](
      new RequestCounterAllocatorImpl(RequestCounter.MaxValue, 0L, loggerFactory)
    )
  }
}
