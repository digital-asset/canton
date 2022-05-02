// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.DomainParametersChange
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingOwnerWithKeys}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}

import scala.concurrent.Future

class TopologyTimestampPlusEpsilonTrackerTest extends BaseTestWordSpec with HasExecutionContext {

  import com.digitalasset.canton.topology.client.EffectiveTimeTestHelpers._

  private def setup(
      prepareO: Option[(CantonTimestamp, NonNegativeFiniteDuration)] = Some(
        (ts.immediatePredecessor, epsilonFD)
      )
  ): (TopologyTimestampPlusEpsilonTracker, InMemoryTopologyStore) = {
    val store = new InMemoryTopologyStore(loggerFactory)
    val tracker =
      new TopologyTimestampPlusEpsilonTracker(DefaultProcessingTimeouts.testing, loggerFactory)
    prepareO.foreach { case (ts, topologyChangeDelay) =>
      val crypto = new TestingOwnerWithKeys(
        DefaultTestIdentities.domainManager,
        loggerFactory,
        parallelExecutionContext,
      )
      val tx = crypto.mkDmGov(
        DomainParametersChange(
          DefaultTestIdentities.domainId,
          DynamicDomainParameters.initialValues(topologyChangeDelay),
        ),
        crypto.SigningKeys.key1,
      )
      store.updateState(CantonTimestamp.MinValue, Seq(), positive = Seq(tx)).futureValue
      unwrap(
        TopologyTimestampPlusEpsilonTracker.initializeFromStore(tracker, store, ts)
      ).futureValue
    }
    (tracker, store)
  }

  private def unwrap[T](fut: FutureUnlessShutdown[T]): Future[T] =
    fut.onShutdown(fail("should not receive a shutdown"))

  private lazy val ts = CantonTimestamp.Epoch
  private lazy val epsilonFD = NonNegativeFiniteDuration.ofMillis(250)
  private lazy val epsilon = epsilonFD.duration

  private def assertEffectiveTimeForUpdate(
      tracker: TopologyTimestampPlusEpsilonTracker,
      ts: CantonTimestamp,
      expected: CantonTimestamp,
  ) = {
    val eff = unwrap(tracker.adjustTimestampForUpdate(ts)).futureValue
    eff.value shouldBe expected
    tracker.effectiveTimeProcessed(eff)
  }

  private def adjustEpsilon(
      tracker: TopologyTimestampPlusEpsilonTracker,
      newEpsilon: NonNegativeFiniteDuration,
  ) = {

    val adjustedTs = unwrap(tracker.adjustTimestampForUpdate(ts)).futureValue
    tracker.adjustEpsilon(adjustedTs, ts, newEpsilon)

    // until adjustedTs, we should still get the old epsilon
    forAll(Seq(1L, 100L, 250L)) { delta =>
      val ts1 = ts.plusMillis(delta)
      assertEffectiveTimeForUpdate(tracker, ts1, ts1.plus(epsilon))
    }

  }

  "timestamp plus epsilon" should {

    "epsilon is constant properly project the timestamp" in {
      val (tracker, _) = setup()

      assertEffectiveTimeForUpdate(tracker, ts, ts.plus(epsilon))
      assertEffectiveTimeForUpdate(tracker, ts.plusSeconds(5), ts.plusSeconds(5).plus(epsilon))
      unwrap(
        tracker
          .adjustTimestampForTick(ts.plusSeconds(5))
      ).futureValue.value shouldBe ts.plusSeconds(5).plus(epsilon)

    }

    "properly project during epsilon increase" in {
      val (tracker, _) = setup()
      val newEpsilon = NonNegativeFiniteDuration.ofSeconds(1)
      adjustEpsilon(tracker, newEpsilon)
      // after adjusted ts, we should get the new epsilon
      forAll(Seq(0L, 100L, 250L)) { delta =>
        val ts1 = ts.plus(epsilon).immediateSuccessor.plusMillis(delta)
        assertEffectiveTimeForUpdate(tracker, ts1, ts1.plus(newEpsilon.duration))
      }
    }

    "properly deal with epsilon decrease" in {
      val (tracker, _) = setup()
      val newEpsilon = NonNegativeFiniteDuration.ofMillis(100)
      adjustEpsilon(tracker, newEpsilon)

      // after adjusted ts, we should get the new epsilon, avoiding the "deadzone" of ts + 2*oldEpsilon
      forAll(Seq(150L, 250L, 500L)) { case delta =>
        val ts1 = ts.plus(epsilon).immediateSuccessor.plusMillis(delta)
        assertEffectiveTimeForUpdate(tracker, ts1, ts1.plus(newEpsilon.duration))
      }
    }

    "gracefully deal with epsilon decrease on a buggy domain" in {

      val (tracker, _) = setup()
      val newEpsilon = NonNegativeFiniteDuration.ofMillis(100)
      adjustEpsilon(tracker, newEpsilon)

      // after adjusted ts, we should get a warning and an adjustment in the "deadzone" of ts + 2*oldEpsilon
      val (_, prepared) = Seq(0L, 100L, 149L).foldLeft(
        (ts.plus(epsilon).plus(epsilon).immediateSuccessor, Seq.empty[(CantonTimestamp, Long)])
      ) { case ((expected, res), delta) =>
        // so expected timestamp is at 2*epsilon
        (expected.immediateSuccessor, res :+ ((expected, delta)))
      }
      forAll(prepared) { case (expected, delta) =>
        val ts1 = ts.plus(epsilon).immediateSuccessor.plusMillis(delta)
        // tick should not advance the clock
        unwrap(
          tracker.adjustTimestampForTick(ts1)
        ).futureValue.value shouldBe expected.immediatePredecessor
        // update should advance the clock
        val res = loggerFactory.assertLogs(
          unwrap(tracker.adjustTimestampForUpdate(ts1)).futureValue,
          _.errorMessage should include("Broken or malicious domain"),
        )
        res.value shouldBe expected
        tracker.effectiveTimeProcessed(res)
      }
    }

    "block until we are in sync again" in {
      val (tracker, _) = setup()
      val eps3 = epsilon.dividedBy(3)
      val eps2 = epsilon.dividedBy(2)
      epsilon.toMillis shouldBe 250 // this test assumes this
      // first, we'll kick off the computation
      val fut1 = unwrap(tracker.adjustTimestampForUpdate(ts)).futureValue
      // then, we tick another update with a third and with half epsilon
      val fut2 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(eps3))).futureValue
      val fut3 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(eps2))).futureValue
      // if we didn't get stuck by now, we didn't get blocked. now, let's get futures that gets blocked
      val futB = unwrap(tracker.adjustTimestampForUpdate(ts.plus(epsilon).plusSeconds(1)))
      val futB1 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(epsilon).plusMillis(1001)))
      val futB2 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(epsilon).plusMillis(1200)))
      val futC = unwrap(tracker.adjustTimestampForTick(ts.plus(epsilon).plusSeconds(2)))
      Threading.sleep(500) // just to be sure that the future truly is blocked
      futB.isCompleted shouldBe false
      futB1.isCompleted shouldBe false
      futB2.isCompleted shouldBe false
      futC.isCompleted shouldBe false
      // now, release the first one, we should complete the first, but not the second
      Seq(fut2, fut3, fut1).foreach(tracker.effectiveTimeProcessed)
      val effB = futB.futureValue
      // now, clearing the effB should release B1 and B2, but not C, as C must wait until B2 is finished
      // as there might have been an epsilon change
      tracker.effectiveTimeProcessed(effB)
      // the one following within epsilon should complete as well
      val effB1 = futB1.futureValue
      val effB2 = futB2.futureValue
      // finally, release
      Seq(effB1, effB2).foreach { ts =>
        Threading.sleep(500) // just to be sure that the future is truly blocked
        // should only complete after we touched the last one
        futC.isCompleted shouldBe false
        tracker.effectiveTimeProcessed(ts)
      }
      // finally, should complete now
      futC.futureValue
    }
  }

}
