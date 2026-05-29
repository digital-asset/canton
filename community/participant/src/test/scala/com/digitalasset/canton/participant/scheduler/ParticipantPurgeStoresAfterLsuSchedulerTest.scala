// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.{
  Active,
  LsuSource,
  LsuTarget,
  Status,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryRegisteredSynchronizersStore,
  InMemorySynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasManager,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.scheduler.IndividualSchedule
import com.digitalasset.canton.scheduler.JobSchedule.NextRun
import com.digitalasset.canton.scheduler.JobScheduler.{Done, MoreWorkToPerform, ScheduledRunResult}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.DummyCopyTopologyStore
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  KnownPhysicalSynchronizerId,
  PhysicalSynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasExecutionContext,
  SequencerAlias,
  SynchronizerAlias,
}
import org.scalatest.Assertion

import java.util.concurrent.LinkedBlockingQueue

/** This test sets up a scheduler which will delete chunks from two obsolete stores. We use fakes so
  * that we can do assertions on the last result and in between runs.
  */
final class ParticipantPurgeStoresAfterLsuSchedulerTest
    extends BaseTestWordSpec
    with HasExecutionContext {

  /** Fake schedule for which each call to `determineNextRun` will wait for the test to call `step`,
    * so that we can make assertions on the last result and the state of the system in between runs.
    */
  private class SteppingSchedule extends IndividualSchedule {
    val gotResults = new LinkedBlockingQueue[ScheduledRunResult](1)
    val nextRuns = new LinkedBlockingQueue[Option[NextRun]](1)

    override def determineNextRun(result: ScheduledRunResult)(implicit
        traceContext: TraceContext
    ): Option[NextRun] = {
      gotResults.put(result) // pass control to test thread
      nextRuns.take() // receive control back from the test thread
    }

    // Runs some assertions and allows the next run to proceed
    def step(assertions: ScheduledRunResult => Assertion): Unit = {
      val gotResult = gotResults.take()
      // At this point the last scheduler job should have run, and we can make assertions about what should have happened.
      assertions(gotResult)
      // Allow the cycle to loop
      nextRuns.put(Some(NextRun(NonNegativeFiniteDuration.Zero, this)))
    }
  }

  /** Fake topology store, defining deleteDataChunk to decrement the `size` to a minimum of 0, and
    * return whether the size ended up being decremented.
    */
  private case class SizeTopologyStore(startingSize: Int)
      extends DummyCopyTopologyStore(None, loggerFactory) {
    var size = startingSize

    override def deleteDataChunk(chunkSize: PositiveInt)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.pure {
      if (size == 0) false
      else {
        size = (size - chunkSize.value).max(0)
        true
      }
    }
  }

  "A ParticipantCleanObsoleteTopologyScheduler" should {
    "delete a chunk from each obsolete store until done" in {
      val store1 = SizeTopologyStore(startingSize = 3)
      val store2 = SizeTopologyStore(startingSize = 1)

      val schedule = new SteppingSchedule()

      val scheduler = new ParticipantPurgeStoresAfterLsuScheduler(
        schedule = Some(schedule),
        getPurgeableStores = () => Seq(store1, store2),
        chunkSize = PositiveInt.tryCreate(2),
        timeouts,
        loggerFactory,
      )

      val f = scheduler.start()

      // On startup it assumes there's work to perform. Before it proceeds, no state has changed.
      schedule.step { result =>
        result shouldBe MoreWorkToPerform
        store1.size shouldBe 3
        store2.size shouldBe 1
      }
      // The previous run deleted from both stores, so it assumes there's more work to do
      schedule.step { result =>
        result shouldBe MoreWorkToPerform
        store1.size shouldBe 1
        store2.size shouldBe 0
      }
      // The previous run deleted from store1, so assumes there's more work to do
      schedule.step { result =>
        result shouldBe MoreWorkToPerform
        store1.size shouldBe 0
        store2.size shouldBe 0
      }
      // The previous run did not delete anything (both stores were already empty), so report that we're done
      schedule.step(_ shouldBe Done)

      // Cleanup
      scheduler.stop()
      f.futureValue
    }

    "identify the obsolete topology stores by their status" in {
      val alias = SynchronizerAlias.tryCreate("da")

      val oldPsid = DefaultTestIdentities.physicalSynchronizerId
      val newPsid = oldPsid.incrementSerial

      val configStore = {
        val synchronizers = new InMemoryRegisteredSynchronizersStore(loggerFactory)
        synchronizers.addMapping(alias, oldPsid).futureValueUS
        synchronizers.addMapping(alias, newPsid).futureValueUS
        val aliasManager =
          SynchronizerAliasManager.create(synchronizers, loggerFactory).futureValueUS
        new InMemorySynchronizerConnectionConfigStore(aliasManager, loggerFactory)
      }

      val sequencerConnection = GrpcSequencerConnection(
        endpoints = NonEmpty.mk(Set, Endpoint("host", Port.tryCreate(42))),
        transportSecurity = false,
        customTrustCertificates = None,
        sequencerAlias = SequencerAlias.tryCreate("sequencer1"),
        sequencerId = None,
      )

      def setStatus(psid: PhysicalSynchronizerId, status: Status): Assertion = {
        val cfg = new SynchronizerConnectionConfig(
          alias,
          SequencerConnections.single(sequencerConnection),
        )
        // Ensure it's there.
        configStore.upsert(psid, (cfg, status, None)).futureValueUS.value.discard
        configStore
          .setStatus(alias, KnownPhysicalSynchronizerId(psid), status)
          .futureValueUS
          .value
          .discard
        succeed
      }

      val oldStore = SizeTopologyStore(startingSize = 1)
      val newStore = SizeTopologyStore(startingSize = 1)

      val schedule = new SteppingSchedule()
      val scheduler = {
        val stateManager = mock[SyncPersistentStateManager]
        val oldPersistentState = mock[SyncPersistentState]
        val newPersistentState = mock[SyncPersistentState]
        when(oldPersistentState.purgeableStores).thenReturn(Seq(oldStore))
        when(newPersistentState.purgeableStores).thenReturn(Seq(newStore))

        when(stateManager.get(oldPsid)).thenReturn(Some(oldPersistentState))
        when(stateManager.get(newPsid)).thenReturn(Some(newPersistentState))

        ParticipantPurgeStoresAfterLsuScheduler.create(
          Some(schedule),
          PositiveInt.two,
          configStore,
          stateManager,
          timeouts,
          loggerFactory,
        )
      }
      val f = scheduler.start()

      schedule.step { result =>
        result shouldBe MoreWorkToPerform // We start by assuming there's something to do
        oldStore.size shouldBe 1
        newStore.size shouldBe 1
      }

      schedule.step { result =>
        // Neither store has a status yet, nothing to be purged.
        result shouldBe Done
        oldStore.size shouldBe 1
        newStore.size shouldBe 1

        // Now old store becomes active. No status for new store yet.
        setStatus(oldPsid, Active)
      }

      schedule.step { result =>
        // (old = Active, new = None): Nothing obsolete yet, so nothing purged.
        (result, oldStore.size, newStore.size) shouldBe (Done, 1, 1)

        // Now new synchronizer is registered, and gets status LsuTarget.
        setStatus(newPsid, LsuTarget)
      }

      schedule.step { result =>
        // (old = Active, new = LsuTarget): Nothing obsolete yet, so nothing purged.
        (result, oldStore.size, newStore.size) shouldBe (Done, 1, 1)

        // Now it's upgrade time and the old synchronizer gets status LsuSource
        setStatus(oldPsid, LsuSource)
      }

      schedule.step { result =>
        // (old = LsuSource, new = LsuTarget): Nothing obsolete yet, so nothing purged.
        (result, oldStore.size, newStore.size) shouldBe (Done, 1, 1)

        // Next at upgrade time the new synchronizer gets status Active
        setStatus(newPsid, Active)
      }

      schedule.step { result =>
        // (old = LsuSource, new = Active): Now we can purge the old store. New store unaffected.
        result shouldBe MoreWorkToPerform
        oldStore.size shouldBe 0
        newStore.size shouldBe 1
      }

      schedule.step { result =>
        // (old = LsuSource, new = Active). Nothing more to purge.
        result shouldBe Done
        oldStore.size shouldBe 0
        newStore.size shouldBe 1
      }

      // Clean up
      scheduler.stop()
      f.futureValue
    }
  }
}
