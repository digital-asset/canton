// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.client.BaseTopologySnapshot.HostingView
import com.digitalasset.canton.topology.client.FreezeIgnoringHostingViewAccessorTest.NamedSnapshot
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.store.NoPackageDependencies
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  TestingIdentityFactory,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  ProtocolVersionChecksAnyWordSpec,
}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference

class FreezeIgnoringHostingViewAccessorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ProtocolVersionChecksAnyWordSpec {

  private lazy val topology = TestingTopology()
    .withReversedTopology(
      Map(
        participant1 -> Map(party1.toLf -> Submission),
        participant2 -> Map(party1.toLf -> Confirmation, party2.toLf -> Observation),
      )
    )

  private lazy val factory = topology.build(loggerFactory)

  // party3 is unknown, so that the empty party info goes through the wrappers too.
  private val parties = Seq(party1.toLf, party2.toLf, party3.toLf)

  /** Records the views the party loaders of the wrapped snapshot are called with. */
  private class CountingSnapshot(parent: TopologySnapshotLoader)
      extends ForwardingTopologySnapshot(parent.timestamp, parent, loggerFactory) {

    val loadsPerView: AtomicReference[Map[HostingView, Int]] = new AtomicReference(Map.empty)

    private def record(view: HostingView): Unit =
      loadsPerView.updateAndGet(loads => loads.updated(view, loads.getOrElse(view, 0) + 1)).discard

    override private[client] def loadActiveParticipantsOf(
        party: PartyId,
        view: HostingView,
        participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
          Map[ParticipantId, ParticipantAttributes]
        ],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] = {
      record(view)
      super.loadActiveParticipantsOf(party, view, participantStates)
    }

    override private[client] def loadBatchActiveParticipantsOf(
        parties: Seq[PartyId],
        view: HostingView,
        loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
          Map[ParticipantId, ParticipantAttributes]
        ],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PartyId, PartyInfo]] = {
      record(view)
      super.loadBatchActiveParticipantsOf(parties, view, loadParticipantStates)
    }
  }

  private def caching(parent: TopologySnapshotLoader): CachingTopologySnapshot =
    new CachingTopologySnapshot(
      parent,
      CachingConfigs.testing,
      loggerFactory,
      FutureSupervisor.Noop,
    )

  private def everyWrapper(factory: TestingIdentityFactory): Seq[NamedSnapshot] = {
    val base = factory.topologySnapshot()
    Seq(
      NamedSnapshot("store-based", base),
      NamedSnapshot(
        "forwarding",
        new ForwardingTopologySnapshot(base.timestamp, base, loggerFactory),
      ),
      NamedSnapshot("caching", caching(base)),
      NamedSnapshot(
        "validating",
        new ValidatingTopologySnapshot(
          factory.topologyStore(),
          base,
          NoPackageDependencies,
          loggerFactory,
        ),
      ),
    )
  }

  "activeParticipantsOfPartiesWithInfoIgnoringFreeze" should {
    "return the same as activeParticipantsOfPartiesWithInfo without a Freezing entry through every wrapper" in {
      forEvery(everyWrapper(factory)) { case NamedSnapshot(name, snapshot) =>
        withClue(name) {
          val ignoringFreeze =
            snapshot.activeParticipantsOfPartiesWithInfoIgnoringFreeze(parties).futureValueUS
          ignoringFreeze shouldBe snapshot
            .activeParticipantsOfPartiesWithInfo(parties)
            .futureValueUS
          ignoringFreeze(party1.toLf).participants.keySet shouldBe Set(participant1, participant2)
        }
      }
    }

    "keep the other hosts of a party with a Freezing entry through every wrapper" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      val frozenFactory = topology
        .withHostingStates(Map(party1.toLf -> Map(participant2 -> HostingState.Freezing)))
        .value
        .build(loggerFactory)

      forEvery(everyWrapper(frozenFactory)) { case NamedSnapshot(name, snapshot) =>
        withClue(name) {
          val ignoringFreeze =
            snapshot.activeParticipantsOfPartiesWithInfoIgnoringFreeze(parties).futureValueUS
          val default = snapshot.activeParticipantsOfPartiesWithInfo(parties).futureValueUS

          ignoringFreeze(party1.toLf).participants.keySet shouldBe Set(participant1)
          default(party1.toLf).participants shouldBe empty
          ignoringFreeze(party2.toLf) shouldBe default(party2.toLf)
        }
      }
    }

    "not be answered from the cache entry of the default view, nor the other way round" in {
      val counting = new CountingSnapshot(factory.topologySnapshot())
      val cachingSnapshot = caching(counting)

      cachingSnapshot.activeParticipantsOfPartiesWithInfo(parties).futureValueUS.discard
      counting.loadsPerView.get() shouldBe Map(HostingView.Default -> 1)

      cachingSnapshot
        .activeParticipantsOfPartiesWithInfoIgnoringFreeze(parties)
        .futureValueUS
        .discard
      counting.loadsPerView.get() shouldBe Map(
        HostingView.Default -> 1,
        HostingView.IgnoringFreeze -> 1,
      )

      cachingSnapshot.activeParticipantsOfPartiesWithInfo(parties).futureValueUS.discard
      cachingSnapshot
        .activeParticipantsOfPartiesWithInfoIgnoringFreeze(parties)
        .futureValueUS
        .discard
      cachingSnapshot.activeParticipantsOf(party1.toLf).futureValueUS.discard
      withClue("each view is answered from its own cache entries") {
        counting.loadsPerView.get() shouldBe Map(
          HostingView.Default -> 1,
          HostingView.IgnoringFreeze -> 1,
        )
      }
    }
  }
}

object FreezeIgnoringHostingViewAccessorTest {
  private final case class NamedSnapshot(name: String, snapshot: TopologySnapshotLoader)
}
