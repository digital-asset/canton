// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either.*
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.ConfigTransforms
import com.digitalasset.canton.participant.ParticipantNodeBootstrap
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrap
import com.digitalasset.canton.synchronizer.sequencer.SequencerNodeBootstrap
import com.digitalasset.canton.tracing.TraceContext
import monocle.macros.syntax.lens.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class CommunityEnvironmentTest extends AnyWordSpec with CommunityEnvironmentFixture {

  trait CallResult[A] {
    def get: A
  }

  "Environment" when {
    "starting with startAndReconnect" should {
      "succeed normally" in new TestEnvironment {

        val pp = mockParticipant
        Seq("p1", "p2").foreach(setupParticipantFactory(_, pp))
        Seq("s1", "s2").foreach(setupSequencerFactory(_, mockSequencer))
        Seq("m1", "m2").foreach(setupMediatorFactory(_, mockMediator))

        environment.startAndReconnect() shouldBe Either.unit
        verify(pp.getNode.valueOrFail("node should be set"), times(2))
          .reconnectSynchronizersIgnoreFailures(any[Boolean])(
            any[TraceContext],
            any[ExecutionContext],
          )

      }

      "write ports file if desired" in new TestEnvironment {

        override def config: CantonConfig = {
          val tmp = sampleConfig.focus(_.parameters.portsFile).replace(Some("my-ports.txt"))
          (ConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
            config
              .focus(_.ledgerApi)
              .replace(LedgerApiServerConfig(internalPort = Some(Port.tryCreate(42))))
          })(tmp)
        }

        val f = new java.io.File("my-ports.txt")
        f.deleteOnExit()

        val pp = mockParticipant
        when(pp.config).thenReturn(
          config.participantsByString.get("p1").valueOrFail("config should be there")
        )
        Seq("p1", "p2").foreach(setupParticipantFactory(_, pp))
        Seq("s1", "s2").foreach(setupSequencerFactory(_, mockSequencer))
        Seq("m1", "m2").foreach(setupMediatorFactory(_, mockMediator))

        clue("write ports file") {
          environment.startAndReconnect() shouldBe Either.unit
        }
        assert(f.exists())

      }

      "not start if manual start is desired" in new TestEnvironment {
        override def config: CantonConfig =
          sampleConfig.focus(_.parameters.manualStart).replace(true)

        // These would throw on start, as all methods return null.
        val mySequencer: SequencerNodeBootstrap = mock[SequencerNodeBootstrap]
        val myMediator: MediatorNodeBootstrap = mock[MediatorNodeBootstrap]
        val myParticipant: ParticipantNodeBootstrap = mock[ParticipantNodeBootstrap]

        Seq("p1", "p2").foreach(setupParticipantFactory(_, myParticipant))
        Seq("s1", "s2").foreach(setupSequencerFactory(_, mySequencer))
        Seq("m1", "m2").foreach(setupMediatorFactory(_, myMediator))

        environment.startAndReconnect() shouldBe Either.unit
      }

      "report exceptions" in new TestEnvironment {
        val exception = new RuntimeException("wurstsalat")

        Seq("p1", "p2").foreach(setupParticipantFactory(_, throw exception))
        Seq("s1", "s2").foreach(setupSequencerFactory(_, throw exception))
        Seq("m1", "m2").foreach(setupMediatorFactory(_, throw exception))

        assertThrows[RuntimeException](environment.startAndReconnect())

      }
    }
    "starting with startAll" should {
      "report exceptions" in new TestEnvironment {
        val exception = new RuntimeException("nope")

        // p1, d1 and d2 will successfully come up
        val s1: SequencerNodeBootstrap = mockSequencer
        val s2: SequencerNodeBootstrap = mockSequencer
        val m1: MediatorNodeBootstrap = mockMediator
        val m2: MediatorNodeBootstrap = mockMediator
        val p1: ParticipantNodeBootstrap = mockParticipant
        setupParticipantFactory("p1", p1)
        setupSequencerFactory("s1", s1)
        setupSequencerFactory("s2", s2)
        setupMediatorFactory("m1", m1)
        setupMediatorFactory("m2", m2)

        // p2 will fail to come up
        setupParticipantFactory("p2", throw exception)
        the[RuntimeException] thrownBy environment.startAll() shouldBe exception
        // start all will kick off stuff in the background but the "parTraverseWithLimit"
        // will terminate eagerly. so we actually have to wait until the processes finished
        // in the background
        eventually() {
          environment.sequencers.running.toSet shouldBe Set(s1, s2)
          environment.mediators.running.toSet shouldBe Set(m1, m2)
          environment.participants.running should contain.only(p1)
        }
      }
    }
  }

}
