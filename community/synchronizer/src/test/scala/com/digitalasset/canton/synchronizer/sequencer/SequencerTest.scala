// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.parallel.*
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveDouble, PositiveInt}
import com.digitalasset.canton.crypto.{HashPurpose, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.messages.{EnvelopeContent, UnsignedProtocolMessage}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.ConnectionSerializedEvent
import com.digitalasset.canton.sequencing.client.RequestSigner
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing.BftSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerLsuConfig,
  SequencerNodeParameters,
  TimeAdvancingTopologyConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.version.{
  IgnoreInSerializationTestExhaustivenessCheck,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  MockedNodeParameters,
  config,
}
import com.digitalasset.nonempty.NonEmpty
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.duration.*

class SequencerTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  private val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val carole = ParticipantId("carole")
  private val topologyClientMember = SequencerId(synchronizerId.uid)

  // Config to turn on Pekko logging
  private lazy val pekkoConfig = {
    import scala.jdk.CollectionConverters.*
    ConfigFactory.parseMap(
      Map[String, Object](
        "pekko.loglevel" -> "DEBUG",
        "pekko.stdout-level" -> "OFF",
        "pekko.loggers" -> List("org.apache.pekko.event.slf4j.Slf4jLogger").asJava,
      ).asJava
    )
  }

  class Env extends FlagCloseableAsync {
    override val timeouts: ProcessingTimeout = SequencerTest.this.timeouts
    protected val logger: TracedLogger = SequencerTest.this.logger
    private implicit val actorSystem: ActorSystem = ActorSystem(
      classOf[SequencerTest].getSimpleName,
      Some(pekkoConfig),
      None,
      Some(parallelExecutionContext),
    )
    val store = new InMemorySequencerStore(
      protocolVersion = testedProtocolVersion,
      sequencerMember = topologyClientMember,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
      sequencerMetrics = SequencerMetrics.noop("sequencer-test"),
    )
    val clock = new WallClock(timeouts, loggerFactory = loggerFactory)
    private val testingTopology = TestingTopology(
      sequencerGroup = SequencerGroup(
        active = NonEmpty(Seq, SequencerId(synchronizerId.uid)),
        passive = Seq.empty,
        threshold = PositiveInt.one,
      ),
      participants = Seq(
        alice,
        bob,
        carole,
      ).map((_, ParticipantAttributes(ParticipantPermission.Confirmation))).toMap,
    )
      .build(loggerFactory)

    val crypto: SynchronizerCryptoClient = valueOrFail(
      testingTopology
        .forOwner(SequencerId(synchronizerId.uid))
        .forSynchronizer(synchronizerId, defaultStaticSynchronizerParameters)
        .toRight("crypto error")
    )("building crypto")
    val aliceCrypto: SynchronizerCryptoClient = valueOrFail(
      testingTopology
        .forOwner(alice)
        .forSynchronizer(synchronizerId, defaultStaticSynchronizerParameters)
        .toRight("crypto error")
    )("building alice crypto")

    val metrics: SequencerMetrics = SequencerMetrics.noop("sequencer-test")

    private val nodeParameters = SequencerNodeParameters(
      general = MockedNodeParameters.cantonNodeParameters(ProcessingTimeout()),
      protocol = CantonNodeParameters.Protocol.Impl(
        devVersionSupport = false,
        alphaVersionSupport = false,
        betaVersionSupport = true,
        dontWarnOnDeprecatedPV = false,
      ),
      maxConfirmationRequestsBurstFactor = PositiveDouble.tryCreate(1.0),
      asyncWriter = AsyncWriterParameters(),
      timeAdvancingTopology = TimeAdvancingTopologyConfig(),
      delayRequestsBeforeLsuTrafficInit = false,
      lsuConfig = SequencerLsuConfig(),
    )

    private val factory = new BftSequencerFactory(
      BftBlockOrdererConfig(),
      PublicServerConfig(),
      BlockSequencerConfig(),
      health = None,
      new MemoryStorage(loggerFactory, timeouts),
      testedProtocolVersion,
      topologyClientMember,
      nodeParameters,
      metrics,
      new ExecutorServiceMetrics(NoOpMetricsFactory),
      loggerFactory,
      None,
    )

    val sequencer: Sequencer =
      factory
        .create(
          topologyClientMember,
          clock,
          crypto,
          FutureSupervisor.Noop,
          SequencerTrafficConfig(),
          lsuSequencingBounds = None,
          drSequencingTimeUpperBound = None,
          runtimeReady = FutureUnlessShutdown.unit,
        )
        .futureValueUS

    def readAsSeq(
        member: Member,
        limit: Int,
        startingTimestamp: Option[CantonTimestamp] = None,
    ): FutureUnlessShutdown[Seq[ConnectionSerializedEvent]] =
      for {
        source <- valueOrFail(sequencer.read(member, startingTimestamp))(s"read for $member")
        events <- FutureUnlessShutdown.outcomeF(
          source
            .take(limit.toLong)
            .map {
              case Right(event) => event
              case Left(err) => fail(s"The sequencer does not emit tombstone-errors: $err")
            }
            .idleTimeout(30.seconds)
            .runWith(Sink.seq)
        )
      } yield events

    def asDeliverEvent(
        event: DecompressedSequencedEvent[ClosedEnvelope]
    ): Deliver[Batch[ClosedEnvelope]] =
      event match {
        case deliver: Deliver[Batch[ClosedEnvelope]] => deliver
        case other => fail(s"Expected deliver event but got $other")
      }

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("sequencer", sequencer.close()),
      SyncCloseable("factory", factory.close()),
      AsyncCloseable(
        "actorSystem",
        actorSystem.terminate(),
        config.NonNegativeFiniteDuration(10.seconds),
      ),
    )
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  class TestProtocolMessage()
      extends UnsignedProtocolMessage
      with IgnoreInSerializationTestExhaustivenessCheck {
    override def psid: PhysicalSynchronizerId = fail("shouldn't be used")

    override def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type] =
      fail("shouldn't be used")

    override protected val companionObj: AnyRef = TestProtocolMessage

    override def toProtoSomeEnvelopeContentV32: v32.EnvelopeContent.SomeEnvelopeContent =
      v32.EnvelopeContent.SomeEnvelopeContent.Empty

    override def productElement(n: Int): Any = fail("shouldn't be used")
    override def productArity: Int = fail("shouldn't be used")
    override def canEqual(that: Any): Boolean = fail("shouldn't be used")
  }

  object TestProtocolMessage

  "send" should {
    "correctly deliver event to each recipient" in { env =>
      import env.*

      val messageId = MessageId.tryCreate("test-message")
      val message1 = new TestProtocolMessage()
      val message2 = new TestProtocolMessage()

      val submission = SubmissionRequest.tryCreate(
        alice,
        messageId,
        Batch.closeEnvelopes(
          Batch.of(
            testedProtocolVersion,
            (message1, Recipients.cc(bob)),
            (message2, Recipients.cc(carole)),
          )
        ),
        clock.now.plusSeconds(10),
        None,
        None,
        testedProtocolVersion,
      )

      for {
        _ <- valueOrFail(
          List(alice, bob, carole, topologyClientMember).parTraverse(
            sequencer.registerMemberInternal(_, CantonTimestamp.Epoch)
          )
        )(
          "member registration"
        )
        signedSubmission <- RequestSigner(aliceCrypto, loggerFactory)
          .signRequest(
            submission,
            HashPurpose.SubmissionRequestSignature,
            aliceCrypto.currentSnapshotApproximation.futureValueUS,
            None, // not needed for unit tests; session signing keys disabled
          )
          .valueOrFail("sign request")
        _ <- sequencer.sendAsyncSigned(signedSubmission).valueOrFail("send")
        aliceDeliverEvent <- readAsSeq(alice, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
        bobDeliverEvent <- readAsSeq(bob, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
        caroleDeliverEvent <- readAsSeq(carole, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
      } yield {
        aliceDeliverEvent.messageIdO.value shouldBe messageId // as alice is the sender
        aliceDeliverEvent.batch.envelopes shouldBe empty // as we didn't send a message to ourself

        bobDeliverEvent.messageIdO shouldBe None
        bobDeliverEvent.batch.envelopes.map(
          _.toClosedUncompressedEnvelopeUnsafe.bytes
        ) should contain only
          EnvelopeContent(message1, testedProtocolVersion).toByteString

        caroleDeliverEvent.messageIdO shouldBe None
        caroleDeliverEvent.batch.envelopes.map(
          _.toClosedUncompressedEnvelopeUnsafe.bytes
        ) should contain only
          EnvelopeContent(message2, testedProtocolVersion).toByteString
      }
    }
  }
}
