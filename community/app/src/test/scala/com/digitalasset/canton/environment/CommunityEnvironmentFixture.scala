// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{CantonConfig, TestingConfigInternal}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.metrics.{ParticipantHistograms, ParticipantMetrics}
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.participant.{
  LedgerApiServerBootstrapUtils,
  ParticipantNode,
  ParticipantNodeBootstrap,
  ParticipantNodeBootstrapFactory,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.synchronizer.mediator.{
  MediatorNodeBootstrap,
  MediatorNodeBootstrapFactory,
  MediatorNodeConfig,
  MediatorNodeParameters,
}
import com.digitalasset.canton.synchronizer.metrics.{MediatorMetrics, SequencerMetrics}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfig,
  SequencerNodeParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  SequencerNodeBootstrap,
  SequencerNodeBootstrapFactory,
}
import com.digitalasset.canton.time.TestingTimeService
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, ConfigStubs, HasExecutionContext}
import com.digitalasset.daml.lf.engine.Engine
import org.apache.pekko.actor.ActorSystem
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.Suite

import java.io.ByteArrayOutputStream
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, Future}

/** Shared fixture for testing Canton Community Environments. Can be mixed into any BaseTest to
  * provide a `TestEnvironment`.
  */
trait CommunityEnvironmentFixture extends BaseTest with HasExecutionContext { this: Suite =>

  // we don't care about any values of this config, so just mock
  lazy val participant1Config: ParticipantNodeConfig = ConfigStubs.participant
  lazy val participant2Config: ParticipantNodeConfig = ConfigStubs.participant

  lazy val sampleConfig: CantonConfig = CantonConfig(
    sequencers = Map(
      InstanceName.tryCreate("s1") -> ConfigStubs.sequencer,
      InstanceName.tryCreate("s2") -> ConfigStubs.sequencer,
    ),
    mediators = Map(
      InstanceName.tryCreate("m1") -> ConfigStubs.mediator,
      InstanceName.tryCreate("m2") -> ConfigStubs.mediator,
    ),
    participants = Map(
      InstanceName.tryCreate("p1") -> participant1Config,
      InstanceName.tryCreate("p2") -> participant2Config,
    ),
  )

  /** The core environment setup class. Instantiate this inside your tests. */
  trait TestEnvironment {
    def config: CantonConfig = sampleConfig

    private val createParticipantMock =
      mock[(String, ParticipantNodeConfig) => ParticipantNodeBootstrap]
    private val createSequencerMock =
      mock[(String, SequencerNodeConfig) => SequencerNodeBootstrap]
    private val createMediatorMock =
      mock[(String, MediatorNodeConfig) => MediatorNodeBootstrap]

    def mockSequencer: SequencerNodeBootstrap = {
      val sequencer = mock[SequencerNodeBootstrap]
      when(sequencer.start()).thenReturn(EitherT.pure[Future, String](()))
      when(sequencer.name).thenReturn(InstanceName.tryCreate("mockD"))
      sequencer
    }

    def mockMediator: MediatorNodeBootstrap = {
      val mediator = mock[MediatorNodeBootstrap]
      when(mediator.start()).thenReturn(EitherT.pure[Future, String](()))
      when(mediator.name).thenReturn(InstanceName.tryCreate("mockD"))
      mediator
    }

    def mockParticipantAndNode: (ParticipantNodeBootstrap, ParticipantNode) = {
      val bootstrap = mock[ParticipantNodeBootstrap]
      val node = mock[ParticipantNode]
      val metrics = new ParticipantMetrics(
        new ParticipantHistograms(MetricName("test"))(new HistogramInventory),
        new NoOpMetricsFactory,
      )
      val closeContext = CloseContext(mock[FlagCloseable])
      when(bootstrap.name).thenReturn(InstanceName.tryCreate("mockP"))
      when(bootstrap.start()).thenReturn(EitherT.pure[Future, String](()))
      when(bootstrap.getNode).thenReturn(Some(node))
      when(
        node.reconnectSynchronizersIgnoreFailures(any[Boolean])(
          any[TraceContext],
          any[ExecutionContext],
        )
      ).thenReturn(EitherT.pure[FutureUnlessShutdown, SyncServiceError](()))
      when(bootstrap.metrics).thenReturn(metrics)
      when(bootstrap.closeContext).thenReturn(closeContext)
      when(node.config).thenReturn(participant1Config)
      (bootstrap, node)
    }

    def mockParticipant: ParticipantNodeBootstrap = mockParticipantAndNode._1

    val environment = new Environment(
      config,
      TestingConfigInternal(initializeGlobalOpenTelemetry = false),
      new ParticipantNodeBootstrapFactory {
        override protected def createLedgerApiBootstrapUtils(
            arguments: this.Arguments,
            engine: Engine,
            testingTimeService: TestingTimeService,
        )(implicit
            executionContext: ExecutionContextIdlenessExecutorService,
            actorSystem: ActorSystem,
        ): LedgerApiServerBootstrapUtils = mock[LedgerApiServerBootstrapUtils]

        override def create(
            arguments: NodeFactoryArguments[
              ParticipantNodeConfig,
              ParticipantNodeParameters,
              ParticipantMetrics,
            ],
            testingTimeService: TestingTimeService,
        )(implicit
            executionContext: ExecutionContextIdlenessExecutorService,
            scheduler: ScheduledExecutorService,
            actorSystem: ActorSystem,
            executionSequencerFactory: ExecutionSequencerFactory,
        ): Either[String, ParticipantNodeBootstrap] = Right(
          createParticipantMock(arguments.name, arguments.config)
        )
      },
      new SequencerNodeBootstrapFactory {
        override def create(
            arguments: NodeFactoryArguments[
              SequencerNodeConfig,
              SequencerNodeParameters,
              SequencerMetrics,
            ]
        )(implicit
            executionContext: ExecutionContextIdlenessExecutorService,
            scheduler: ScheduledExecutorService,
            actorSystem: ActorSystem,
        ): Either[String, SequencerNodeBootstrap] =
          Right(createSequencerMock(arguments.name, arguments.config))
      },
      new MediatorNodeBootstrapFactory {
        override def create(
            arguments: NodeFactoryArguments[
              MediatorNodeConfig,
              MediatorNodeParameters,
              MediatorMetrics,
            ]
        )(implicit
            executionContext: ExecutionContextIdlenessExecutorService,
            scheduler: ScheduledExecutorService,
            executionSequencerFactory: ExecutionSequencerFactory,
            actorSystem: ActorSystem,
        ): Either[String, MediatorNodeBootstrap] =
          Right(createMediatorMock(arguments.name, arguments.config))
      },
      loggerFactory,
    )

    protected def setupParticipantFactory(create: => ParticipantNodeBootstrap): Unit =
      setupParticipantFactoryInternal(anyString(), create)

    protected def setupParticipantFactory(id: String, create: => ParticipantNodeBootstrap): Unit =
      setupParticipantFactoryInternal(ArgumentMatchers.eq(id), create)

    private def setupParticipantFactoryInternal(
        idMatcher: => String,
        create: => ParticipantNodeBootstrap,
    ): Unit =
      when(createParticipantMock(idMatcher, any[ParticipantNodeConfig])).thenAnswer(create)

    protected def setupSequencerFactory(id: String, create: => SequencerNodeBootstrap): Unit =
      when(createSequencerMock(eqTo(id), any[SequencerNodeConfig])).thenAnswer(create)

    protected def setupMediatorFactory(id: String, create: => MediatorNodeBootstrap): Unit =
      when(createMediatorMock(eqTo(id), any[MediatorNodeConfig])).thenAnswer(create)
  }
}

object CommunityEnvironmentFixture {

  def captureStdout(block: => Unit): String = {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      block
    }
    stream.toString
  }

}
