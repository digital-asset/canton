// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton._
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString
import com.digitalasset.canton.config._
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.topology.NodeId
import com.digitalasset.canton.lifecycle.ShutdownFailedException
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.CommunityDbMigrationsFactory
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.tracing.TracingConfig
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class NodesTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  val clock = new SimClock(loggerFactory = loggerFactory)
  trait TestNode extends CantonNode
  case class TestNodeConfig() extends LocalNodeConfig with ConfigDefaults[TestNodeConfig] {
    override val init: InitConfig = InitConfig()
    override val adminApi: CommunityAdminServerConfig = CommunityAdminServerConfig()
    override val storage: CommunityStorageConfig = CommunityStorageConfig.Memory()
    override val crypto: CryptoConfig = CryptoConfig()
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig()
    override val caching: CachingConfigs = CachingConfigs()
    override val nodeTypeName: String = "test-node"
    override def clientAdminApi = adminApi.clientConfig
    override def withDefaults: TestNodeConfig = this
  }
  object TestNodeParameters extends LocalNodeParameters {
    override def delayLoggingThreshold: NonNegativeFiniteDuration = ???
    override def enablePreviewFeatures: Boolean = ???
    override def enableAdditionalConsistencyChecks: Boolean = ???
    override def processingTimeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override def logMessagePayloads: Boolean = ???
    override def logQueryCost: Option[QueryCostMonitoringConfig] = ???
    override def tracing: TracingConfig = ???
    override def sequencerClient: SequencerClientConfig = ???
    override def cachingConfigs: CachingConfigs = ???
    override def nonStandardConfig: Boolean = ???
  }
  class TestNodeBootstrap extends CantonNodeBootstrap[TestNode] {
    override def name: LengthLimitedString = ???
    override def clock: Clock = ???
    override def crypto: Crypto = ???
    override def getId: Option[NodeId] = ???
    override def isInitialized: Boolean = ???
    override def start(): EitherT[Future, String, Unit] = EitherT.pure[Future, String](())
    override def initializeWithProvidedId(id: NodeId): EitherT[Future, String, Unit] = ???
    override def getNode: Option[TestNode] = ???
    override def close(): Unit = ???
    override protected def loggerFactory: NamedLoggerFactory = ???
  }

  class TestNodeFactory {
    private class CreateResult(result: => TestNodeBootstrap) {
      def get = result
    }
    private val createResult = new AtomicReference[CreateResult](
      new CreateResult(new TestNodeBootstrap)
    )
    def setupCreate(result: => TestNodeBootstrap): Unit =
      createResult.set(new CreateResult(result))

    def create(name: String, config: TestNodeConfig): TestNodeBootstrap = createResult.get.get
  }

  class TestNodes(factory: TestNodeFactory, configs: Map[String, TestNodeConfig])
      extends ManagedNodes[TestNode, TestNodeConfig, TestNodeParameters.type, TestNodeBootstrap](
        factory.create,
        new CommunityDbMigrationsFactory(loggerFactory),
        configs,
        _ => TestNodeParameters,
        NodesTest.this.loggerFactory,
      ) {
    protected val executionContext: ExecutionContextIdlenessExecutorService =
      NodesTest.this.executorService
  }

  trait Fixture {
    val configs = Map(
      "n1" -> TestNodeConfig()
    )
    val nodeFactory = new TestNodeFactory
    val nodes = new TestNodes(nodeFactory, configs)
  }

  "starting a node" should {
    "return config not found error if using a bad id" in new Fixture {
      nodes.start("nope") shouldEqual Left(ConfigurationNotFound("nope"))
    }
    "not error if the node is already running when we try to start" in new Fixture {
      nodes.start("n1").map(_ => ()) shouldBe Right(()) // first create should work
      nodes.start("n1").map(_ => ()) shouldBe Right(()) // second is now a noop
    }
    "return an initialization failure if an exception is thrown during startup" in new Fixture {
      val exception = new RuntimeException("Nope!")
      nodeFactory.setupCreate { throw exception }

      the[RuntimeException] thrownBy nodes.start("n1") shouldBe exception
    }
  }
  "stopping a node" should {
    "return config not found error if using a bad id" in new Fixture {
      nodes.stop("nope") shouldEqual Left(ConfigurationNotFound("nope"))
    }
    "return successfully if the node is not running" in new Fixture {
      nodes.stop("n1") shouldBe Right(())
    }
    "return an initialization failure if an exception is thrown during shutdown" in new Fixture {
      val anException = new RuntimeException("Nope!")
      val node = new TestNodeBootstrap {
        override def close() = {
          throw anException
        }
      }
      nodeFactory.setupCreate(node)

      nodes.start("n1") shouldBe Right(node)

      loggerFactory.assertThrowsAndLogs[ShutdownFailedException](
        nodes.stop("n1"),
        entry => {
          entry.warningMessage should fullyMatch regex "Closing .* failed! Reason:"
          entry.throwable.value shouldBe anException
        },
      )
    }
  }
}
