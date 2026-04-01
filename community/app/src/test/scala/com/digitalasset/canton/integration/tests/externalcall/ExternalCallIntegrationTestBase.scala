// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  CommunityIntegrationTest,
  ConfigTransform,
  EnvironmentDefinition,
  EnvironmentSetup,
}
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}
import com.digitalasset.canton.participant.config.ExtensionServiceTokenEndpointConfig
import com.digitalasset.canton.protocol.StaticSynchronizerParameters as StaticSynchronizerParametersInternal
import com.digitalasset.canton.time.{RemoteClock, SimClock}
import com.digitalasset.canton.{SynchronizerAlias}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import com.digitalasset.canton.BaseTest

import java.nio.file.Path

/** Base trait for external call integration tests.
  *
  * Provides common infrastructure for testing external call functionality:
  * - Mock HTTP server setup and teardown
  * - Configuration transforms for enabling extension services
  * - Helper methods for creating and exercising contracts with external calls
  * - Common test fixtures (parties, DARs)
  *
  * Concrete test classes should extend this trait along with CommunityIntegrationTest,
  * SharedEnvironment, and register appropriate plugins (UseH2, UsePostgres, UseBftSequencer).
  */
trait ExternalCallIntegrationTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  /** Create an external-call test environment with a dev-protocol synchronizer.
    *
    * External-call test DARs currently require LF-dev support at submission time, so these tests
    * must bootstrap the shared synchronizer at protocol version `dev` instead of relying on the
    * default single-synchronizer fixtures.
    */
  protected def externalCallEnvironmentDefinition(
      base: EnvironmentDefinition
  ): EnvironmentDefinition =
    base
      .addConfigTransforms(ConfigTransforms.setProtocolVersion(ProtocolVersion.dev)*)
      .addConfigTransform(
        ConfigTransforms.updateSequencerConfig("sequencer1")(
          _.focus(_.parameters.alphaVersionSupport).replace(true)
        )
      )
      .addConfigTransform(
        ConfigTransforms.updateMediatorConfig("mediator1")(
          _.focus(_.parameters.alphaVersionSupport).replace(true)
        )
      )
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.extensionSettings.validateExtensionsOnStartup)
            .replace(false)
            .focus(_.parameters.engine.extensionSettings.failOnExtensionValidationError)
            .replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.start()
        sequencers.local.foreach(_.start())
        mediators.local.foreach(_.start())

        val topologyChangeDelay = environment.clock match {
          case _: RemoteClock | _: SimClock => NonNegativeFiniteDuration.Zero
          case _ => StaticSynchronizerParametersInternal.defaultTopologyChangeDelay.toConfig
        }
        val staticSynchronizerParameters =
          StaticSynchronizerParameters.defaults(
            sequencer1.config.crypto,
            ProtocolVersion.dev,
            topologyChangeDelay = topologyChangeDelay,
          )
        val synchronizerId = bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = staticSynchronizerParameters,
        )
        initializedSynchronizers.put(
          SynchronizerAlias.tryCreate(daName.unwrap),
          InitializedSynchronizer(
            synchronizerId,
            staticSynchronizerParameters.toInternal,
            synchronizerOwners = Set(sequencer1),
          ),
        )

        sequencers.local.foreach(_.health.wait_for_initialized())
        mediators.local.foreach(_.health.wait_for_initialized())
        participants.local.foreach(_.health.wait_for_initialized())
      }

  /** Port for the mock external call server - each test class should use a unique port */
  protected lazy val mockServerPort: Int = MockExternalCallServer.findFreePort()

  /** Mock server instance - initialized in test setup */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var mockServer: MockExternalCallServer = _

  /** Common party references */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var alice: PartyId = _
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var bob: PartyId = _
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var charlie: PartyId = _

  /** Path to the external call test DAR */
  protected lazy val externalCallTestDarPath: String =
    BaseTest.getResourcePath("ExternalCallTest-1.0.0.dar")

  /** Configuration transform to enable external call extension on a participant.
    *
    * @param extensionId The extension ID to configure (e.g., "test-ext")
    * @param port The port of the mock HTTP server
    * @param participantName The participant to configure (e.g., "participant1")
    */
  protected def enableExternalCallExtension(
      extensionId: String,
      port: Int,
      participantName: String = "participant1",
  ): ConfigTransform = {
    val extensionConfig = ExtensionServiceConfig(
      name = extensionId,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = false,
      auth = ExtensionServiceAuthConfig.NoAuth,
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxRetries = com.digitalasset.canton.config.RequireTypes.NonNegativeInt.tryCreate(2),
    )
    ConfigTransforms.updateParticipantConfig(participantName)(
      _.focus(_.parameters.engine.extensions).modify(_ + (extensionId -> extensionConfig))
    )
  }

  /** Configuration transform to enable extension on all participants */
  protected def enableExternalCallExtensionOnAll(
      extensionId: String,
      port: Int,
  ): ConfigTransform = {
    val extensionConfig = ExtensionServiceConfig(
      name = extensionId,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = false,
      auth = ExtensionServiceAuthConfig.NoAuth,
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxRetries = com.digitalasset.canton.config.RequireTypes.NonNegativeInt.tryCreate(2),
    )
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.parameters.engine.extensions).modify(_ + (extensionId -> extensionConfig))
    )
  }

  /** Configuration transform to enable OAuth-backed HTTPS external call extension on a participant. */
  protected def enableOAuthExternalCallExtension(
      extensionId: String,
      port: Int,
      privateKeyFile: Path,
      trustCollectionFile: Path,
      participantName: String = "participant1",
      tokenEndpointPath: String = "/oauth2/token",
  ): ConfigTransform = {
    val extensionConfig = ExtensionServiceConfig(
      name = extensionId,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = true,
      trustCollectionFile = Some(trustCollectionFile),
      auth = ExtensionServiceAuthConfig.OAuth(
        tokenEndpoint = ExtensionServiceTokenEndpointConfig(
          host = "localhost",
          port = Port.tryCreate(port),
          path = tokenEndpointPath,
          trustCollectionFile = Some(trustCollectionFile),
        ),
        clientId = participantName,
        privateKeyFile = privateKeyFile,
        scope = Some("external.call.invoke"),
      ),
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxRetries = com.digitalasset.canton.config.RequireTypes.NonNegativeInt.tryCreate(2),
    )
    ConfigTransforms.updateParticipantConfig(participantName)(
      _.focus(_.parameters.engine.extensions).modify(_ + (extensionId -> extensionConfig))
    )
  }

  /** Initialize the mock server.
    * Should be called in test setup (e.g., in withSetup block).
    */
  protected def initializeMockServer(): Unit = {
    mockServer = new MockExternalCallServer(mockServerPort, loggerFactory)
    mockServer.start()
  }

  /** Shutdown the mock server.
    * Should be called in afterAll.
    */
  protected def shutdownMockServer(): Unit = {
    if (mockServer != null) {
      mockServer.stop()
    }
  }

  /** Helper to verify mock server received expected number of token calls */
  protected def verifyTokenCallCount(expected: Int): Unit = {
    val actual = mockServer.getTokenCallCount
    withClue(s"Expected $expected token calls but got $actual") {
      actual shouldBe expected
    }
  }

  /** Reset the mock server state between tests */
  protected def resetMockServer(): Unit = {
    if (mockServer != null) {
      mockServer.reset()
    }
  }

  /** Helper to setup echo handler on the mock server */
  protected def setupEchoHandler(functionId: String = "echo"): Unit = {
    mockServer.setEchoHandler(functionId)
  }

  /** Helper to verify mock server received expected number of calls */
  protected def verifyCallCount(functionId: String, expected: Int): Unit = {
    val actual = mockServer.getCallCount(functionId)
    withClue(s"Expected $expected calls to $functionId but got $actual") {
      actual shouldBe expected
    }
  }

  /** Convert a string to hex-encoded bytes */
  protected def toHex(s: String): String = {
    s.getBytes("UTF-8").map("%02x".format(_)).mkString
  }

  /** Convert hex string to original string */
  protected def fromHex(hex: String): String = {
    hex.grouped(2).map(Integer.parseInt(_, 16).toChar).mkString
  }
}

/** Mixin for tests that need the mock server started before all tests.
  * This trait should be mixed into concrete test classes that also extend
  * SharedEnvironment or IsolatedEnvironments.
  */
trait MockServerSetup extends org.scalatest.BeforeAndAfterAll {
  self: ExternalCallIntegrationTestBase with CommunityIntegrationTest with EnvironmentSetup =>

  abstract override def afterAll(): Unit = {
    shutdownMockServer()
    super.afterAll()
  }
}
