// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.sequencer.SequencerConnectClient
import com.digitalasset.canton.common.sequencer.SequencerConnectClient.SynchronizerClientBootstrapInfo
import com.digitalasset.canton.common.sequencer.grpc.GrpcSequencerConnectClient
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  ProcessingTimeout,
  RequireTypes,
}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  SynchronizerTrustCertificate,
}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias, config}

trait SequencerConnectServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  // TODO(i16601): Remove after the shutdown rework
  override protected def destroyEnvironment(environment: TestConsoleEnvironment): Unit =
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      ConcurrentEnvironmentLimiter.destroy(getClass.getName, numPermits) {
        manualDestroyEnvironment(environment)
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        mayContain = Seq(
          // Handle DB queries during shutdown
          _.warningMessage should include("DB_STORAGE_DEGRADATION"),
          _.errorMessage should include("retryWithDelay failed unexpectedly"),
        ),
      ),
    )

  protected def sequencerPlugin: EnvironmentSetupPlugin
  registerPlugin(sequencerPlugin)

  protected def localSequencer(implicit
      env: TestConsoleEnvironment
  ): Option[LocalSequencerReference]

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  lazy val alias: SynchronizerAlias = SynchronizerAlias.tryCreate("sequencer1")

  protected def getSequencerConnectClient()(implicit
      env: TestConsoleEnvironment
  ): SequencerConnectClient

  "SequencerConnectService" should {
    "respond to handshakes" in { implicit env =>
      val grpcSequencerConnectClient = getSequencerConnectClient()

      val unsupportedPV = TestProtocolVersions.UnsupportedPV

      val includeAlphaVersions = testedProtocolVersion.isAlpha
      val includeBetaVersions = testedProtocolVersion.isBeta

      val successfulRequest =
        HandshakeRequest(
          ProtocolVersionCompatibility.supportedProtocols(
            includeAlphaVersions = includeAlphaVersions,
            includeBetaVersions = includeBetaVersions,
            release = ReleaseVersion.current,
          ),
          None,
        )
      val successfulRequestWithMinimumVersion =
        HandshakeRequest(
          ProtocolVersionCompatibility.supportedProtocols(
            includeAlphaVersions = includeAlphaVersions,
            includeBetaVersions = includeBetaVersions,
            release = ReleaseVersion.current,
          ),
          Some(testedProtocolVersion),
        )
      val failingRequest = HandshakeRequest(Seq(unsupportedPV), None)

      grpcSequencerConnectClient
        .handshake(successfulRequest, dontWarnOnDeprecatedPV = true)
        .futureValueUS
        .value shouldBe HandshakeResponse.Success(testedProtocolVersion)

      grpcSequencerConnectClient
        .handshake(successfulRequestWithMinimumVersion, dontWarnOnDeprecatedPV = true)
        .futureValueUS
        .value shouldBe HandshakeResponse.Success(testedProtocolVersion)

      inside(
        grpcSequencerConnectClient
          .handshake(failingRequest, dontWarnOnDeprecatedPV = true)
          .futureValueUS
          .value
      ) { case HandshakeResponse.Failure(serverVersion, reason) =>
        serverVersion shouldBe testedProtocolVersion
        reason should include(unsupportedPV.toString)
      }
    }

    "respond to GetSynchronizerParameters requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      val fetchedSynchronizerParameters =
        grpcSequencerConnectClient.getSynchronizerParameters().futureValueUS.value
      val cryptoProvider = CryptoProvider.Jce

      val defaultSynchronizerParametersConfig = SynchronizerParametersConfig(
        requiredSigningAlgorithmSpecs = Some(cryptoProvider.signingAlgorithms.supported),
        requiredEncryptionAlgorithmSpecs = Some(cryptoProvider.encryptionAlgorithms.supported),
        requiredSymmetricKeySchemes = Some(cryptoProvider.symmetric.supported),
        requiredHashAlgorithms = Some(cryptoProvider.hash.supported),
        requiredCryptoKeyFormats =
          Some(cryptoProvider.supportedCryptoKeyFormatsForProtocol(testedProtocolVersion)),
        requiredSignatureFormats =
          Some(cryptoProvider.supportedSignatureFormatsForProtocol(testedProtocolVersion)),
      )
      val expectedSynchronizerParameters = defaultSynchronizerParametersConfig
        .toStaticSynchronizerParameters(CryptoConfig(), testedProtocolVersion, NonNegativeInt.zero)
        .value

      fetchedSynchronizerParameters shouldBe expectedSynchronizerParameters

      sequencer1.synchronizer_parameters.static
        .get()
        .toInternal shouldBe expectedSynchronizerParameters
    }

    "respond to SynchronizerClientBootstrapInfo requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      val bi = grpcSequencerConnectClient
        .getSynchronizerClientBootstrapInfo()
        .futureValueUS
        .value
      bi shouldBe SynchronizerClientBootstrapInfo(daId, sequencer1.id)
    }

    "respond to GetSynchronizerId requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      grpcSequencerConnectClient
        .getSynchronizerId()
        .futureValueUS
        .value shouldBe daId
    }

    "respond to isActive requests" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      grpcSequencerConnectClient
        .isActive(participant1.id, waitForActive = false)
        .futureValueUS
        .value shouldBe false

      localSequencer.map { sequencer =>
        participant1.synchronizers.connect_local(sequencer, alias)

        utils.retry_until_true(Seq(participant1).forall(_.synchronizers.active(alias)))

        grpcSequencerConnectClient
          .isActive(participant1.id, waitForActive = false)
          .futureValueUS
          .value shouldBe true
      }
    }
  }
}

trait GrpcSequencerConnectServiceIntegrationTest extends SequencerConnectServiceIntegrationTest {

  private def getSequencerConnectClientInternal(endpoint: Endpoint, timeouts: ProcessingTimeout)(
      implicit env: TestConsoleEnvironment
  ): GrpcSequencerConnectClient = {
    val grpcSequencerConnection =
      GrpcSequencerConnection(
        NonEmpty(Seq, endpoint),
        transportSecurity = false,
        customTrustCertificates = None,
        SequencerAlias.Default,
        None,
      )

    new GrpcSequencerConnectClient(
      sequencerConnection = grpcSequencerConnection,
      synchronizerAlias = alias,
      timeouts = timeouts,
      traceContextPropagation = TracingConfig.Propagation.Enabled,
      loggerFactory = loggerFactory,
    )(env.executionContext)
  }

  protected def getSequencerConnectClient()(implicit
      env: TestConsoleEnvironment
  ): GrpcSequencerConnectClient = {
    val publicApi = sequencerNodeConfig.publicApi
    val endpoint = Endpoint(publicApi.address, publicApi.port)
    getSequencerConnectClientInternal(endpoint, timeouts)
  }

  protected def localSequencer(implicit
      env: TestConsoleEnvironment
  ): Option[LocalSequencerReference] =
    Some(env.sequencer1)

  protected def sequencerNodeConfig(implicit
      env: TestConsoleEnvironment
  ): SequencerNodeConfig = env.sequencer1.config

  "GrpcSequencerConnectService" should {
    "report inability to connect to sequencer on is-active requests as a left rather than an exception" in {
      implicit env =>
        import env.*

        val publicApi = sequencerNodeConfig.publicApi
        val badSequencerNodeEndpoint =
          Endpoint(publicApi.address, RequireTypes.Port.tryCreate(0))
        val grpcSequencerConnectClient = getSequencerConnectClientInternal(
          badSequencerNodeEndpoint,
          // Lower the timeout to avoid lengthy retries of 60 seconds by default
          timeouts.copy(verifyActive = config.NonNegativeDuration.ofMillis(500)),
        )

        val errorFromLeft = grpcSequencerConnectClient
          .isActive(participant1.id, waitForActive = false)
          .leftMap(_.message)
          .futureValueUS
        errorFromLeft.left.value should include regex "Request failed for .*. Is the server running?"
    }

    "reject onboarding when the synchronizer is locked" in { implicit env =>
      import env.*
      import com.digitalasset.canton.admin.api.client.data.OnboardingRestriction

      val grpcSequencerConnectClient = getSequencerConnectClient()
      val syncId = sequencer1.synchronizer_id

      // Lock the synchronizer
      sequencer1.topology.synchronizer_parameters.propose_update(
        syncId,
        _.update(onboardingRestriction =
          OnboardingRestriction.RestrictedLocked: OnboardingRestriction
        ),
      )

      //  Wait for the topology transaction to be sequenced
      sequencer1.topology.synchronisation.await_idle()

      //  Attempt registration
      val identityTxs = participant1.topology.transactions.identity_transactions()

      // Wait for the result value
      val result = grpcSequencerConnectClient
        .registerOnboardingTopologyTransactions(participant1.id, identityTxs)
        .value
        .futureValueUS

      // Check that it is a Left containing the gRPC error string
      inside(result) { case Left(error) =>
        error.toString should include("FAILED_PRECONDITION")
        error.toString should include("Synchronizer is locked for onboarding")
      }

      sequencer1.topology.synchronizer_parameters.propose_update(
        syncId,
        _.update(onboardingRestriction =
          OnboardingRestriction.UnrestrictedOpen: OnboardingRestriction
        ),
      )
    }

    "reject onboarding for mediators" in { implicit env =>
      import env.*

      val grpcSequencerConnectClient = getSequencerConnectClient()

      //  Attempt registration
      val identityTxs = mediator1.topology.transactions.identity_transactions()

      // Wait for the result value
      val result = grpcSequencerConnectClient
        .registerOnboardingTopologyTransactions(mediator1.id, identityTxs)
        .value
        .futureValueUS

      // Check that it is a Left containing the gRPC error string
      inside(result) { case Left(error) =>
        error.toString should include("FAILED_PRECONDITION")
        error.toString should include("This endpoint is only for participants")
      }
    }

    "reject onboarding for offboarded or active participants" in { implicit env =>
      import env.*
      import com.digitalasset.canton.topology.transaction.TopologyChangeOp

      val syncId = sequencer1.synchronizer_id
      val client = getSequencerConnectClient()
      val p1Id = participant1.id

      val existingStc = participant1.topology.transactions
        .list(
          store = TopologyStoreId.Authorized,
          filterMappings =
            Seq(com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.code),
        )
        .result
        .find(_.mapping.select[SynchronizerTrustCertificate].exists(_.synchronizerId == syncId))
        .map(_.transaction)
        .getOrElse(fail("Participant 1 should already have an STC"))

      participant1.topology.synchronizer_trust_certificates.propose(
        participantId = p1Id,
        synchronizerId = syncId,
        change = TopologyChangeOp.Remove,
        store = Some(TopologyStoreId.Authorized),
        synchronize = None,
      )

      sequencer1.topology.synchronisation.await_idle()

      val identityTxs = participant1.topology.transactions.identity_transactions()
      val allTxs = identityTxs :+ existingStc

      val result = client
        .registerOnboardingTopologyTransactions(p1Id, allTxs)
        .value
        .futureValueUS

      inside(result) { case Left(error) =>
        val errorStr = error.toString
        errorStr should include("FAILED_PRECONDITION")
        errorStr should include(
          s"Participant ${participant1.id} is either active on the synchronizer or has previously been offboarded"
        )
      }
    }

    "allow onboarding for authorized participants when restricted" in { implicit env =>
      import env.*
      import com.digitalasset.canton.admin.api.client.data.OnboardingRestriction

      val syncId = sequencer1.synchronizer_id
      val client = getSequencerConnectClient()

      // Set to RestrictedOpen
      sequencer1.topology.synchronizer_parameters.propose_update(
        syncId,
        _.update(onboardingRestriction =
          OnboardingRestriction.RestrictedOpen: OnboardingRestriction
        ),
      )
      // if no permission for participant2 exists, add one
      if (
        sequencer1.topology.participant_synchronizer_permissions
          .find(syncId, participant2.id)
          .isEmpty
      ) {
        sequencer1.topology.participant_synchronizer_permissions.propose(
          synchronizerId = syncId,
          participantId = participant2.id,
          permission = ParticipantPermission.Submission,
        )
      }
      sequencer1.topology.synchronisation.await_idle()

      val identityTxs = participant2.topology.transactions.identity_transactions()

      // Check if p2 already has an STC for this synchronizer. If not, add one
      val existingStc = participant2.topology.transactions
        .list(
          store = TopologyStoreId.Authorized,
          filterMappings = Seq(SynchronizerTrustCertificate.code),
          filterNamespace = participant2.namespace.filterString,
        )
        .result
        .map(_.transaction)
        .find(tx =>
          tx.mapping
            .select[SynchronizerTrustCertificate]
            .exists(_.synchronizerId == syncId)
        )
      val trustCert = existingStc.getOrElse {
        participant2.topology.synchronizer_trust_certificates.propose(
          participantId = participant2.id,
          synchronizerId = syncId,
          store = Some(TopologyStoreId.Authorized),
          synchronize = None,
        )
      }

      // Attempt registration
      val result = client
        .registerOnboardingTopologyTransactions(participant2.id, identityTxs :+ trustCert)
        .value
        .futureValueUS
      result shouldBe Right(())

      eventually() {
        val isActiveResult = client
          .isActive(participant2.id, waitForActive = false) // calls the gRPC endpoint
          .value
          .futureValueUS
        isActiveResult shouldBe Right(true)
      }
    }
  }
}

// TODO(#12363) Add a DB sequencer test (env. default) when the DB Sequencer supports group addressing

//abstract class GrpcSequencerConnectServiceTestDefault extends GrpcSequencerConnectServiceIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//}

abstract class GrpcSequencerConnectServiceIntegrationTestPostgres
    extends GrpcSequencerConnectServiceIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}

class GrpcSequencerConnectServiceIntegrationTestPostgresBft
    extends GrpcSequencerConnectServiceIntegrationTestPostgres {

  override lazy val sequencerPlugin =
    new UseBftSequencer(loggerFactory)
}
