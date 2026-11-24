// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, SequencerApiClientConfig}
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.OnShutdownRunner.PureOnShutdownRunner
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{
  ClientChannelBuilder,
  ClientChannelParams,
  GrpcManagedChannel,
}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.RequestSigner
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import io.grpc.*
import org.scalatest.OptionValues.*

import java.util.concurrent.Executor
import scala.concurrent.{ExecutionContext, Future}

object SequencerTestHelper {

  def createChannel(
      clientConfig: SequencerApiClientConfig,
      loggerFactory: NamedLoggerFactory,
      executor: Executor,
  ): ManagedChannel = {
    // Create the gRPC channel for the synchronizer da sequencer/public API
    val channelBuilder = ClientChannelBuilder(loggerFactory)
    val daEndpoint = Endpoint(clientConfig.address, clientConfig.port)

    channelBuilder
      .create(
        daEndpoint,
        useTls = false,
        executor,
        trustCertificate = None,
        params = ClientChannelParams.ForTesting,
      )
      .build()
  }

  def createChannel(
      sequencer: LocalSequencerReference,
      loggerFactory: NamedLoggerFactory,
      executor: Executor,
  ): ManagedChannel =
    createChannel(sequencer.config.publicApi.clientConfig, loggerFactory, executor)

  def closeChannel(channel: ManagedChannel, logger: TracedLogger, name: String): Unit =
    if (channel != null) {
      LifeCycle.close(LifeCycle.toCloseableChannel(channel, logger, name))(logger)
    }

  /** The endpoint must be the one actually dialed by the channel: PV38+ sequencer proofs bind it.
    * Sequencers at PV38+ must use the P2P listener, which binds the v31 authentication service.
    * Participants, mediators, and sequencers in synchronizers on older protocol versions use the
    * public listener.
    */
  def requestToken(
      channel: ManagedChannel,
      endpoint: Endpoint,
      synchronizerId: PhysicalSynchronizerId,
      memberId: Member,
      crypto: SynchronizerCrypto,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationToken] = {
    val config = AuthenticationTokenManagerConfig(retries =
      NonNegativeInt.zero
    ) // Disable retries to speedup negative tests.

    val tokenProvider = AuthenticationTokenProvider(
      synchronizerId,
      memberId,
      crypto,
      // enabled dev-support as otherwise nightly dev-pv-test fails
      ProtocolVersionCompatibility.supportedProtocols(
        includeDevVersion = protocolVersion.isDev,
        includeAlphaVersions = protocolVersion.isAlpha,
        includeBetaVersions = protocolVersion.isBeta,
        release = ReleaseVersion.current,
      ),
      config,
      metricsO = None,
      metricsContext = MetricsContext.Empty,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    val logger = loggerFactory.getTracedLogger(this.getClass)
    val grpcChannel = GrpcManagedChannel(
      "sequencer-authentication-channel",
      channel,
      new PureOnShutdownRunner(logger),
      logger,
    )

    tokenProvider
      .generateToken(
        endpoint,
        grpcChannel,
      )
      .map(_.token)
  }

  def mkCallCredentials(
      synchronizerId: PhysicalSynchronizerId,
      memberId: Member,
      token: AuthenticationToken,
  ): CallCredentials = new CallCredentials {
    override def applyRequestMetadata(
        requestInfo: CallCredentials.RequestInfo,
        appExecutor: Executor,
        applier: CallCredentials.MetadataApplier,
    ): Unit = {
      val metadata = new Metadata()
      metadata.put(Constant.MEMBER_ID_METADATA_KEY, memberId.toProtoPrimitive)
      metadata.put(Constant.AUTH_TOKEN_METADATA_KEY, token)
      metadata.put(Constant.SYNCHRONIZER_ID_METADATA_KEY, synchronizerId.toProtoPrimitive)
      applier.apply(metadata)
    }

    override def thisUsesUnstableApi(): Unit = {}
  }

  def mkSubmissionRequest(sender: Member): SubmissionRequest =
    SubmissionRequest.tryCreate(
      sender,
      MessageId.randomMessageId(),
      Batch.empty(BaseTest.testedProtocolVersion),
      CantonTimestamp.MaxValue,
      aggregationRule = None,
      submissionCost = None,
      BaseTest.testedProtocolVersion,
    )

  def sendSubmissionRequest(
      sequencerServiceStub: SequencerServiceStub,
      sender: Member,
  )(implicit ec: ExecutionContext): Future[Unit] =
    sendSubmissionRequest(sequencerServiceStub, mkSubmissionRequest(sender))

  def signSubmissionRequest(
      request: SubmissionRequest,
      signingNode: LocalInstanceReference,
      psid: PhysicalSynchronizerId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      env: TestConsoleEnvironment,
  ): EitherT[FutureUnlessShutdown, String, SignedContent[SubmissionRequest]] = {
    val cryptoApi = synchronizerCryptoFor(signingNode, psid)

    for {
      syncCryptoApi <- EitherT.liftF(cryptoApi.currentSnapshotApproximation)

      signedRequest <- RequestSigner(cryptoApi, env.loggerFactoryInternal)
        .signRequest(
          request = request,
          hashPurpose = HashPurpose.SubmissionRequestSignature,
          snapshot = syncCryptoApi,
          signingTimestampOverrides = None,
        )
        .leftMap(err => s"Error signing submission request: $err")

    } yield signedRequest
  }

  def sendSubmissionRequest(
      sequencerServiceStub: SequencerServiceStub,
      submissionRequest: SubmissionRequest,
      signature: Signature = Signature.noSignature,
  )(implicit ec: ExecutionContext): Future[Unit] = {

    val signedContent = SignedContent(
      submissionRequest,
      signature,
      None,
      BaseTest.testedProtocolVersion,
    )
    val response = sequencerServiceStub
      .sendAsync(v30.SendAsyncRequest(signedContent.toByteString))

    response.map(_ => ())
  }

  private def synchronizerCryptoFor(
      instance: LocalInstanceReference,
      psid: PhysicalSynchronizerId,
  ): SynchronizerCryptoClient = instance match {
    case mediator: LocalMediatorReference =>
      mediator.underlying.value.replicaManager.mediatorRuntime.value.mediator.syncCrypto

    case participant: LocalParticipantReference =>
      participant.underlying.value.sync.connectedSynchronizerFor(psid).value.synchronizerCrypto

    case sequencer: LocalSequencerReference =>
      sequencer.underlying.value.sequencer.syncCrypto

    case other =>
      throw new IllegalStateException(s"Unsupported kind of local instance reference: $other")
  }
}
