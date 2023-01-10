// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.HasFutureSupervision
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoHandshakeValidator, SyncCryptoApiProvider}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.config.ParticipantProtocolConfig
import com.digitalasset.canton.participant.domain.DomainRegistryError.HandshakeErrors.DomainIdMismatch
import com.digitalasset.canton.participant.domain.DomainRegistryHelpers.DomainHandle
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.store.{
  SyncDomainPersistentState,
  SyncDomainPersistentStateFactory,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.sequencing.client.{
  RecordingConfig,
  ReplayConfig,
  RequestSigner,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  CachingDomainTopologyClient,
  DomainTopologyClientWithInit,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreFactory, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersionCompatibility
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

trait DomainRegistryHelpers extends FlagCloseable with NamedLogging { this: HasFutureSupervision =>
  def participantId: ParticipantId
  protected def participantNodeParameters: ParticipantNodeParameters
  def aliasManager: DomainAliasManager

  implicit def ec: ExecutionContextExecutor
  override protected def executionContext: ExecutionContext = ec
  implicit def materializer: Materializer
  implicit def tracer: Tracer

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  protected def getDomainHandle(
      config: DomainConnectionConfig,
      protocolConfig: ParticipantProtocolConfig,
      sequencerConnection: SequencerConnection,
      syncDomainPersistentStateFactory: SyncDomainPersistentStateFactory,
  )(
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      cryptoApiProvider: SyncCryptoApiProvider,
      cryptoConfig: CryptoConfig,
      topologyStoreFactory: TopologyStoreFactory,
      clock: Clock,
      testingConfig: TestingConfigInternal,
      recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
      replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
      trustDomain: (
          DomainId,
          StaticDomainParameters,
          TraceContext,
      ) => FutureUnlessShutdown[Either[ParticipantTopologyManagerError, Unit]],
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      metrics: DomainAlias => SyncDomainMetrics,
      agreementClient: AgreementClient,
      sequencerConnectClient: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, DomainHandle] = {
    for {
      domainId <- getDomainId(config.domain, sequencerConnectClient).mapK(
        FutureUnlessShutdown.outcomeK
      )
      indexedDomainId <- EitherT
        .right(syncDomainPersistentStateFactory.indexedDomainId(domainId))
        .mapK(FutureUnlessShutdown.outcomeK)

      // Perform the version handshake
      success <- performHandshake(
        sequencerConnectClient,
        config.domain,
        domainId,
        protocolConfig,
      ).mapK(FutureUnlessShutdown.outcomeK)

      _ = logger.info(
        s"Version handshake with domain using protocol version ${success.serverProtocolVersion} succeeded."
      )

      _ <- aliasManager
        .processHandshake(config.domain, domainId)
        .leftMap(toDomainRegistryError)
        .mapK(FutureUnlessShutdown.outcomeK)

      staticDomainParameters <- getDomainParameters(config.domain, sequencerConnectClient).mapK(
        FutureUnlessShutdown.outcomeK
      )

      _ <- CryptoHandshakeValidator
        .validate(staticDomainParameters, cryptoConfig)
        .leftMap(DomainRegistryError.HandshakeErrors.DomainCryptoHandshakeFailed.Error(_))
        .toEitherT[FutureUnlessShutdown]

      _ <- EitherT
        .fromEither[Future](verifyDomainId(config, domainId))
        .mapK(FutureUnlessShutdown.outcomeK)
      sequencerId = SequencerId(domainId.unwrap)

      acceptedAgreement <- agreementClient
        .isRequiredAgreementAccepted(domainId, staticDomainParameters.protocolVersion)
        .leftMap(e =>
          DomainRegistryError.HandshakeErrors.ServiceAgreementAcceptanceFailed.Error(e.reason)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      targetDomainStore = topologyStoreFactory.forId(DomainStore(domainId))

      // check and issue the domain trust certificate
      _ <- EitherT(trustDomain(domainId, staticDomainParameters, traceContext)).leftMap {
        case ParticipantTopologyManagerError.IdentityManagerParentError(
              TopologyManagerError.NoAppropriateSigningKeyInStore.Failure(_)
            ) =>
          DomainRegistryError.ConfigurationErrors.CanNotIssueDomainTrustCertificate.Error()
        case err =>
          DomainRegistryError.DomainRegistryInternalError.FailedToAddParticipantDomainStateCert(err)
      }

      // fetch or create persistent state for the domain
      persistentState <- syncDomainPersistentStateFactory
        .lookupOrCreatePersistentState(
          config.domain,
          indexedDomainId,
          staticDomainParameters,
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      domainLoggerFactory = loggerFactory.append("domain", config.domain.unwrap)

      topologyClient <- EitherT.right(
        FutureUnlessShutdown.outcomeF(
          CachingDomainTopologyClient.create(
            clock,
            domainId,
            staticDomainParameters.protocolVersion,
            targetDomainStore,
            Map(),
            packageDependencies,
            participantNodeParameters.cachingConfigs,
            timeouts,
            futureSupervisor,
            domainLoggerFactory,
          )
        )
      )

      _ = cryptoApiProvider.ips.add(topologyClient)

      domainCryptoApi <- EitherT.fromEither[FutureUnlessShutdown](
        cryptoApiProvider
          .forDomain(domainId)
          .toRight(
            DomainRegistryError.DomainRegistryInternalError
              .InvalidState("crypto api for domain is unavailable"): DomainRegistryError
          )
      )

      sequencerClientFactory = {
        // apply optional domain specific overrides to the nodes general sequencer client config
        val sequencerClientConfig = participantNodeParameters.sequencerClient.copy(
          initialConnectionRetryDelay = config.initialRetryDelay
            .getOrElse(participantNodeParameters.sequencerClient.initialConnectionRetryDelay),
          maxConnectionRetryDelay = config.maxRetryDelay.getOrElse(
            participantNodeParameters.sequencerClient.maxConnectionRetryDelay
          ),
        )

        // Yields a unique path inside the given directory for record/replay purposes.
        def updateMemberRecordingPath(recordingConfig: RecordingConfig): RecordingConfig = {
          val namePrefix =
            s"${participantId.show.stripSuffix("...")}-${domainId.show.stripSuffix("...")}"
          recordingConfig.setFilename(namePrefix)
        }

        def ifParticipant[C](configO: Option[C]): Member => Option[C] = {
          case _: ParticipantId => configO
          case _ => None // unauthenticated members don't need it
        }
        SequencerClient(
          sequencerConnection,
          domainId,
          sequencerId,
          domainCryptoApi,
          cryptoApiProvider.crypto,
          acceptedAgreement.map(_.id),
          sequencerClientConfig,
          participantNodeParameters.tracing.propagation,
          testingConfig,
          staticDomainParameters,
          participantNodeParameters.processingTimeouts,
          clock,
          topologyClient,
          futureSupervisor,
          ifParticipant(recordSequencerInteractions.get().map(updateMemberRecordingPath)),
          ifParticipant(
            replaySequencerConfig
              .get()
              .map(config =>
                config.copy(recordingConfig = updateMemberRecordingPath(config.recordingConfig))
              )
          ),
          metrics(config.domain).sequencerClient,
          participantNodeParameters.loggingConfig,
          domainLoggerFactory,
          ProtocolVersionCompatibility.supportedProtocolsParticipant(includeUnstableVersions =
            protocolConfig.devVersionSupport
          ),
          protocolConfig.minimumProtocolVersion,
        )
      }

      active <- isActive(config.domain, sequencerConnectClient)

      // if the participant is being restarted and has completed topology initialization previously
      // then we can skip it
      _ <-
        if (active) EitherT.pure[FutureUnlessShutdown, DomainRegistryError](())
        else {
          logger.debug(s"Participant is not yet active on domain $domainId. Initialising topology")
          for {
            success <- ParticipantInitializeTopology(
              domainId,
              config.domain,
              participantId,
              clock,
              config.timeTracker,
              participantNodeParameters.processingTimeouts,
              authorizedStore,
              targetDomainStore,
              loggerFactory,
              sequencerClientFactory,
              cryptoApiProvider.crypto,
              staticDomainParameters.protocolVersion,
            )
            _ <- EitherT.cond[FutureUnlessShutdown](
              success,
              (),
              DomainRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
                s"Domain ${config.domain} has rejected our on-boarding attempt"
              ),
            )
            // make sure the participant is immediately active after pushing our topology,
            // or whether we have to stop here to wait for a asynchronous approval at the domain
            _ <- {
              logger.debug("Now waiting to become active")
              waitForActive(config.domain, sequencerConnectClient)
            }
          } yield ()
        }

      sequencerClient <- sequencerClientFactory
        .create(
          participantId,
          persistentState.sequencedEventStore,
          persistentState.sendTrackerStore,
          RequestSigner(domainCryptoApi),
        )
        .leftMap[DomainRegistryError](
          DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield DomainHandle(
      domainId,
      config.domain,
      staticDomainParameters,
      sequencerClient,
      topologyClient,
      targetDomainStore,
      persistentState,
      timeouts,
    )
  }

  private def getDomainId(domainAlias: DomainAlias, sequencerConnectClient: SequencerConnectClient)(
      implicit traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, DomainId] =
    sequencerConnectClient
      .getDomainId(domainAlias)
      .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))

  private def performHandshake(
      sequencerConnectClient: SequencerConnectClient,
      alias: DomainAlias,
      domainId: DomainId,
      protocolConfig: ParticipantProtocolConfig,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, HandshakeResponse.Success] =
    for {
      success <- sequencerConnectClient
        .handshake(
          alias,
          HandshakeRequest(
            ProtocolVersionCompatibility.supportedProtocolsParticipant(includeUnstableVersions =
              protocolConfig.devVersionSupport
            ),
            protocolConfig.minimumProtocolVersion,
          ),
          protocolConfig.dontWarnOnDeprecatedPV,
        )
        .leftMap(DomainRegistryHelpers.toDomainRegistryError(alias))
        .subflatMap {
          case success: HandshakeResponse.Success => success.asRight
          case HandshakeResponse.Failure(_, reason) =>
            DomainRegistryError.HandshakeErrors.HandshakeFailed.Error(reason).asLeft
        }
      _ <- aliasManager
        .processHandshake(alias, domainId)
        .leftMap(toDomainRegistryError)

    } yield success

  private def toDomainRegistryError(
      error: DomainAliasManager.Error
  )(implicit loggingContext: ErrorLoggingContext): DomainRegistryError =
    error match {
      case DomainAliasManager.GenericError(reason) =>
        DomainRegistryError.HandshakeErrors.HandshakeFailed.Error(reason)
      case DomainAliasManager.DomainAliasDuplication(domainId, alias, previousDomainId) =>
        DomainRegistryError.HandshakeErrors.DomainAliasDuplication.Error(
          domainId,
          alias,
          previousDomainId,
        )
    }

  // if participant has provided domain id previously, compare and make sure the domain being
  // connected to is the one expected
  private def verifyDomainId(config: DomainConnectionConfig, domainId: DomainId)(implicit
      loggingContext: ErrorLoggingContext
  ): Either[DomainIdMismatch.Error, Unit] =
    config.domainId match {
      case None => Right(())
      case Some(configuredDomainId) =>
        Either.cond(
          configuredDomainId == domainId,
          (),
          DomainRegistryError.HandshakeErrors.DomainIdMismatch
            .Error(expected = configuredDomainId, observed = domainId),
        )
    }

  private def getDomainParameters(
      domainAlias: DomainAlias,
      sequencerConnectClient: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, StaticDomainParameters] =
    sequencerConnectClient
      .getDomainParameters(domainAlias)
      .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))

  private def isActive(domainAlias: DomainAlias, sequencerConnectClient: SequencerConnectClient)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] =
    sequencerConnectClient
      .isActive(participantId, waitForActive = false)
      .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))
      .mapK(FutureUnlessShutdown.outcomeK)

  private def waitForActive(
      domainAlias: DomainAlias,
      sequencerConnectClient: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] =
    sequencerConnectClient
      .isActive(participantId, waitForActive = true)
      .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))
      .flatMap { isActive =>
        EitherT
          .cond[Future](
            isActive,
            (),
            DomainRegistryError.ConnectionErrors.ParticipantIsNotActive
              .Error(s"Participant $participantId is not active"),
          )
          .leftWiden[DomainRegistryError]
      }
      .mapK(FutureUnlessShutdown.outcomeK)
}

object DomainRegistryHelpers {
  private[domain] case class DomainHandle(
      domainId: DomainId,
      alias: DomainAlias,
      staticParameters: StaticDomainParameters,
      sequencer: SequencerClient,
      topologyClient: DomainTopologyClientWithInit,
      topologyStore: TopologyStore[TopologyStoreId.DomainStore],
      domainPersistentState: SyncDomainPersistentState,
      timeouts: ProcessingTimeout,
  )

  def toDomainRegistryError(alias: DomainAlias)(
      error: SequencerConnectClient.Error
  )(implicit loggingContext: ErrorLoggingContext): DomainRegistryError =
    error match {
      case SequencerConnectClient.Error.DeserializationFailure(e) =>
        DomainRegistryError.DomainRegistryInternalError.DeserializationFailure(e)
      case SequencerConnectClient.Error.InvalidResponse(cause) =>
        DomainRegistryError.DomainRegistryInternalError.InvalidResponse(cause, None)
      case SequencerConnectClient.Error.InvalidState(cause) =>
        DomainRegistryError.DomainRegistryInternalError.InvalidState(cause)
      case SequencerConnectClient.Error.Transport(message) =>
        DomainRegistryError.ConnectionErrors.DomainIsNotAvailable.Error(alias, message)
    }

}
