// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{Fingerprint, Nonce, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.SilentLogPolicy
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcClient, GrpcManagedChannel}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.{
  AuthenticateRequest,
  AuthenticateResponse,
  ChallengeRequest,
  ChallengeResponse,
  LogoutRequest,
}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub
import com.digitalasset.canton.sequencer.api.v31.SequencerAuthenticationServiceGrpc.SequencerAuthenticationServiceStub as VerifierBoundAuthenticationServiceStub
import com.digitalasset.canton.sequencer.api.v31.{
  SequencerAuthenticateRequest,
  SequencerChallengeRequest,
}
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenProvider.unavailableDueToChannelShutdown
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.ErrorKind.{FatalErrorKind, TransientErrorKind}
import com.digitalasset.canton.util.retry.{
  Backoff,
  ErrorKind,
  ExceptionRetryPolicy,
  Jitter,
  Pause,
  RetryWithDelay,
}
import com.digitalasset.canton.validation.{
  ProtoUnvalidatedSeq,
  ProtoUnvalidatedString,
  ProtoValidation,
}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation, ReleaseVersion}
import com.digitalasset.nonempty.NonEmpty
import io.grpc.stub.AbstractStub
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

/** Configures authentication token fetching
  *
  * @param refreshAuthTokenBeforeExpiry
  *   how much time before the auth token expires should we fetch a new one?
  */
final case class AuthenticationTokenManagerConfig(
    refreshAuthTokenBeforeExpiry: config.NonNegativeFiniteDuration =
      AuthenticationTokenManagerConfig.defaultRefreshAuthTokenBeforeExpiry,
    retries: NonNegativeInt = AuthenticationTokenManagerConfig.defaultRetries,
    minRetryInterval: config.NonNegativeFiniteDuration =
      AuthenticationTokenManagerConfig.defaultMinRetryInterval,
    backoff: Option[AuthenticationTokenManagerExponentialBackoffConfig] = None,
)

final case class AuthenticationTokenManagerExponentialBackoffConfig(
    base: NonNegativeInt = NonNegativeInt.tryCreate(2),
    maxRetryInterval: config.NonNegativeFiniteDuration =
      AuthenticationTokenManagerExponentialBackoffConfig.defaultMaxRetryInterval,
    jitter: Option[AuthenticationTokenManagerExponentialBackoffJitterConfig] =
      AuthenticationTokenManagerExponentialBackoffConfig.defaultJitter,
)

sealed trait AuthenticationTokenManagerExponentialBackoffJitterConfig

object AuthenticationTokenManagerConfig {
  private val defaultRefreshAuthTokenBeforeExpiry: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(20)
  private val defaultRetries: NonNegativeInt = NonNegativeInt.tryCreate(20)
  private val defaultMinRetryInterval: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofMillis(500)
}

object AuthenticationTokenManagerExponentialBackoffConfig {
  private val defaultMaxRetryInterval: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofMinutes(2)
  private val defaultJitter: Option[AuthenticationTokenManagerExponentialBackoffJitterConfig] =
    None
}

object AuthenticationTokenManagerExponentialBackoffJitterConfig {
  final case object Equal extends AuthenticationTokenManagerExponentialBackoffJitterConfig
  final case object Full extends AuthenticationTokenManagerExponentialBackoffJitterConfig
}

/** Fetch tokens without taking ownership of the supplied channel.
  */
sealed trait AuthenticationTokenProvider extends AutoCloseable {

  def isClosing: Boolean

  def generateToken(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry]

  def logout(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit]
}

/** Shared lifecycle, retries, signing, token parsing, and logout for both authentication
  * mechanisms.
  */
sealed abstract class BaseAuthenticationTokenProvider(
    protected val synchronizerId: PhysicalSynchronizerId,
    protected val member: Member,
    crypto: SynchronizerCrypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    metricsO: Option[SequencerConnectionPoolMetrics],
    metricsContext: MetricsContext,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with AuthenticationTokenProvider {

  protected val challengeRequest: ChallengeRequest =
    ChallengeRequest(
      member.toProtoPrimitive,
      supportedProtocolVersions.map(_.toProtoPrimitive),
      ReleaseVersion.current.toProtoPrimitive,
    )

  /** Mechanism-specific signing context returned with a challenge: [[scala.Unit]] for member
    * authentication, or a mandatory [[MemberAuthentication.Verifier]] for P2P authentication.
    */
  protected type Binding

  /** Fetch a challenge using this provider's authentication RPC and derive its signing context. For
    * P2P authentication, the context binds the disclosed verifier identity and the endpoint
    * actually dialed by the channel. Each retry fetches a fresh challenge.
    */
  protected def getChallenge(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, (ChallengeResponse, Binding)]

  /** Adapt the mechanism-specific context to the shared signing API: member authentication returns
    * `None`, while P2P authentication always supplies the verifier binding.
    */
  protected def verifier(binding: Binding): Option[MemberAuthentication.Verifier]

  /** Redeem the signed challenge using this provider's authentication RPC and normalize the token
    * response for shared parsing. A failed P2P redemption never falls back to the member RPC.
    */
  protected def redeem(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      request: AuthenticateRequest,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, AuthenticateResponse]

  final override def generateToken(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] =
    generateTokenWithChallenge(endpoint, channel, getChallenge(endpoint, channel))

  protected final def generateTokenWithChallenge(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      challenge: => EitherT[FutureUnlessShutdown, Status, (ChallengeResponse, Binding)],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] = {
    def generateTokenET: FutureUnlessShutdown[Either[Status, AuthenticationTokenWithExpiry]] =
      synchronizeWithClosing(functionFullName) {
        (for {
          challengeAndBinding <- challenge
          (challenge, binding) = challengeAndBinding
          nonce <- Nonce
            .fromProtoPrimitive(challenge.nonce)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(s"Invalid nonce: $err"))
            .toEitherT[FutureUnlessShutdown]
          token <- authenticate(
            endpoint,
            channel,
            nonce,
            challenge.fingerprints,
            synchronizerId.protocolVersion,
            verifier(binding),
          )
        } yield token).value
      }.map {
        case Left(status) if unavailableDueToChannelShutdown(status) =>
          // Produce an exception so `exceptionRetryPolicy` can detect that the channel is shut down and stop retrying
          throw new StatusRuntimeException(status)
        case s => s
      }

    val operation = "generate sequencer authentication token"
    val maxRetriesInt = config.retries.value
    val minRetryDuration = config.minRetryInterval.underlying
    val retryWithDelay =
      config.backoff.fold[RetryWithDelay](
        Pause(
          logger,
          this,
          maxRetries = maxRetriesInt,
          delay = minRetryDuration,
          operation,
        )
      ) { backoffConfig =>
        val maxRetryDuration = backoffConfig.maxRetryInterval.underlying
        val baseInt = backoffConfig.base.unwrap
        val jitter =
          backoffConfig.jitter.fold(
            Jitter.none(maxRetryDuration, baseInt)
          ) {
            case AuthenticationTokenManagerExponentialBackoffJitterConfig.Equal =>
              Jitter.equal(maxRetryDuration, base = baseInt)
            case AuthenticationTokenManagerExponentialBackoffJitterConfig.Full =>
              Jitter.full(maxRetryDuration, base = baseInt)
          }
        Backoff(
          logger,
          this,
          maxRetries = maxRetriesInt,
          initialDelay = minRetryDuration,
          maxDelay = maxRetryDuration,
          operationName = operation,
        )(jitter)
      }

    EitherT(
      retryWithDelay.unlessShutdown(
        generateTokenET,
        AuthenticationTokenProvider.exceptionRetryPolicy,
      )
    )
  }

  private def authenticate(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      nonce: Nonce,
      fingerprintsP: ProtoUnvalidatedSeq[ProtoUnvalidatedString],
      protocolVersion: ProtocolVersion,
      verifierO: Option[MemberAuthentication.Verifier],
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] =
    for {
      fingerprintsValid <- ProtoValidation
        .validateThen(
          fingerprintsP,
          "fingerprints",
          ProtocolVersionValidation.PV(protocolVersion),
          ProtoValidation.MaxCollectionSize,
        )(Fingerprint.fromProtoPrimitive)
        .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        .toEitherT[FutureUnlessShutdown]
      fingerprintsNel <- NonEmpty
        .from(fingerprintsValid)
        .toRight(
          Status.INVALID_ARGUMENT
            .withDescription(s"Failed to deserialize fingerprints $fingerprintsP")
        )
        .toEitherT[FutureUnlessShutdown]
      signature <- MemberAuthentication
        .signSynchronizerNonce(
          member,
          nonce,
          synchronizerId,
          fingerprintsNel,
          crypto,
          verifierO,
        )
        .leftMap(err => Status.INTERNAL.withDescription(err.toString))
      _ = metricsO.foreach(
        _.connectionRequests.inc()(metricsContext.withExtraLabels("endpoint" -> "Authenticate"))
      )

      request = AuthenticateRequest(
        member = member.toProtoPrimitive,
        signature = signature.toProtoV30.some,
        nonce = nonce.toProtoPrimitive,
      )
      response <- redeem(endpoint, channel, request)
      AuthenticateResponse(tokenP, expiryOP) = response
      parsedResponseE = for {
        token <- AuthenticationToken.fromProtoPrimitive(tokenP).leftMap(_.toString)
        expiresAtP <- ProtoConverter.required("expires_at", expiryOP).leftMap(_.toString)
        expiresAt <- CantonTimestamp.fromProtoTimestamp(expiresAtP).leftMap(_.toString)
      } yield AuthenticationTokenWithExpiry(token, expiresAt)
      token <- EitherT.fromEither[FutureUnlessShutdown](
        parsedResponseE.leftMap(err =>
          Status.INTERNAL.withDescription(s"Received invalid authentication token: $err")
        )
      )
    } yield token

  protected final def recordChallengeRequest(): Unit =
    metricsO.foreach(
      _.connectionRequests.inc()(metricsContext.withExtraLabels("endpoint" -> "Challenge"))
    )

  protected final def sendAuthenticationGrpcRequest[Svc <: AbstractStub[Svc], Response](
      client: GrpcClient[Svc],
      endpoint: Endpoint,
      description: String,
  )(send: Svc => Future[Response])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Response] =
    CantonGrpcUtil
      .sendGrpcRequest(client, s"sequencer-authentication-channel-$endpoint")(
        send,
        description,
        timeouts.network.duration,
        logger,
        logPolicy = SilentLogPolicy,
        retryPolicy = _ => false,
      )
      .leftMap(_.status)

  /** Obtain a fresh token, then revoke the member's tokens through the member Logout RPC, which
    * accepts a bearer token regardless of the handshake used to obtain it.
    */
  final override def logout(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    for {
      // Generate a new token to use as "entry point" to invalidate all tokens
      tokenWithExpiry <- generateToken(endpoint, channel)
      token = tokenWithExpiry.token
      _ = metricsO.foreach(
        _.connectionRequests.inc()(metricsContext.withExtraLabels("endpoint" -> "Logout"))
      )
      _ <- CantonGrpcUtil
        .sendGrpcRequest(
          GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_)),
          s"sequencer-authentication-channel-$endpoint",
        )(
          _.logout(LogoutRequest(token.toProtoPrimitive)),
          "logout from sequencer",
          timeouts.network.duration,
          logger,
          logPolicy = SilentLogPolicy,
        )
        .leftMap(_.status)
    } yield ()
}

/** Member authentication for participants and mediators at every PV, and sequencers up to PV37.
  */
final class MemberAuthenticationTokenProvider private[authentication] (
    synchronizerId: PhysicalSynchronizerId,
    member: Member,
    crypto: SynchronizerCrypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    metricsO: Option[SequencerConnectionPoolMetrics],
    metricsContext: MetricsContext,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends BaseAuthenticationTokenProvider(
      synchronizerId,
      member,
      crypto,
      supportedProtocolVersions,
      config,
      metricsO,
      metricsContext,
      timeouts,
      loggerFactory,
    ) {
  override protected type Binding = Unit

  override protected def verifier(binding: Unit): Option[MemberAuthentication.Verifier] = None

  @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
  override protected def getChallenge(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, (ChallengeResponse, Unit)] = {
    recordChallengeRequest()
    sendAuthenticationGrpcRequest(
      GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_)),
      endpoint,
      "obtain challenge from sequencer",
    )(_.challenge(challengeRequest)).map(_ -> ())
  }

  @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
  override protected def redeem(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      request: AuthenticateRequest,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, AuthenticateResponse] =
    sendAuthenticationGrpcRequest(
      GrpcClient.create(channel, new SequencerAuthenticationServiceStub(_)),
      endpoint,
      "authenticate with sequencer",
    )(_.authenticate(request))
}

/** Mandatory verifier-bound authentication for sequencers after PV37, with no fallback.
  */
final class P2PAuthenticationTokenProvider private[authentication] (
    synchronizerId: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    crypto: SynchronizerCrypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    metricsO: Option[SequencerConnectionPoolMetrics],
    metricsContext: MetricsContext,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends BaseAuthenticationTokenProvider(
      synchronizerId,
      sequencerId,
      crypto,
      supportedProtocolVersions,
      config,
      metricsO,
      metricsContext,
      timeouts,
      loggerFactory,
    ) {
  override protected type Binding = MemberAuthentication.Verifier

  override protected def verifier(binding: Binding): Option[MemberAuthentication.Verifier] =
    Some(binding)

  /** Authenticate a reverse callback only if its verifier is the incoming token-authenticated peer.
    * The check happens before signing, preventing the peer from redirecting us to a third
    * sequencer.
    */
  def generateTokenForPeer(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      expectedVerifier: SequencerId,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] =
    generateTokenWithChallenge(
      endpoint,
      channel,
      getSequencerChallenge(endpoint, channel, Some(expectedVerifier)),
    )

  override protected def getChallenge(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, (ChallengeResponse, Binding)] =
    getSequencerChallenge(endpoint, channel, None)

  // The disclosed identity is a claim, not proof. Binding the signature to it and the dialed
  // endpoint prevents redemption at a different verifier; P2P subsequently validates the token.
  @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
  private def getSequencerChallenge(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      expectedVerifierO: Option[SequencerId],
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, (ChallengeResponse, Binding)] = {
    recordChallengeRequest()
    for {
      response <- sendAuthenticationGrpcRequest(
        GrpcClient.create(channel, new VerifierBoundAuthenticationServiceStub(_)),
        endpoint,
        "obtain verifier-bound challenge from sequencer",
      )(
        _.sequencerChallenge(
          SequencerChallengeRequest(
            challengeRequest.member,
            challengeRequest.memberProtocolVersions,
            challengeRequest.clientVersion,
          )
        )
      )
      parsed <- EitherT.fromEither[FutureUnlessShutdown](
        (for {
          verifierString <- ProtoValidation.validate(
            response.verifier,
            "verifier",
            ProtocolVersionValidation.PV(synchronizerId.protocolVersion),
          )
          verifier <- SequencerId.fromProtoPrimitive(verifierString, "verifier")
          challenge <- ProtoConverter.required("challenge", response.challenge)
        } yield (challenge, verifier)).leftMap(err =>
          Status.INVALID_ARGUMENT.withDescription(err.toString)
        )
      )
      (challenge, verifierId) = parsed
      _ <- EitherT.cond[FutureUnlessShutdown](
        expectedVerifierO.forall(_ == verifierId),
        (),
        Status.UNAUTHENTICATED.withDescription(
          "Callback verifier does not match the authenticated P2P peer"
        ),
      )
    } yield (challenge, MemberAuthentication.Verifier(verifierId, endpoint))
  }

  @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
  override protected def redeem(
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      request: AuthenticateRequest,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, Status, AuthenticateResponse] =
    sendAuthenticationGrpcRequest(
      GrpcClient.create(channel, new VerifierBoundAuthenticationServiceStub(_)),
      endpoint,
      "authenticate with verifier-bound sequencer",
    )(
      _.sequencerAuthenticate(
        SequencerAuthenticateRequest(request.member, request.signature, request.nonce)
      )
    ).map(response => AuthenticateResponse(response.token, response.expiresAt))
}

object AuthenticationTokenProvider {

  def apply(
      synchronizerId: PhysicalSynchronizerId,
      member: Member,
      crypto: SynchronizerCrypto,
      supportedProtocolVersions: Seq[ProtocolVersion],
      config: AuthenticationTokenManagerConfig,
      metricsO: Option[SequencerConnectionPoolMetrics],
      metricsContext: MetricsContext,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): AuthenticationTokenProvider =
    member match {
      case sequencerId: SequencerId
          if MemberAuthentication.requiresVerifier(sequencerId, synchronizerId.protocolVersion) =>
        new P2PAuthenticationTokenProvider(
          synchronizerId,
          sequencerId,
          crypto,
          supportedProtocolVersions,
          config,
          metricsO,
          metricsContext,
          timeouts,
          loggerFactory,
        )
      case _ =>
        new MemberAuthenticationTokenProvider(
          synchronizerId,
          member,
          crypto,
          supportedProtocolVersions,
          config,
          metricsO,
          metricsContext,
          timeouts,
          loggerFactory,
        )
    }

  private[authentication] val exceptionRetryPolicy: ExceptionRetryPolicy =
    new ExceptionRetryPolicy {
      override protected def determineExceptionErrorKind(
          exception: Throwable,
          logger: TracedLogger,
      )(implicit tc: TraceContext): ErrorKind =
        exception match {
          case ex: StatusRuntimeException if unavailableDueToChannelShutdown(ex.getStatus) =>
            FatalErrorKind

          // Ideally we would like to retry only on retryable gRPC status codes (such as `UNAVAILABLE`),
          // but as this could be hard to get right, we compromise by retrying on all gRPC status codes,
          // and use a finite number of retries.
          case _: StatusRuntimeException => TransientErrorKind()
          case _ => FatalErrorKind
        }
    }

  def unavailableDueToChannelShutdown(status: Status): Boolean =
    status.getCode == Status.Code.UNAVAILABLE &&
      (status.getDescription.contains("Channel shutdown invoked") ||
        status.getDescription.contains("Channel shutdownNow invoked"))
}
