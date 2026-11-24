// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.crypto.{Nonce, Signature}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.OnShutdownRunner.PureOnShutdownRunner
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.GrpcManagedChannel
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.{
  AuthenticateRequest,
  ChallengeRequest,
}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc
import com.digitalasset.canton.sequencer.api.v31.{
  SequencerAuthenticateRequest,
  SequencerAuthenticateResponse,
  SequencerChallengeRequest,
  SequencerChallengeResponse,
  SequencerAuthenticationServiceGrpc as SequencerAuthenticationServiceGrpc31,
}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
  MemberAuthentication,
  MemberAuthenticationTokenProvider,
  P2PAuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.ChannelTokenFetcher
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerBatchingConfig,
  SequencerLimits,
}
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerAuthenticationService
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.{Member, SequencerId, TestingTopology, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

class VerifierBoundAuthenticationTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val sequencer2Id = daSequencerId
  private val sequencer2Endpoint = Endpoint("sequencer.example", Port.tryCreate(5000))
  private val sequencer2Verifier = MemberAuthentication.Verifier(sequencer2Id, sequencer2Endpoint)
  private val attackerEndpoint = Endpoint("attacker.example", Port.tryCreate(6000))

  private class Sequencer2Fixture(pv: Int) extends AutoCloseable {
    val version = ProtocolVersion.parseUnchecked(pv)
    val psid = physicalSynchronizerId.copy(protocolVersion = version)
    val topology = TestingTopology()
      .withSynchronizers(psid)
      .withSimpleParticipants(participant1)
      .build(loggerFactory)
    val crypto = topology.forOwnerAndSynchronizer(sequencerId, psid)
    val sequencer2TokenStore =
      new MemberAuthenticationStore(PositiveInt.tryCreate(10), loggerFactory)
    val sequencer2MemberAuthSvc = new MemberAuthenticationService(
      psid,
      crypto,
      sequencer2TokenStore,
      new SimClock(loggerFactory = loggerFactory),
      Duration.ofMinutes(1),
      Duration.ofHours(1),
      useExponentialRandomTokenExpiration = false,
      _ => (),
      FutureUnlessShutdown.unit,
      DefaultProcessingTimeouts.testing,
      SequencerBatchingConfig(),
      loggerFactory,
    )
    val sequencer2AuthGrpcSvc = new GrpcSequencerAuthenticationService(
      sequencer2MemberAuthSvc,
      version,
      disableReleaseVersionHandshakeCheck = true,
      SequencerLimits(),
      loggerFactory,
    )
    val sequencer2AuthSvc = sequencer2AuthGrpcSvc.forVerifier(sequencer2Verifier)

    def tokenProvider(
        authenticateAs: Member,
        supportedVersions: Seq[ProtocolVersion] = Seq(version),
    ): AuthenticationTokenProvider =
      AuthenticationTokenProvider(
        psid,
        authenticateAs,
        crypto.crypto,
        supportedVersions,
        AuthenticationTokenManagerConfig(retries = NonNegativeInt.zero),
        None,
        MetricsContext.Empty,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

    def request(authenticateAs: Member): ChallengeRequest =
      ChallengeRequest(authenticateAs.toProtoPrimitive, Seq(pv), "")

    def v31Request(authenticateAs: Member): SequencerChallengeRequest =
      SequencerChallengeRequest(authenticateAs.toProtoPrimitive, Seq(pv), "")

    // "FromSequencer2" names the intended token issuer, not necessarily the immediate RPC destination.
    // The client calls v31AuthSvc on the in-process server: by default this is sequencer2AuthSvc,
    // but passing relayM makes the client call M, whose handlers forward to sequencer2AuthSvc.
    def fetchTokenFromSequencer2(
        authenticateAs: Member,
        dialedEndpoint: Endpoint = sequencer2Endpoint,
        expectedVerifierO: Option[SequencerId] = None,
        v31AuthSvc: SequencerAuthenticationServiceGrpc31.SequencerAuthenticationService =
          sequencer2AuthSvc,
    ): Either[Status, AuthenticationTokenWithExpiry] =
      withTokenProvider(authenticateAs, v31AuthSvc) { (provider, channel) =>
        new ChannelTokenFetcher(
          provider,
          dialedEndpoint,
          channel,
          expectedVerifierO,
        ).apply.value.futureValueUS
      }

    def fetchTokenAndLogout(authenticateAs: Member): AuthenticationTokenWithExpiry =
      withTokenProvider(authenticateAs, sequencer2AuthSvc) { (provider, channel) =>
        val token =
          provider.generateToken(sequencer2Endpoint, channel).value.futureValueUS.value
        provider.logout(sequencer2Endpoint, channel).value.futureValueUS.value
        channel.channel.isShutdown shouldBe false
        provider.close()
        provider.isClosing shouldBe true
        channel.channel.isShutdown shouldBe false
        token
      }

    private def withTokenProvider[A](
        authenticateAs: Member,
        v31AuthSvc: SequencerAuthenticationServiceGrpc31.SequencerAuthenticationService,
    )(test: (AuthenticationTokenProvider, GrpcManagedChannel) => A): A = {
      val serverName = InProcessServerBuilder.generateName()
      val sequencer2Server = InProcessServerBuilder
        .forName(serverName)
        .addService(
          SequencerAuthenticationServiceGrpc
            .bindService(sequencer2AuthGrpcSvc, parallelExecutionContext)
        )
        .addService(
          SequencerAuthenticationServiceGrpc31.bindService(v31AuthSvc, parallelExecutionContext)
        )
        .build()
        .start()
      val authenticationTokenProvider = tokenProvider(authenticateAs)
      val rawChannel = InProcessChannelBuilder.forName(serverName).build()
      val grpcChannel = GrpcManagedChannel(
        "verifier-bound-authentication-test",
        rawChannel,
        new PureOnShutdownRunner(logger),
        logger,
      )
      try test(authenticationTokenProvider, grpcChannel)
      finally {
        authenticationTokenProvider.close()
        rawChannel.shutdownNow()
        sequencer2Server.shutdownNow()
      }
    }

    override def close(): Unit = {
      sequencer2MemberAuthSvc.close()
      topology.crypto.close()
    }
  }

  private def withFixture(pv: Int)(test: Sequencer2Fixture => Unit): Unit = {
    val fixture = new Sequencer2Fixture(pv)
    try test(fixture)
    finally fixture.close()
  }

  "Verifier-bound authentication" should {
    "select providers by member and synchronizer PV, not advertised supported versions" in {
      Seq(37, 38).foreach { pv =>
        withFixture(pv) { fixture =>
          Seq(sequencerId, participant1, mediatorId).foreach { member =>
            val provider = fixture.tokenProvider(
              member,
              Seq(ProtocolVersion.parseUnchecked(37), ProtocolVersion.parseUnchecked(38)),
            )
            try {
              if (pv > 37 && member == sequencerId)
                provider shouldBe a[P2PAuthenticationTokenProvider]
              else provider shouldBe a[MemberAuthenticationTokenProvider]
            } finally provider.close()
          }
        }
      }
    }

    "revoke tokens using either provider without closing the supplied channel" in {
      Seq(37, 38).foreach { pv =>
        withFixture(pv) { fixture =>
          Seq(sequencerId, participant1, mediatorId).foreach { member =>
            val token = fixture.fetchTokenAndLogout(member)
            fixture.sequencer2MemberAuthSvc
              .validateToken(fixture.psid, member, token.token)
              .isLeft shouldBe true
            fixture.sequencer2TokenStore.fetchTokens(member) shouldBe empty
          }
        }
      }
    }

    "keep member sequencer, participant, and mediator authentication on PV37" in withFixture(37) {
      fixture =>
        Seq(sequencerId, participant1, mediatorId).foreach { member =>
          fixture.fetchTokenFromSequencer2(authenticateAs = member).isRight shouldBe true
        }
    }

    "leave participant and mediator authentication unchanged on PV38" in withFixture(38) {
      fixture =>
        Seq(participant1, mediatorId).foreach { member =>
          fixture.fetchTokenFromSequencer2(authenticateAs = member).isRight shouldBe true
          inside(
            fixture.sequencer2AuthSvc
              .sequencerChallenge(fixture.v31Request(member))
              .failed
              .futureValue
          ) { case error: StatusRuntimeException =>
            error.getStatus.getCode shouldBe Status.Code.FAILED_PRECONDITION
          }
        }
    }

    "discover the verifier automatically and authenticate sequencers on PV38" in withFixture(38) {
      fixture =>
        val token = fixture.fetchTokenFromSequencer2(authenticateAs = sequencerId).value
        fixture.sequencer2MemberAuthSvc
          .validateToken(fixture.psid, sequencerId, token.token)
          .isRight shouldBe true
    }

    "issue sequencer1 a token when the expected verifier matches sequencer2 on PV38" in
      withFixture(38) { fixture =>
        val sequencer1 = sequencerId
        val authenticatedIncomingCaller = sequencer2Id
        // Supply the caller identity as the callback interceptor would. This tests matching-identity
        // success, not the incoming stream or interceptor. The mismatched-identity test checks rejection.
        val token = fixture
          .fetchTokenFromSequencer2(
            authenticateAs = sequencer1,
            expectedVerifierO = Some(authenticatedIncomingCaller),
          )
          .value
        fixture.sequencer2MemberAuthSvc
          .validateToken(fixture.psid, sequencer1, token.token)
          .isRight shouldBe true
      }

    "reject member sequencer challenges and authentication on PV38" in withFixture(38) { fixture =>
      inside(
        fixture.sequencer2AuthGrpcSvc.challenge(fixture.request(sequencerId)).failed.futureValue
      ) { case error: StatusRuntimeException =>
        error.getStatus.getCode shouldBe Status.Code.FAILED_PRECONDITION
      }
      inside(
        fixture.sequencer2AuthGrpcSvc
          .authenticate(
            AuthenticateRequest(
              member = sequencerId.toProtoPrimitive,
              signature = None,
              nonce = com.google.protobuf.ByteString.EMPTY,
            )
          )
          .failed
          .futureValue
      ) { case error: StatusRuntimeException =>
        error.getStatus.getCode shouldBe Status.Code.FAILED_PRECONDITION
      }
      fixture.sequencer2MemberAuthSvc
        .validateSignature(
          sequencerId,
          Signature.noSignature,
          Nonce.generate(fixture.crypto.pureCrypto),
        )
        .value
        .futureValueUS
        .isLeft shouldBe true
    }

    "not enable the v31 service below PV38" in withFixture(37) { fixture =>
      inside(
        fixture.sequencer2AuthSvc
          .sequencerChallenge(fixture.v31Request(sequencerId))
          .failed
          .futureValue
      ) { case error: StatusRuntimeException =>
        error.getStatus.getCode shouldBe Status.Code.UNIMPLEMENTED
      }
    }

    "never fall back when the v31 service is unavailable on PV38" in withFixture(38) { fixture =>
      val unavailable = new SequencerAuthenticationServiceGrpc31.SequencerAuthenticationService {
        override def sequencerChallenge(
            request: SequencerChallengeRequest
        ): Future[SequencerChallengeResponse] =
          Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
        override def sequencerAuthenticate(
            request: SequencerAuthenticateRequest
        ): Future[SequencerAuthenticateResponse] =
          Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
      }
      fixture
        .fetchTokenFromSequencer2(authenticateAs = sequencerId, v31AuthSvc = unavailable)
        .left
        .value
        .getCode shouldBe Status.Code.UNIMPLEMENTED
      fixture.sequencer2TokenStore.fetchTokens(sequencerId) shouldBe empty
    }

    "let sequencer1 redeem sequencer2's challenge only once with a verifier-bound signature, " +
      "despite an earlier unbound attempt" in withFixture(38) { fixture =>
        val sequencer1 = sequencerId
        val (sequencer2Nonce, sequencer1KeyFingerprints) =
          fixture.sequencer2MemberAuthSvc.generateChallenge(sequencer1).value.futureValueUS.value

        def requestSignedBySequencer1(
            verifierO: Option[MemberAuthentication.Verifier]
        ): SequencerAuthenticateRequest = {
          val sequencer2ChallengeSignedBySequencer1 = MemberAuthentication
            .signSynchronizerNonce(
              sequencer1,
              sequencer2Nonce,
              fixture.psid,
              sequencer1KeyFingerprints,
              fixture.crypto.crypto,
              verifierO,
            )
            .value
            .futureValueUS
            .value
          SequencerAuthenticateRequest(
            sequencer1.toProtoPrimitive,
            Some(sequencer2ChallengeSignedBySequencer1.toProtoV30),
            sequencer2Nonce.toProtoPrimitive,
          )
        }

        // Sequencer2 rejects sequencer1's member-style proof, which does not bind a verifier.
        loggerFactory.suppressWarningsAndErrors {
          inside(
            fixture.sequencer2AuthSvc
              .sequencerAuthenticate(requestSignedBySequencer1(verifierO = None))
              .failed
              .futureValue
          ) { case error: StatusRuntimeException =>
            error.getStatus.getCode shouldBe Status.Code.INVALID_ARGUMENT
          }
        }
        fixture.sequencer2TokenStore.fetchTokens(sequencer1) shouldBe empty

        // The rejected attempt leaves the nonce usable: binding sequencer2's identity and endpoint
        // lets sequencer1 redeem the same challenge successfully.
        val sequencer1BoundRequest = requestSignedBySequencer1(verifierO = Some(sequencer2Verifier))
        fixture.sequencer2AuthSvc
          .sequencerAuthenticate(sequencer1BoundRequest)
          .futureValue
          .token
          .isEmpty shouldBe false
        // Successful redemption consumes sequencer2's nonce, so replaying the same proof fails.
        loggerFactory.suppressWarningsAndErrors {
          inside(
            fixture.sequencer2AuthSvc
              .sequencerAuthenticate(sequencer1BoundRequest)
              .failed
              .futureValue
          ) { case error: StatusRuntimeException =>
            error.getStatus.getCode shouldBe Status.Code.UNAUTHENTICATED
          }
        }
      }

    "reject a callback to a different sequencer before sending a signature" in withFixture(38) {
      fixture =>
        val sequencer1 = sequencerId
        val maliciousSequencerM = SequencerId(UniqueIdentifier.tryCreate("M", namespace))

        // Sequencer1 performs the reverse-authentication callback after M opens a stream to it.
        // `expectedVerifierO = Some(maliciousSequencerM)` models the identity sequencer1's callback
        //  interceptor would obtain from M's authenticated incoming stream; this test does
        //  not open that stream.
        //
        // `v31AuthSvc = relayM` installs M as sequencer1's immediate callback RPC destination.
        //  Its challenge handler below forwards to sequencer2 and returns sequencer2's
        //  disclosed identity unchanged.
        //
        //  Sequencer1 signing that challenge would allow M to redeem the proof at sequencer2
        //  and obtain a token to impersonate sequencer1 with sequencer2,
        //  rather than authenticate sequencer1 at M.

        val authenticateCalls = new AtomicInteger()
        val relayM = new SequencerAuthenticationServiceGrpc31.SequencerAuthenticationService {
          override def sequencerChallenge(
              request: SequencerChallengeRequest
          ): Future[SequencerChallengeResponse] =
            fixture.sequencer2AuthSvc.sequencerChallenge(request)
          override def sequencerAuthenticate(
              request: SequencerAuthenticateRequest
          ): Future[SequencerAuthenticateResponse] = {
            authenticateCalls.incrementAndGet()
            fixture.sequencer2AuthSvc.sequencerAuthenticate(request)
          }
        }
        fixture
          .fetchTokenFromSequencer2(
            authenticateAs = sequencer1,
            expectedVerifierO = Some(maliciousSequencerM),
            // Sequencer1's callback contacts M's relay, not sequencer2 directly.
            //  M forwards to sequencer2, the intended token issuer from the attacker's perspective.
            v31AuthSvc = relayM,
          )
          .left
          .value
          .getCode shouldBe Status.Code.UNAUTHENTICATED
        authenticateCalls.get() shouldBe 0
    }

    "reject a relay that substitutes the expected verifier identity" in withFixture(38) { fixture =>
      val sequencer1 = sequencerId
      val maliciousSequencerM = SequencerId(UniqueIdentifier.tryCreate("M", namespace))

      // As above, sequencer1 performs the callback to M after M authenticates an incoming stream.
      //  expectedVerifierO supplies that caller identity without opening the incoming stream here.
      //  Unlike the unchanged relay above, M replaces sequencer2's disclosed identity with M's,
      //  so sequencer1's callback identity check passes, and it signs a proof bound to M.
      //  M forwards that proof to sequencer2 to try to obtain a token impersonating sequencer1.
      //  Sequencer2 checks against its own identity, not M's, and rejects the proof.
      val relayM = new SequencerAuthenticationServiceGrpc31.SequencerAuthenticationService {
        override def sequencerChallenge(
            request: SequencerChallengeRequest
        ): Future[SequencerChallengeResponse] =
          fixture.sequencer2AuthSvc
            .sequencerChallenge(request)
            .map(
              _.copy(verifier = maliciousSequencerM.toProtoPrimitive)
            )
        override def sequencerAuthenticate(
            request: SequencerAuthenticateRequest
        ): Future[SequencerAuthenticateResponse] =
          fixture.sequencer2AuthSvc.sequencerAuthenticate(request)
      }
      loggerFactory.suppressWarningsAndErrors {
        fixture
          .fetchTokenFromSequencer2(
            authenticateAs = sequencer1,
            expectedVerifierO = Some(maliciousSequencerM),
            // Sequencer1 contacts M's relay; M tries to redeem its proof at sequencer2.
            v31AuthSvc = relayM,
          )
          .isLeft shouldBe true
      }
      fixture.sequencer2TokenStore.fetchTokens(sequencer1) shouldBe empty
    }

    "reject a challenge relayed through a wholly different endpoint" in withFixture(38) { fixture =>
      val sequencer1 = sequencerId
      val endpointM = attackerEndpoint

      // M relays sequencer2's challenge unchanged. Sequencer1 signs the endpoint it dialed, M's.
      //  Sequencer2 checks against its own endpoint, so the relayed proof cannot authenticate.
      endpointM should not be sequencer2Verifier.endpoint
      loggerFactory.suppressWarningsAndErrors {
        fixture
          .fetchTokenFromSequencer2(authenticateAs = sequencer1, dialedEndpoint = endpointM)
          .isLeft shouldBe true
      }
      fixture.sequencer2TokenStore.fetchTokens(sequencer1) shouldBe empty
    }

    "reject a challenge relayed through a different hostname only" in
      withFixture(38) { fixture =>
        val sequencer1 = sequencerId
        val sequencer2Endpoint = sequencer2Verifier.endpoint
        val endpointM = sequencer2Endpoint.copy(host = attackerEndpoint.host)

        // M relays from the same port but a different host, which must also invalidate the proof.
        loggerFactory.suppressWarningsAndErrors {
          fixture
            .fetchTokenFromSequencer2(
              authenticateAs = sequencer1,
              dialedEndpoint = endpointM,
            )
            .isLeft shouldBe true
        }
        fixture.sequencer2TokenStore.fetchTokens(sequencer1) shouldBe empty
      }

    "reject a challenge relayed through a different port only" in
      withFixture(38) { fixture =>
        val sequencer1 = sequencerId
        val sequencer2Endpoint = sequencer2Verifier.endpoint
        val endpointM = sequencer2Endpoint.copy(port = attackerEndpoint.port)

        // M relays from the same hostname but a different port, which must also invalidate the proof.
        loggerFactory.suppressWarningsAndErrors {
          fixture
            .fetchTokenFromSequencer2(
              authenticateAs = sequencer1,
              dialedEndpoint = endpointM,
            )
            .isLeft shouldBe true
        }
        fixture.sequencer2TokenStore.fetchTokens(sequencer1) shouldBe empty
      }
  }
}
