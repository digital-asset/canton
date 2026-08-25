// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.crypto.{Fingerprint, Nonce}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.ChallengeRequest
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerLimits
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticationService
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class GrpcSequencerAuthenticationServiceTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec
    with MockitoSugar {

  private def mkAuthService(
      authenticationService: MemberAuthenticationService,
      sequencerLimits: SequencerLimits,
      disableReleaseVersionHandshakeCheck: Boolean,
  ): GrpcSequencerAuthenticationService =
    new GrpcSequencerAuthenticationService(
      authenticationService,
      testedProtocolVersion,
      disableReleaseVersionHandshakeCheck,
      sequencerLimits,
      loggerFactory,
    )(parallelExecutionContext)

  private def mkChallengeRequest(
      member: String,
      memberProtocolVersions: Seq[Int],
  ): ChallengeRequest =
    ChallengeRequest(
      member = member,
      memberProtocolVersions = memberProtocolVersions,
      clientVersion = "",
    )

  private def mkChallengeResult: (
      EitherT[
        FutureUnlessShutdown,
        MemberAuthentication.AuthenticationError,
        (Nonce, NonEmpty[Seq[Fingerprint]]),
      ],
      Nonce,
      Fingerprint,
  ) = {
    val expectedNonce =
      Nonce.fromProtoPrimitive(ByteString.copyFrom(Array.fill[Byte](Nonce.length)(1))).value
    val expectedFingerprint = Fingerprint.tryFromString("member-auth-key")
    val result: EitherT[
      FutureUnlessShutdown,
      MemberAuthentication.AuthenticationError,
      (Nonce, NonEmpty[Seq[Fingerprint]]),
    ] = EitherT(
      FutureUnlessShutdown.pure(
        Right((expectedNonce, NonEmpty(Seq, expectedFingerprint))): Either[
          MemberAuthentication.AuthenticationError,
          (Nonce, NonEmpty[Seq[Fingerprint]]),
        ]
      )
    )
    (result, expectedNonce, expectedFingerprint)
  }

  private def mockAuthServiceForChallenge(
      member: Member,
      challengeResult: EitherT[
        FutureUnlessShutdown,
        MemberAuthentication.AuthenticationError,
        (Nonce, NonEmpty[Seq[Fingerprint]]),
      ],
  ): MemberAuthenticationService = {
    val authenticationService = mock[MemberAuthenticationService]
    when(authenticationService.generateChallenge(eqTo(member))(anyTraceContext)).thenReturn(
      challengeResult
    )
    authenticationService
  }

  "GrpcSequencerAuthenticationService" should {
    "accept challenge requests with member protocol versions at the configured limit" in {
      val sequencerLimits = SequencerLimits()
      val maxMemberProtocolVersions = sequencerLimits.maxMemberProtocolVersions.value
      val member = DefaultTestIdentities.participant1
      val (challengeResult, expectedNonce, expectedFingerprint) = mkChallengeResult
      val authenticationService = mockAuthServiceForChallenge(member, challengeResult)
      val service = mkAuthService(
        authenticationService,
        sequencerLimits,
        disableReleaseVersionHandshakeCheck = true,
      )
      val request = mkChallengeRequest(
        member.toProtoPrimitive,
        Seq.fill(maxMemberProtocolVersions)(testedProtocolVersion.toProtoPrimitive),
      )

      val response = service.challenge(request).futureValue
      response.nonce shouldBe expectedNonce.toProtoPrimitive
      // The response's repeated field is a ProtoUnvalidatedSeq, so read it back through the bound
      // rather than comparing the wrapper itself against a Seq.
      ProtoValidation
        .validateLength(
          response.fingerprints,
          "fingerprints",
          testedProtocolVersionValidation,
          ProtoValidation.MaxCollectionSize,
        )
        .value shouldBe Seq(expectedFingerprint.unwrap.toProtoUnvalidated)
    }

    "reject challenge requests with too many member protocol versions" onlyRunWithOrGreaterThan ProtocolVersion.boundsCheck in {
      val sequencerLimits = SequencerLimits()
      val maxMemberProtocolVersions = sequencerLimits.maxMemberProtocolVersions.value
      val request = mkChallengeRequest("", Seq.fill(maxMemberProtocolVersions + 1)(30))

      val authenticationService = mock[MemberAuthenticationService]
      val service = mkAuthService(
        authenticationService,
        sequencerLimits,
        disableReleaseVersionHandshakeCheck = false,
      )

      inside(service.challenge(request).failed.futureValue) { case ex: StatusRuntimeException =>
        ex.getStatus.getCode shouldBe Code.INVALID_ARGUMENT
        ex.getStatus.getDescription should include(
          s"exceeding the maximum of $maxMemberProtocolVersions"
        )
      }

      verifyNoInteractions(authenticationService)
    }
  }
}
