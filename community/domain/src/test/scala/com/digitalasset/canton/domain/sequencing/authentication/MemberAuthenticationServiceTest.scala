// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import cats.implicits._
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.topology._
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  MissingToken,
  NonMatchingDomainId,
  ParticipantDisabled,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration => JDuration}
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class MemberAuthenticationServiceTest extends AsyncWordSpec with BaseTest {

  import DefaultTestIdentities._

  val p1 = participant1

  val clock: SimClock = new SimClock(loggerFactory = loggerFactory)

  val topology = TestingTopology().withParticipants(participant1).build()
  val syncCrypto = topology.forOwnerAndDomain(participant1, domainId)

  def service(
      participantIsActive: Boolean,
      nonceDuration: JDuration = JDuration.ofMinutes(1),
      tokenDuration: JDuration = JDuration.ofHours(1),
      invalidateMemberCallback: Member => Unit = _ => (),
  ): MemberAuthenticationService =
    new MemberAuthenticationService(
      domainId,
      syncCrypto,
      new InMemoryMemberAuthenticationStore(),
      None,
      clock,
      nonceDuration,
      tokenDuration,
      memberT => invalidateMemberCallback(memberT.value),
      Future.unit,
      loggerFactory,
      ParticipantAuditor.noop,
    ) {
      override def isParticipantActive(participant: ParticipantId)(implicit
          traceContext: TraceContext
      ): Future[Boolean] =
        Future.successful(participantIsActive)
    }

  def getMemberAuthentication(member: Member) =
    MemberAuthentication(member).getOrElse(fail("unsupported"))

  "ParticipantAuthenticationService" should {

    def generateToken(sut: MemberAuthenticationService) =
      for {
        challenge <- sut.generateNonce(p1)
        (nonce, fingerprints) = challenge
        signature <- getMemberAuthentication(p1)
          .signDomainNonce(p1, nonce, domainId, fingerprints, None, syncCrypto.crypto)
        tokenAndExpiry <- sut.validateSignature(p1, signature, nonce)
      } yield tokenAndExpiry.token

    "generate nonce, verify signature, generate token, and verify token" in {
      val sut = service(true)
      (for {
        token <- generateToken(sut)
        _ <- sut.validateToken(domainId, p1, token)
      } yield ()).value.map { result =>
        result should matchPattern { case Right(()) =>
        }
      }
    }

    "should fail every method if participant is not active" in {
      val sut = service(false)
      for {
        generateNonceError <- leftOrFail(sut.generateNonce(p1))("generating nonce")
        validateSignatureError <- leftOrFail(sut.validateSignature(p1, null, Nonce.generate()))(
          "validateSignature"
        )
        validateTokenError <- leftOrFail(sut.validateToken(domainId, p1, null))(
          "token validation should fail"
        )
      } yield {
        generateNonceError shouldBe ParticipantDisabled(p1)
        validateSignatureError shouldBe ParticipantDisabled(p1)
        validateTokenError shouldBe MissingToken(p1)
      }
    }

    "should check whether the intended domain is the one the participant is connecting to" in {
      val sut = service(false)
      val wrongDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong::domain"))

      for {
        error <- leftOrFail(sut.validateToken(wrongDomainId, p1, null))("should fail domain check")
      } yield error shouldBe NonMatchingDomainId(p1, wrongDomainId)
    }
  }
}
