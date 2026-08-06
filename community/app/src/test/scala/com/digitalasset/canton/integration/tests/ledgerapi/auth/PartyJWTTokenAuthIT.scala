// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{
  AuthServiceJWTCodec,
  DecodedJwt,
  JwtSigner,
  PartyJWTPayload,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.daml.ledger.api.testtool.infrastructure.ExternalPartyKeySpec
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.ledger.api.v2.crypto as lapicrypto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.api.auth.PartyJWTAuthService
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*

import java.security.interfaces.{ECPrivateKey, EdECPrivateKey}
import java.security.{KeyPairGenerator, PrivateKey}
import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future, Promise}

class PartyJWTTokenAuthIT
    extends ServiceCallAuthTests
    with ErrorsAssertions
    with UserManagementAuth
    with IdentityProviderConfigAuth {

  registerPlugin(ExpectedAudienceOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String =
    "Retrieving party info with party based token authorization"

  protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  override val adminToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(ExpectedAudience),
    scope = None,
  )

  private val adminContext: ServiceCallContext =
    ServiceCallContext(token =
      Some(
        toHeader(
          payload = adminToken,
          enforceFormat = Some(StandardJWTTokenFormat.Audience),
        )
      )
    )

  private val suppressionRule: SuppressionRule =
    SuppressionRule.forLogger[AuthInterceptor] ||
      SuppressionRule.forLogger[PartyJWTAuthService]

  private def runTests(
      prefix: String,
      keySpec: ExternalPartyKeySpec,
      jwtSigner: PrivateKey => JwtSigner,
  ): Unit = {
    case class UserWithExternalParty(
        userId: String,
        partyId: String,
        privateKey: PrivateKey,
        jwtSigner: JwtSigner,
    )

    def createUserWithExternalParty(
        partyHint: String,
        userId: String,
        userIdentityProviderId: String,
        userPrimaryPartyAuthentication: Boolean,
    )(implicit env: TestConsoleEnvironment): Future[UserWithExternalParty] = {
      val keyPair = keySpec.keyInstance()
      val pb = keyPair.getPublic
      val partyManagement =
        stub(pproto.PartyManagementServiceGrpc.stub(channel), adminContext.token)
      val synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive
      implicit val ec: ExecutionContext = env.executionContext

      def signTopology(response: pproto.GenerateExternalPartyTopologyResponse): ByteString = {
        val signing = keySpec.signatureInstance()
        signing.initSign(keyPair.getPrivate)
        signing.update(response.multiHash.toByteArray)
        ByteString.copyFrom(signing.sign())
      }

      for {
        generated <- partyManagement.generateExternalPartyTopology(
          pproto.GenerateExternalPartyTopologyRequest(
            synchronizer = synchronizerId,
            partyHint = partyHint,
            publicKey = Some(
              lapicrypto.SigningPublicKey(
                format = keySpec.keyFormat,
                keyData = ByteString.copyFrom(pb.getEncoded),
                keySpec = keySpec.keySpec,
              )
            ),
            localParticipantObservationOnly = false,
            otherConfirmingParticipantUids = Seq(),
            confirmationThreshold = 1,
            observingParticipantUids = Seq(),
          )
        )
        signature = signTopology(generated)
        resp <- partyManagement
          .allocateExternalParty(
            pproto.AllocateExternalPartyRequest(
              synchronizer = synchronizerId,
              onboardingTransactions = generated.topologyTransactions.map(x =>
                pproto.AllocateExternalPartyRequest
                  .SignedTransaction(transaction = x, signatures = Seq.empty)
              ),
              multiHashSignatures = Seq(
                lapicrypto.Signature(
                  format = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
                  signature = signature,
                  signedBy = generated.publicKeyFingerprint,
                  signingAlgorithmSpec = keySpec.signatureAlgorithmSpec,
                )
              ),
              waitForAllocation = Some(true),
              identityProviderId = userIdentityProviderId,
              userId = "",
            )
          )
        (user, _) <- createUserByAdmin(
          userId,
          identityProviderId = userIdentityProviderId,
          primaryPartyAuthentication = userPrimaryPartyAuthentication,
          primaryParty = resp.partyId,
          rights = Vector(
            uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs(resp.partyId)))
          ),
        )
      } yield UserWithExternalParty(
        userId = userId,
        partyId = resp.partyId,
        privateKey = keyPair.getPrivate(),
        jwtSigner = jwtSigner(keyPair.getPrivate()),
      )
    }

    case class Setup(
        user1: UserWithExternalParty,
        userNoPrimaryPartyAuthentication: UserWithExternalParty,
        userIdp: UserWithExternalParty,
    )

    val cachedSetup = Promise[Setup]()
    val startedSetup = new AtomicBoolean(false)
    def getSetup()(implicit
        env: TestConsoleEnvironment
    ): Future[Setup] = {
      if (startedSetup.compareAndSet(false, true)) {
        import env.*

        val setup = for {
          user1 <- createUserWithExternalParty(
            partyHint = s"$prefix-party-11",
            userId = s"$prefix-user-1",
            userPrimaryPartyAuthentication = true,
            userIdentityProviderId = "",
          )
          userNoPrimaryPartyAuthentication <- createUserWithExternalParty(
            partyHint = s"$prefix-party-2",
            userId = s"$prefix-user-2",
            userPrimaryPartyAuthentication = false,
            userIdentityProviderId = "",
          )
          idp <- createConfig(
            context = adminContext,
            idpId = Some(s"$prefix-idp"),
          )
          userIdp <- createUserWithExternalParty(
            partyHint = s"$prefix-party-idp",
            userId = s"$prefix-user-idp",
            userPrimaryPartyAuthentication = true,
            userIdentityProviderId = idp.identityProviderId,
          )
        } yield Setup(user1, userNoPrimaryPartyAuthentication, userIdp)

        cachedSetup.completeWith(setup)
      }

      cachedSetup.future
    }

    def serviceCallWithPartyJWT(
        payload: PartyJWTPayload,
        signer: JwtSigner,
        requestPartyId: Option[String] = None, // Defaults to PartyID in payload
    )(implicit env: TestConsoleEnvironment): Future[Unit] = {
      val actualRequestPartyId = requestPartyId.getOrElse(payload.partyId)
      val token = Some(
        signer
          .signPayload(AuthServiceJWTCodec.compactPrint(payload))
          .valueOrFail("failed to sign payload")
          .value
      )
      stub(pproto.PartyManagementServiceGrpc.stub(channel), token)
        .getParties(
          pproto.GetPartiesRequest(parties = Seq(actualRequestPartyId), identityProviderId = "")
        )
        .map(_ => ())(env.executionContext)
    }

    serviceCallName should {
      "allow access" taggedAs securityAsset
        .setHappyCase(
          "Ledger API client can make a call with a JWT with intended audience"
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectSuccess {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.user1.partyId,
                  userId = setup.user1.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.user1.jwtSigner,
              )
            } yield ()
          }
        }
      }

      "deny access if primaryPartyAuthentication is not set" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat =
          "Ledger API does not accept Party JWTs if primaryPartyAuthentication is not set"
        )
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.userNoPrimaryPartyAuthentication.partyId,
                  userId = setup.userNoPrimaryPartyAuthentication.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.userNoPrimaryPartyAuthentication.jwtSigner,
              )
            } yield ()
          }
        }
      }

      "deny access on invalid signature" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat =
          "Ledger API client cannot make a call with a JWT signed with the wrong key"
        )
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.user1.partyId,
                  userId = setup.user1.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.userNoPrimaryPartyAuthentication.jwtSigner, // Wrong signature
              )
            } yield ()
          }
        }
      }

      "deny access when party is not the primary party" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat =
          "Ledger API client can accept a JWT signed by the wrong party"
        )
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.userNoPrimaryPartyAuthentication.partyId, // Wrong party
                  userId = setup.user1.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.userNoPrimaryPartyAuthentication.jwtSigner, // Matches party
              )
            } yield ()
          }
        }
      }

      "deny access on expired JWT" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat = "Ledger API client cannot make a call with an expired JWT")
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.user1.partyId,
                  userId = setup.user1.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp =
                    Some(Instant.now().minusNanos(Duration.ofMinutes(5).toNanos)), // In the past
                  scope = None,
                ),
                signer = setup.user1.jwtSigner,
              )
            } yield ()
          }
        }
      }

      "deny access on participant audience mismatch" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat =
          "Ledger API client cannot make a call with an JWT targeting a different participant"
        )
      ) in { implicit env =>
        import env.*
        val wrongParticipant = participant1.id.toLf.replace('1', '2')
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.user1.partyId,
                  userId = setup.user1.userId,
                  participantId = wrongParticipant, // Wrong
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.user1.jwtSigner,
              )
            } yield ()
          }
        }
      }

      // TODO(i34173) will fix this problem
      "deny access if identityProviderId is not the default" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat = "Ledger API does not accept Party JWTs for IDPs")
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.userIdp.partyId,
                  userId = setup.userIdp.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.userIdp.jwtSigner,
              )
            } yield ()
          }
        }
      }

      "deny access if synchronizer is unknown" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat =
          "Ledger API does not accept Party JWTs without a correct synchronizer ID"
        )
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.user1.partyId,
                  userId = setup.user1.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = "badsynchronizer", // Bad
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = setup.user1.jwtSigner,
              )
            } yield ()
          }
        }
      }

      "deny access on invalid alg" taggedAs securityAsset.setAttack(
        attackUnauthenticated(threat = "Algorithm confusion attack")
      ) in { implicit env =>
        import env.*
        case class WrongAlgJwtSigner(delegate: JwtSigner) extends JwtSigner {
          def kid = delegate.kid
          def alg = if (delegate.alg == "ES256") "ES384" else "ES256"
          def sign(jwt: DecodedJwt[String]) = delegate.sign(jwt)
        }
        loggerFactory.suppress(suppressionRule) {
          expectUnauthenticated {
            for {
              setup <- getSetup()
              _ <- serviceCallWithPartyJWT(
                payload = PartyJWTPayload(
                  partyId = setup.user1.partyId,
                  userId = setup.user1.userId,
                  participantId = participant1.id.toLf,
                  synchronizerId = env.synchronizer1Id.logical.toProtoPrimitive,
                  exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
                  scope = None,
                ),
                signer = WrongAlgJwtSigner(setup.user1.jwtSigner), // Will add wrong "alg" value
              )
            } yield ()
          }
        }
      }

    }
  }

  "using Ed25519" should {
    runTests(
      prefix = "Ed25519",
      keySpec = ExternalPartyKeySpec.EcCurve25519,
      jwtSigner = {
        case pk: EdECPrivateKey => JwtSigner.EdDSA(pk)
        case pk => throw new RuntimeException("expected EdECPrivateKey: " + pk)
      },
    )
  }

  "using EcDsaSha256" should {
    val keyPairGenerator = KeyPairGenerator.getInstance("EC")
    keyPairGenerator.initialize(256)
    runTests(
      prefix = "EcDsaSha256",
      keySpec = ExternalPartyKeySpec.EcP256,
      jwtSigner = {
        case pk: ECPrivateKey => JwtSigner.ES256(pk)
        case pk => throw new RuntimeException("expected ECPrivateKey: " + pk)
      },
    )
  }

  "using EcDsaSha384" should {
    val keyPairGenerator = KeyPairGenerator.getInstance("EC")
    keyPairGenerator.initialize(384)
    runTests(
      prefix = "EcDsaSha384",
      keySpec = ExternalPartyKeySpec.EcP384,
      jwtSigner = {
        case pk: ECPrivateKey => JwtSigner.ES384(pk)
        case pk => throw new RuntimeException("expected ECPrivateKey: " + pk)
      },
    )
  }

  //  plugin to override the configuration and use authorization with audiences
  case class ExpectedAudienceOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.authServices).replace(
          Seq[AuthServiceConfig](
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = jwtSecret,
                targetAudience = Some(ExpectedAudience),
                targetScope = None,
              ),
            AuthServiceConfig.PartyJwt(),
          )
        )
      }(config)
  }
}
