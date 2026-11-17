// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.auth.{AuthorizedUser, CantonAdminToken}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommandHelpers
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import monocle.macros.syntax.lens.*

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.Future

class UserConfigAuthIT
    extends ServiceCallAuthTests
    with ErrorsAssertions
    with SubmitAndWaitDummyCommandHelpers {

  registerPlugin(ExpectedAudienceOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def prerequisiteParties: List[String] = List(randomParty)

  override def serviceCallName: String =
    "Any service call with user-config based token authorization"

  protected def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token)
      .listKnownPackages(ListKnownPackagesRequest())

  def partyCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    submitAndWait(
      token = context.token,
      userId = "",
      party = getPartyId(randomParty),
    )
  }

  private def toContext(
      payload: StandardJWTPayload,
      secretOverwrite: Option[String] = None,
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Audience),
        secret = secretOverwrite.getOrElse(privilegedJwtSecret.unwrap),
      )
    )
  )

  private val cantonAdminToken = CantonAdminToken.create(new SymbolicPureCrypto())

  private val configuredUser = randomUserId()
  private val otherUser = randomUserId()

  private val privilegedJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)
  private val privilegedAudience: String = "Privileged-Audience"

  val configuredUserToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = configuredUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(privilegedAudience),
    scope = None,
  )

  val otherUserToken: StandardJWTPayload = configuredUserToken.copy(userId = otherUser)

  val multipleAudiencesWithExpectedToken: StandardJWTPayload =
    configuredUserToken.copy(audiences = List(privilegedAudience, "additionalAud"))

  val noAudienceToken: StandardJWTPayload =
    configuredUserToken.copy(audiences = List.empty)

  val wrongAudienceToken: StandardJWTPayload =
    configuredUserToken.copy(audiences = List("aud1", "aud2"))

  val expiredToken: StandardJWTPayload =
    configuredUserToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val scopeToken: StandardJWTPayload = configuredUserToken.copy(scope = Some("scope1 scope2"))

  serviceCallName should {
    "allow access to a configured endpoint requiring admin rights with canton admin token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a canton admin token"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(ServiceCallContext(token = Some(cantonAdminToken.secret)))
      }
    }

    "allow access to a configured endpoint requiring admin rights" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a token for configured user"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(
          serviceCall(toContext(configuredUserToken))
        )
      )
    }

    "deny access to a configured endpoint requiring admin rights" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call with a token for a user not in configuration"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectUnauthenticated(
          serviceCall(toContext(otherUserToken))
        )
      )
    }

    "allow access to an endpoint with the token with multiple audiences which is matching expected audience" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token with intended audience"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess {
          serviceCall(toContext(multipleAudiencesWithExpectedToken))
        }
      )
    }

    "allow access to an endpoint with the token which is matching intended audience and scopes defined" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token with intended audience"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(serviceCall(toContext(scopeToken)))
      )
    }

    "deny access with no intended audience" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a privileged token with no intended audience"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(noAudienceToken)))
      }
    }

    "deny access with wrong intended audience" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a privileged token with wrong intended audience"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(wrongAudienceToken)))
      }
    }

    "deny calls with an expired token" taggedAs adminSecurityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired privileged token")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(expiredToken)))
      }
    }

    "deny unauthenticated access" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present JWT")
    ) in { implicit env =>
      import env.*
      expectUnauthenticated(serviceCall(noToken))
    }
  }

  private val authorizedUser = AuthorizedUser(
    userId = configuredUser,
    allowedServices = List(
      "com.daml.ledger.api.v2.admin.PackageManagementService"
    ),
  )

  //  plugin to override the configuration and use authorization with audience
  case class ExpectedAudienceOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(cantonAdminToken.secret))
          .focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = Some(ExpectedAudience),
                  targetScope = None,
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = privilegedJwtSecret,
                  targetAudience = Some(privilegedAudience),
                  targetScope = None,
                  users = List(authorizedUser),
                ),
            )
          )
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
      }(config)
  }
}
