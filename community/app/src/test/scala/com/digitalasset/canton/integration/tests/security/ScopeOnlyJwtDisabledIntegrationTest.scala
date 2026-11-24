// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.jwt.{AuthServiceJWTCodec, JwtSigner, StandardJWTPayload, StandardJWTTokenFormat}
import com.digitalasset.canton.auth.AuthorizedUser
import com.digitalasset.canton.config.CantonRequireTypes.{InstanceName, NonEmptyString}
import com.digitalasset.canton.config.Confidential.Implicits.*
import com.digitalasset.canton.config.{AuthServiceConfig, DeprecatedFeaturesConfig}
import com.digitalasset.canton.console.{CommandFailure, ExternalLedgerApiClient}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthInterceptorSuppressionRule
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.config.RemoteParticipantConfig
import com.digitalasset.canton.user.store.UserManagementStore
import monocle.macros.syntax.lens.*

import java.time.{Duration, Instant}

/** Scope-only JWTs are rejected on the Ledger API and on the Admin API while
  * `canton.features.deprecated.enable-scope-only-jwt` is off, which it is here.
  */
// TODO (i#33090): remove together with the scope-only token format
sealed trait ScopeOnlyJwtDisabledIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val secret = NonEmptyString.tryCreate("not-safe-for-production")
  private val participantAdmin = UserManagementStore.DefaultParticipantAdminUserId

  // the status of every configured node, including the remote participant whose scope-only token
  // is rejected, is queried when the environment starts
  override def beforeAll(): Unit =
    loggerFactory.suppress(AuthInterceptorSuppressionRule) {
      super.beforeAll()
    }

  // The console reaches the Admin API through remote participants, whose token is part of the
  // configuration, so the tokens are minted before the participant starts, without an expiry
  // that could lapse before the tests run.
  private val scopeOnlyRemote = "participant1-scope-only-token"
  private val audienceRemote = "participant1-audience-token"

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
        config
          .focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(secret = secret, targetAudience = None, targetScope = None)
            )
          )
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
          // the admin API has no user store, so a user-list auth service grants the admin the
          // services the console calls: the api info check of every command and the vault
          .focus(_.adminApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig.UnsafeJwtHmac256(
                secret = secret,
                targetAudience = None,
                targetScope = None,
                users = Seq(
                  AuthorizedUser(
                    userId = participantAdmin,
                    allowedServices = Seq(
                      "com.digitalasset.canton.connection.v30.ApiInfoService",
                      "com.digitalasset.canton.crypto.admin.v30.VaultService",
                    ),
                  )
                ),
              )
            )
          )
      })
      .addConfigTransform { config =>
        val participant1 = config.participants(InstanceName.tryCreate("participant1"))
        def remoteWithToken(token: String): RemoteParticipantConfig =
          RemoteParticipantConfig(
            adminApi = participant1.adminApi.clientConfig,
            ledgerApi = participant1.ledgerApi.clientConfig,
            token = Some(token),
          )
        config
          .focus(_.remoteParticipants)
          .modify(
            _ ++ Map(
              InstanceName.tryCreate(scopeOnlyRemote) -> remoteWithToken(
                signedToken(StandardJWTTokenFormat.Scope, participantId = None, exp = None)
              ),
              // the Admin API does not validate the participant id of a token, so the audience
              // can name the participant before it has an id
              InstanceName.tryCreate(audienceRemote) -> remoteWithToken(
                signedToken(
                  StandardJWTTokenFormat.Audience,
                  participantId = Some("participant1"),
                  exp = None,
                )
              ),
            )
          )
      }
      // the remote participants are aliases of participant1 without a Ledger API store to verify
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(Set(scopeOnlyRemote, audienceRemote))
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  private def signedToken(
      format: StandardJWTTokenFormat,
      participantId: Option[String],
      exp: Option[Instant] = Some(Instant.now().plus(Duration.ofMinutes(5))),
  ): String = {
    val payload = StandardJWTPayload(
      issuer = None,
      userId = participantAdmin,
      participantId = participantId,
      exp = exp,
      format = format,
      audiences = List.empty,
      scope = Option.when(format == StandardJWTTokenFormat.Scope)(
        AuthServiceJWTCodec.scopeLedgerApiFull
      ),
    )
    JwtSigner
      .HMAC256(secret.unwrap)
      .signPayload(AuthServiceJWTCodec.compactPrint(payload))
      .fold(err => fail(s"Failed to sign the token: ${err.message}"), _.value)
  }

  // relies on the default `daml_ledger_api` scope instead of an audience
  private def scopeOnlyToken: String =
    signedToken(StandardJWTTokenFormat.Scope, participantId = None)

  // carries the participant audience `https://daml.com/jwt/aud/participant/<participantId>`
  private def participantAudienceToken(implicit env: TestConsoleEnvironment): String = {
    import env.*

    signedToken(StandardJWTTokenFormat.Audience, Some(participant1.uid.toProtoPrimitive))
  }

  "A node with scope-only JWTs disabled" when {
    "a scope-only token is presented" must {
      "reject it on the Ledger API naming the feature flag" in { implicit env =>
        import env.*

        val client = ExternalLedgerApiClient.forReference(participant1, scopeOnlyToken)

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          client.ledger_api.users.list(),
          entries => {
            forAtLeast(1, entries)(
              _.warningMessage should include(DeprecatedFeaturesConfig.ScopeOnlyJwtDisabledMessage)
            )
            forAtLeast(1, entries)(_.commandFailureMessage should include("UNAUTHENTICATED"))
          },
        )
      }

      "reject it on the Admin API naming the feature flag" in { implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          rp(scopeOnlyRemote).keys.public.list(),
          entries => {
            forAtLeast(1, entries)(
              _.warningMessage should include(DeprecatedFeaturesConfig.ScopeOnlyJwtDisabledMessage)
            )
            forAtLeast(1, entries)(_.commandFailureMessage should include("UNAUTHENTICATED"))
          },
        )
      }
    }

    "an audience-based token is presented" must {
      "accept it on the Ledger API" in { implicit env =>
        import env.*

        val client = ExternalLedgerApiClient.forReference(participant1, participantAudienceToken)

        client.ledger_api.users.list().users.map(_.id) should contain(participantAdmin)
      }

      "accept it on the Admin API" in { implicit env =>
        import env.*

        rp(audienceRemote).keys.public.list() should not be empty
      }
    }
  }
}

final class ScopeOnlyJwtDisabledIntegrationTestH2 extends ScopeOnlyJwtDisabledIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
