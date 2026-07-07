// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.ServerConfig
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.ParticipantSelector
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}
import org.scalatest.Assertion

import java.io.File

/** Verifies that the JSON API rejects request bodies exceeding the effective
  * `maxInboundMessageSize` with HTTP 413 (Content Too Large), exercising the different ways the
  * limit can be configured:
  *
  *   - participant1: the JSON API `maxInboundMessageSize` is set explicitly.
  *   - participant2: the JSON API limit is left unset and is inherited from the gRPC Ledger API
  *     `maxInboundMessageSize`.
  *   - participant3: neither is set, so the shared default applies
  *     ([[ServerConfig.defaultMaxInboundMessageSize]], 10 MiB).
  *
  * The 413 is produced by pekko-http while parsing the request entity (from `Content-Length`), i.e.
  * before authentication and routing, so these checks do not require a valid token.
  */
@UnstableTest // TODO(i34011): remove this once the test is no longer flaky
class JsonMaxInboundMessageSizeTest
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def packageFiles: List[File] = List.empty

  private val jsonApiLimit: NonNegativeInt = NonNegativeInt.tryCreate(1024)
  private val grpcLimit: NonNegativeInt = NonNegativeInt.tryCreate(4096)
  // The limit that applies when nothing is configured for either API.
  private val unconfiguredLimit: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2, participant3).foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
        }
        createChannel(participant1)
      }
      .prependConfigTransforms(
        // participant1: explicitly configured limit
        ConfigTransforms.enableHttpLedgerApi(
          "participant1",
          maxInboundMessageSize = Some(jsonApiLimit),
        ),
        // participant2: JSON API limit unset -> inherits the gRPC Ledger API limit.
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.ledgerApi.maxInboundMessageSize).replace(grpcLimit)
        ),
        // participant3: no explicit limits (fall back to gRPC 10mb limit)
        ConfigTransforms.enableHttpLedgerApi("participant3"),
      )

  private val submitPath = Uri.Path("/v2/commands/submit-and-wait")
  private val noAuth: List[HttpHeader] = List.empty

  private def jsonOfPadding(padding: Int): String = s"""{"padding":"${"x" * padding}"}"""

  /** Posts an oversized body (expecting 413 naming `limit`) and a body of `withinLimitPadding`
    * bytes (expecting any status other than 413) to the JSON API of the selected participant.
    */
  private def assertLimitEnforced(
      participant: ParticipantSelector,
      limit: NonNegativeInt,
      withinLimitPadding: Int,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    val oversized = jsonOfPadding(limit.unwrap + 1024)
    val oversizedBytes = oversized.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    oversizedBytes should be > limit.unwrap
    val withinLimit = jsonOfPadding(withinLimitPadding)
    val withinLimitBytes = withinLimit.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    withinLimitBytes should be < limit.unwrap

    (for {
      http <- adHocHttp(participant)
      (rejectedStatus, rejectedBody) <- postJsonStringRequestEncoded(
        http.uri withPath submitPath,
        oversized,
        noAuth,
      )
      (acceptedStatus, _) <- postJsonStringRequestEncoded(
        http.uri withPath submitPath,
        withinLimit,
        noAuth,
      )
    } yield {
      // Oversized body: rejected by size, before routing/auth. pekko-http names the configured limit
      // in the EntityStreamSizeException message ("... exceeded size limit (<limit> bytes)!").
      rejectedStatus shouldBe StatusCodes.ContentTooLarge
      rejectedBody.toLowerCase should include("exceeded size limit")
      rejectedBody should include(limit.unwrap.toString)

      // Body within the limit: never rejected for size (it fails later for auth/other reasons).
      acceptedStatus should not be StatusCodes.ContentTooLarge
    }).futureValue
  }

  "json service" should {
    "reject bodies above the explicitly configured JSON API limit" in { implicit env =>
      assertLimitEnforced(_.participant1, jsonApiLimit, withinLimitPadding = 64)
    }

    "inherit the gRPC Ledger API limit when the JSON API limit is unset" in { implicit env =>
      // The accepted body is above the JSON API limit configured on participant1 (1024) but below
      // the gRPC limit (4096) used for participant2 here
      assertLimitEnforced(
        _.participant2,
        grpcLimit,
        withinLimitPadding = jsonApiLimit.unwrap + 1000,
      )
    }

    "fall back to the default limit when neither API is configured" in { implicit env =>
      assertLimitEnforced(
        _.participant3,
        unconfiguredLimit,
        withinLimitPadding = grpcLimit.unwrap * 2,
      )
    }
  }
}
