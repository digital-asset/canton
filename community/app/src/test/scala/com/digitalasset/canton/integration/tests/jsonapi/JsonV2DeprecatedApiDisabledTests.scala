// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2.admin.package_management_service
import com.daml.ledger.api.v2.transaction_filter
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsPackageCodecs.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsUpdateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.{JsCommands, JsGetActiveContractsResponse, LegacyDTOs}
import com.digitalasset.canton.http.util.ClientUtil.uniqueId
import com.digitalasset.canton.http.util.GrpcHttpErrorCodes.`gRPC status as pekko http`
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.DeprecatedApiDisabled
import com.digitalasset.canton.version.ApiDeprecation
import com.google.rpc.Code
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.Assertion

/** Checks that the JSON API endpoints and request fields deprecated in 3.4 and 3.5 are rejected
  * with `DEPRECATED_API_DISABLED` unless the
  * `canton.participants.<participant>.features.deprecated` flag that covers them is set (none is
  * set here), and that each rejection names its own flag. The positive counterpart is
  * `JsonV2Tests`, which enables all flags. The gRPC counterpart is
  * `GrpcDeprecatedApiDisabledIntegrationTest`.
  */
// TODO(#35974) remove together with the deprecated endpoints in 3.7
final class JsonV2DeprecatedApiDisabledTests
    extends AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val allTransactionsFilter = LegacyDTOs.TransactionFilter(
    filtersByParty = Map.empty,
    filtersForAnyParty = Some(
      transaction_filter.Filters(
        cumulative = Seq(
          transaction_filter.CumulativeFilter(
            identifierFilter = transaction_filter.CumulativeFilter.IdentifierFilter
              .WildcardFilter(transaction_filter.WildcardFilter(includeCreatedEventBlob = true))
          )
        )
      )
    ),
  )

  private val allTransactionsFormat = transaction_filter.EventFormat(
    filtersByParty = Map.empty,
    filtersForAnyParty = allTransactionsFilter.filtersForAnyParty,
    verbose = false,
  )

  private def legacyUpdatesRequest(verbose: Boolean) = LegacyDTOs.GetUpdatesRequest(
    beginExclusive = 0,
    endInclusive = None,
    filter = Some(allTransactionsFilter),
    verbose = verbose,
    updateFormat = None,
  )

  private def assertDeprecated34EndpointDisabled(status: StatusCode, body: Json): Assertion =
    assertRejected(ApiDeprecation.Canton34.Endpoints, status, body)

  private def assertDeprecated34EndpointDisabled(body: String): Assertion =
    assertRejectedBody(ApiDeprecation.Canton34.Endpoints, body)

  private def assertDeprecated34ParametersDisabled(status: StatusCode, body: Json): Assertion =
    assertRejected(ApiDeprecation.Canton34.Parameters, status, body)

  private def assertDeprecated35EndpointDisabled(status: StatusCode, body: Json): Assertion =
    assertRejected(ApiDeprecation.Canton35.Endpoints, status, body)

  private def assertRejected(
      deprecation: ApiDeprecation,
      status: StatusCode,
      body: Json,
  ): Assertion = {
    status shouldBe Code.FAILED_PRECONDITION.asPekkoHttp
    assertRejectedBody(deprecation, body.toString())
  }

  private def assertRejectedBody(deprecation: ApiDeprecation, body: String): Assertion = {
    val error = decode[JsCantonError](body).value
    error.code should include(DeprecatedApiDisabled.id)
    // the rejection names the flag of its own deprecation, not the one of the other
    error.cause should include(deprecation.featureFlag)
  }

  "JSON API with deprecated 3.4 and 3.5 APIs disabled" should {
    "reject the legacy filter/verbose fields on /v2/updates but accept updateFormat" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
          for {
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/updates"),
                legacyUpdatesRequest(verbose = true).asJson,
                headers,
              )
              .map { case (status, body) => assertDeprecated34ParametersDisabled(status, body) }
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/updates"),
                legacyUpdatesRequest(verbose = false).asJson,
                headers,
              )
              .map { case (status, body) => assertDeprecated34ParametersDisabled(status, body) }
            // the non-deprecated shape keeps working
            _ <- fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/updates") withQuery Query(
                  ("stream_idle_timeout_ms", "500")
                ),
                LegacyDTOs
                  .GetUpdatesRequest(
                    beginExclusive = 0,
                    endInclusive = None,
                    filter = None,
                    verbose = false,
                    updateFormat = Some(
                      transaction_filter.UpdateFormat(
                        includeTransactions = Some(
                          transaction_filter.TransactionFormat(
                            eventFormat = Some(allTransactionsFormat),
                            transactionShape =
                              transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
                          )
                        ),
                        includeReassignments = None,
                        includeTopologyEvents = None,
                      )
                    ),
                  )
                  .asJson
                  .noSpaces,
                headers,
              )
              .map { case (status, _) => status shouldBe StatusCodes.OK }
          } yield succeed
        }
    }

    "reject the legacy filter/verbose fields on /v2/state/active-contracts but accept eventFormat" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
          for {
            endOffset <- fixture.client.stateService.getLedgerEndOffset()
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/state/active-contracts"),
                LegacyDTOs
                  .GetActiveContractsRequest(
                    filter = Some(allTransactionsFilter),
                    activeAtOffset = endOffset,
                    verbose = false,
                    eventFormat = None,
                    streamContinuationToken = None,
                  )
                  .asJson,
                headers,
              )
              .map { case (status, body) => assertDeprecated34ParametersDisabled(status, body) }
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/state/active-contracts"),
                LegacyDTOs
                  .GetActiveContractsRequest(
                    filter = None,
                    activeAtOffset = endOffset,
                    verbose = false,
                    eventFormat = Some(allTransactionsFormat),
                    streamContinuationToken = None,
                  )
                  .asJson,
                headers,
              )
              .map { case (status, body) =>
                status shouldBe StatusCodes.OK
                decode[Seq[JsGetActiveContractsResponse]](body.toString()).value shouldBe empty
              }
          } yield succeed
        }
    }

    "reject the deprecated update endpoints" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
        for {
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/updates/trees"),
              legacyUpdatesRequest(verbose = true).asJson,
              headers,
            )
            .map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/updates/flats"),
              legacyUpdatesRequest(verbose = true).asJson,
              headers,
            )
            .map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/updates/transaction-by-id"),
              LegacyDTOs
                .GetTransactionByIdRequest(
                  updateId = "some-update-id",
                  requestingParties = Seq(alice.unwrap),
                  transactionFormat = None,
                )
                .asJson,
              headers,
            )
            .map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
          _ <- fixture
            .postJsonRequest(
              Uri.Path("/v2/updates/transaction-by-offset"),
              LegacyDTOs
                .GetTransactionByOffsetRequest(
                  offset = 1,
                  requestingParties = Seq(alice.unwrap),
                  transactionFormat = None,
                )
                .asJson,
              headers,
            )
            .map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
          _ <- getRequestInternal(
            fixture.uri
              .withPath(Uri.Path("/v2/updates/transaction-tree-by-offset/1"))
              .withQuery(Query(("parties", alice.unwrap))),
            headers,
          ).map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
          _ <- getRequestInternal(
            fixture.uri
              .withPath(Uri.Path("/v2/updates/transaction-tree-by-id/some-update-id"))
              .withQuery(Query(("parties", alice.unwrap))),
            headers,
          ).map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
        } yield succeed
      }
    }

    "reject the deprecated websocket endpoints with the first message" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, _) =>
          // the websocket is established and the error is sent in-band, like for the deprecated
          // request fields of the non-deprecated websocket endpoints
          def firstMessage(path: String, jwt: Jwt) = {
            val webSocketFlow = websocket(fixture.uri.withPath(Uri.Path(path)), jwt)
            Source
              .single(TextMessage(legacyUpdatesRequest(verbose = true).asJson.noSpaces))
              .concatMat(Source.maybe[Message])(Keep.left)
              .via(webSocketFlow)
              .take(1)
              .collect { case m: TextMessage => m.getStrictText }
              .toMat(Sink.seq)(Keep.right)
              .run()
              .map(_.loneElement)
          }
          for {
            jwt <- jwtForParties(fixture.uri)(List(alice), List())
            flatsMessage <- firstMessage("/v2/updates/flats", jwt)
            treesMessage <- firstMessage("/v2/updates/trees", jwt)
          } yield {
            assertDeprecated34EndpointDisabled(flatsMessage)
            assertDeprecated34EndpointDisabled(treesMessage)
          }
        }
    }

    "reject the deprecated command, package-vetting and interactive-submission endpoints" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (alice, headers) =>
          for {
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/commands/submit-and-wait-for-transaction-tree"),
                JsCommands(
                  commands = Seq.empty,
                  commandId = uniqueId(),
                  actAs = Seq(alice.unwrap),
                ).asJson,
                headers,
              )
              .map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
            // the endpoint is rejected before its request body is decoded
            _ <- fixture
              .postJsonStringRequest(
                fixture.uri withPath Uri.Path("/v2/commands/submit-and-wait-for-transaction-tree"),
                "not a JsCommands",
                headers,
              )
              .map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
            _ <- fixture
              .postJsonRequest(
                Uri.Path("/v2/package-vetting"),
                package_management_service.UpdateVettedPackagesRequest.defaultInstance.asJson,
                headers,
              )
              .map { case (status, body) => assertDeprecated35EndpointDisabled(status, body) }
            _ <- getRequestInternal(
              fixture.uri
                .withPath(Uri.Path("/v2/interactive-submission/preferred-package-version"))
                .withQuery(Query(("parties", alice.unwrap), ("package-name", "SomePackage"))),
              headers,
            ).map { case (status, body) => assertDeprecated34EndpointDisabled(status, body) }
          } yield succeed
        }
    }
  }
}
