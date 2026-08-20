// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service
import com.digitalasset.canton.http.json.v2.JsUpdateServiceCodecs.*
import com.digitalasset.canton.http.{JsonApiConfig, RequestTimeoutDirective, WebsocketConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}

import scala.concurrent.duration.*

/** Integration tests for the client-supplied `Request-Timeout` header on the JSON Ledger API.
  *
  * The header (value in milliseconds) is validated against the `[LowerBound; UpperBound]` window
  * enforced by [[com.digitalasset.canton.http.RequestTimeoutDirective]]:
  *   - no header -> default request timeout applies;
  *   - below or above the lower bound / non-positive / non-numeric -> `400 Bad Request`, no
  *     processing;
  *   - within the window -> enforced verbatim, `408` if the request exceeds it.
  *
  * The deterministic (fast) cases are exercised against `/v2/version`. The actual `408`-on-timeout
  * is exercised against a `/v2/updates` list request that has no end offset: with no new updates to
  * report it stays open, idling for [[idleWaitTime]] before returning, which is long enough for a
  * small client timeout to fire first.
  */
class JsonRequestTimeoutTest
    extends CantonFixture
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def useTls: Boolean = true

  private val idleWaitTime: FiniteDuration = 5.seconds

  override def wsConfig: Option[WebsocketConfig] = Some(
    WebsocketConfig(httpListWaitTime = idleWaitTime)
  )

  private val headerName = RequestTimeoutDirective.RequestTimeoutHeaderName

  private def withRequestTimeout(headers: List[HttpHeader], millis: String): List[HttpHeader] =
    headers :+ RawHeader(headerName, millis)

  private def invalidMessage(rawValue: String): String =
    s"Invalid $headerName header value '$rawValue': " +
      "expected a positive integer number of milliseconds."

  private def belowLowerBoundMessage(rawValue: String): String =
    s"$headerName '$rawValue' ms is below the minimum of " +
      s"${JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis} ms."

  private def openEndedUpdatesBody(party: String): String =
    update_service
      .GetUpdatesRequest(
        beginExclusive = 0,
        endInclusive = None,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(party -> Filters(Nil)),
                    filtersForAnyParty = None,
                    verbose = false,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
          )
        ),
        descendingOrder = false,
      )
      .asJson
      .noSpaces

  private val versionPath = Uri.Path("/v2/version")
  private val updatesPath = Uri.Path("/v2/updates")

  "Ledger JSON API Request-Timeout header" should {

    "process the request normally when no header is present" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture.getRequestString(versionPath, headers).map { case (status, _) =>
          status should be(StatusCodes.OK)
        }
      }
    }

    "process the request normally for an in-window header value" in httpTestFixture { fixture =>
      val inWindow =
        ((JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis + JsonApiConfig.defaultRequestTimeoutUpperBound.toMillis) / 2).toString
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture
          .getRequestString(versionPath, withRequestTimeout(headers, inWindow))
          .map { case (status, _) =>
            status should be(StatusCodes.OK)
          }
      }
    }

    "reject with 400 a header value below the lower bound" in httpTestFixture { fixture =>
      val belowLower = (JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis - 1).toString
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture
          .getRequestString(versionPath, withRequestTimeout(headers, belowLower))
          .map { case (status, body) =>
            status should be(StatusCodes.BadRequest)
            body should be(belowLowerBoundMessage(belowLower))
          }
      }
    }

    "reject with 400 a non-positive header value" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture
          .getRequestString(versionPath, withRequestTimeout(headers, "0"))
          .map { case (status, body) =>
            status should be(StatusCodes.BadRequest)
            body should be(belowLowerBoundMessage("0"))
          }
      }
    }

    "reject with 400 a non-numeric header value" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (_, headers) =>
        fixture
          .getRequestString(versionPath, withRequestTimeout(headers, "not-a-number"))
          .map { case (status, body) =>
            status should be(StatusCodes.BadRequest)
            body should be(invalidMessage("not-a-number"))
          }
      }
    }

    "complete a slow request (no 408) when no header is present" in httpTestFixture { fixture =>
      fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
        postJsonStringRequestEncoded(
          fixture.uri.withPath(updatesPath),
          openEndedUpdatesBody(party.toString),
          headers,
        ).map { case (status, _) =>
          status should be(StatusCodes.OK)
        }
      }
    }

    "return 408 when the request exceeds an in-window client timeout" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
          val timeoutHeaders =
            withRequestTimeout(
              headers,
              JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis.toString,
            )
          postJsonStringRequestEncoded(
            fixture.uri.withPath(updatesPath),
            openEndedUpdatesBody(party.toString),
            timeoutHeaders,
          ).map { case (status, _) =>
            // The 408 Request Timeout response carries an empty entity, so there is no body to assert.
            status should be(StatusCodes.RequestTimeout)
          }
        }
    }
  }
}
