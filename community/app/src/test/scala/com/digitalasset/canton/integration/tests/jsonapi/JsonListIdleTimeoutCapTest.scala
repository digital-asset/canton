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
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.JsUpdateServiceCodecs.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import io.circe.syntax.*
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}

import scala.concurrent.duration.*

class JsonListIdleTimeoutCapTest
    extends CantonFixture
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def useTls: Boolean = true

  private val maxWaitTime: FiniteDuration = 1.second

  override def wsConfig: Option[WebsocketConfig] = Some(
    WebsocketConfig(httpListMaxWaitTime = maxWaitTime)
  )

  private val updatesPath = Uri.Path("/v2/updates")

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
                    filtersByContractId = Seq.empty,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
      .asJson
      .noSpaces

  "Ledger JSON API list endpoints" should {
    "cap a huge client-supplied stream_idle_timeout_ms to the configured maximum" in httpTestFixture {
      fixture =>
        fixture.getUniquePartyAndAuthHeaders("Alice").flatMap { case (party, headers) =>
          val hugeIdleTimeoutMs = 1.hour.toMillis.toString
          val startNanos = System.nanoTime()
          postJsonStringRequestEncoded(
            fixture.uri
              .withPath(updatesPath)
              .withQuery(Uri.Query(("stream_idle_timeout_ms", hugeIdleTimeoutMs))),
            openEndedUpdatesBody(party.toString),
            headers,
          ).map { case (status, _) =>
            val elapsed = (System.nanoTime() - startNanos).nanos
            status should be(StatusCodes.OK)
            // In config we cap to 1 second -> but to avoid flakiness there is big buffer added
            // If it flakes feel free to add even more (as long as it is below 1 hour it means the cap works)
            elapsed should be < 30.seconds
          }
        }
    }
  }
}
