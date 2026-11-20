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
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.ServerConfig
import com.digitalasset.canton.http.json.v2.JsUpdateServiceCodecs.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.getResponseDataString
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.ParticipantSelector
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import io.circe.syntax.*
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import org.scalatest.Assertion
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Future
import scala.concurrent.duration.*

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
  *
  * The same limit bounds every single inbound websocket message: a (possibly fragmented) message
  * exceeding it is rejected and the connection closed, before the fragments are assembled.
  */
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

  private val updatesPath = Uri.Path("/v2/updates")

  /** A well-formed updates request that the Ledger API rejects (negative `beginExclusive`), so the
    * server replies with a single error message once the request was assembled and decoded. The
    * JSON is left-padded with `padding` whitespace characters to control its size.
    */
  private def paddedInvalidUpdatesRequest(padding: Int): String = {
    val json = update_service
      .GetUpdatesRequest(
        beginExclusive = -1,
        endInclusive = None,
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map.empty,
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = false,
                    filtersByContractId = Nil,
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
    " " * padding + json
  }

  /** Sends the text as one websocket message split into many fragments (continuation frames). */
  private def fragmented(text: String, fragmentSize: Int = 128): TextMessage =
    TextMessage.Streamed(Source(text.grouped(fragmentSize).toList))

  /** Sends a single message on a fresh websocket and returns all replies. The client never closes
    * the connection itself, so the returned future only completes once the server closes it.
    */
  private def sendWebsocketMessage(
      uri: Uri,
      jwt: com.daml.jwt.Jwt,
      message: Message,
  ): Future[Seq[String]] = {
    val (upgrade, replies) = Source
      .single(message)
      .concatMat(Source.maybe[Message])(Keep.left)
      .viaMat(websocket(uri, jwt))(Keep.right)
      .mapAsync(1) {
        case m: TextMessage => m.toStrict(10.seconds).map(_.text)
        case other => Future.failed(new IllegalStateException(s"unexpected message $other"))
      }
      .toMat(Sink.seq)(Keep.both)
      .run()
    upgrade.flatMap { u =>
      u.response.status shouldBe StatusCodes.SwitchingProtocols
      replies
    }
  }

  private val webSocketTimeout = timeout(Span(30, Seconds))

  /** Sends only the request headers, with `Content-Length` set to `declaredSize`.
    *
    * Trick: pekko-http rejects an oversized request from the declared size alone. Sending the body
    * too can lose the 413 to a broken pipe.
    */
  private def postJsonHeadersOnly(uri: Uri, declaredSize: Int): Future[(StatusCode, String)] =
    singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        headers = noAuth,
        entity = HttpEntity.Default(
          ContentTypes.`application/json`,
          declaredSize.toLong,
          Source.maybe[ByteString],
        ),
      )
    ).flatMap(resp => getResponseDataString(resp, debug = true).map(body => (resp.status, body)))

  /** Posts an oversized body (expecting 413 naming `limit`) and a body of `withinLimitPadding`
    * bytes (expecting any status other than 413) to the JSON API of the selected participant.
    */
  private def assertLimitEnforced(
      participant: ParticipantSelector,
      limit: NonNegativeInt,
      withinLimitPadding: Int,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    val oversizedBytes = limit.unwrap + 1024
    val withinLimit = jsonOfPadding(withinLimitPadding)
    val withinLimitBytes = withinLimit.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    withinLimitBytes should be < limit.unwrap

    (for {
      http <- adHocHttp(participant)
      (rejectedStatus, rejectedBody) <- postJsonHeadersOnly(
        http.uri withPath submitPath,
        oversizedBytes,
      )
      (acceptedStatus, _) <- postJsonStringRequestEncoded(
        http.uri withPath submitPath,
        withinLimit,
        noAuth,
      )
    } yield {
      // Oversized request: rejected by size, before routing/auth. pekko-http names the configured
      // limit in the EntityStreamSizeException message ("... exceeded size limit (<limit> bytes)!").
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

    "process a fragmented websocket message within the limit" in { implicit env =>
      val replies = (for {
        http <- adHocHttp(_.participant1)
        jwt <- http.jwt(http.uri)
        replies <- sendWebsocketMessage(
          http.uri withPath updatesPath,
          jwt,
          fragmented(paddedInvalidUpdatesRequest(jsonApiLimit.unwrap / 2)),
        )
      } yield replies).futureValue(webSocketTimeout)

      // The request was assembled, decoded and processed: the Ledger API rejects the offset.
      inside(replies) { case Seq(error) => error should include("NEGATIVE_OFFSET") }
    }

    "reject a fragmented websocket message above the limit and close the connection" in {
      implicit env =>
        val message = paddedInvalidUpdatesRequest(jsonApiLimit.unwrap * 8)
        message.length should be > jsonApiLimit.unwrap
        val replies = (for {
          http <- adHocHttp(_.participant1)
          jwt <- http.jwt(http.uri)
          replies <- sendWebsocketMessage(http.uri withPath updatesPath, jwt, fragmented(message))
        } yield replies).futureValue(webSocketTimeout)

        // Rejected before decoding (no NEGATIVE_OFFSET), and the server closed the connection.
        inside(replies) { case Seq(error) =>
          error should include("JSON_API_WEBSOCKET_MESSAGE_TOO_LARGE")
          error should include(jsonApiLimit.unwrap.toString)
        }
    }

    "close the connection on an endless fragmented websocket message" in { implicit env =>
      val sentBytes = new AtomicLong(0L)
      val chunk = " " * 4096
      val endless = TextMessage.Streamed(
        Source
          .single("{")
          .concat(Source.repeat(chunk).map { c =>
            sentBytes.addAndGet(c.length.toLong)
            c
          })
      )
      val replies = (for {
        http <- adHocHttp(_.participant1)
        jwt <- http.jwt(http.uri)
        replies <- sendWebsocketMessage(http.uri withPath updatesPath, jwt, endless)
      } yield replies).futureValue(webSocketTimeout)

      logger.info(s"Endless websocket message rejected after sending ${sentBytes.get()} bytes")
      inside(replies) { case Seq(error) =>
        error should include("JSON_API_WEBSOCKET_MESSAGE_TOO_LARGE")
      }
    }

    "reject an endless binary websocket message and close the connection" in { implicit env =>
      val endless = BinaryMessage.Streamed(Source.repeat(ByteString(Array.fill(4096)(' '.toByte))))
      val replies = (for {
        http <- adHocHttp(_.participant1)
        jwt <- http.jwt(http.uri)
        replies <- sendWebsocketMessage(http.uri withPath updatesPath, jwt, endless)
      } yield replies).futureValue(webSocketTimeout)

      inside(replies) { case Seq(error) =>
        error should include("JSON_API_WEBSOCKET_BINARY_MESSAGE_NOT_SUPPORTED")
      }
    }
  }
}
