// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2.admin.user_management_service.{CreateUserRequest, User}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.JsUserManagementCodecs.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.{
  authorizationHeader,
  getResponseDataString,
}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.error.JsonApiErrors
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.google.protobuf
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.pattern
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.Assertion

import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.*

import JsonDarUploadTest.StalledUpload

/** Exercises the DAR endpoints of the JSON Ledger API: `POST /v2/dars`, `POST /v2/dars/validate`
  * and the legacy `POST /v2/packages`.
  *
  * The endpoints are admin-only, so the suite runs with JWT authentication instead of the wildcard
  * authentication of the other JSON API suites.
  *
  * A "stalled" upload sends part of its body and then goes quiet without closing the connection.
  * The server must not hold a thread while it waits for the rest (#37359).
  */
class JsonDarUploadTest
    extends CantonFixture
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken {

  registerPlugin(JwtAuthConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val uploadPath = Uri.Path("/v2/dars")
  private val validatePath = Uri.Path("/v2/dars/validate")
  private val legacyUploadPath = Uri.Path("/v2/packages")
  private val darPaths = Seq(uploadPath, validatePath, legacyUploadPath)

  private lazy val dar: protobuf.ByteString = {
    val darPath = Paths.get(
      getClass.getClassLoader
        .getResource(s"test-${LanguageVersion.latestStableLfVersion.pretty}.dar")
        .toURI
    )
    protobuf.ByteString.copyFrom(Files.readAllBytes(darPath))
  }

  /** Comfortably above the size of the test DAR. */
  private val maxInboundMessageSize = 256 * 1024

  private lazy val adminHeaders: List[HttpHeader] = headersWithAdminAuth

  private def nonAdminHeaders(fixture: HttpServiceTestFixtureData): Future[List[HttpHeader]] = {
    val userId = s"non-admin-${UUID.randomUUID()}"
    val user = User.defaultInstance.copy(id = userId)
    fixture
      .postJsonRequest(
        Uri.Path("/v2/users"),
        CreateUserRequest(user = Some(user), rights = Nil).asJson,
        adminHeaders,
      )
      .map { case (status, _) =>
        status shouldBe StatusCodes.OK
        authorizationHeader(Jwt(toHeader(standardToken(userId))))
      }
  }

  /** More uploads than pekko's default dispatcher has threads (`min(max(cores, 8), 64)`), so a
    * server that blocks a thread per upload would stop responding.
    */
  private val stalledUploadCount =
    math.min(math.max(Runtime.getRuntime.availableProcessors(), 8), 64) + 4

  /** The default client pool allows 4 connections per host and would queue the stalled uploads. */
  private lazy val wideConnectionPool: ConnectionPoolSettings =
    ConnectionPoolSettings(system).withMaxConnections(128).withMaxOpenRequests(256)

  /** Starts an upload that sends `sent` and then stalls. A non-chunked upload declares a few bytes
    * more than it sends; a chunked one declares no size at all.
    */
  private def stalledUpload(
      uri: Uri,
      headers: List[HttpHeader],
      sent: ByteString = ByteString("d"),
      chunked: Boolean = false,
  ): StalledUpload = {
    val (rest, restSource) = Source.maybe[ByteString].preMaterialize()
    val data = Source.single(sent).concat(restSource)
    val entity: RequestEntity =
      if (chunked) HttpEntity.Chunked.fromData(ContentTypes.`application/octet-stream`, data)
      else HttpEntity.Default(ContentTypes.`application/octet-stream`, sent.length + 3L, data)
    val response = Http()
      .singleRequest(
        HttpRequest(method = HttpMethods.POST, uri = uri, headers = headers, entity = entity),
        settings = wideConnectionPool,
      )
      .flatMap(response =>
        getResponseDataString(response, debug = true).map(body => (response.status, body))
      )
    StalledUpload(
      response,
      () => rest.tryFailure(new RuntimeException("The test released the stalled upload")).discard,
    )
  }

  /** Runs `check` on the responses to a stalled upload to each DAR endpoint. The uploads start
    * inside `check` and are released after it, so its log assertions see only the rejections.
    */
  private def withStalledUploads[A](
      fixture: HttpServiceTestFixtureData,
      headers: List[HttpHeader],
  )(check: (=> Seq[(StatusCode, String)]) => A): A = {
    val started = new AtomicReference(Seq.empty[StalledUpload])
    lazy val responses: Seq[(StatusCode, String)] = {
      val uploads = darPaths.map(path => stalledUpload(fixture.uri withPath path, headers))
      started.set(uploads)
      Future.traverse(uploads)(_.response).futureValue
    }
    try check(responses)
    finally started.get().foreach(_.release())
  }

  /** The error body of a rejection is redacted, so only the HTTP status and the gRPC code can be
    * checked.
    */
  private def assertRejected(
      responses: Seq[(StatusCode, String)],
      status: StatusCode,
      grpcStatus: Status.Code,
  ): Assertion = {
    responses.map(_._1) shouldBe Seq.fill(responses.size)(status)
    forAll(responses.map(_._2)) { body =>
      decode[JsCantonError](body).map(_.grpcCodeValue) shouldBe Right(Some(grpcStatus.value()))
    }
  }

  private def assertWarnings(errorCode: String): Seq[LogEntry] => Assertion =
    LogEntry.assertLogSeq(
      Seq((_.warningMessage should include(errorCode), s"the rejection is logged as $errorCode"))
    )

  "the DAR endpoints" should {

    "upload and validate a DAR for an admin" in httpTestFixture { fixture =>
      Future
        .traverse(darPaths)(path => fixture.postBinaryContent(path, dar, adminHeaders))
        .map(responses => responses.map(_._1) shouldBe Seq.fill(darPaths.size)(StatusCodes.OK))
    }

    "reject an unauthenticated request without waiting for its body" in httpTestFixture { fixture =>
      Future.successful(
        withStalledUploads(fixture, List.empty) { responses =>
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            assertRejected(responses, StatusCodes.Unauthorized, Status.Code.UNAUTHENTICATED),
            assertWarnings("UNAUTHENTICATED"),
          )
        }
      )
    }

    "reject a request without the admin claim without waiting for its body" in httpTestFixture {
      fixture =>
        nonAdminHeaders(fixture).map { headers =>
          withStalledUploads(fixture, headers) { responses =>
            loggerFactory.assertLoggedWarningsAndErrorsSeq(
              assertRejected(responses, StatusCodes.Forbidden, Status.Code.PERMISSION_DENIED),
              assertWarnings("PERMISSION_DENIED"),
            )
          }
        }
    }

    "reject an admin token of another participant without waiting for its body" in httpTestFixture {
      fixture =>
        val otherParticipantAdmin = standardToken(participantAdmin, participantId = Some("other"))
        val headers = authorizationHeader(Jwt(toHeader(otherParticipantAdmin)))
        Future.successful(
          withStalledUploads(fixture, headers) { responses =>
            loggerFactory.assertLoggedWarningsAndErrorsSeq(
              assertRejected(responses, StatusCodes.Forbidden, Status.Code.PERMISSION_DENIED),
              assertWarnings("PERMISSION_DENIED"),
            )
          }
        )
    }

    "keep serving uploads while many authorized uploads stall" in httpTestFixture { fixture =>
      val stalled = (1 to stalledUploadCount).map(_ =>
        stalledUpload(fixture.uri withPath uploadPath, adminHeaders)
      )
      for {
        // Give the stalled uploads a head start, so that the server holds all of them when the
        // regular upload arrives
        (status, _) <- pattern.after(500.millis, system.scheduler)(
          fixture.postBinaryContent(uploadPath, dar, adminHeaders)
        )
        stillStalled = stalled.count(!_.response.isCompleted)
        _ = stalled.foreach(_.release())
        _ <- Future.traverse(stalled)(_.response.map(_ => ()).recover { case _ => () })
      } yield {
        status shouldBe StatusCodes.OK
        stillStalled shouldBe stalledUploadCount
      }
    }

    "reject a chunked upload that outgrows the size limit with 413" in httpTestFixture { fixture =>
      // The upload stalls after one byte more than the limit. If it kept sending, the client could
      // fail with a broken pipe before it reads the 413.
      val oversized = stalledUpload(
        fixture.uri withPath uploadPath,
        adminHeaders,
        sent = ByteString(Array.fill[Byte](maxInboundMessageSize + 1)(0)),
        chunked = true,
      )
      oversized.response
        .map { case (status, body) =>
          status shouldBe StatusCodes.ContentTooLarge
          decode[JsCantonError](body).map(_.code) shouldBe Right(
            JsonApiErrors.RequestBodyTooLarge.id
          )
        }
        .thereafter(_ => oversized.release())
    }
  }

  /** Accepts tokens with the expected audience and tokens issued for a participant, and caps the
    * request size at `maxInboundMessageSize`.
    */
  private case class JwtAuthConfig(protected val loggerFactory: NamedLoggerFactory)
      extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.authServices)
          .replace(
            Seq(Some(ExpectedAudience), None).map(targetAudience =>
              AuthServiceConfig.UnsafeJwtHmac256(
                secret = jwtSecret,
                targetAudience = targetAudience,
                targetScope = None,
              )
            )
          )
          .focus(_.httpLedgerApi.maxInboundMessageSize)
          .replace(Some(NonNegativeInt.tryCreate(maxInboundMessageSize)))
      }(config)
  }
}

object JsonDarUploadTest {

  /** An upload that stalled after its first bytes.
    *
    * @param response
    *   the response, or the failure of the request once released
    * @param release
    *   aborts the upload, which closes the connection
    */
  private final case class StalledUpload(
      response: Future[(StatusCode, String)],
      release: () => Unit,
  )
}
