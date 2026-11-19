// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.base.error.{
  BaseErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorClass,
  ErrorCode,
  ErrorResource,
  NoBaseLogging,
}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.DirectExecutionContext.directExecutionContext
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import sttp.model.StatusCode

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Errors whose [[com.digitalasset.base.error.ErrorCategory.redactDetails]] is set must not expose
  * any detail on the API. The gRPC Ledger API enforces this in `SerializableErrorCodeComponents`;
  * these tests pin the same guarantee for the JSON Ledger API.
  */
class JsCantonErrorRedactionTest extends AnyWordSpec with Matchers with Inside with BaseTest {

  import JsCantonErrorRedactionTest.*

  private def mustNotLeak(error: JsCantonError): Unit = {
    error.cause should not include Secret
    error.code should not include Secret
    error.context.values.mkString should not include Secret
    error.resources.map(_._2).mkString should not include Secret
    error.context shouldBe empty
    error.resources shouldBe empty
  }

  "JsCantonError.redacted" should {
    "construct a redacted error without identifiers" in {
      val error = JsCantonError.redacted(Status.Code.PERMISSION_DENIED, None, None)
      error shouldBe JsCantonError(
        code = "NA",
        cause = "A security-sensitive error has been received",
        correlationId = None,
        traceId = None,
        context = Map.empty,
        resources = Seq.empty,
        errorCategory = -1,
        grpcCodeValue = Some(Status.Code.PERMISSION_DENIED.value()),
        retryInfo = None,
        definiteAnswer = None,
      )
    }
  }

  "JsCantonError.fromErrorCode" should {

    "redact the details of an internal (security sensitive) error" in {
      val error = JsCantonError.fromErrorCode(RedactedInternalError.Reject(Secret)(noLogging))
      mustNotLeak(error)
      error shouldBe JsCantonError(
        code = "NA",
        cause = "A security-sensitive error has been received",
        correlationId = Some("corrId"),
        traceId = Some("tid"),
        context = Map.empty,
        resources = Seq.empty,
        errorCategory = -1,
        grpcCodeValue = Some(Status.Code.INTERNAL.value()),
        retryInfo = None,
        definiteAnswer = None,
      )
    }

    "redact the details of a permission denied error" in {
      val error = JsCantonError.fromErrorCode(RedactedPermissionError.Reject(Secret)(noLogging))
      mustNotLeak(error)
      error.grpcCodeValue shouldBe Some(Status.Code.PERMISSION_DENIED.value())
    }

    "keep the details of an error whose category does not redact" in {
      val error = JsCantonError.fromErrorCode(PlainError.Reject("this is visible")(noLogging))
      error.code shouldBe "TEST_PLAIN_ERROR"
      error.cause shouldBe "this is visible"
      error.context should contain("ck" -> "cv")
      error.resources should contain(ErrorResource.DevErrorType.asString -> "some/resource")
      error.errorCategory shouldBe ErrorCategory.InvalidIndependentOfSystemState.asInt
      error.grpcCodeValue shouldBe Some(Status.Code.INVALID_ARGUMENT.value())
    }
  }

  "Endpoints error handling" should {

    "redact an unexpected exception" in {
      loggerFactory.assertLogs(
        inside(handle(Failure(new RuntimeException(Secret)))) {
          case Success(Left((status, error))) =>
            status shouldBe StatusCode.InternalServerError
            mustNotLeak(error)
            error.grpcCodeValue shouldBe Some(Status.Code.INTERNAL.value())
        },
        _.errorMessage should include("LEDGER_API_INTERNAL_ERROR"),
      )
    }

    "redact a legacy StatusRuntimeException with a sensitive status code" in {
      val sre = new StatusRuntimeException(Status.INTERNAL.withDescription(Secret))
      inside(handle(Failure(sre))) { case Success(Left((status, error))) =>
        status shouldBe StatusCode.InternalServerError
        mustNotLeak(error)
        error shouldBe JsCantonError(
          code = "NA",
          cause = "A security-sensitive error has been received",
          correlationId = None,
          traceId = None,
          context = Map.empty,
          resources = Seq.empty,
          errorCategory = -1,
          grpcCodeValue = Some(Status.Code.INTERNAL.value()),
          retryInfo = None,
          definiteAnswer = None,
        )
      }
    }

    "keep the description of a legacy StatusRuntimeException with a non sensitive status code" in {
      val sre = new StatusRuntimeException(Status.NOT_FOUND.withDescription("resource is missing"))
      inside(handle(Failure(sre))) { case Success(Left((status, error))) =>
        status shouldBe StatusCode.NotFound
        error.cause should include("resource is missing")
        error.grpcCodeValue shouldBe Some(Status.Code.NOT_FOUND.value())
      }
    }
  }

  private def handle(
      result: Try[Either[JsCantonError, Unit]]
  ): Try[Either[(StatusCode, JsCantonError), Unit]] =
    new TestEndpoints(loggerFactory, directExecutionContext).handleError(result)

  private class TestEndpoints(
      val loggerFactory: NamedLoggerFactory,
      override protected implicit val executionContext: ExecutionContext,
  ) extends Endpoints {

    override protected val requestLogger: ApiRequestLogger =
      new ApiRequestLogger(ApiLoggingConfig(), loggerFactory)

    def handleError(
        result: Try[Either[JsCantonError, Unit]]
    ): Try[Either[CustomError, Unit]] =
      handleErrorResponse[Unit](TraceContext.empty)(result)
  }
}

object JsCantonErrorRedactionTest {

  private val Secret = "a-secret-internal-detail"

  private val noLogging: BaseErrorLogger =
    new NoBaseLogging(properties = Map.empty, correlationId = Some("corrId"), traceId = Some("tid"))

  private implicit val errorClass: ErrorClass = ErrorClass.root()

  private object RedactedInternalError
      extends ErrorCode("TEST_INTERNAL_ERROR", ErrorCategory.SystemInternalAssumptionViolated) {
    final case class Reject(override val cause: String)(implicit logger: BaseErrorLogger)
        extends DamlErrorWithDefiniteAnswer(cause = cause, extraContext = Map("ck" -> Secret)) {
      override def resources: Seq[(ErrorResource, String)] =
        Seq((ErrorResource.DevErrorType, Secret))
    }
  }

  private object RedactedPermissionError
      extends ErrorCode("TEST_PERMISSION_ERROR", ErrorCategory.InsufficientPermission) {
    final case class Reject(override val cause: String)(implicit logger: BaseErrorLogger)
        extends DamlErrorWithDefiniteAnswer(cause = cause)
  }

  private object PlainError
      extends ErrorCode("TEST_PLAIN_ERROR", ErrorCategory.InvalidIndependentOfSystemState) {
    final case class Reject(override val cause: String)(implicit logger: BaseErrorLogger)
        extends DamlErrorWithDefiniteAnswer(cause = cause, extraContext = Map("ck" -> "cv")) {
      override def resources: Seq[(ErrorResource, String)] =
        Seq((ErrorResource.DevErrorType, "some/resource"))
    }
  }
}
