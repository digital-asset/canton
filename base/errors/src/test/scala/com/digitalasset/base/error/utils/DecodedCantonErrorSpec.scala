// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error.utils

import com.digitalasset.base.error.utils.DecodedCantonError.{
  MaxUnvalidatedStringLength,
  MaxValidStatusMessageLength,
}
import com.digitalasset.base.error.{
  BaseError,
  BaseErrorLogger,
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorClass,
  ErrorCode,
  ErrorResource,
  NoBaseLogging,
}
import com.google.protobuf.any.Any
import com.google.protobuf.any.Any.toJavaProto
import com.google.rpc.error_details.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import com.google.rpc.status.Status as RpcStatus
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.util.{Failure, Success, Try}

import ErrorDetails.ErrorDetail

class DecodedCantonErrorSpec
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with OptionValues
    with ScalaCheckPropertyChecks {
  behavior of DecodedCantonError.getClass.getSimpleName

  it should s"correctly deserialize a gRPC status code to a ${BaseError.getClass.getSimpleName}" in
    forAll(errorCategoriesTable) { errorCategory =>
      forAll(propertyMapTable) { propertyMap =>
        forAll(correlationIdTable) { correlationId =>
          forAll(traceIdTable) { traceId =>
            val fromGrpc: RpcStatus => DecodedCantonError =
              DecodedCantonError.fromGrpcStatus(_).value
            val toGrpc: DecodedCantonError => RpcStatus = _.toRpcStatusWithForwardedRequestId

            val someGrpcStatus =
              createGrpcStatus(errorCategory, propertyMap, correlationId, traceId)

            checkEquivalence(someGrpcStatus, toGrpc(fromGrpc(someGrpcStatus)))
          }
        }
      }
    }

  it should "fallback to the original cause if error message format is not recognized" in {
    implicit val contextErrorLogger: BaseErrorLogger = NoBaseLogging

    val nonStandardMessage = "!!!Some non-standard message"
    val error = BenignError.Reject("nvm").rpcStatus().copy(message = nonStandardMessage)
    val deserializedError = DecodedCantonError.fromGrpcStatus(error).value

    deserializedError.cause shouldBe nonStandardMessage
  }

  private val errorGrpcStatus =
    createGrpcStatus(ErrorCategory.TransientServerFailure, Map("1" -> "2"), Some("c"), Some("t"))

  it should "return a Left on missing category information in error metadata" in {
    val errInfo = errorGrpcStatus.details.find(_ is ErrorInfo).value.unpack[ErrorInfo]
    val modifiedErrInfo = errInfo.copy(metadata = errInfo.metadata.removed("category"))
    val modifiedErrorGrpcStatus = errorGrpcStatus.copy(details =
      errorGrpcStatus.details.filterNot(_ is ErrorInfo) :+ Any.pack(modifiedErrInfo)
    )

    DecodedCantonError.fromGrpcStatus(modifiedErrorGrpcStatus).left.value should include(
      "category key not found in error metadata"
    )
  }

  // Error statuses can be received from untrusted parties (e.g. the reason of a LocalReject sent by
  // another participant, or a status returned by a remote gRPC server). Decoding must therefore never
  // throw on malformed input, but instead return a Left.

  private def replaceDetail[T <: GeneratedMessage](
      status: RpcStatus,
      newDetail: T,
  )(implicit companion: GeneratedMessageCompanion[T]): RpcStatus =
    status.copy(details = status.details.filterNot(_ is companion) :+ Any.pack(newDetail))

  private def withErrorInfoReason(reason: String): RpcStatus = {
    val errInfo = errorGrpcStatus.details.find(_ is ErrorInfo).value.unpack[ErrorInfo]
    replaceDetail(errorGrpcStatus, errInfo.copy(reason = reason))
  }

  private def withRetryDelay(seconds: Long, nanos: Int): RpcStatus =
    replaceDetail(
      errorGrpcStatus,
      RetryInfo(retryDelay = Some(com.google.protobuf.duration.Duration(seconds, nanos))),
    )

  private def assertDecodesToLeft(status: RpcStatus): Assertion =
    Try(DecodedCantonError.fromGrpcStatus(status)) match {
      case Success(Left(_)) => succeed
      case Success(Right(decoded)) => fail(s"Expected a Left, but decoding succeeded: $decoded")
      case Failure(ex) => fail(s"Expected a Left, but decoding threw $ex", ex)
    }

  it should "use a well-formed status as baseline for the malformed-input tests" in {
    errorGrpcStatus.details.exists(_ is RetryInfo) shouldBe true
    DecodedCantonError.fromGrpcStatus(withErrorInfoReason("VALID_ID_123")).isRight shouldBe true
    DecodedCantonError
      .fromGrpcStatus(withRetryDelay(seconds = 10L, nanos = 0))
      .isRight shouldBe true
  }

  it should s"return a Left on an empty ${classOf[ErrorInfo].getSimpleName} reason" in {
    assertDecodesToLeft(withErrorInfoReason(""))
  }

  it should s"return a Left on an ${classOf[ErrorInfo].getSimpleName} reason with invalid characters" in {
    assertDecodesToLeft(withErrorInfoReason("lower_case"))
    assertDecodesToLeft(withErrorInfoReason("WITH SPACE"))
    assertDecodesToLeft(withErrorInfoReason("WITH-DASH"))
  }

  it should s"return a Left on a too long ${classOf[ErrorInfo].getSimpleName} reason" in {
    assertDecodesToLeft(withErrorInfoReason("A" * 64))
    assertDecodesToLeft(withErrorInfoReason("A" * 10000))
  }

  // The embedded untrusted part is truncated to `MaxUnvalidatedStringLength`; allow some space for the fixed message prefix
  private val MaxDecodingErrorMessageLength = MaxUnvalidatedStringLength + 100

  it should "truncate decoding error messages that embed untrusted content" in {
    val longReason = "a" * 100000
    DecodedCantonError
      .fromGrpcStatus(withErrorInfoReason(longReason))
      .left
      .value
      .length should be <= MaxDecodingErrorMessageLength

    val errInfo = errorGrpcStatus.details.find(_ is ErrorInfo).value.unpack[ErrorInfo]
    val invalidCategory = replaceDetail(
      errorGrpcStatus,
      errInfo.copy(metadata = errInfo.metadata.updated("category", "x" * 100000)),
    )
    DecodedCantonError
      .fromGrpcStatus(invalidCategory)
      .left
      .value
      .length should be <= MaxDecodingErrorMessageLength

    val missingCategory = replaceDetail(
      errorGrpcStatus,
      errInfo.copy(metadata = errInfo.metadata.removed("category").updated("k", "v" * 100000)),
    )
    DecodedCantonError
      .fromGrpcStatus(missingCategory)
      .left
      .value
      .length should be <= MaxDecodingErrorMessageLength
  }

  it should s"return a Left on a ${classOf[RetryInfo].getSimpleName} delay exceeding the FiniteDuration range" in {
    // Valid java.time.Duration, but larger than Long.MaxValue nanoseconds
    assertDecodesToLeft(withRetryDelay(seconds = Long.MaxValue / 1_000_000_000L + 1, nanos = 0))
    assertDecodesToLeft(withRetryDelay(seconds = Long.MaxValue, nanos = 0))
  }

  it should s"return a Left on a ${classOf[RetryInfo].getSimpleName} delay overflowing java.time.Duration" in {
    // nanos are normalized into seconds, which overflows Long.MaxValue.
    // Such values are out of spec for google.protobuf.Duration (seconds within +-315,576,000,000,
    // nanos within +-999,999,999), but neither protobuf parsing nor ScalaPB's `asJavaDuration`
    // enforce these limits, so an attacker can still send them.
    assertDecodesToLeft(withRetryDelay(seconds = Long.MaxValue, nanos = Int.MaxValue))
  }

  it should s"return a Left on a negative ${classOf[RetryInfo].getSimpleName} delay below the FiniteDuration range" in {
    assertDecodesToLeft(withRetryDelay(seconds = Long.MinValue, nanos = 0))
  }

  // The status message is parsed with regexes (`RedactedMessage` and `ErrorCodeMsg`). As `.` does not
  // match line terminators, a long message with many partial matches followed by a newline causes
  // backtracking with quadratic runtime (a ~240KB message takes more than a minute). The decoder
  // therefore truncates the message before parsing it. Without truncation, the ~1MB messages used
  // below would take tens of minutes to decode.
  it should "decode a long ErrorCodeMsg-like status message without excessive backtracking" in {
    val message = "A(1," + "): " * 350000 + "\n"
    val decoded = DecodedCantonError.fromGrpcStatus(errorGrpcStatus.copy(message = message)).value
    decoded.cause.length should be <= MaxValidStatusMessageLength
  }

  it should "decode a long RedactedMessage-like status message without excessive backtracking" in {
    val message = BaseError.RedactedMessage.Prefix + " " + " with tid " * 100000 + "\n"
    val decoded = DecodedCantonError.fromGrpcStatus(errorGrpcStatus.copy(message = message)).value
    decoded.correlationId.forall(_.length <= MaxValidStatusMessageLength) shouldBe true
    decoded.traceId.forall(_.length <= MaxValidStatusMessageLength) shouldBe true
  }

  it should s"return a Left on invalid number of ${classOf[ErrorInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[ErrorInfo](
      "exactly one",
      Map(0 -> false, 1 -> true, 2 -> false),
    )
  }

  it should s"return a Left on invalid number of ${classOf[RequestInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[RequestInfo]("at most one", Map(0 -> true, 1 -> true, 2 -> false))
  }

  it should s"return a Left on invalid number of ${classOf[RetryInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[RetryInfo]("at most one", Map(0 -> true, 1 -> true, 2 -> false))
  }

  it should s"allow any number of ${classOf[ResourceInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[ResourceInfo]("doesn't matter", Map(0 -> true, 1 -> true, 2 -> true))
  }

  it should s"use ${ErrorCategory.UnredactedSecurityAlert} for an unredacted security alert" in {
    val status =
      createGrpcStatus(ErrorCategory.UnredactedSecurityAlert, Map.empty, Some("c"), Some("t"))
    val decoded = DecodedCantonError.fromGrpcStatus(status).value
    decoded.code.code.category shouldBe ErrorCategory.UnredactedSecurityAlert
  }

  it should s"handle redacted security alerts" in {
    val status = createGrpcStatus(ErrorCategory.SecurityAlert, Map.empty, Some("c"), Some("t"))
    val decoded = DecodedCantonError.fromGrpcStatus(status).value
    decoded.cause shouldBe "A security-sensitive error has been received"
    decoded.correlationId shouldBe Some("c")
    decoded.traceId shouldBe Some("t")
  }

  private def testErrorDetails[T <: GeneratedMessage](
      arityMsg: String,
      allowedNumbers: Map[Int, Boolean],
  )(implicit
      expectedTypeCompanion: GeneratedMessageCompanion[T]
  ): Assertion = {
    allowedNumbers.foreach { case (times, expectPass) =>
      val errorDetails = errorGrpcStatus.details
      val errDetail = errorDetails.find(_ is expectedTypeCompanion).value
      val modifiedStatus = errorGrpcStatus.copy(details =
        errorDetails.filterNot(_ is expectedTypeCompanion) ++ (1 to times).map(_ => errDetail)
      )

      val actual = DecodedCantonError.fromGrpcStatus(modifiedStatus)

      if (expectPass)
        actual.isRight shouldBe true
      else
        actual shouldBe Left(
          s"Could not extract error detail. Expected $arityMsg ${expectedTypeCompanion.scalaDescriptor.fullName} in status details, but got $times"
        )
    }
    succeed
  }

  private def createGrpcStatus(
      errorCategory: ErrorCategory,
      propertyMap: Map[String, String],
      correlationId: Option[String],
      traceId: Option[String],
  ) = {
    implicit val contextErrorLogger: NoBaseLogging =
      new NoBaseLogging(propertyMap, correlationId, traceId)
    implicit val errorCode: ErrorCode =
      new ErrorCode("SOME_ERROR_CODE_ID", errorCategory)(ErrorClass(List.empty)) {}
    {
      new DamlErrorWithDefiniteAnswer(
        cause = "Some cause",
        throwableO = Some(new RuntimeException("oups")),
        definiteAnswer = true,
        extraContext = Map("key" -> "val"),
      ) {
        override def resources: Seq[(ErrorResource, String)] =
          super.resources :+ (ErrorResource.CommandId -> "some-cmd-id")
      }
    }.rpcStatus()
  }

  private def propertyMapTable = Table(
    "property map",
    Map("p1" -> "k1", "p2" -> "k2"),
    Map("p1" -> "k1"),
    Map.empty[String, String],
  )

  private def correlationIdTable = Table(
    "correlation id",
    Some("corr-id"),
    None,
  )

  private def traceIdTable = Table("trace id", Some("trace-id"), None)

  private def errorCategoriesTable = {
    val grpcAwareErrorCats = ErrorCategory.all.filter(_.grpcCode.nonEmpty)
    Table(
      "error category",
      grpcAwareErrorCats*
    )
  }

  // Manual equality check for rpc statuses since encoding of maps (error details)
  // in ByteString is non-deterministic
  private def checkEquivalence(
      rpcStatusOriginal: RpcStatus,
      rpcStatusFromReconstructedError: RpcStatus,
  ): Assertion = {
    def refine(details: Seq[com.google.protobuf.any.Any]): Seq[ErrorDetail] =
      ErrorDetails.from(details.map(toJavaProto))

    rpcStatusFromReconstructedError.code shouldBe rpcStatusOriginal.code
    rpcStatusFromReconstructedError.message shouldBe rpcStatusOriginal.message
    val recontructedErrorDetails = refine(rpcStatusFromReconstructedError.details)
    val originalErrorDetails = refine(rpcStatusOriginal.details)
    recontructedErrorDetails shouldBe originalErrorDetails
  }
}
