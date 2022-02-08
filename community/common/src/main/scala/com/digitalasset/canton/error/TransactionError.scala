// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.ErrorCode.truncateResourceForTransport
import com.daml.error.{ErrorCategory, ErrorClass, ErrorCode}
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.participant.state.v2.Update.CommandRejected.{
  FinalReason,
  RejectionReasonTemplate,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.rpc.code.Code
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.Status

abstract class ErrorCodeWithEnum[T](id: String, category: ErrorCategory, val protoCode: T)(implicit
    parent: ErrorClass
) extends ErrorCode(id, category) {
  override implicit val code: ErrorCodeWithEnum[T] = this
}

trait TransactionError extends BaseCantonError {

  @Deprecated
  def createRejectionDeprecated(
      rewrite: Map[ErrorCode, Status.Code]
  )(implicit loggingContext: ErrorLoggingContext): RejectionReasonTemplate = {
    FinalReason(_rpcStatus(rewrite.get(this.code)))
  }

  def createRejection(implicit loggingContext: ErrorLoggingContext): RejectionReasonTemplate = {
    FinalReason(rpcStatus)
  }

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)

  def rpcStatus(implicit loggingContext: ErrorLoggingContext): RpcStatus = _rpcStatus(None)

  def _rpcStatus(
      overrideCode: Option[Status.Code]
  )(implicit loggingContext: ErrorLoggingContext): RpcStatus = {

    // yes, this is a horrible duplication of ErrorCode.asGrpcError. why? because
    // scalapb does not really support grpc rich errors. there is literally no method
    // that supports turning scala com.google.rpc.status.Status into java com.google.rpc.Status
    // objects. however, the sync-api uses the scala variant whereas we have to return StatusRuntimeExceptions.
    // therefore, we have to compose the status code a second time here ...
    // the ideal fix would be to extend scalapb accordingly ...
    val ErrorCode.StatusInfo(codeGrpc, message, contextMap, correlationId) =
      code.getStatusInfo(this)(loggingContext)

    val definiteAnswerKey = com.daml.ledger.grpc.GrpcStatuses.DefiniteAnswerKey

    val metadata = if (code.category.securitySensitive) Map.empty[String, String] else contextMap
    val errorInfo = com.google.rpc.error_details.ErrorInfo(
      reason = code.id,
      metadata = metadata.updated(definiteAnswerKey, definiteAnswer.toString),
    )

    val retryInfoO = retryable.map { ri =>
      val dr = com.google.protobuf.duration.Duration(
        java.time.Duration.ofMillis(ri.duration.toMillis)
      )
      com.google.protobuf.any.Any.pack(com.google.rpc.error_details.RetryInfo(Some(dr)))
    }

    val requestInfoO = correlationId.map { ci =>
      com.google.protobuf.any.Any.pack(com.google.rpc.error_details.RequestInfo(requestId = ci))
    }

    val resourceInfos =
      if (code.category.securitySensitive) Seq()
      else
        truncateResourceForTransport(resources).map { case (rs, item) =>
          com.google.protobuf.any.Any
            .pack(
              com.google.rpc.error_details
                .ResourceInfo(resourceType = rs.asString, resourceName = item)
            )
        }

    val details = Seq[com.google.protobuf.any.Any](
      com.google.protobuf.any.Any.pack(errorInfo)
    ) ++ retryInfoO.toList ++ requestInfoO.toList ++ resourceInfos

    com.google.rpc.status.Status(
      overrideCode.getOrElse(codeGrpc).value(),
      message,
      details,
    )

  }

}

/** Transaction errors are derived from BaseCantonError and need to be logged explicitly */
abstract class TransactionErrorImpl(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    override val definiteAnswer: Boolean = false,
)(implicit override val code: ErrorCode)
    extends TransactionError

abstract class LoggingTransactionErrorImpl(
    cause: String,
    throwableO: Option[Throwable] = None,
    definiteAnswer: Boolean = false,
)(implicit code: ErrorCode, loggingContext: ErrorLoggingContext)
    extends TransactionErrorImpl(cause, throwableO, definiteAnswer)(code) {

  def log(): Unit = logWithContext()(loggingContext)

  // automatically log the error on generation
  log()
}

trait TransactionErrorWithEnum[T] extends TransactionError {
  override def code: ErrorCodeWithEnum[T]
}

/** Transaction errors are derived from BaseCantonError and need to be logged explicitly */
abstract class TransactionErrorWithEnumImpl[T](
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    override val definiteAnswer: Boolean = false,
)(implicit override val code: ErrorCodeWithEnum[T])
    extends TransactionErrorWithEnum[T]

trait TransactionParentError[T <: TransactionError]
    extends TransactionError
    with ParentCantonError[T]

object TransactionError {
  val NoErrorDetails = Seq.empty[com.google.protobuf.any.Any]
  val NotSupported: SubmissionResult.SynchronousError = SubmissionResult.SynchronousError(
    RpcStatus.of(Code.UNIMPLEMENTED.value, "Not supported", TransactionError.NoErrorDetails)
  )

  def internalError(reason: String): SubmissionResult.SynchronousError =
    SubmissionResult.SynchronousError(RpcStatus.of(Code.INTERNAL.value, reason, NoErrorDetails))

}
