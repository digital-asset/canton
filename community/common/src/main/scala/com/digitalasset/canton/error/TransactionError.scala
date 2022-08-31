// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

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
  )(implicit loggingContext: ErrorLoggingContext): RejectionReasonTemplate =
    FinalReason(rpcStatus(rewrite.get(this.code)))

  def createRejection(implicit loggingContext: ErrorLoggingContext): RejectionReasonTemplate =
    FinalReason(rpcStatus())

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  /** Parameter has no effect at the moment, as submission ranks are not supported.
    * Setting to false for the time being.
    */
  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)
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
