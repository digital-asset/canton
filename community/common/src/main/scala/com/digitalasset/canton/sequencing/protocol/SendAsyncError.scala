// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.error.ErrorCategory
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0

/** Synchronous error returned by a sequencer. */
sealed trait SendAsyncError extends HasProtoV0[v0.SendAsyncResponse.Error] with PrettyPrinting {

  val message: String

  protected def toProtoReason: v0.SendAsyncResponse.Error.Reason

  override def toProtoV0: v0.SendAsyncResponse.Error =
    v0.SendAsyncResponse.Error(toProtoReason)

  override def pretty: Pretty[SendAsyncError] = prettyOfClass(unnamedParam(_.message.unquoted))

  def category: ErrorCategory

}

object SendAsyncError {

  /** The request could not be deserialized to be processed */
  case class RequestInvalid(message: String) extends SendAsyncError {
    override def toProtoReason = v0.SendAsyncResponse.Error.Reason.RequestInvalid(message)
    override def category: ErrorCategory = ErrorCategory.InvalidIndependentOfSystemState
  }

  /** The request server could read the request but refused to accept it */
  case class RequestRefused(message: String) extends SendAsyncError {
    override def toProtoReason = v0.SendAsyncResponse.Error.Reason.RequestRefused(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  /** The Sequencer is overloaded and declined to handle the request */
  case class Overloaded(message: String) extends SendAsyncError {
    override def toProtoReason = v0.SendAsyncResponse.Error.Reason.Overloaded(message)
    override def category: ErrorCategory = ErrorCategory.ContentionOnSharedResources
  }

  /** The sequencer is unable to process requests (if the service is running it could mean the sequencer is going through a crash recovery process) */
  case class Unavailable(message: String) extends SendAsyncError {
    override def toProtoReason = v0.SendAsyncResponse.Error.Reason.Unavailable(message)
    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  /** The Sequencer was unable to handle the send as the sender was unknown so could not asynchronously deliver them a deliver event or error */
  case class SenderUnknown(message: String) extends SendAsyncError {
    override def toProtoReason = v0.SendAsyncResponse.Error.Reason.SenderUnknown(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  /** The Sequencer declined to process new requests as it is shutting down */
  case class ShuttingDown(message: String = "Sequencer shutting down") extends SendAsyncError {
    override def toProtoReason = v0.SendAsyncResponse.Error.Reason.ShuttingDown(message)
    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  def fromProtoV0(
      error: v0.SendAsyncResponse.Error
  ): ParsingResult[SendAsyncError] =
    error.reason match {
      case v0.SendAsyncResponse.Error.Reason.Empty =>
        ProtoDeserializationError.FieldNotSet("SendAsyncResponse.error.reason").asLeft
      case v0.SendAsyncResponse.Error.Reason.RequestInvalid(message) =>
        RequestInvalid(message).asRight
      case v0.SendAsyncResponse.Error.Reason.RequestRefused(message) =>
        RequestRefused(message).asRight
      case v0.SendAsyncResponse.Error.Reason.Overloaded(message) => Overloaded(message).asRight
      case v0.SendAsyncResponse.Error.Reason.Unavailable(message) => Unavailable(message).asRight
      case v0.SendAsyncResponse.Error.Reason.SenderUnknown(message) =>
        SenderUnknown(message).asRight
      case v0.SendAsyncResponse.Error.Reason.ShuttingDown(message) => ShuttingDown(message).asRight
    }
}

case class SendAsyncResponse(error: Option[SendAsyncError])
    extends HasProtoV0[v0.SendAsyncResponse] {
  override def toProtoV0: v0.SendAsyncResponse =
    v0.SendAsyncResponse(error.map(_.toProtoV0))
}

object SendAsyncResponse {
  def fromProtoV0(
      responseP: v0.SendAsyncResponse
  ): ParsingResult[SendAsyncResponse] =
    for {
      error <- responseP.error.traverse(SendAsyncError.fromProtoV0)
    } yield SendAsyncResponse(error)
}
