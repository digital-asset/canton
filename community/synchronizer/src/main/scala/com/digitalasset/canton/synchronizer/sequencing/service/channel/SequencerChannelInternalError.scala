// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.channel.errors.SequencerChannelErrors
import io.grpc.{Status, StatusRuntimeException}
import org.slf4j.event.Level

/** Errors of the sequencer channel pool and its channels.
  *
  * Mapped to a gRPC StatusRuntimeException at the API boundary via
  * [[SequencerChannelInternalError.toGrpcError]].
  */
private[channel] sealed abstract class SequencerChannelInternalError(val level: Level)
    extends Product
    with Serializable {
  def message: String
}

private[channel] object SequencerChannelInternalError {

  /** Client sent something invalid: bad metadata, member mismatch, duplicate channel.
    *
    * @param message
    *   detailed message
    */
  final case class InvalidRequest(override val message: String)
      extends SequencerChannelInternalError(Level.INFO)
  final case class PermissionDenied(override val message: String)
      extends SequencerChannelInternalError(Level.WARN)
  case object PoolCapacityExhausted extends SequencerChannelInternalError(Level.INFO) {
    override val message: String =
      "Sequencer channel pool has reached its maximum capacity. Try again later."
  }
  case object PoolClosed extends SequencerChannelInternalError(Level.INFO) {
    override val message: String = "Sequencer channel pool closed."
  }

  case object PendingConnectionClosed extends SequencerChannelInternalError(Level.DEBUG) {
    override val message: String =
      "Pending connection was closed before its initialization completed."
  }

  def toGrpcError(
      err: SequencerChannelInternalError
  )(implicit loggingContext: ErrorLoggingContext): StatusRuntimeException =
    err match {
      case PoolCapacityExhausted =>
        SequencerChannelErrors.PoolCapacityExhausted.Error().asGrpcError
      case other =>
        plainStatus(other).withDescription(other.message).asRuntimeException()
    }

  private def plainStatus(err: SequencerChannelInternalError): Status =
    err match {
      case _: InvalidRequest => Status.INVALID_ARGUMENT
      case _: PermissionDenied => Status.PERMISSION_DENIED
      case _ => Status.UNAVAILABLE
    }
}
