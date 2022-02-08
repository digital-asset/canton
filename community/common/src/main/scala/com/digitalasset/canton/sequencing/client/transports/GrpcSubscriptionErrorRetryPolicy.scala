// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.sequencing.client.CheckedSubscriptionErrorRetryPolicy
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

class GrpcSubscriptionErrorRetryPolicy(protected val loggerFactory: NamedLoggerFactory)
    extends CheckedSubscriptionErrorRetryPolicy[GrpcSubscriptionError]
    with NamedLogging {
  override protected def retryInternal(error: GrpcSubscriptionError, receivedItems: Boolean)(
      implicit traceContext: TraceContext
  ): Boolean = {
    error.grpcError match {
      case _: GrpcError.GrpcServiceUnavailable =>
        val causes = Seq(error.grpcError.status.getDescription) ++ GrpcError.collectCauses(
          Option(error.grpcError.status.getCause)
        )
        logger.info(
          s"Trying to reconnect to give the sequencer the opportunity to become available again (after ${causes
            .mkString(", ")})"
        )
        true

      case error: GrpcError.GrpcRequestRefusedByServer =>
        val retry = error.isAuthenticationTokenMissing
        if (retry)
          logger.info(
            s"Trying to reconnect to give the sequencer the opportunity to refresh the authentication token."
          )
        else
          logger.debug("Not trying to reconnect.")
        retry

      case serverError: GrpcError.GrpcServerError
          if receivedItems && serverError.status.getCode == Status.INTERNAL.getCode =>
        // a connection reset by an intermediary can cause GRPC to raise an INTERNAL error.
        // (this is seen when the GCloud load balancer times out subscriptions on the global domain)
        // if we've received any items during the course of the subscription we will assume its fine to reconnect.
        // if there is actually an application issue with the server, we'd expect it to immediately fail and then
        // it will not retry its connection
        logger.debug(
          s"After successfully receiving some events the sequencer subscription received an error. Retrying subscription."
        )
        true

      case serverError: GrpcError.GrpcServerError
          if receivedItems && serverError.status.getCode == Status.UNKNOWN.getCode =>
        serverError.status.getCause match {
          case _: java.nio.channels.ClosedChannelException =>
            // In this conversation https://gitter.im/grpc/grpc?at=5f464aa854288c687ee06a25
            // someone who maintains the grpc codebase explains:
            // "'Channel closed' is when we have no knowledge as to what went wrong; it could be anything".
            // In practice, we've seen this peculiar error sometimes appear when the sequencer goes unavailable,
            // so let's make sure to retry.
            logger.debug(
              s"Closed channel exception can appear when the server becomes unavailable. Retrying."
            )
            true
          case _ => false
        }

      case _: GrpcError.GrpcClientGaveUp | _: GrpcError.GrpcClientError |
          _: GrpcError.GrpcServerError =>
        logger.info("Not reconnecting.")
        false
    }
  }
}
