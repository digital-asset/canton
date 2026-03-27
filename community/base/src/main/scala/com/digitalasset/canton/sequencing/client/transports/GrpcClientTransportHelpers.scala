// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicBoolean

trait GrpcClientTransportHelpers {
  this: FlagCloseable & NamedLogging =>

  /** Retry policy to retry once for authentication failures to allow re-authentication and
    * optionally retry when unavailable.
    */
  protected def retryPolicy(
      retryOnUnavailable: Boolean
  )(implicit traceContext: TraceContext): GrpcError => Boolean = {
    // we allow one retry if the failure was due to an auth token expiration
    // if it's not refresh upon the next call we shouldn't retry again
    val hasRetriedDueToTokenExpiration = new AtomicBoolean(false)

    error =>
      if (isClosing) false // don't even think about retrying if we're closing
      else
        error match {
          case requestRefused: GrpcError.GrpcRequestRefusedByServer
              if !hasRetriedDueToTokenExpiration
                .get() && requestRefused.isAuthenticationTokenMissing =>
            logger.info(
              "Retrying once to give the sequencer the opportunity to refresh the authentication token."
            )
            hasRetriedDueToTokenExpiration.set(true) // don't allow again
            true
          // Retrying to recover from transient failures, e.g.:
          // - network outages
          // - sequencer starting up during integration tests
          case _: GrpcServiceUnavailable => retryOnUnavailable
          // don't retry on anything else as the request may have been received and a subsequent send may cause duplicates
          case _ => false
        }
  }
}
