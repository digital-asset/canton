// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

private[http] object GrpcHttpErrorCodes {
  import com.google.rpc.Code as G
  import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes as A}

  implicit final class `gRPC status as pekko http`(private val self: G) extends AnyVal {
    // some version of this mapping _should_ already exist somewhere, right? -SC
    def asPekkoHttp: StatusCode = self match {
      case G.OK => A.OK
      case G.INVALID_ARGUMENT | G.FAILED_PRECONDITION | G.OUT_OF_RANGE => A.BadRequest
      case G.UNAUTHENTICATED => A.Unauthorized
      case G.PERMISSION_DENIED => A.Forbidden
      case G.NOT_FOUND => A.NotFound
      case G.ABORTED | G.ALREADY_EXISTS => A.Conflict
      case G.RESOURCE_EXHAUSTED => A.TooManyRequests
      case G.CANCELLED => ClientClosedRequest
      case G.DATA_LOSS | G.UNKNOWN | G.UNRECOGNIZED | G.INTERNAL => A.InternalServerError
      case G.UNIMPLEMENTED => A.NotImplemented
      case G.UNAVAILABLE => A.ServiceUnavailable
      case G.DEADLINE_EXCEEDED => A.GatewayTimeout
    }

    def asPekkoHttpForJsonApi: StatusCode = self match {
      case G.UNAUTHENTICATED | G.CANCELLED => A.InternalServerError
      case _ => self.asPekkoHttp
    }
  }

  private[this] val ClientClosedRequest =
    A.custom(
      499,
      "Client Closed Request",
      "The client closed the request before the server could respond.",
    )
}
