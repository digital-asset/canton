// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** An interface for authorizing the ledger API access to a participant.
  *
  * The AuthService is responsible for converting request metadata (such as the HTTP headers) into a
  * [[ClaimSet]]. These claims are then used by the ledger API server to check whether the request
  * is authorized.
  *
  *   - The authorization information MUST be specified in the `Authorization` header.
  *   - The value of the `Authorization` header MUST start with `Bearer ` (notice the trailing space
  *     of the prefix).
  *   - An [[AuthService]] implementation MAY use other headers when converting metadata to claims.
  *
  * For example, a participant could:
  *   - Ask all ledger API users to attach an `Authorization` header with a JWT token as the header
  *     value.
  *   - Implement `decodeMetadata()` such that it reads the JWT token from the corresponding HTTP
  *     header, validates the token, and converts the token payload to [[ClaimSet]].
  */
trait AuthService {

  /** Return empty [[com.digitalasset.canton.auth.ClaimSet.Unauthenticated]] to reject requests with
    * a UNAUTHENTICATED error status. Return [[com.digitalasset.canton.auth.ClaimSet.Claims]] with
    * only a single [[com.digitalasset.canton.auth.ClaimPublic]] claim to reject all non-public
    * requests with a PERMISSION_DENIED status. Return a failed future to reject requests with an
    * INTERNAL error status.
    * @param authToken
    *   The value of the `Authorization` header, (for instance http or grpc)
    */
  def decodeToken(authToken: Option[String], serviceName: String)(implicit
      traceContext: TraceContext
  ): Future[ClaimSet]

}
