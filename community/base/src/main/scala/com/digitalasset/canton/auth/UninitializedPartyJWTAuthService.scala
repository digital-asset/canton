// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** This is a stub holding configuration for PartyJWTAuthService.
  *
  * At the point where we initialize other auth services, we don't have the dependencies to
  * initialize PartyJWTAuthService, since the latter depends on the topology and crypto API.
  *
  * Therefore, we initialize this instead, and replace it by the actual service in ApiServiceOwner.
  */
final case class UninitializedPartyJWTAuthService(
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    maxTokenLife: Option[Long] = None,
) extends AuthService {
  override def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[AuthService.Result] =
    // Never authenticate anything, this is not a real service.
    Future.successful(AuthService.Result(ClaimSet.Unauthenticated))
}
