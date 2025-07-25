// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.crypto.RandomOps
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.util.TimingSafeComparisonUtil.constantTimeEquals

import scala.concurrent.Future

final case class CantonAdminToken(secret: String)
object CantonAdminToken {
  def create(randomOps: RandomOps): CantonAdminToken = {
    val secret = HexString.toHexString(randomOps.generateRandomByteString(64))
    new CantonAdminToken(secret)
  }
}

/** AuthService interceptor used for internal canton services
  *
  * Internal Canton services such as the PingService or the DarService require access to the
  * Ledger-Api server. However, if the Ledger-Api server is configured with JWT, they will fail. But
  * we can't expect that Canton obtains an oauth token from a third party service during startup.
  *
  * Therefore, we create on each startup a master token which is only ever shared internally.
  */
class CantonAdminTokenAuthService(adminTokenO: Option[CantonAdminToken]) extends AuthService {
  override def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[ClaimSet] = {
    val bearerTokenRegex = "Bearer (.*)".r
    val authTokenOpt = for {
      adminToken <- adminTokenO
      authKey <- authToken
      token <- bearerTokenRegex.findFirstMatchIn(authKey).map(_.group(1))
      _ <- if (constantTimeEquals(token, adminToken.secret)) Some(()) else None
    } yield ()
    authTokenOpt
      .fold(deny)(_ => wildcard)
  }

  private val wildcard = Future.successful(ClaimSet.Claims.Wildcard: ClaimSet)
  private val deny = Future.successful(ClaimSet.Unauthenticated: ClaimSet)
}
