// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Internal TEA-only AuthService that grants read-only visibility across all parties.
  *
  * Resolves a valid token to [[ClaimPublic]] + [[ClaimReadAsAnyParty]], satisfying the
  * `GetCompletions` call TEA makes with an empty parties list.
  *
  * Using a dedicated class rather than tweaking [[CantonAdminTokenAuthService]] because the knobs
  * that would unlock [[ClaimReadAsAnyParty]] or [[ClaimActAsAnyParty]] there are gated behind
  * `non-standard-config`, and we want no operator action required. [[ClaimActAsAnyParty]] is also
  * broader than needed since TEA only reads the completions stream and never submits commands.
  */
class TeaTokenAuthService(tokenDispenser: CantonAdminTokenDispenser) extends AuthService {
  override def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit traceContext: TraceContext): Future[ClaimSet] = {
    val prefix = "Bearer "
    val isValid = authToken
      .filter(_.startsWith(prefix))
      .map(_.stripPrefix(prefix))
      .exists(tokenDispenser.checkToken)
    if (isValid) permit else deny
  }

  private val permit = Future.successful(
    ClaimSet.Claims.Empty.copy(
      claims = List[Claim](ClaimPublic, ClaimReadAsAnyParty)
    )
  )

  private val deny = Future.successful(ClaimSet.Unauthenticated: ClaimSet)
}
