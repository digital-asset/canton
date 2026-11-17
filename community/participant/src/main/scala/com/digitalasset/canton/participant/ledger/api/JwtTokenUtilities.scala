// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.jwt.{
  AuthServiceJWTCodec,
  Jwt,
  JwtSigner,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.digitalasset.canton.user.store.UserManagementStore

import java.time.{Duration, Instant}

/** some helpers in other to use with ad-hoc JWT authentication */
object JwtTokenUtilities {

  /** Builds an HMAC256-signed token for the given audience (`aud` claim) for ad-hoc authentication.
    */
  def buildUnsafeToken(
      secret: String,
      audience: String,
      userId: Option[String] = None,
      exp: Option[Instant] = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
  ): String = {
    val payload = StandardJWTPayload(
      issuer = None,
      userId = userId.getOrElse(UserManagementStore.DefaultParticipantAdminUserId),
      participantId = None,
      exp = exp,
      format = StandardJWTTokenFormat.Audience,
      audiences = List(audience),
      scope = None,
    )
    // stolen from com.digitalasset.canton.ledger.api.auth.Main
    val jwtPayload = AuthServiceJWTCodec.compactPrint(payload)
    val signed: Jwt = JwtSigner
      .HMAC256(secret)
      .signPayload(jwtPayload)
      .fold(err => throw new RuntimeException(err.message), identity)
    signed.value
  }

}
