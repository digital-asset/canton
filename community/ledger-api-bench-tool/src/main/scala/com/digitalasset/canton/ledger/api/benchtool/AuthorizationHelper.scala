// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool

import com.daml.grpc.AuthCallCredentials
import com.daml.jwt.{
  AuthServiceJWTCodec,
  DecodedJwt,
  JwtSigner,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import io.grpc.stub.AbstractStub

object AuthorizationHelper {
  def maybeAuthedService[T <: AbstractStub[T]](userTokenO: Option[String])(service: T): T =
    userTokenO.fold(service)(token => AuthCallCredentials.authorizingStub(service, token))
}

/** @param audience
  *   the audience (`aud` claim) of the minted tokens, as configured by `target-audience` on the
  *   participant
  */
class AuthorizationHelper(authorizationTokenSecret: String, audience: String) {

  /** @return
    *   audience-based user token signed with HMAC256
    */
  def tokenFor(userId: String): String = {
    val payload = StandardJWTPayload(
      issuer = None,
      participantId = None,
      userId = userId,
      exp = None,
      format = StandardJWTTokenFormat.Audience,
      audiences = List(audience),
      scope = None,
    )
    JwtSigner
      .HMAC256(authorizationTokenSecret)
      .sign(
        jwt = DecodedJwt(
          header = """{"alg": "HS256", "typ": "JWT"}""",
          payload = AuthServiceJWTCodec.compactPrint(payload),
        )
      )
      .getOrElse(sys.error("Failed to generate token"))
      .value
  }

}
