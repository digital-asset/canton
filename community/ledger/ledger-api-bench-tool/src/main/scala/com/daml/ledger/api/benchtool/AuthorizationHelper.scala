// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.digitalasset.canton.ledger.api.auth.client.LedgerCallCredentials
import com.digitalasset.canton.ledger.api.auth.{
  AuthServiceJWTCodec,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import io.grpc.stub.AbstractStub

object AuthorizationHelper {
  def maybeAuthedService[T <: AbstractStub[T]](userTokenO: Option[String])(service: T): T = {
    userTokenO.fold(service)(token => LedgerCallCredentials.authenticatingStub(service, token))
  }
}

class AuthorizationHelper(val authorizationTokenSecret: String) {

  /** @return user token signed with HMAC256
    */
  def tokenFor(userId: String): String = {
    val payload = StandardJWTPayload(
      issuer = None,
      participantId = None,
      userId = userId,
      exp = None,
      format = StandardJWTTokenFormat.Scope,
      audiences = List.empty,
    )
    JwtSigner.HMAC256
      .sign(
        jwt = DecodedJwt(
          header = """{"alg": "HS256", "typ": "JWT"}""",
          payload = AuthServiceJWTCodec.compactPrint(payload),
        ),
        secret = authorizationTokenSecret,
      )
      .getOrElse(sys.error("Failed to generate token"))
      .value
  }

}
