// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.auth0.jwt.algorithms.Algorithm
import com.daml.jwt.{ECDSAVerifier, HMAC256Verifier, JwksVerifier, RSA256Verifier}
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard}
import com.daml.platform.apiserver.{AuthServiceConfig => DamlAuthServiceConfig}
import com.digitalasset.canton.config.RequireTypes._

sealed trait AuthServiceConfig {
  def create(): AuthService
  def damlConfig: DamlAuthServiceConfig
}

object AuthServiceConfig {

  /** [default] Allows everything */
  object Wildcard extends AuthServiceConfig {
    override def create(): AuthService = AuthServiceWildcard
    override def damlConfig: DamlAuthServiceConfig = DamlAuthServiceConfig.Wildcard
  }

  /** [UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING */
  case class UnsafeJwtHmac256(secret: NonEmptyString) extends AuthServiceConfig {
    // note that HMAC256Verifier only returns an error for a `null` secret and UnsafeJwtHmac256 therefore can't throw an
    // exception when reading secret from a config value
    private val verifier =
      HMAC256Verifier(secret.unwrap).valueOr(err =>
        throw new IllegalArgumentException(s"Invalid hmac secret ($secret): $err")
      )
    override def create(): AuthService = AuthServiceJWT(verifier)

    override def damlConfig: DamlAuthServiceConfig =
      DamlAuthServiceConfig.UnsafeJwtHmac256(secret.unwrap)
  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt) */
  case class JwtRs256Crt(certificate: String) extends AuthServiceConfig {
    private val verifier = RSA256Verifier
      .fromCrtFile(certificate)
      .valueOr(err => throw new IllegalArgumentException(s"Failed to create RSA256 verifier: $err"))
    override def create(): AuthService = AuthServiceJWT(verifier)

    override def damlConfig: DamlAuthServiceConfig = DamlAuthServiceConfig.JwtRs256(certificate)
  }

  /** "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)" */
  case class JwtEs256Crt(certificate: String) extends AuthServiceConfig {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private val verifier = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA256(_, null))
      .valueOr(err =>
        throw new IllegalArgumentException(s"Failed to create ECDSA256 verifier: $err")
      )
    override def create(): AuthService = AuthServiceJWT(verifier)

    override def damlConfig: DamlAuthServiceConfig = DamlAuthServiceConfig.JwtEs256(certificate)
  }

  /** Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt) */
  case class JwtEs512Crt(certificate: String) extends AuthServiceConfig {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    private val verifier = ECDSAVerifier
      .fromCrtFile(certificate, Algorithm.ECDSA512(_, null))
      .valueOr(err =>
        throw new IllegalArgumentException(s"Failed to create ECDSA512 verifier: $err")
      )
    override def create(): AuthService = AuthServiceJWT(verifier)

    override def damlConfig: DamlAuthServiceConfig = DamlAuthServiceConfig.JwtEs512(certificate)
  }

  /** Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL */
  case class JwtRs256Jwks(url: NonEmptyString) extends AuthServiceConfig {
    private val verifier = JwksVerifier(url.unwrap)
    override def create(): AuthService = AuthServiceJWT(verifier)

    override def damlConfig: DamlAuthServiceConfig = DamlAuthServiceConfig.JwtRs256Jwks(url.unwrap)
  }

}
