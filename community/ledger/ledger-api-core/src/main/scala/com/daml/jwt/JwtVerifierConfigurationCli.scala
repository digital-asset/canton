// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.algorithms.Algorithm
import com.digitalasset.canton.DiscardOps

import java.nio.file.Paths
import scala.util.Try

// TODO(#12301) Only member of package com.daml.jwt in community/ledger.
//              Only used in triggers.
//              Therefore not moving this package and file.
//              Consider moving this file closer to triggers as it is moving over as a next step of repo consolidation.
//              Note: there is a JwtVerifierConfigurationCliSpec too.
object JwtVerifierConfigurationCli {
  def parse[C](parser: scopt.OptionParser[C])(setter: (JwtVerifierBase, C) => C): Unit = {
    def setJwtVerifier(jwtVerifier: JwtVerifierBase, c: C): C = setter(jwtVerifier, c)

    import parser.opt

    opt[String]("auth-jwt-hs256-unsafe")
      .optional()
      .hidden()
      .validate(v => Either.cond(v.nonEmpty, (), "HMAC secret must be a non-empty string"))
      .text(
        "[UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING"
      )
      .action { (secret, config) =>
        val verifier = HMAC256Verifier(secret)
          .valueOr(err => sys.error(s"Failed to create HMAC256 verifier: $err"))
        setJwtVerifier(verifier, config)
      }
      .discard

    opt[String]("auth-jwt-rs256-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-rs256-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { (path, config) =>
        val verifier = RSA256Verifier
          .fromCrtFile(path)
          .valueOr(err => sys.error(s"Failed to create RSA256 verifier: $err"))
        setJwtVerifier(verifier, config)
      }
      .discard

    opt[String]("auth-jwt-es256-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-es256-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { (path, config) =>
        val verifier = ECDSAVerifier
          .fromCrtFile(path, Algorithm.ECDSA256(_, null))
          .valueOr(err => sys.error(s"Failed to create ECDSA256 verifier: $err"))
        setJwtVerifier(verifier, config)
      }
      .discard

    opt[String]("auth-jwt-es512-crt")
      .optional()
      .validate(
        validatePath(_, "The certificate file specified via --auth-jwt-es512-crt does not exist")
      )
      .text(
        "Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt)"
      )
      .action { (path, config) =>
        val verifier = ECDSAVerifier
          .fromCrtFile(path, Algorithm.ECDSA512(_, null))
          .valueOr(err => sys.error(s"Failed to create ECDSA512 verifier: $err"))
        setJwtVerifier(verifier, config)
      }
      .discard

    opt[String]("auth-jwt-rs256-jwks")
      .optional()
      .validate(v => Either.cond(v.length > 0, (), "JWK server URL must be a non-empty string"))
      .text(
        "Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL"
      )
      .action { (url, config) =>
        val verifier = JwksVerifier(url)
        setJwtVerifier(verifier, config)
      }
      .discard

    ()
  }

  private def validatePath(path: String, message: String): Either[String, Unit] = {
    val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
    if (valid) Right(()) else Left(message)
  }
}
