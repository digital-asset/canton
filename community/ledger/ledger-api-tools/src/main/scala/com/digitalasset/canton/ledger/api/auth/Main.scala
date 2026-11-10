// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.jwt.{
  AuthServiceJWTCodec,
  DecodedJwt,
  Jwt,
  JwtSigner,
  KeyUtils,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.interfaces.RSAPublicKey
import java.time.Instant

// TODO(i12336): Remove it unless it's needed
object Main {

  object ErrorCodes {
    val InvalidUsage = 100
    val GenerateTokensError = 101
  }

  final case class Config(
      command: Option[Command] = None
  )

  sealed abstract class Command

  final case class GenerateJwks(
      output: Option[File] = None,
      publicKeys: List[File] = List(),
  ) extends Command

  final case class GenerateToken(
      output: Option[File] = None,
      signingKey: Option[File] = None,
      issuer: Option[String] = None,
      userId: String = "",
      participantId: Option[String] = None,
      audience: Option[String] = None,
      exp: Option[Instant] = None,
      kid: Option[String] = None,
  ) extends Command

  /** By default, RSA key Ids are generated from their file name. */
  private[this] def defaultKeyId(file: File): String = {
    val fileName = file.getName
    val pos = fileName.lastIndexOf(".")
    if (pos > 0 && pos < (fileName.length - 1)) {
      fileName.substring(0, pos)
    } else {
      fileName
    }
  }

  def main(args: Array[String]): Unit =
    parseConfig(args) match {
      case Some(Config(Some(GenerateJwks(Some(outputFile), publicKeys)))) =>
        // Load RSA keys. They ID of each key is its file name.
        val keys: Map[String, RSAPublicKey] = publicKeys
          .map(f =>
            defaultKeyId(f) -> KeyUtils
              .readRSAPublicKeyFromCrt(f)
              .fold(
                t =>
                  handleGenerateTokensError(
                    "Error loading RSA public key from a X509 certificate file."
                  )(t.getMessage),
                x => x,
              )
          )
          .toMap

        // Generate and write JWKS for all keys
        val jwks = KeyUtils.generateJwks(keys)
        Files.write(outputFile.toPath, jwks.getBytes(StandardCharsets.UTF_8))

        ()

      case Some(
            Config(
              Some(
                GenerateToken(
                  Some(outputFile),
                  Some(signingKeyFile),
                  issuerO,
                  userId,
                  participantIdO,
                  audienceO,
                  exp,
                  kid,
                )
              )
            )
          ) =>
        val keyId = kid.getOrElse(defaultKeyId(signingKeyFile))

        // --audience and --participantId are validated by the parser's checkConfig
        val payload = StandardJWTPayload(
          issuer = issuerO,
          userId = userId,
          participantId = participantIdO,
          exp = exp,
          format = StandardJWTTokenFormat.Audience,
          audiences = audienceO.toList,
          scope = None,
        )
        val signingKey = KeyUtils
          .readRSAPrivateKeyFromDer(signingKeyFile)
          .fold(
            t =>
              handleGenerateTokensError(
                "Error loading RSA private key from a PKCS8/DER file. Use the following command to convert a PEM encoded private key: openssl pkcs8 -topk8 -inform PEM -outform DER -in private-key.pem -nocrypt > private-key.der."
              )(t.getMessage),
            x => x,
          )
        val jwtPayload = AuthServiceJWTCodec.compactPrint(payload)
        val jwtHeader = s"""{"alg": "RS256", "typ": "JWT", "kid": "$keyId"}"""
        val signed: Jwt = JwtSigner
          .RSA256(signingKey)
          .sign(DecodedJwt(jwtHeader, jwtPayload))
          .fold(e => handleGenerateTokensError("Error signing JWT token")(e.prettyPrint), identity)

        def changeExtension(file: File, extension: String): File = {
          val filename = file.getName
          new File(file.getParentFile, filename + extension)
        }

        Files.write(outputFile.toPath, signed.value.getBytes(StandardCharsets.UTF_8))

        Files.write(
          changeExtension(outputFile, "-bearer.txt").toPath,
          signed.value.getBytes(StandardCharsets.UTF_8),
        )

        Files.write(
          changeExtension(outputFile, "-payload.json").toPath,
          jwtPayload.getBytes(StandardCharsets.UTF_8),
        )

        Files.write(
          changeExtension(outputFile, "-header.json").toPath,
          jwtHeader.getBytes(StandardCharsets.UTF_8),
        )

        ()
      case Some(_) =>
        configParser.displayToErr(configParser.usage)
        sys.exit(ErrorCodes.InvalidUsage)
      case None =>
        sys.exit(ErrorCodes.InvalidUsage)
    }

  private def handleGenerateTokensError(message: String)(details: String): Nothing = {
    Console.println(s"$message. Details: $details")
    sys.exit(ErrorCodes.GenerateTokensError)
  }

  private def parseConfig(args: collection.Seq[String]): Option[Config] =
    configParser.parse(args, Config())

  private val configParser = new scopt.OptionParser[Config]("ledger-api-auth") {
    cmd("generate-jwks")
      .text("Generate a JWKS JSON object for the given set of RSA public keys")
      .action((_, c) => c.copy(command = Some(GenerateJwks())))
      .children(
        opt[File]("output")
          .required()
          .text("The output file")
          .valueName("<paths>")
          .action((x, c) =>
            c.copy(command = c.command.map(_.asInstanceOf[GenerateJwks].copy(output = Some(x))))
          ),
        opt[Seq[File]]("keys")
          .required()
          .text("List of RSA certificates (.crt)")
          .valueName("<paths>")
          .action((x, c) =>
            c.copy(
              command = c.command.map(_.asInstanceOf[GenerateJwks].copy(publicKeys = x.toList))
            )
          ),
      )
      .discard

    cmd("generate-token")
      .text("Generate a signed access token for the Daml ledger API")
      .action((_, c) => c.copy(command = Some(GenerateToken())))
      .children(
        opt[File]("output")
          .required()
          .text("The output file")
          .valueName("<paths>")
          .action((x, c) =>
            c.copy(command = c.command.map(_.asInstanceOf[GenerateToken].copy(output = Some(x))))
          ),
        opt[File]("key")
          .required()
          .text("The RSA private key (.der)")
          .valueName("<path>")
          .action((x, c) =>
            c.copy(
              command = c.command.map(_.asInstanceOf[GenerateToken].copy(signingKey = Some(x)))
            )
          ),
        opt[String]("issuer")
          .required()
          .text("Issuer emitting the token")
          .valueName("<issuer>")
          .action((x, c) =>
            c.copy(command = c.command.map(_.asInstanceOf[GenerateToken].copy(issuer = Some(x))))
          ),
        opt[String]("user")
          .required()
          .text("User to generate tokens for")
          .valueName("<user>")
          .action((x, c) =>
            c.copy(command = c.command.map(_.asInstanceOf[GenerateToken].copy(userId = x)))
          ),
        opt[String]("participantId")
          .optional()
          .text(
            "Restrict validity of the token to this participant ID: the token's audience becomes " +
              "https://daml.com/jwt/aud/participant/<participantId>. " +
              "Required unless --audience is given, and mutually exclusive with it."
          )
          .action((x, c) =>
            c.copy(command =
              c.command.map(_.asInstanceOf[GenerateToken].copy(participantId = Some(x)))
            )
          ),
        opt[String]("audience")
          .optional()
          .text(
            "The audience (aud claim) of the token, as configured by target-audience on the participant. " +
              "Required unless --participantId is given, and mutually exclusive with it."
          )
          .action((x, c) =>
            c.copy(command = c.command.map(_.asInstanceOf[GenerateToken].copy(audience = Some(x))))
          ),
        opt[String]("exp")
          .optional()
          .text("Token expiration date, in ISO 8601 format. Default: no expiration date.")
          .action((x, c) =>
            c.copy(command =
              c.command.map(_.asInstanceOf[GenerateToken].copy(exp = Some(Instant.parse(x))))
            )
          ),
        opt[String]("kid")
          .optional()
          .text("The key id, as used in JWKS. Default: the file name of the RSA private key.")
          .action((x, c) =>
            c.copy(command =
              c.command.map(_.asInstanceOf[GenerateToken].copy(exp = Some(Instant.parse(x))))
            )
          ),
      )
      .discard

    // The two options are mutually exclusive: the codec only writes the participant audience when
    // no audience is given, and a participant configured with a `target-audience` never extracts
    // the participant ID from the token, so a bespoke audience cannot restrict the participant.
    checkConfig {
      case Config(Some(GenerateToken(_, _, _, _, None, None, _, _))) =>
        failure(
          "Specify the audience with --audience, or the participant with --participantId to use the participant audience."
        )
      case Config(Some(GenerateToken(_, _, _, _, Some(_), Some(_), _, _))) =>
        failure(
          "--audience and --participantId are mutually exclusive: a participant with a configured target-audience does not restrict tokens to a participant ID."
        )
      case _ => success
    }.discard
  }
}
