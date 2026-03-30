// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import pureconfig.{ConfigReader, ConfigWriter}
import pureconfig.error.CannotConvert
import pureconfig.generic.FieldCoproductHint
import pureconfig.generic.semiauto.*

import java.net.URI
import java.nio.file.Path

/** Configuration for a single engine extension service.
  *
  * Extension services allow Daml contracts to make external calls to deterministic
  * services. The participant manages connection pooling and HTTP calls on behalf
  * of the engine.
  *
  * @param name Human-readable name for the extension
  * @param host Hostname of the extension service
  * @param port Port of the extension service
  * @param useTls Whether to use TLS for the connection
  * @param trustCollectionFile Optional trust collection for the resource endpoint
  * @param tlsInsecure If true, skip TLS certificate validation (dev only)
  * @param auth Authentication configuration
  * @param connectTimeout Connection timeout
  * @param requestTimeout Request timeout for individual HTTP requests
  * @param maxTotalTimeout Maximum total time for the entire operation including retries
  * @param maxRetries Maximum number of retry attempts
  * @param retryInitialDelay Initial delay before first retry
  * @param retryMaxDelay Maximum delay between retries
  * @param requestIdHeader Name of the request ID header
  * @param declaredFunctions Functions that this extension provides, for validation
  */
final case class ExtensionServiceConfig(
    name: String,
    host: String,
    port: Port,
    useTls: Boolean = true,
    trustCollectionFile: Option[Path] = None,
    tlsInsecure: Boolean = false,
    auth: ExtensionServiceAuthConfig = ExtensionServiceAuthConfig.NoAuth,
    connectTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(500),
    requestTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(8),
    maxTotalTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(25),
    maxRetries: NonNegativeInt = NonNegativeInt.tryCreate(3),
    retryInitialDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(1000),
    retryMaxDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
    requestIdHeader: String = "X-Request-Id",
    declaredFunctions: Seq[ExtensionFunctionDeclaration] = Seq.empty,
)

object ExtensionServiceConfig {
  implicit val reader: ConfigReader[ExtensionServiceConfig] =
    deriveReader[ExtensionServiceConfig].emap { config =>
      config.auth match {
        case _: ExtensionServiceAuthConfig.OAuth if !config.useTls =>
          Left(CannotConvert(config.useTls.toString, "ExtensionServiceConfig", "OAuth requires the resource server to use TLS."))
        case _ =>
          Right(config)
      }
    }

  implicit val writer: ConfigWriter[ExtensionServiceConfig] =
    deriveWriter[ExtensionServiceConfig]
}

/** Declaration of a function provided by an extension service.
  *
  * Used for validation on startup to ensure DARs have matching extensions configured.
  *
  * @param functionId The function identifier
  * @param configHash Expected configuration hash for version validation
  */
final case class ExtensionFunctionDeclaration(
    functionId: String,
    configHash: String,
)

object ExtensionFunctionDeclaration {
  implicit val reader: ConfigReader[ExtensionFunctionDeclaration] =
    deriveReader[ExtensionFunctionDeclaration]

  implicit val writer: ConfigWriter[ExtensionFunctionDeclaration] =
    deriveWriter[ExtensionFunctionDeclaration]
}

final case class ExtensionServiceTokenEndpointConfig(
    host: String,
    port: Port,
    path: String,
    trustCollectionFile: Option[Path] = None,
    tlsInsecure: Boolean = false,
) {
  def uri: URI = URI.create(s"https://$host:$port$path")
}

object ExtensionServiceTokenEndpointConfig {
  implicit val reader: ConfigReader[ExtensionServiceTokenEndpointConfig] =
    deriveReader[ExtensionServiceTokenEndpointConfig].emap { config =>
      if (!config.path.startsWith("/")) {
        Left(CannotConvert(config.path, "token-endpoint.path", "Token endpoint path must start with '/'."))
      } else {
        val uri = URI.create(s"https://example.invalid${config.path}")
        if (uri.getQuery != null) {
          Left(CannotConvert(config.path, "token-endpoint.path", "Token endpoint path must not contain a query string."))
        } else if (uri.getFragment != null) {
          Left(CannotConvert(config.path, "token-endpoint.path", "Token endpoint path must not contain a fragment."))
        } else {
          Right(config)
        }
      }
    }

  implicit val writer: ConfigWriter[ExtensionServiceTokenEndpointConfig] =
    deriveWriter[ExtensionServiceTokenEndpointConfig]
}

sealed trait ExtensionServiceAuthConfig extends Product with Serializable

object ExtensionServiceAuthConfig {
  case object NoAuth extends ExtensionServiceAuthConfig

  final case class OAuth(
      tokenEndpoint: ExtensionServiceTokenEndpointConfig,
      clientId: String,
      privateKeyFile: Path,
      keyId: Option[String] = None,
      scope: Option[String] = None,
  ) extends ExtensionServiceAuthConfig

  implicit val typeHint: FieldCoproductHint[ExtensionServiceAuthConfig] =
    new FieldCoproductHint[ExtensionServiceAuthConfig]("type") {
      override def fieldValue(name: String): String =
        name match {
          case "NoAuth" => "none"
          case "OAuth" => "oauth"
          case other => other.toLowerCase
        }
    }

  implicit val noAuthReader: ConfigReader[NoAuth.type] =
    deriveReader[NoAuth.type]

  implicit val oauthReader: ConfigReader[OAuth] =
    deriveReader[OAuth]

  implicit val reader: ConfigReader[ExtensionServiceAuthConfig] =
    deriveReader[ExtensionServiceAuthConfig]

  implicit val noAuthWriter: ConfigWriter[NoAuth.type] =
    deriveWriter[NoAuth.type]

  implicit val oauthWriter: ConfigWriter[OAuth] =
    deriveWriter[OAuth]

  implicit val writer: ConfigWriter[ExtensionServiceAuthConfig] =
    deriveWriter[ExtensionServiceAuthConfig]
}

/** Configuration for engine extensions in the participant.
  *
  * @param validateExtensionsOnStartup Whether to validate extension configurations on startup
  * @param failOnExtensionValidationError Whether to fail startup if extension validation fails
  * @param echoMode If true, external calls return the input as output (for testing)
  */
final case class EngineExtensionsConfig(
    validateExtensionsOnStartup: Boolean = true,
    failOnExtensionValidationError: Boolean = true,
    echoMode: Boolean = false,
)

object EngineExtensionsConfig {
  val default: EngineExtensionsConfig = EngineExtensionsConfig()

  implicit val reader: ConfigReader[EngineExtensionsConfig] =
    deriveReader[EngineExtensionsConfig]

  implicit val writer: ConfigWriter[EngineExtensionsConfig] =
    deriveWriter[EngineExtensionsConfig]
}
