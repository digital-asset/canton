// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.daml.tls.TlsClientConfigOnlyTrustFile
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port}
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PositiveFiniteDuration}

/** Configuration for a single engine extension service.
  *
  * Extension services allow Daml contracts to make external calls to deterministic services. The
  * participant manages connection pooling and HTTP calls on behalf of the engine.
  *
  * @param address
  *   Hostname of the extension service
  * @param port
  *   Port of the extension service
  * @param version
  *   API version path segment exposed by the extension service
  * @param tls
  *   Optional TLS configuration for the connection.
  * @param auth
  *   Authentication configuration for outbound calls to the extension service
  * @param connectTimeout
  *   Connection timeout
  * @param requestTimeout
  *   Request timeout for individual HTTP requests
  * @param maxRetries
  *   Maximum number of retry attempts
  * @param retryInitialDelay
  *   Initial delay before first retry
  * @param retryMaxDelay
  *   Maximum delay between retries
  * @param validateOnStartup
  *   Whether participant startup should validate that the configured extension service endpoint is
  *   reachable.
  */
final case class ExtensionServiceConfig(
    address: String,
    port: Port,
    version: String = "v1",
    tls: Option[TlsClientConfigOnlyTrustFile] = None,
    auth: ExtensionServiceAuthConfig = ExtensionServiceAuthConfig.None,
    connectTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofMillis(500),
    requestTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(8),
    maxRetries: NonNegativeInt = NonNegativeInt.tryCreate(3),
    retryInitialDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(1000),
    retryMaxDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
    validateOnStartup: Boolean = false,
)

/** Authentication configuration for outbound calls to an extension service. */
sealed trait ExtensionServiceAuthConfig extends Product with Serializable

object ExtensionServiceAuthConfig {

  /** [default] Send no authorization header. */
  case object None extends ExtensionServiceAuthConfig

  /** Send the configured file contents as an HTTP bearer token. */
  final case class BearerTokenFile(tokenFile: ExistingFile) extends ExtensionServiceAuthConfig
}
