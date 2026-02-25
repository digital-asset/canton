// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.jwt.JwtTimestampLeeway
import com.daml.nonempty.NonEmpty
import com.daml.tls.{TlsClientConfig, TlsClientConfigOnlyTrustFile, TlsServerConfig}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.config.AdminServerConfig.defaultAddress
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.ActiveRequestsMetrics.GrpcServerMetricsX
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{
  CantonCommunityServerInterceptors,
  CantonServerBuilder,
  CantonServerInterceptors,
}
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TracingConfig
import io.grpc.ServerInterceptor
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

import scala.concurrent.duration.DurationInt
import scala.math.Ordering.Implicits.infixOrderingOps

/** Configuration to limit the number of open requests per service
  *
  * @param active
  *   map of service name to maximum number of parallel active requests or streams
  * @param warnOnUndefinedLimits
  *   emit warning if a limit is not configured for a stream
  * @param throttleLoggingRatePerSecond
  *   maximum rate for logging rejections for requests exceeding the limit (prevents DOS on logs)
  */
final case class ActiveRequestLimitsConfig(
    active: Map[String, NonNegativeInt] = Map.empty,
    warnOnUndefinedLimits: Boolean = false,
    throttleLoggingRatePerSecond: NonNegativeInt = NonNegativeInt.tryCreate(10),
)

/** Configuration for hosting a server api */
trait ServerConfig extends Product with Serializable {

  /** The name of the api */
  def name: String

  /** The address of the interface to be listening on */
  val address: String

  /** Port to be listening on (must be greater than 0). If the port is None, a default port will be
    * assigned on startup.
    *
    * NOTE: If you rename this field, adapt the corresponding product hint for config reading. In
    * the configuration the field is still called `port` for usability reasons.
    */
  protected val internalPort: Option[Port]

  /** Returns the configured or the default port that must be assigned after config loading and
    * before config usage.
    *
    * We split between `port` and `internalPort` to offer a clean API to users of the config in the
    * form of `port`, which must always return a configured or default port, and the internal
    * representation that may be None before being assigned a default port.
    */
  def port: Port =
    internalPort.getOrElse(
      throw new IllegalStateException("Accessing server port before default was set")
    )

  /** If defined, dictates to use TLS when connecting to this node through the given `address` and
    * `port`. Server authentication is always enabled. Subclasses may decide whether to support
    * client authentication.
    */
  def sslContext: Option[SslContext]

  /** If any defined, enforces token based authorization when accessing this node through the given
    * `address` and `port`.
    */
  def authServices: Seq[AuthServiceConfig]

  /** Leeway parameters for the jwt processing algorithms used in the authorization services
    */
  def jwtTimestampLeeway: Option[JwtTimestampLeeway]

  /** The configuration of the admin-token based authorization that will be supported when accessing
    * this node through the given `address` and `port`.
    */
  def adminTokenConfig: AdminTokenConfig

  /** server cert chain file if TLS is defined
    *
    * Used for synchronizer internal GRPC sequencer connections
    */
  def serverCertChainFile: Option[PemFileOrString]

  /** server keep alive settings */
  def keepAliveServer: Option[KeepAliveServerConfig]

  /** maximum inbound message size in bytes on the ledger api and the admin api */
  def maxInboundMessageSize: NonNegativeInt

  /** maximum expiration time accepted for tokens */
  def maxTokenLifetime: NonNegativeDuration

  /** settings for the jwks cache */
  def jwksCacheConfig: JwksCacheConfig

  /** configure limits for open streams per service */
  def limits: Option[ActiveRequestLimitsConfig]

  /** Use the configuration to instantiate the interceptors for this server */
  def instantiateServerInterceptors(
      api: String,
      tracingConfig: TracingConfig,
      apiLoggingConfig: ApiLoggingConfig,
      loggerFactory: NamedLoggerFactory,
      grpcMetrics: GrpcServerMetricsX,
      authServices: Seq[AuthServiceConfig],
      adminTokenDispenser: Option[CantonAdminTokenDispenser],
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      adminTokenConfig: AdminTokenConfig,
      jwksCacheConfig: JwksCacheConfig,
      telemetry: Telemetry,
      additionalInterceptors: Seq[ServerInterceptor] = Seq.empty,
      requestLimits: Option[ActiveRequestLimitsConfig],
  ): CantonServerInterceptors = new CantonCommunityServerInterceptors(
    api,
    tracingConfig,
    apiLoggingConfig,
    loggerFactory,
    grpcMetrics,
    authServices,
    adminTokenDispenser,
    jwtTimestampLeeway,
    adminTokenConfig,
    jwksCacheConfig,
    telemetry,
    additionalInterceptors,
    requestLimits,
  )

}

object ServerConfig {
  val defaultMaxInboundMessageSize: NonNegativeInt = NonNegativeInt.tryCreate(10 * 1024 * 1024)
  val defaultMaxInboundMetadataSize: NonNegativeInt = NonNegativeInt.tryCreate(8 * 1024)
}

/** A variant of [[ServerConfig]] that by default listens to connections only on the loopback
  * interface.
  */
final case class AdminServerConfig(
    override val address: String = defaultAddress,
    override val internalPort: Option[Port] = None,
    tls: Option[TlsServerConfig] = None,
    override val jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    override val keepAliveServer: Option[BasicKeepAliveServerConfig] = Some(
      BasicKeepAliveServerConfig()
    ),
    override val maxInboundMessageSize: NonNegativeInt = ServerConfig.defaultMaxInboundMessageSize,
    override val authServices: Seq[AuthServiceConfig] = Seq.empty,
    override val adminTokenConfig: AdminTokenConfig = AdminTokenConfig(),
    override val maxTokenLifetime: NonNegativeDuration = NonNegativeDuration(5.minutes),
    override val jwksCacheConfig: JwksCacheConfig = JwksCacheConfig(),
    override val limits: Option[ActiveRequestLimitsConfig] = None,
) extends ServerConfig {
  override val name: String = "admin"
  def clientConfig: FullClientConfig =
    FullClientConfig(
      address,
      port,
      tls = tls.map(_.clientConfig),
      keepAliveClient = keepAliveServer.map(_.clientConfigFor),
    )

  override def sslContext: Option[SslContext] = tls.map(CantonServerBuilder.sslContext(_))

  override def serverCertChainFile: Option[PemFileOrString] = tls.map(_.certChainFile)
}
object AdminServerConfig {
  val defaultAddress: String = "127.0.0.1"
}

/** GRPC keep alive server configuration. */
trait KeepAliveServerConfig {

  /** time sets the time without read activity before sending a keepalive ping. Do not set to small
    * numbers (default is 40s) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def time: NonNegativeFiniteDuration

  /** timeout sets the time waiting for read activity after sending a keepalive ping (default is
    * 20s) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#keepAliveTimeout-long-java.util.concurrent.TimeUnit-]]
    */
  def timeout: NonNegativeFiniteDuration

  /** permitKeepAliveTime sets the most aggressive keep-alive time that clients are permitted to
    * configure (default is 20s) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#permitKeepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def permitKeepAliveTime: NonNegativeFiniteDuration

  /** permitKeepAliveWithoutCalls allows the clients to send keep alive signals outside any ongoing
    * grpc subscription (default false) Corresponds to
    * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyServerBuilder.html#permitKeepAliveTime-long-java.util.concurrent.TimeUnit-]]
    */
  def permitKeepAliveWithoutCalls: Boolean

  /** A sensible default choice of client config for the given server config */
  def clientConfigFor: KeepAliveClientConfig = {
    val clientKeepAliveTime = permitKeepAliveTime.max(time)
    KeepAliveClientConfig(clientKeepAliveTime, timeout)
  }
}

final case class BasicKeepAliveServerConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(40),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
    permitKeepAliveTime: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
    permitKeepAliveWithoutCalls: Boolean = false,
) extends KeepAliveServerConfig

final case class LedgerApiKeepAliveServerConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(10),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(20),
    permitKeepAliveTime: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(10),
    permitKeepAliveWithoutCalls: Boolean = false,
) extends KeepAliveServerConfig

/** gRPC keep alive client configuration
  *
  * Settings according to
  * [[https://grpc.github.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html#keepAliveTime-long-java.util.concurrent.TimeUnit-]]
  *
  * @param time
  *   Sets the time without read activity before sending a keepalive ping. Do not set to small
  *   numbers (default is 40s)
  * @param timeout
  *   Sets the time waiting for read activity after sending a keepalive ping (default is 15s).
  *
  * The default timeout was previously 20s and was lowered to 15s, because besides configuring the
  * gRPC KeepAliveManager, it also sets up the socket's TCP_USER_TIMEOUT (see
  * [[https://man7.org/linux/man-pages/man7/tcp.7.html]]). 15s gives a larger margin to detect a
  * faulty connection earlier and retry a submission on another sequencer via amplification, thereby
  * avoiding a request failure.
  */
final case class KeepAliveClientConfig(
    time: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(40),
    timeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(15),
)

/** Base trait for Grpc transport client configuration classes, abstracts access to the configs */
trait ClientConfig {
  def address: String
  def port: Port
  def tlsConfig: Option[TlsClientConfig]
  def keepAliveClient: Option[KeepAliveClientConfig]
  def endpointAsString: String = address + ":" + port.unwrap
}

/** This trait is only there to force presence of `tls` field in the config classes */
trait TlsField[A] {
  def tls: Option[A]
}

/** A full feature complete client configuration to a corresponding server configuration, the class
  * is aimed to be used in configs
  */
final case class FullClientConfig(
    override val address: String = "127.0.0.1",
    override val port: Port,
    override val tls: Option[TlsClientConfig] = None,
    override val keepAliveClient: Option[KeepAliveClientConfig] = Some(KeepAliveClientConfig()),
) extends ClientConfig
    with TlsField[TlsClientConfig] {
  override def tlsConfig: Option[TlsClientConfig] = tls
}

/** A client configuration for a public server configuration, the class is aimed to be used in
  * configs
  */
final case class SequencerApiClientConfig(
    override val address: String,
    override val port: Port,
    override val tls: Option[TlsClientConfigOnlyTrustFile] = None,
) extends ClientConfig
    with TlsField[TlsClientConfigOnlyTrustFile] {

  override def keepAliveClient: Option[KeepAliveClientConfig] = None

  override def tlsConfig: Option[TlsClientConfig] = tls.map(_.toTlsClientConfig)

  def asSequencerConnection(
      sequencerAlias: SequencerAlias,
      sequencerId: Option[SequencerId],
  ): GrpcSequencerConnection = {
    val endpoint = Endpoint(address, port)
    GrpcSequencerConnection(
      NonEmpty(Seq, endpoint),
      tls.exists(_.enabled),
      tls.flatMap(_.trustCollectionFile).map(_.pemBytes),
      sequencerAlias,
      sequencerId = sequencerId,
    )
  }
}

/** Configuration for jwks cache underpinning JWT token validation.
  */
final case class JwksCacheConfig(
    cacheMaxSize: Long = JwksCacheConfig.DefaultCacheMaxSize,
    cacheExpiration: NonNegativeFiniteDuration = JwksCacheConfig.DefaultCacheExpiration,
    connectionTimeout: NonNegativeFiniteDuration = JwksCacheConfig.DefaultConnectionTimeout,
    readTimeout: NonNegativeFiniteDuration = JwksCacheConfig.DefaultReadTimeout,
)

object JwksCacheConfig {
  private val DefaultCacheMaxSize: Long = 1000
  private val DefaultCacheExpiration: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(5)
  private val DefaultConnectionTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)
  private val DefaultReadTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(10)
}

/** Configuration for admin-token based authorization.
  *
  * The given token will be valid forever but it will not be used internally. Other admin-tokens
  * will be generated and rotated periodically. The fixed token is only used for testing purposes
  * and should not be used in production.
  *
  * The admin-token based authorization will create tokens and rotate them periodically. Each
  * admin-token is valid for the defined token duration. The half of this value is used as the
  * rotation interval, after which a new admin-token is generated (if needed).
  */
final case class AdminTokenConfig(
    fixedAdminToken: Option[String] = None,
    adminTokenDuration: PositiveFiniteDuration = AdminTokenConfig.DefaultAdminTokenDuration,
    actAsAnyPartyClaim: Boolean = false,
    adminClaim: Boolean = false,
) {

  def merge(other: AdminTokenConfig): AdminTokenConfig =
    AdminTokenConfig(
      fixedAdminToken = fixedAdminToken.orElse(other.fixedAdminToken),
      adminTokenDuration = adminTokenDuration.min(other.adminTokenDuration),
      actAsAnyPartyClaim = actAsAnyPartyClaim || other.actAsAnyPartyClaim,
      adminClaim = adminClaim || other.adminClaim,
    )
}

object AdminTokenConfig {

  val DefaultAdminTokenDuration: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(5)
}
