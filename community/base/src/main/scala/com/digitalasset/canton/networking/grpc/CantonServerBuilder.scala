// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.MetricsFactory
import com.digitalasset.canton.tracing.TracingConfig
import io.grpc.*
import io.grpc.netty.{GrpcSslContexts, NettyServerBuilder}
import io.grpc.util.MutableHandlerRegistry
import io.netty.handler.ssl.{SslContext, SslContextBuilder}

import java.net.InetSocketAddress
import java.util.concurrent.{Executor, TimeUnit}
import scala.annotation.nowarn

/** The [[io.grpc.ServerBuilder]] is pretty "loose" with its type parameters
  * causing some issues for `scalac` and IntelliJ.
  * Here we provide a wrapper hiding these type issues.
  */
trait CantonServerBuilder {
  def mutableHandlerRegistry(): CantonMutableHandlerRegistry

  def addService(service: BindableService, withLogging: Boolean): CantonServerBuilder

  def addService(service: ServerServiceDefinition, withLogging: Boolean = true): CantonServerBuilder

  def build: Server

  def maxInboundMessageSize(bytes: NonNegativeInt): CantonServerBuilder
}

trait CantonMutableHandlerRegistry extends AutoCloseable {
  def addService(
      service: ServerServiceDefinition,
      withLogging: Boolean = true,
  ): (ServerServiceDefinition, CantonMutableHandlerRegistry)

  def addServiceU(
      service: ServerServiceDefinition,
      withLogging: Boolean = true,
  ): Unit = addService(service, withLogging).discard

  def removeService(service: ServerServiceDefinition): CantonMutableHandlerRegistry

  def removeServiceU(service: ServerServiceDefinition): Unit = removeService(service).discard
}

object CantonServerBuilder {

  /** Creates our wrapper for a grpc ServerBuilder.
    * As we only create our servers from our configuration this is intentionally private.
    */
  private class BaseBuilder(
      serverBuilder: ServerBuilder[_ <: ServerBuilder[_]],
      interceptors: CantonServerInterceptors,
  ) extends CantonServerBuilder {

    override def mutableHandlerRegistry(): CantonMutableHandlerRegistry =
      new CantonMutableHandlerRegistry {
        val registry = new MutableHandlerRegistry()
        serverBuilder.fallbackHandlerRegistry(registry)

        override def addService(
            service: ServerServiceDefinition,
            withLogging: Boolean,
        ): (ServerServiceDefinition, CantonMutableHandlerRegistry) = {
          val serverServiceDefinition = interceptors.addAllInterceptors(service, withLogging)
          registry.addService(serverServiceDefinition)

          // addAllInterceptors call returns a new wrapped ServerServiceDefinition reference
          // Hence, return the new reference for allowing removal in removeService.
          serverServiceDefinition -> this
        }

        override def removeService(
            service: ServerServiceDefinition
        ): CantonMutableHandlerRegistry = {

          registry.removeService(service)
          this
        }

        override def close(): Unit =
          for (_ <- 0 until registry.getServices.size()) {
            registry
              .removeService(registry.getServices.get(registry.getServices.size() - 1))
              .discard[Boolean]
          }
      }

    override def addService(service: BindableService, withLogging: Boolean): CantonServerBuilder = {
      serverBuilder.addService(interceptors.addAllInterceptors(service.bindService(), withLogging))
      this
    }

    override def maxInboundMessageSize(bytes: NonNegativeInt): CantonServerBuilder = {
      serverBuilder.maxInboundMessageSize(bytes.unwrap)
      this
    }

    override def addService(
        service: ServerServiceDefinition,
        withLogging: Boolean,
    ): CantonServerBuilder = {
      serverBuilder.addService(interceptors.addAllInterceptors(service, withLogging))
      this
    }

    override def build: Server = serverBuilder.build()
  }

  def configureKeepAlive(
      keepAlive: Option[KeepAliveServerConfig],
      builder: NettyServerBuilder,
  ): NettyServerBuilder =
    keepAlive.fold(builder) { ka =>
      val time = ka.time.unwrap.toMillis
      val timeout = ka.timeout.unwrap.toMillis
      val permitTime = ka.permitKeepAliveTime.unwrap.toMillis
      val permitKAWOCalls = ka.permitKeepAliveWithoutCalls
      builder
        .keepAliveTime(time, TimeUnit.MILLISECONDS)
        .keepAliveTimeout(timeout, TimeUnit.MILLISECONDS)
        .permitKeepAliveTime(
          permitTime,
          TimeUnit.MILLISECONDS,
        ) // gracefully allowing a bit more aggressive keep alives from clients
        .permitKeepAliveWithoutCalls(permitKAWOCalls)
    }

  /** Create a GRPC server build using conventions from our configuration.
    * @param config server configuration
    * @return builder to attach application services and interceptors
    */
  def forConfig(
      config: ServerConfig,
      metricsPrefix: MetricName,
      @nowarn("cat=deprecation") metricsFactory: MetricsFactory,
      executor: Executor,
      loggerFactory: NamedLoggerFactory,
      apiLoggingConfig: ApiLoggingConfig,
      tracing: TracingConfig,
      grpcMetrics: GrpcServerMetrics,
      adminToken: Option[CantonAdminToken],
      telemetry: Telemetry,
  ): CantonServerBuilder = {
    val builder =
      NettyServerBuilder
        .forAddress(new InetSocketAddress(config.address, config.port.unwrap))
        .executor(executor)
        .maxInboundMessageSize(config.maxInboundMessageSize.unwrap)

    val builderWithSsl = config.sslContext match {
      case Some(sslContext) =>
        builder.sslContext(sslContext)
      case None =>
        builder
    }

    new BaseBuilder(
      reifyBuilder(configureKeepAlive(config.keepAliveServer, builderWithSsl)),
      config.instantiateServerInterceptors(
        tracing,
        apiLoggingConfig,
        metricsPrefix,
        metricsFactory,
        loggerFactory,
        grpcMetrics,
        config.authServices,
        adminToken,
        telemetry,
      ),
    )
  }

  private def baseSslBuilder(config: BaseTlsArguments): SslContextBuilder = {
    import scala.jdk.CollectionConverters.*
    val s1 =
      GrpcSslContexts.forServer(config.certChainFile.unwrap, config.privateKeyFile.unwrap)
    val s2 = config.protocols.fold(s1)(protocols => s1.protocols(protocols: _*))
    config.ciphers.fold(s2)(ciphers => s2.ciphers(ciphers.asJava))
  }

  def baseSslContext(config: TlsBaseServerConfig): SslContext = baseSslBuilder(config).build()

  def sslContext(config: TlsServerConfig): SslContext = {
    // TODO(#7086): secrets service support not yet implemented for canton admin services
    config.secretsUrl.foreach { url =>
      throw new IllegalArgumentException(
        s"Canton admin services do not yet support 'Secrets Service' $url."
      )
    }

    val s1 = baseSslBuilder(config)
    val s2 = config.trustCollectionFile.fold(s1)(trustCollection =>
      s1.trustManager(trustCollection.unwrap)
    )
    val s3 = s2.clientAuth(config.clientAuth.clientAuth)
    s3.build()
  }

  /** We know this operation is safe due to the definition of [[io.grpc.ServerBuilder]].
    * This method isolates the usage of `asInstanceOf` to only here.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def reifyBuilder(builder: ServerBuilder[_]): ServerBuilder[_ <: ServerBuilder[_]] =
    builder.asInstanceOf[ServerBuilder[_ <: ServerBuilder[_]]]

}
