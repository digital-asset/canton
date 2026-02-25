// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tls

import com.daml.tls.TlsVersion.TlsVersion
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext
import org.slf4j.LoggerFactory

import java.io.File
import scala.jdk.CollectionConverters.*

// Interacting with java libraries makes null a necessity
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.AsInstanceOf"))
final case class TlsConfiguration(
    enabled: Boolean,
    certChainFile: Option[File] = None, // mutual auth is disabled if null
    privateKeyFile: Option[File] = None,
    trustCollectionFile: Option[File] = None, // System default if null
) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** If enabled and all required fields are present, it returns an SslContext suitable for client
    * usage
    */
  def client(enabledProtocols: Seq[TlsVersion] = Seq.empty): Option[SslContext] =
    if (enabled) {
      val enabledProtocolsNames =
        if (enabledProtocols.isEmpty)
          null
        else
          enabledProtocols.map(_.version).asJava
      val sslContext = GrpcSslContexts
        .forClient()
        .keyManager(
          certChainFile.orNull,
          privateKeyFile.orNull,
        )
        .trustManager(trustCollectionFile.orNull)
        .protocols(enabledProtocolsNames)
        .sslProvider(SslContext.defaultClientProvider())
        .build()
      logTlsProtocolsAndCipherSuites(sslContext, isServer = false)
      Some(sslContext)
    } else None

  private[tls] def logTlsProtocolsAndCipherSuites(
      sslContext: SslContext,
      isServer: Boolean,
  ): Unit = {
    val (who, provider) =
      if (isServer)
        ("Server", SslContext.defaultServerProvider())
      else
        ("Client", SslContext.defaultClientProvider())
    val tlsInfo = TlsInfo.fromSslContext(sslContext)
    logger.info(s"$who TLS - enabled via $provider")
    logger.debug(
      s"$who TLS - supported protocols: ${filterSSLv2Hello(tlsInfo.supportedProtocols).mkString(", ")}."
    )
    logger.info(
      s"$who TLS - enabled protocols: ${filterSSLv2Hello(tlsInfo.enabledProtocols).mkString(", ")}."
    )
    logger.debug(
      s"$who TLS $who - supported cipher suites: ${tlsInfo.supportedCipherSuites.mkString(", ")}."
    )
    logger.info(s"$who TLS - enabled cipher suites: ${tlsInfo.enabledCipherSuites.mkString(", ")}.")
  }

  /** Netty incorrectly hardcodes the report that the SSLv2Hello protocol is enabled. There is no
    * way to stop it from doing it, so we just filter the netty's erroneous claim. We also make sure
    * that the SSLv2Hello protocol is knocked out completely at the JSSE level through the
    * ProtocolDisabler
    */
  private def filterSSLv2Hello(protocols: Seq[String]): Seq[String] =
    protocols.filter(_ != ProtocolDisabler.sslV2Protocol)

}

object TlsConfiguration {
  val Empty: TlsConfiguration = TlsConfiguration(
    enabled = true,
    certChainFile = None,
    privateKeyFile = None,
    trustCollectionFile = None,
  )
}
