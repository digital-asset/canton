// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tls

import com.digitalasset.canton.config.{PemFile, PemFileOrString}
import io.grpc.netty.shaded.io.netty.handler.ssl.{ClientAuth, SslContext}
import org.slf4j.LoggerFactory

sealed trait TlsConfig {
  def certChainFile: PemFileOrString
  def privateKeyFile: PemFile
  def minimumServerProtocolVersion: Option[String]
  def ciphers: Option[Seq[String]]

  def protocols: Option[Seq[String]] =
    minimumServerProtocolVersion.map { minVersion =>
      val knownTlsVersions =
        Seq(
          TlsVersion.V1.version,
          TlsVersion.V1_1.version,
          TlsVersion.V1_2.version,
          TlsVersion.V1_3.version,
        )
      knownTlsVersions
        .find(_ == minVersion)
        .fold[Seq[String]](
          throw new IllegalArgumentException(s"Unknown TLS protocol version $minVersion")
        )(versionFound => knownTlsVersions.filter(_ >= versionFound))
    }
}

/** A wrapper for TLS server parameters supporting only server side authentication
  *
  * Same parameters as the more complete `TlsServerConfig`
  */
final case class BaseServerTlsConfig(
    certChainFile: PemFileOrString,
    privateKeyFile: PemFile,
    minimumServerProtocolVersion: Option[String] = Some(
      TlsServerConfig.defaultMinimumServerProtocol
    ),
    ciphers: Option[Seq[String]] = TlsServerConfig.defaultCiphers,
) extends TlsConfig

/** A wrapper for TLS related server parameters supporting mutual authentication.
  *
  * Certificates and keys must be provided in the PEM format. It is recommended to create them with
  * OpenSSL. Other formats (such as GPG) may also work, but have not been tested.
  *
  * @param certChainFile
  *   a file containing a certificate chain, containing the certificate chain from the server to the
  *   root CA. The certificate chain is used to authenticate the server. The order of certificates
  *   in the chain matters, i.e., it must start with the server certificate and end with the root
  *   certificate.
  * @param privateKeyFile
  *   a file containing the server's private key. The key must not use a password.
  * @param trustCollectionFile
  *   a file containing certificates of all nodes the server trusts. Used for client authentication.
  *   It depends on the enclosing configuration whether client authentication is mandatory, optional
  *   or unsupported. If client authentication is enabled and this parameter is absent, the
  *   certificates in the JVM trust store will be used instead.
  * @param clientAuth
  *   indicates whether server requires, requests, or does not request auth from clients. Normally
  *   the ledger api server requires client auth under TLS, but using this setting this requirement
  *   can be loosened. See
  *   https://github.com/digital-asset/daml/commit/edd73384c427d9afe63bae9d03baa2a26f7b7f54
  * @param minimumServerProtocolVersion
  *   minimum supported TLS protocol. Set None (or null in config file) to default to JVM settings.
  * @param ciphers
  *   supported ciphers. Set to None (or null in config file) to default to JVM settings.
  * @param enableCertRevocationChecking
  *   whether to enable certificate revocation checking per
  *   https://tersesystems.com/blog/2014/03/22/fixing-certificate-revocation/
  */
// Information in this ScalaDoc comment has been taken from https://grpc.io/docs/guides/auth/.
final case class TlsServerConfig(
    certChainFile: PemFileOrString,
    privateKeyFile: PemFile,
    trustCollectionFile: Option[PemFileOrString] = None,
    clientAuth: ServerAuthRequirementConfig = ServerAuthRequirementConfig.Optional,
    minimumServerProtocolVersion: Option[String] = Some(
      TlsServerConfig.defaultMinimumServerProtocol
    ),
    ciphers: Option[Seq[String]] = TlsServerConfig.defaultCiphers,
    enableCertRevocationChecking: Boolean = false,
) extends TlsConfig {
  lazy val clientConfig: TlsClientConfig = {
    val clientCert = clientAuth match {
      case ServerAuthRequirementConfig.Require(cert) => Some(cert)
      case _ => None
    }
    TlsClientConfig(trustCollectionFile = Some(certChainFile), clientCert = clientCert)
  }

  /** This is a side-effecting method. It modifies JVM TLS properties according to the TLS
    * configuration.
    */
  def setJvmTlsProperties(): Unit = {
    if (enableCertRevocationChecking) OcspProperties.enableOcsp()
    ProtocolDisabler.disableSSLv2Hello()
  }

  override def protocols: Option[Seq[String]] = {
    val disallowedTlsVersions =
      Seq(
        TlsVersion.V1.version,
        TlsVersion.V1_1.version,
      )
    minimumServerProtocolVersion match {
      case Some(minVersion) if disallowedTlsVersions.contains(minVersion) =>
        throw new IllegalArgumentException(s"Unsupported TLS version: $minVersion")
      case _ =>
        super.protocols
    }
  }

}

object TlsServerConfig {

  // default OWASP strong cipher set with broad compatibility (B list)
  // https://cheatsheetseries.owasp.org/cheatsheets/TLS_Cipher_String_Cheat_Sheet.html
  lazy val defaultCiphers = {
    val candidates = Seq(
      "TLS_AES_256_GCM_SHA384",
      "TLS_CHACHA20_POLY1305_SHA256",
      "TLS_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
      "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    )
    val logger = LoggerFactory.getLogger(TlsServerConfig.getClass)
    val filtered = candidates.filter { x =>
      io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl
        .availableOpenSslCipherSuites()
        .contains(x) ||
      io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.availableJavaCipherSuites().contains(x)
    }
    if (filtered.isEmpty) {
      val len = io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl
        .availableOpenSslCipherSuites()
        .size() + io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl
        .availableJavaCipherSuites()
        .size()
      logger.warn(
        s"All of Canton's default TLS ciphers are unsupported by your JVM (netty reports $len ciphers). Defaulting to JVM settings."
      )
      if (!io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.isAvailable) {
        logger.info(
          "Netty OpenSSL is not available because of an issue",
          io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.unavailabilityCause(),
        )
      }
      None
    } else {
      logger.debug(
        s"Using ${filtered.length} out of ${candidates.length} Canton's default TLS ciphers"
      )
      Some(filtered)
    }
  }

  val defaultMinimumServerProtocol = "TLSv1.2"

  /** Netty incorrectly hardcodes the report that the SSLv2Hello protocol is enabled. There is no
    * way to stop it from doing it, so we just filter the netty's erroneous claim. We also make sure
    * that the SSLv2Hello protocol is knocked out completely at the JSSE level through the
    * ProtocolDisabler
    */
  private def filterSSLv2Hello(protocols: Seq[String]): Seq[String] =
    protocols.filter(_ != ProtocolDisabler.sslV2Protocol)

  def logTlsProtocolsAndCipherSuites(
      sslContext: SslContext,
      isServer: Boolean,
  ): Unit = {
    val (who, provider, logger) =
      if (isServer)
        (
          "Server",
          SslContext.defaultServerProvider(),
          LoggerFactory.getLogger(TlsServerConfig.getClass),
        )
      else
        (
          "Client",
          SslContext.defaultClientProvider(),
          LoggerFactory.getLogger(TlsClientConfig.getClass),
        )

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

}

/** A wrapper for TLS related client configurations
  *
  * @param trustCollectionFile
  *   a file containing certificates of all nodes the client trusts. If none is specified, defaults
  *   to the JVM trust store
  * @param clientCert
  *   the client certificate
  * @param enabled
  *   allows enabling TLS without `trustCollectionFile` or `clientCert`
  */
final case class TlsClientConfig(
    trustCollectionFile: Option[PemFileOrString],
    clientCert: Option[TlsClientCertificate],
    enabled: Boolean = true,
) {
  def withoutClientCert: TlsClientConfigOnlyTrustFile =
    TlsClientConfigOnlyTrustFile(
      trustCollectionFile = trustCollectionFile,
      enabled = enabled,
    )
}

/** A wrapper for TLS related client configurations without client auth support (currently public
  * sequencer api)
  *
  * @param trustCollectionFile
  *   a file containing certificates of all nodes the client trusts. If none is specified, defaults
  *   to the JVM trust store
  * @param enabled
  *   allows enabling TLS without `trustCollectionFile`
  */
final case class TlsClientConfigOnlyTrustFile(
    trustCollectionFile: Option[PemFileOrString],
    enabled: Boolean = true,
) {
  def toTlsClientConfig: TlsClientConfig = TlsClientConfig(
    trustCollectionFile = trustCollectionFile,
    clientCert = None,
    enabled = enabled,
  )
}

final case class TlsClientCertificate(certChainFile: PemFileOrString, privateKeyFile: PemFile)

/** Configuration on whether server requires auth, requests auth, or no auth */
sealed trait ServerAuthRequirementConfig {
  def clientAuth: ClientAuth
}
object ServerAuthRequirementConfig {

  /** A variant of [[ServerAuthRequirementConfig]] by which the server requires auth from clients */
  final case class Require(adminClient: TlsClientCertificate) extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.REQUIRE
  }

  /** A variant of [[ServerAuthRequirementConfig]] by which the server merely requests auth from
    * clients
    */
  case object Optional extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.OPTIONAL
  }

  /** A variant of [[ServerAuthRequirementConfig]] by which the server does not even request auth
    * from clients
    */
  case object None extends ServerAuthRequirementConfig {
    val clientAuth = ClientAuth.NONE
  }
}
