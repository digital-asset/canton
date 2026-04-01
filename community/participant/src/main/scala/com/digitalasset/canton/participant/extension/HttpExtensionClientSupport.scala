// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}
import com.digitalasset.canton.time.Clock

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path}
import java.security.{KeyStore, SecureRandom}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.time.Duration
import java.util.UUID
import javax.net.ssl.{SSLContext, TrustManager, TrustManagerFactory, X509TrustManager}
import scala.annotation.unused
import scala.jdk.CollectionConverters.*
import scala.util.Using
import scala.util.Random

private[participant] trait HttpExtensionClientRuntime {
  def nowMillis(): Long
  def sleepMillis(ms: Long): Unit
  def newRequestId(): String
  def nextRetryJitterDouble(): Double
}

private[participant] object HttpExtensionClientRuntime {
  val system: HttpExtensionClientRuntime = new HttpExtensionClientRuntime {
    override def nowMillis(): Long = System.currentTimeMillis()
    override def sleepMillis(ms: Long): Unit = Threading.sleep(ms)
    override def newRequestId(): String = UUID.randomUUID().toString
    override def nextRetryJitterDouble(): Double = Random.nextDouble()
  }

  def fromClock(clock: Clock): HttpExtensionClientRuntime = new HttpExtensionClientRuntime {
    override def nowMillis(): Long = clock.now.toInstant.toEpochMilli
    override def sleepMillis(ms: Long): Unit = Threading.sleep(ms)
    override def newRequestId(): String = UUID.randomUUID().toString
    override def nextRetryJitterDouble(): Double = Random.nextDouble()
  }
}

private[extension] final case class HttpExtensionClientRequest(
    uri: URI,
    timeout: Duration,
    headers: Seq[(String, String)],
    body: String,
)

private[extension] final case class HttpExtensionClientResponse(
    statusCode: Int,
    body: String,
    headers: Map[String, Seq[String]],
)

private[extension] trait HttpExtensionClientTransport {
  def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse
}

private[extension] final case class HttpExtensionClientResources(
    resourceTransport: HttpExtensionClientTransport,
    tokenTransport: Option[HttpExtensionClientTransport] = None,
)

private[extension] trait HttpExtensionClientResourcesFactory {
  def create(config: ExtensionServiceConfig): HttpExtensionClientResources
}

private[extension] final case class JdkHttpExtensionClientSettings(
    version: HttpClient.Version,
    connectTimeout: Duration,
    insecureTls: Boolean,
    trustCollectionFile: Option[Path],
)

private[extension] object JdkHttpExtensionClientResourcesFactory {

  def settingsFor(config: ExtensionServiceConfig): JdkHttpExtensionClientSettings =
    JdkHttpExtensionClientSettings(
      version = HttpClient.Version.HTTP_1_1,
      connectTimeout = Duration.ofMillis(config.connectTimeout.underlying.toMillis),
      insecureTls = config.useTls && config.tlsInsecure,
      trustCollectionFile = if (config.useTls) config.trustCollectionFile else None,
    )

  def tokenSettingsFor(config: ExtensionServiceConfig): Option[JdkHttpExtensionClientSettings] =
    config.auth match {
      case auth: ExtensionServiceAuthConfig.OAuth =>
        Some(
          JdkHttpExtensionClientSettings(
            version = HttpClient.Version.HTTP_1_1,
            connectTimeout = Duration.ofMillis(config.connectTimeout.underlying.toMillis),
            insecureTls = auth.tokenEndpoint.tlsInsecure,
            trustCollectionFile = auth.tokenEndpoint.trustCollectionFile,
          )
        )
      case _ =>
        None
    }

  def buildClient(settings: JdkHttpExtensionClientSettings): HttpClient = {
    val builder = HttpClient
      .newBuilder()
      .version(settings.version)
      .connectTimeout(settings.connectTimeout)

    if (settings.insecureTls) {
      builder.sslContext(JdkHttpExtensionClientResourcesFactory.createInsecureSSLContext())
    } else {
      settings.trustCollectionFile.foreach { trustCollectionFile =>
        builder.sslContext(
          JdkHttpExtensionClientResourcesFactory.createTrustCollectionSSLContext(trustCollectionFile)
        )
      }
    }

    builder.build()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def createTrustCollectionSSLContext(trustCollectionFile: Path): SSLContext = {
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(null, null)

    val certificateFactory = CertificateFactory.getInstance("X.509")
    val certificates = Using.resource(Files.newInputStream(trustCollectionFile)) { inputStream =>
      certificateFactory.generateCertificates(inputStream).asScala.toSeq.map {
        case certificate: X509Certificate => certificate
        case certificate =>
          throw new IllegalArgumentException(
            s"Unsupported certificate type in trust collection: ${certificate.getType}"
          )
      }
    }

    if (certificates.isEmpty) {
      throw new IllegalArgumentException(
        s"No certificates found in trust collection file: $trustCollectionFile"
      )
    }

    certificates.zipWithIndex.foreach { case (certificate, index) =>
      trustStore.setCertificateEntry(s"trusted-$index", certificate)
    }

    val trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(trustStore)

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, trustManagerFactory.getTrustManagers, new SecureRandom())
    sslContext
  }

  /** Create an insecure SSL context for development (trusts all certificates) */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def createInsecureSSLContext(): SSLContext = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager {
      def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      def getAcceptedIssuers(): Array[X509Certificate] = Array.empty
    })

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, trustAllCerts, new SecureRandom())
    sslContext
  }
}

private[extension] final class JdkHttpExtensionClientResourcesFactory(
    @unused _loggerFactory: NamedLoggerFactory
) extends HttpExtensionClientResourcesFactory {

  override def create(config: ExtensionServiceConfig): HttpExtensionClientResources = {
    val resourceSettings = JdkHttpExtensionClientResourcesFactory.settingsFor(config)
    val resourceTransport = new JdkHttpExtensionClientTransport(
      JdkHttpExtensionClientResourcesFactory.buildClient(resourceSettings)
    )
    val tokenTransport = JdkHttpExtensionClientResourcesFactory
      .tokenSettingsFor(config)
      .map(settings => new JdkHttpExtensionClientTransport(JdkHttpExtensionClientResourcesFactory.buildClient(settings)))

    HttpExtensionClientResources(
      resourceTransport = resourceTransport,
      tokenTransport = tokenTransport,
    )
  }
}

private[extension] final class JdkHttpExtensionClientTransport(httpClient: HttpClient)
    extends HttpExtensionClientTransport {

  override def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse = {
    val requestBuilder = HttpRequest
      .newBuilder()
      .uri(request.uri)
      .timeout(request.timeout)

    val builderWithHeaders = request.headers.foldLeft(requestBuilder) { case (builder, (header, value)) =>
      builder.header(header, value)
    }

    val httpRequest = builderWithHeaders
      .POST(HttpRequest.BodyPublishers.ofString(request.body))
      .build()

    val response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString())

    HttpExtensionClientResponse(
      statusCode = response.statusCode(),
      body = response.body(),
      headers = response.headers().map().asScala.iterator.map { case (header, values) =>
        header -> values.asScala.toSeq
      }.toMap,
    )
  }
}
