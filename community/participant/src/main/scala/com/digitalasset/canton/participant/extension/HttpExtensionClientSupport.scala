// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ExtensionServiceConfig

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.UUID
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import scala.annotation.unused
import scala.jdk.CollectionConverters.*
import scala.util.Random

private[extension] trait HttpExtensionClientRuntime {
  def nowMillis(): Long
  def sleepMillis(ms: Long): Unit
  def newRequestId(): String
  def nextRetryJitterDouble(): Double
}

private[extension] object HttpExtensionClientRuntime {
  val system: HttpExtensionClientRuntime = new HttpExtensionClientRuntime {
    override def nowMillis(): Long = System.currentTimeMillis()
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
)

private[extension] object JdkHttpExtensionClientResourcesFactory {

  def settingsFor(config: ExtensionServiceConfig): JdkHttpExtensionClientSettings =
    JdkHttpExtensionClientSettings(
      version = HttpClient.Version.HTTP_1_1,
      connectTimeout = Duration.ofMillis(config.connectTimeout.underlying.toMillis),
      insecureTls = config.useTls && config.tlsInsecure,
    )

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
    val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(config)
    val builder = HttpClient
      .newBuilder()
      .version(settings.version)
      .connectTimeout(settings.connectTimeout)

    if (settings.insecureTls) {
      builder.sslContext(JdkHttpExtensionClientResourcesFactory.createInsecureSSLContext())
    }

    HttpExtensionClientResources(
      resourceTransport = new JdkHttpExtensionClientTransport(builder.build())
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
