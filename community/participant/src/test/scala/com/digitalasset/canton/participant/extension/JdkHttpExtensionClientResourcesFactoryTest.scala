// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
  ExtensionServiceTokenEndpointConfig,
}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpsConfigurator, HttpsServer}
import org.scalatest.wordspec.AnyWordSpec

import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, KeyStore, SecureRandom}
import java.time.Duration
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLHandshakeException}
import scala.jdk.CollectionConverters.*
import scala.util.Using

class JdkHttpExtensionClientResourcesFactoryTest extends AnyWordSpec with BaseTest {

  private val tlsDirectory = Paths.get("community/app/src/test/resources/tls")
  private val rootCaFile = tlsDirectory.resolve("root-ca.crt")
  private val serverCertFile = tlsDirectory.resolve("public-api.crt")
  private val serverPrivateKeyFile = tlsDirectory.resolve("public-api.pem")

  private def makeConfig(
      name: String,
      useTls: Boolean = true,
      tlsInsecure: Boolean = false,
      connectTimeoutMs: Long = 500L,
      port: Int = 8443,
      trustCollectionFile: Option[Path] = None,
      auth: ExtensionServiceAuthConfig = ExtensionServiceAuthConfig.NoAuth,
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = useTls,
      trustCollectionFile = trustCollectionFile,
      tlsInsecure = tlsInsecure,
      auth = auth,
      connectTimeout = NonNegativeFiniteDuration.ofMillis(connectTimeoutMs),
    )

  private def oauthAuth(
      tokenTlsInsecure: Boolean = false,
      tokenTrustCollectionFile: Option[Path] = None,
      tokenPort: Int = 443,
  ): ExtensionServiceAuthConfig.OAuth =
    ExtensionServiceAuthConfig.OAuth(
      tokenEndpoint = ExtensionServiceTokenEndpointConfig(
        host = "localhost",
        port = Port.tryCreate(tokenPort),
        path = "/oauth2/token",
        trustCollectionFile = tokenTrustCollectionFile,
        tlsInsecure = tokenTlsInsecure,
      ),
      clientId = "participant1",
      privateKeyFile = Paths.get("/tmp/test-private-key.der"),
    )

  private def withHttpsServer(test: Int => Any): Unit =
    withHttpsServerRecordingMethods { (port, _) =>
      test(port)
    }

  private def withHttpsServerRecordingMethods(
      test: (Int, CopyOnWriteArrayList[String]) => Any
  ): Unit = {
    val requestMethods = new CopyOnWriteArrayList[String]()
    val server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0)
    server.setHttpsConfigurator(new HttpsConfigurator(buildServerSslContext()))
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          requestMethods.add(exchange.getRequestMethod)
          val body = "ok".getBytes(StandardCharsets.UTF_8)
          exchange.sendResponseHeaders(200, body.length.toLong)
          Using.resource(exchange.getResponseBody)(_.write(body))
          exchange.close()
        }
      },
    )
    server.start()
    try test(server.getAddress.getPort, requestMethods)
    finally server.stop(0)
  }

  private def buildServerSslContext(): SSLContext = {
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val serverCertificate = Using.resource(Files.newInputStream(serverCertFile)) { inputStream =>
      certificateFactory.generateCertificate(inputStream).asInstanceOf[X509Certificate]
    }
    val privateKey = loadPkcs8PrivateKey(serverPrivateKeyFile)
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null, null)
    keyStore.setKeyEntry("server", privateKey, Array.emptyCharArray, Array(serverCertificate))

    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(keyStore, Array.emptyCharArray)

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, null, new SecureRandom())
    sslContext
  }

  private def loadPkcs8PrivateKey(path: Path) = {
    val pem = Files.readString(path, StandardCharsets.US_ASCII)
    val encoded = pem.linesIterator
      .filterNot(line => line.startsWith("-----BEGIN") || line.startsWith("-----END"))
      .mkString
    val keySpec = new PKCS8EncodedKeySpec(Base64.getMimeDecoder.decode(encoded))
    KeyFactory.getInstance("RSA").generatePrivate(keySpec)
  }

  private def httpsRequest(port: Int, path: String): HttpExtensionClientRequest =
    HttpExtensionClientRequest(
      uri = java.net.URI.create(s"https://localhost:$port$path"),
      timeout = Duration.ofSeconds(1),
      headers = Seq.empty,
      body = "",
    )

  "JdkHttpExtensionClientResourcesFactory.settingsFor" should {

    "mark secure TLS configs as not insecure" in {
      val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "secure", useTls = true, tlsInsecure = false)
      )

      settings.version shouldBe HttpClient.Version.HTTP_1_1
      settings.insecureTls shouldBe false
    }

    "mark insecure TLS configs as insecure" in {
      val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "insecure", useTls = true, tlsInsecure = true)
      )

      settings.insecureTls shouldBe true
    }

    "preserve the per-extension connect timeout" in {
      val settings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "timeout", connectTimeoutMs = 1234L)
      )

      settings.connectTimeout shouldBe Duration.ofMillis(1234L)
    }

    "handle mixed secure and insecure configs independently" in {
      val secureSettings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "secure", useTls = true, tlsInsecure = false, connectTimeoutMs = 500L)
      )
      val insecureSettings = JdkHttpExtensionClientResourcesFactory.settingsFor(
        makeConfig(name = "insecure", useTls = true, tlsInsecure = true, connectTimeoutMs = 1500L)
      )

      secureSettings.insecureTls shouldBe false
      secureSettings.connectTimeout shouldBe Duration.ofMillis(500L)
      insecureSettings.insecureTls shouldBe true
      insecureSettings.connectTimeout shouldBe Duration.ofMillis(1500L)
    }

    "keep connect-timeout fixed for both resource and token clients under oauth" in {
      val oauthConfig = makeConfig(
        name = "oauth-timeout",
        connectTimeoutMs = 4321L,
        auth = oauthAuth(),
      )

      val resourceSettings = JdkHttpExtensionClientResourcesFactory.settingsFor(oauthConfig)
      val tokenSettings = JdkHttpExtensionClientResourcesFactory.tokenSettingsFor(oauthConfig).value

      resourceSettings.connectTimeout shouldBe Duration.ofMillis(4321L)
      tokenSettings.connectTimeout shouldBe Duration.ofMillis(4321L)
    }

    "keep resource and token tls-insecure settings independent under oauth" in {
      val resourceInsecureConfig = makeConfig(
        name = "resource-insecure",
        tlsInsecure = true,
        auth = oauthAuth(tokenTlsInsecure = false),
      )
      val tokenInsecureConfig = makeConfig(
        name = "token-insecure",
        tlsInsecure = false,
        auth = oauthAuth(tokenTlsInsecure = true),
      )

      JdkHttpExtensionClientResourcesFactory.settingsFor(resourceInsecureConfig).insecureTls shouldBe true
      JdkHttpExtensionClientResourcesFactory
        .tokenSettingsFor(resourceInsecureConfig)
        .value
        .insecureTls shouldBe false

      JdkHttpExtensionClientResourcesFactory.settingsFor(tokenInsecureConfig).insecureTls shouldBe false
      JdkHttpExtensionClientResourcesFactory
        .tokenSettingsFor(tokenInsecureConfig)
        .value
        .insecureTls shouldBe true
    }
  }

  "JdkHttpExtensionClientResourcesFactory.create" should {

    "create only a resource transport for auth.type = none" in {
      val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)

      val resources = factory.create(makeConfig(name = "no-auth"))

      resources.tokenTransport shouldBe None
    }

    "create separate resource and token transports for auth.type = oauth" in {
      val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)

      val resources = factory.create(
        makeConfig(
          name = "oauth",
          auth = oauthAuth(),
        )
      )

      resources.tokenTransport.value should not be theSameInstanceAs(resources.resourceTransport)
    }

    "keep HTTP client ownership per extension without requiring transport sharing" in {
      val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)

      val first = factory.create(makeConfig(name = "oauth-a", auth = oauthAuth()))
      val second = factory.create(makeConfig(name = "oauth-b", auth = oauthAuth()))

      first.resourceTransport should not be theSameInstanceAs(second.resourceTransport)
      first.tokenTransport.value should not be theSameInstanceAs(second.tokenTransport.value)
    }

    "use HTTP POST for token transport requests" in withHttpsServerRecordingMethods {
      (port, requestMethods) =>
        val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)
        val resources = factory.create(
          makeConfig(
            name = "token-post",
            port = port,
            auth = oauthAuth(
              tokenTrustCollectionFile = Some(rootCaFile),
              tokenPort = port,
            ),
          )
        )

        val response = resources.tokenTransport.value.send(httpsRequest(port, "/oauth2/token"))

        response.statusCode shouldBe 200
        requestMethods.asScala.toSeq shouldBe Seq("POST")
    }

    "use custom trust material for resource requests when configured" in withHttpsServer { port =>
      val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)
      val resources = factory.create(
        makeConfig(
          name = "resource-trust",
          port = port,
          trustCollectionFile = Some(rootCaFile),
        )
      )

      val response = resources.resourceTransport.send(httpsRequest(port, "/resource"))

      response.statusCode shouldBe 200
      response.body shouldBe "ok"
    }

    "fall back to the JVM default trust store when resource custom trust is omitted" in withHttpsServer {
      port =>
        val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)
        val resources = factory.create(
          makeConfig(
            name = "resource-default-trust",
            port = port,
          )
        )

        an[SSLHandshakeException] should be thrownBy {
          resources.resourceTransport.send(httpsRequest(port, "/resource"))
        }
    }

    "keep resource and token custom trust material independent under oauth" in withHttpsServer {
      port =>
        val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)
        val resources = factory.create(
          makeConfig(
            name = "resource-trust-only",
            port = port,
            trustCollectionFile = Some(rootCaFile),
            auth = oauthAuth(
              tokenTrustCollectionFile = None,
              tokenPort = port,
            ),
          )
        )

        resources.resourceTransport.send(httpsRequest(port, "/resource")).statusCode shouldBe 200

        an[SSLHandshakeException] should be thrownBy {
          resources.tokenTransport.value.send(httpsRequest(port, "/token"))
        }
    }

    "use endpoint-specific token trust material when configured under oauth" in withHttpsServer { port =>
      val factory = new JdkHttpExtensionClientResourcesFactory(loggerFactory)
      val resources = factory.create(
        makeConfig(
          name = "token-trust-only",
          port = port,
          trustCollectionFile = None,
          auth = oauthAuth(
            tokenTrustCollectionFile = Some(rootCaFile),
            tokenPort = port,
          ),
        )
      )

      an[SSLHandshakeException] should be thrownBy {
        resources.resourceTransport.send(httpsRequest(port, "/resource"))
      }

      val tokenResponse = resources.tokenTransport.value.send(httpsRequest(port, "/token"))

      tokenResponse.statusCode shouldBe 200
      tokenResponse.body shouldBe "ok"
    }
  }
}
