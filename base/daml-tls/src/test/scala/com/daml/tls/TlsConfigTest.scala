// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tls

import com.digitalasset.canton.config.PemFile
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files

class TlsConfigTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  // Create temporary files on disk so ExistingFile validation succeeds
  private val tempCertFile = Files.createTempFile("dummy", ".crt").toFile
  private val tempKeyFile = Files.createTempFile("dummy", ".pem").toFile
  tempCertFile.deleteOnExit()
  tempKeyFile.deleteOnExit()

  // Dummy files to instantiate the configs
  private val dummyCert = PemFile(ExistingFile.tryCreate(tempCertFile))
  private val dummyKey = PemFile(ExistingFile.tryCreate(tempKeyFile))
  private val dummyClientCert = TlsClientCertificate(dummyCert, dummyKey)

  private def buildConfigs(protocolOpt: Option[String]): List[TlsConfig] =
    List[TlsConfig](
      BaseServerTlsConfig(dummyCert, dummyKey, minimumServerProtocolVersion = protocolOpt),
      TlsServerConfig(dummyCert, dummyKey, minimumServerProtocolVersion = protocolOpt),
    )

  "TlsConfig.protocols" should {
    "reject deprecated and insecure TLS versions" in {
      val insecureVersions = Table("version", "TLSv1", "TLSv1.1")
      forAll(insecureVersions) { version =>
        buildConfigs(Some(version)).foreach { config =>
          val ex = intercept[IllegalArgumentException](config.protocols)
          ex.getMessage should include("deprecated and insecure")
        }
      }
    }

    "reject unknown protocol versions" in {
      val unknownVersions = Table("version", "TLSv9.9", "SSLv3", "unknownTLS")
      forAll(unknownVersions) { version =>
        buildConfigs(Some(version)).foreach { config =>
          val ex = intercept[IllegalArgumentException](config.protocols)
          ex.getMessage should include("unknown")
        }
      }
    }

    "filter the known protocols list correctly for valid inputs" in {
      val validVersions = Table(
        ("minimumVersion", "expectedProtocols"),
        ("TLSv1.2", Seq("TLSv1.2", "TLSv1.3")),
        ("TLSv1.3", Seq("TLSv1.3")),
      )
      forAll(validVersions) { (minVersion, expected) =>
        buildConfigs(Some(minVersion)).foreach { config =>
          config.protocols shouldBe Some(expected)
        }
      }
    }

    "return None when minimumServerProtocolVersion is set to None" in {
      buildConfigs(None).foreach { config =>
        config.protocols shouldBe None
      }
    }

    "default to TLSv1.2 when no protocol is specified during instantiation" in {
      val defaultBaseConfig = BaseServerTlsConfig(dummyCert, dummyKey)
      val defaultServerConfig = TlsServerConfig(dummyCert, dummyKey)

      defaultBaseConfig.protocols shouldBe Some(Seq("TLSv1.2", "TLSv1.3"))
      defaultServerConfig.protocols shouldBe Some(Seq("TLSv1.2", "TLSv1.3"))
    }
  }

  "TlsConfig.defaultCiphers" should {
    "evaluate safely without throwing exceptions" in {
      // Ensure the lazy val evaluates cleanly without crashing,
      // regardless of the underlying JVM's OpenSSL capabilities.
      noException should be thrownBy TlsConfig.defaultCiphers
    }
  }

  "TlsServerConfig.clientConfig" should {
    "map ServerAuthRequirementConfig.Require to a client config with the certificate" in {
      val config = TlsServerConfig(
        certChainFile = dummyCert,
        privateKeyFile = dummyKey,
        clientAuth = ServerAuthRequirementConfig.Require(dummyClientCert),
      )

      val generatedClientConfig = config.clientConfig
      generatedClientConfig.clientCert shouldBe Some(dummyClientCert)
      generatedClientConfig.trustCollectionFile shouldBe Some(dummyCert)
    }

    "map Optional and None to a client config without a certificate" in {

      val authTypes: List[ServerAuthRequirementConfig] = List[ServerAuthRequirementConfig](
        ServerAuthRequirementConfig.Optional,
        ServerAuthRequirementConfig.None,
      )

      authTypes.foreach { auth =>
        val config = TlsServerConfig(
          certChainFile = dummyCert,
          privateKeyFile = dummyKey,
          clientAuth = auth,
        )
        config.clientConfig.clientCert shouldBe None
      }
    }
  }

  "TlsClientConfig transformations" should {
    "correctly strip and restore client certificates" in {
      val originalConfig = TlsClientConfig(
        trustCollectionFile = Some(dummyCert),
        clientCert = Some(dummyClientCert),
        enabled = true,
      )

      val strippedConfig = originalConfig.withoutClientCert
      strippedConfig.trustCollectionFile shouldBe Some(dummyCert)
      strippedConfig.enabled shouldBe true

      val restoredConfig = strippedConfig.toTlsClientConfig
      restoredConfig.clientCert shouldBe None
      restoredConfig.trustCollectionFile shouldBe Some(dummyCert)
      restoredConfig.enabled shouldBe true
    }
  }

  "ServerAuthRequirementConfig" should {
    "map to the correct Netty ClientAuth enums" in {
      ServerAuthRequirementConfig.Require(dummyClientCert).clientAuth shouldBe ClientAuth.REQUIRE
      ServerAuthRequirementConfig.Optional.clientAuth shouldBe ClientAuth.OPTIONAL
      ServerAuthRequirementConfig.None.clientAuth shouldBe ClientAuth.NONE
    }
  }

  "TlsServerConfig forwarders" should {
    "maintain backward compatibility by matching TlsConfig defaults" in {
      TlsServerConfig.defaultMinimumServerProtocol shouldBe TlsConfig.defaultMinimumServerProtocol
      TlsServerConfig.defaultCiphers shouldBe TlsConfig.defaultCiphers
    }
  }

  "TlsConfig.defaultCiphers content" should {
    "contain only modern TLS ciphers if present" in {
      TlsConfig.defaultCiphers.foreach { ciphers =>
        ciphers should not be empty
        ciphers.foreach { cipher =>
          cipher should startWith("TLS_")
          cipher should not include "RC4"
          cipher should not include "MD5"
          cipher should not include "DES"
          cipher should not include "SHA1" // OpenSSL-style SHA-1
          cipher should not endWith "_SHA" // Java JSSE-style SHA-1 (e.g. _CBC_SHA)
        }
      }
    }
  }

  "TlsServerConfig.setJvmTlsProperties" should {
    "execute cleanly when enableCertRevocationChecking is enabled" in {
      val config = TlsServerConfig(
        certChainFile = dummyCert,
        privateKeyFile = dummyKey,
        enableCertRevocationChecking = true,
      )

      try {
        noException should be thrownBy config.setJvmTlsProperties()
      } finally {
        // Cleanup: Reset the global JVM properties
        System.clearProperty("com.sun.net.ssl.checkRevocation")
        System.clearProperty("com.sun.security.enableCRLDP")
        java.security.Security.setProperty("ocsp.enable", "false")
      }
    }
  }
}
