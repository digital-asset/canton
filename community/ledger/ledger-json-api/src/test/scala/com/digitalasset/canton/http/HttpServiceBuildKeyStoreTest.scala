// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.JarResourceUtils
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileInputStream
import java.security.cert.{Certificate, X509Certificate}
import javax.net.ssl.{X509KeyManager, X509TrustManager}

class HttpServiceBuildKeyStoreTest extends AnyWordSpec with BaseTest {

  private def certResource(name: String) =
    JarResourceUtils.resourceFile("test-certificates/" + name)

  "HttpService.buildKeyStore" should {

    "include all certificates from a multi-certificate chain file (leaf + intermediate)" in {
      // cert-chain-file contains leaf certificate + intermediate/CA certificate
      val serverChainCrt = certResource("server-chain.crt")
      val caCrt = certResource("ca.crt")
      val serverKey = certResource("server.pem")

      val keyStore = HttpService.buildKeyStore(
        certFile = new FileInputStream(serverChainCrt),
        privateKeyFile = serverKey.toPath,
        caCertFile = new FileInputStream(caCrt),
      )

      val chain = keyStore.getCertificateChain("key")
      chain should not be null
      chain should have length 2

      val leafCert = chain.head.asInstanceOf[java.security.cert.X509Certificate]
      leafCert.getSubjectX500Principal.getName should include("CN=")
      noException should be thrownBy leafCert.verify(chain(1).getPublicKey)
    }

    "work with a single-certificate chain file" in {
      val serverCrt = certResource("server.crt")
      val caCrt = certResource("ca.crt")
      val serverKey = certResource("server.pem")

      val keyStore = HttpService.buildKeyStore(
        certFile = new FileInputStream(serverCrt),
        privateKeyFile = serverKey.toPath,
        caCertFile = new FileInputStream(caCrt),
      )

      val chain = keyStore.getCertificateChain("key")
      chain should not be null
      chain should have length 1
    }
  }

  private def keyStore(certName: String, keyName: String) =
    HttpService.buildKeyStore(
      certFile = new FileInputStream(certResource(certName)),
      privateKeyFile = certResource(keyName).toPath,
      caCertFile = new FileInputStream(certResource("ca.crt")),
    )

  private def subjectOf(certs: Array[Certificate]): String =
    certs.head.asInstanceOf[X509Certificate].getSubjectX500Principal.getName

  "HttpService.buildKeyManagerFactory" should {

    "use the PKIX algorithm and expose the configured key and certificate" in {
      val factory = HttpService.buildKeyManagerFactory(keyStore("server.crt", "server.pem"))
      factory.getAlgorithm shouldBe "PKIX"

      val keyManager = factory.getKeyManagers.collectFirst { case km: X509KeyManager => km }.value
      val alias = keyManager.chooseServerAlias("RSA", null, null)
      alias should not be null
      keyManager.getPrivateKey(alias) should not be null
      subjectOf(keyManager.getCertificateChain(alias).map(c => c: Certificate)) should include(
        "CN=0.0.0.0.server"
      )
    }
  }

  "HttpService.buildTrustManagerFactory" should {

    "use the PKIX algorithm and trust certificates issued by the configured CA" in {
      val factory = HttpService.buildTrustManagerFactory(keyStore("server.crt", "server.pem"))
      factory.getAlgorithm shouldBe "PKIX"

      val trustManager =
        factory.getTrustManagers.collectFirst { case tm: X509TrustManager => tm }.value
      val acceptedIssuers = trustManager.getAcceptedIssuers
      acceptedIssuers should not be empty
      acceptedIssuers.map(_.getSubjectX500Principal.getName) should contain("CN=0.0.0.0.ca")

      val serverChain = keyStore("server.crt", "server.pem")
        .getCertificateChain("key")
        .collect { case c: X509Certificate => c }
      serverChain should not be empty
      subjectOf(serverChain.map(c => c: Certificate)) should include("CN=0.0.0.0.server")

      noException should be thrownBy trustManager.checkServerTrusted(serverChain, "RSA")
      noException should be thrownBy trustManager.checkClientTrusted(serverChain, "RSA")
    }
  }

}
