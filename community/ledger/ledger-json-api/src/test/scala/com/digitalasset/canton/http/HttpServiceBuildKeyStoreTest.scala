// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.JarResourceUtils
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileInputStream

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
}
