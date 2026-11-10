// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.tls.{ServerAuthRequirementConfig, TlsServerConfig}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.PemFile
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.platform.apiserver.tls.OcspResponderFixture
import org.scalatest.wordspec.AnyWordSpec

import java.io.{File, FileInputStream}
import java.security.cert.{CertificateException, CertificateFactory, X509Certificate}
import javax.net.ssl.X509TrustManager
import scala.jdk.CollectionConverters.*
import scala.util.Using

class HttpServiceRevocationCheckingTest
    extends AnyWordSpec
    with BaseTest
    with OcspResponderFixture {

  private def serverConfig(
      revocationChecking: Boolean,
      clientAuth: ServerAuthRequirementConfig,
  ): TlsServerConfig =
    TlsServerConfig(
      certChainFile = PemFile(ExistingFile.tryCreate(serverCrt)),
      privateKeyFile = PemFile(ExistingFile.tryCreate(serverKey)),
      trustCollectionFile = Some(PemFile(ExistingFile.tryCreate(caCrt))),
      clientAuth = clientAuth,
      enableCertRevocationChecking = revocationChecking,
    )

  private def trustManager(
      revocationChecking: Boolean,
      clientAuth: ServerAuthRequirementConfig = ServerAuthRequirementConfig.Optional,
  ): X509TrustManager = {
    val keyStore = HttpService.buildKeyStore(
      certFile = new FileInputStream(serverCrt),
      privateKeyFile = serverKey.toPath,
      caCertFile = new FileInputStream(caCrt),
    )
    HttpService
      .serverTrustManagerFactory(serverConfig(revocationChecking, clientAuth), keyStore)
      .getTrustManagers
      .collectFirst { case tm: X509TrustManager => tm }
      .value
  }

  private def chain(file: File): Array[X509Certificate] =
    Using.resource(new FileInputStream(file)) { in =>
      CertificateFactory
        .getInstance("X.509")
        .generateCertificates(in)
        .asScala
        .collect { case c: X509Certificate => c }
        .toArray
    }

  "HttpService server trust managers" when {
    "certificate revocation checking is enabled" should {
      "accept valid client certificates" in {
        noException should be thrownBy trustManager(revocationChecking = true)
          .checkClientTrusted(chain(clientCrt), "RSA")
      }

      "reject revoked client certificates" in {
        a[CertificateException] should be thrownBy trustManager(revocationChecking = true)
          .checkClientTrusted(chain(clientRevokedCrt), "RSA")
      }

      "not install a revocation checker if client certificates are not requested" in {
        noException should be thrownBy trustManager(
          revocationChecking = true,
          clientAuth = ServerAuthRequirementConfig.None,
        ).checkClientTrusted(chain(clientRevokedCrt), "RSA")
      }
    }

    "certificate revocation checking is not enabled" should {
      "accept revoked client certificates" in {
        noException should be thrownBy trustManager(revocationChecking = false)
          .checkClientTrusted(chain(clientRevokedCrt), "RSA")
      }
    }
  }
}
