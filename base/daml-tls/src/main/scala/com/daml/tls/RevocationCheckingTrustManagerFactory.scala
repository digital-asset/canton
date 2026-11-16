// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.tls

import com.digitalasset.canton.config.PemFileOrString

import java.security.KeyStore
import java.security.cert.{
  CertPathValidator,
  CertificateFactory,
  PKIXBuilderParameters,
  PKIXRevocationChecker,
  X509CertSelector,
  X509Certificate,
}
import javax.net.ssl.{CertPathTrustManagerParameters, TrustManagerFactory, X509TrustManager}
import scala.jdk.CollectionConverters.*

/** Builds a [[javax.net.ssl.TrustManagerFactory]] that checks certificate revocation for the SSL
  * context it is installed in, without modifying JVM-wide properties such as
  * `com.sun.net.ssl.checkRevocation` or `ocsp.enable`.
  *
  * Revocation status is determined via Online Certificate Status Protocol (OCSP) using the
  * responder URL from the certificate's Authority Information Access extension, falling back to
  * Certificate Revocation List (CRL). Validation fails if the status cannot be determined.
  */
object RevocationCheckingTrustManagerFactory {

  /** @param trustCollection
    *   certificates to trust. If absent, the trust anchors of the JVM default trust store are used
    *   (honouring `javax.net.ssl.trustStore` and related properties).
    */
  def apply(trustCollection: Option[PemFileOrString]): TrustManagerFactory = {
    val anchors = trustCollection.fold(defaultTrustAnchors())(loadCertificates)
    require(
      anchors.nonEmpty,
      "TLS trust collection must contain at least one X.509 certificate",
    )

    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    loadEmpty(keyStore)
    anchors.zipWithIndex.foreach { case (certificate, index) =>
      keyStore.setCertificateEntry(s"trust-anchor-$index", certificate)
    }

    val parameters = new PKIXBuilderParameters(keyStore, new X509CertSelector)
    parameters.addCertPathChecker(newRevocationChecker())

    val factory = TrustManagerFactory.getInstance("PKIX")
    factory.init(new CertPathTrustManagerParameters(parameters))
    factory
  }

  private def newRevocationChecker(): PKIXRevocationChecker =
    CertPathValidator.getInstance("PKIX").getRevocationChecker match {
      case checker: PKIXRevocationChecker => checker
      case other =>
        throw new IllegalStateException(
          s"Unexpected PKIX revocation checker type: ${other.getClass.getName}"
        )
    }

  private def loadCertificates(pem: PemFileOrString): Seq[X509Certificate] = {
    val stream = pem.pemStream
    try {
      CertificateFactory
        .getInstance("X.509")
        .generateCertificates(stream)
        .asScala
        .toSeq
        .collect { case certificate: X509Certificate => certificate }
    } finally stream.close()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def defaultTrustAnchors(): Seq[X509Certificate] = {
    val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    factory.init(null: KeyStore)
    factory.getTrustManagers.toSeq
      .collect { case trustManager: X509TrustManager => trustManager }
      .flatMap(_.getAcceptedIssuers.toSeq)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def loadEmpty(keyStore: KeyStore): Unit =
    keyStore.load(null, null)
}
