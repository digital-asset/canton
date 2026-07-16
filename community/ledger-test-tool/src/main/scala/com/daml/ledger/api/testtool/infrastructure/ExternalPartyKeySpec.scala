// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.crypto as lapicrypto
import org.bouncycastle.jce.ECNamedCurveTable

import java.security.{KeyPair, KeyPairGenerator, Signature}

sealed trait ExternalPartyKeySpec {
  def name: String
  def keyInstance(): KeyPair
  val keySpec: lapicrypto.SigningKeySpec
  val keyFormat: lapicrypto.CryptoKeyFormat
  def signatureInstance(): Signature
  val signatureFormat: lapicrypto.SignatureFormat
  val signatureAlgorithmSpec: lapicrypto.SigningAlgorithmSpec
}

object ExternalPartyKeySpec {
  case object EcCurve25519 extends ExternalPartyKeySpec {
    override def name = "EcCurve25519"

    override def keyInstance() =
      KeyPairGenerator.getInstance("Ed25519").generateKeyPair()

    override def signatureInstance() =
      Signature.getInstance("Ed25519")

    override val keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519
    override val keyFormat =
      lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO
    override val signatureFormat = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW
    override val signatureAlgorithmSpec =
      lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519
  }

  case object EcP256 extends ExternalPartyKeySpec {
    override def name = "EcP256"

    override def keyInstance() = {
      val keyGen = KeyPairGenerator.getInstance("EC")
      keyGen.initialize(256)
      keyGen.generateKeyPair()
    }

    override def signatureInstance() =
      Signature.getInstance("SHA256withECDSA")

    override val keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256
    override val keyFormat =
      lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO
    override val signatureFormat = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW
    override val signatureAlgorithmSpec =
      lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256
  }

  case object EcP384 extends ExternalPartyKeySpec {
    override def name = "EcP384"

    override def keyInstance() = {
      val keyGen = KeyPairGenerator.getInstance("EC")
      keyGen.initialize(384)
      keyGen.generateKeyPair()
    }

    override def signatureInstance() =
      Signature.getInstance("SHA384withECDSA")

    override val keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384
    override val keyFormat =
      lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO
    override val signatureFormat = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW
    override val signatureAlgorithmSpec =
      lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384
  }

  case object EcSecp256k1 extends ExternalPartyKeySpec {
    override def name = "EcSecp256k1"

    override def keyInstance() = {
      val spec = ECNamedCurveTable.getParameterSpec("secp256k1")
      val keyGen = KeyPairGenerator.getInstance("EC", "BC")
      keyGen.initialize(spec)
      keyGen.generateKeyPair()
    }

    override def signatureInstance() =
      Signature.getInstance("SHA256withECDSA", "BC")

    override val keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1
    override val keyFormat =
      lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO
    override val signatureFormat = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW
    override val signatureAlgorithmSpec =
      lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256
  }

  lazy val default: ExternalPartyKeySpec = EcCurve25519

  // TODO(i33297): Add ML-DSA-65 and SLH-DSA
  lazy val supported = List[ExternalPartyKeySpec](
    EcCurve25519,
    EcP256,
    EcP384,
    EcSecp256k1,
  )
}
