// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.security.*
import java.security.spec.ECGenParameterSpec

/** Tests for PinnedDataNode signature verification. */
class PinnedDataNodeTest extends AnyWordSpec with Matchers {

  private lazy val (ecKeyPair, ecPubKeyDer) = {
    val kpg = KeyPairGenerator.getInstance("EC")
    kpg.initialize(new ECGenParameterSpec("secp256r1"))
    val kp = kpg.generateKeyPair()
    (kp, kp.getPublic.getEncoded)
  }

  private lazy val (edKeyPair, edPubKeyDer) = {
    val kpg = KeyPairGenerator.getInstance("Ed25519")
    val kp = kpg.generateKeyPair()
    (kp, kp.getPublic.getEncoded)
  }

  private val testNonce = Array.fill(32)(0x42.toByte)
  private val testBody = "price=42.50".getBytes

  private def hashNonceAndBody(nonce: Array[Byte], body: Array[Byte]): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(nonce)
    md.update(body)
    md.digest()
  }

  private def signEcdsa(data: Array[Byte], key: PrivateKey): Array[Byte] = {
    val sig = Signature.getInstance("SHA256withECDSA")
    sig.initSign(key)
    sig.update(data)
    sig.sign()
  }

  private def signEd25519(data: Array[Byte], key: PrivateKey): Array[Byte] = {
    val sig = Signature.getInstance("Ed25519")
    sig.initSign(key)
    sig.update(data)
    sig.sign()
  }

  private def mkNode(
      body: Array[Byte],
      signature: Array[Byte],
      signerKey: Array[Byte],
      signerKeys: Seq[Array[Byte]],
      nonce: Array[Byte] = testNonce,
  ): PinnedDataNode =
    PinnedDataNode(
      endpoint = "oracle:9999",
      payload = ByteString.copyFromUtf8("get_price"),
      signerKeys = signerKeys.map(ByteString.copyFrom),
      maxBytes = 4096,
      timeoutMs = 5000,
      responseBody = ByteString.copyFrom(body),
      responseSignature = ByteString.copyFrom(signature),
      responseSignerKey = ByteString.copyFrom(signerKey),
      responseFetchedAt = 1000000L,
      nonce = ByteString.copyFrom(nonce),
      fetchIndex = 0,
    )

  "PinnedDataNode.verifySignature" should {

    "accept a valid ECDSA signature" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEcdsa(digest, ecKeyPair.getPrivate)

      val node = mkNode(testBody, sig, ecPubKeyDer, Seq(ecPubKeyDer))
      node.verifySignature() shouldBe true
    }

    "accept a valid Ed25519 signature" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEd25519(digest, edKeyPair.getPrivate)

      val node = mkNode(testBody, sig, edPubKeyDer, Seq(edPubKeyDer))
      node.verifySignature() shouldBe true
    }

    "reject a corrupted signature" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEcdsa(digest, ecKeyPair.getPrivate)
      val corruptedSig = sig.clone()
      corruptedSig(0) = (corruptedSig(0) ^ 0xff).toByte

      val node = mkNode(testBody, corruptedSig, ecPubKeyDer, Seq(ecPubKeyDer))
      node.verifySignature() shouldBe false
    }

    "reject when signer key is not in accepted keys" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEcdsa(digest, ecKeyPair.getPrivate)

      // Generate a different key that's NOT the signer
      val kpg = KeyPairGenerator.getInstance("EC")
      kpg.initialize(new ECGenParameterSpec("secp256r1"))
      val otherPubKey = kpg.generateKeyPair().getPublic.getEncoded

      // signerKey matches the actual signer, but accepted keys list only has the other key
      val node = mkNode(testBody, sig, ecPubKeyDer, Seq(otherPubKey))
      node.verifySignature() shouldBe false
    }

    "reject when body is tampered" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEcdsa(digest, ecKeyPair.getPrivate)

      val tamperedBody = "price=99.99".getBytes
      val node = mkNode(tamperedBody, sig, ecPubKeyDer, Seq(ecPubKeyDer))
      node.verifySignature() shouldBe false
    }

    "reject when nonce is different" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEcdsa(digest, ecKeyPair.getPrivate)

      val differentNonce = Array.fill(32)(0x99.toByte)
      val node = mkNode(testBody, sig, ecPubKeyDer, Seq(ecPubKeyDer), nonce = differentNonce)
      node.verifySignature() shouldBe false
    }

    "accept when signer key is one of multiple accepted keys" in {
      val digest = hashNonceAndBody(testNonce, testBody)
      val sig = signEcdsa(digest, ecKeyPair.getPrivate)

      val kpg = KeyPairGenerator.getInstance("EC")
      kpg.initialize(new ECGenParameterSpec("secp256r1"))
      val otherPubKey = kpg.generateKeyPair().getPublic.getEncoded

      val node = mkNode(testBody, sig, ecPubKeyDer, Seq(otherPubKey, ecPubKeyDer))
      node.verifySignature() shouldBe true
    }
  }
}
