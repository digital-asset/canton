// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.daml.lf.engine.{ExternalFetchDescriptor, ExternalFetchResponse}
import org.scalatest.wordspec.AnyWordSpec

import java.io.{DataOutputStream, IOException}
import java.net.{ServerSocket, Socket}
import java.security.*
import java.security.spec.ECGenParameterSpec

/** Tests for TcpExternalFetchResolver and PinnedDataResolver. */
class ExternalFetchResolverTest extends AnyWordSpec with BaseTest {

  // Generate a test ECDSA keypair for signing
  private lazy val (testKeyPair, testPubKeyDer) = {
    val kpg = KeyPairGenerator.getInstance("EC")
    kpg.initialize(new ECGenParameterSpec("secp256r1"))
    val kp = kpg.generateKeyPair()
    (kp, kp.getPublic.getEncoded)
  }

  private def signData(data: Array[Byte], privateKey: PrivateKey): Array[Byte] = {
    val sig = Signature.getInstance("SHA256withECDSA")
    sig.initSign(privateKey)
    sig.update(data)
    sig.sign()
  }

  private val testNonce = Array.fill(32)(0x42.toByte)

  private def hashNonceAndBody(nonce: Array[Byte], body: Array[Byte]): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(nonce)
    md.update(body)
    md.digest()
  }

  "TcpExternalFetchResolver" should {

    "successfully fetch and verify a signed response" in {
      val body = "price=42.50".getBytes
      val digest = hashNonceAndBody(testNonce, body)
      val signature = signData(digest, testKeyPair.getPrivate)

      // Start a mock TCP server
      val server = new ServerSocket(0) // random port
      val port = server.getLocalPort
      val serverThread = new Thread(() => {
        val client = server.accept()
        val in = client.getInputStream
        // Read nonce (32 bytes) + payload
        val nonce = new Array[Byte](32)
        in.read(nonce)
        val payload = in.readAllBytes()

        // Send: length(4 BE) || body || signature
        val out = new DataOutputStream(client.getOutputStream)
        out.writeInt(body.length)
        out.write(body)
        out.write(signature)
        out.flush()
        client.close()
        server.close()
      })
      serverThread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoint = s"localhost:$port",
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 5000,
        nonce = testNonce,
      )

      val response = resolver.resolve(descriptor).futureValueUS
      response.body shouldBe body
      response.signerKey shouldBe testPubKeyDer
      response.signature shouldBe signature

      serverThread.join(5000)
    }

    "reject a response with an invalid signature" in {
      val body = "price=42.50".getBytes
      val badSignature = Array.fill(64)(0xff.toByte) // garbage signature

      val server = new ServerSocket(0)
      val port = server.getLocalPort
      val serverThread = new Thread(() => {
        val client = server.accept()
        val in = client.getInputStream
        in.read(new Array[Byte](32)) // nonce
        in.readAllBytes() // payload

        val out = new DataOutputStream(client.getOutputStream)
        out.writeInt(body.length)
        out.write(body)
        out.write(badSignature)
        out.flush()
        client.close()
        server.close()
      })
      serverThread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoint = s"localhost:$port",
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 5000,
        nonce = testNonce,
      )

      an[SecurityException] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      serverThread.join(5000)
    }

    "fail on connection timeout" in {
      // Use a port that nothing listens on
      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoint = "localhost:1", // unlikely to be open
        payload = "test".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 100, // very short timeout
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }
    }
  }

  "PinnedDataResolver" should {

    "supply pinned data for sequential fetch indices" in {
      val body = "price=42.50".getBytes
      val digest = hashNonceAndBody(testNonce, body)
      val signature = signData(digest, testKeyPair.getPrivate)

      val response0 = ExternalFetchResponse(body, signature, testPubKeyDer, 1000000L)
      val response1 = ExternalFetchResponse("second".getBytes,
        signData(hashNonceAndBody(testNonce, "second".getBytes), testKeyPair.getPrivate),
        testPubKeyDer, 2000000L)

      val resolver = new PinnedDataResolver(Map(0 -> response0, 1 -> response1))

      val descriptor = ExternalFetchDescriptor(
        endpoint = "oracle:9999",
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 5000,
        nonce = testNonce,
      )

      val result0 = resolver.resolve(descriptor).futureValueUS
      result0.body shouldBe body

      // Second call should return index 1
      val result1 = resolver.resolve(descriptor).futureValueUS
      result1.body shouldBe "second".getBytes
    }

    "reject pinned data with bad signature" in {
      val body = "price=42.50".getBytes
      val badSig = Array.fill(64)(0xff.toByte)
      val response = ExternalFetchResponse(body, badSig, testPubKeyDer, 1000000L)

      val resolver = new PinnedDataResolver(Map(0 -> response))

      val descriptor = ExternalFetchDescriptor(
        endpoint = "oracle:9999",
        payload = "test".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 5000,
        nonce = testNonce,
      )

      an[IllegalArgumentException] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }
    }

    "reject pinned data with unknown signer key" in {
      val body = "price=42.50".getBytes
      val digest = hashNonceAndBody(testNonce, body)
      val signature = signData(digest, testKeyPair.getPrivate)

      val otherKeyDer = Array.fill(91)(0xaa.toByte) // not the signer key
      val response = ExternalFetchResponse(body, signature, testPubKeyDer, 1000000L)

      val resolver = new PinnedDataResolver(Map(0 -> response))

      val descriptor = ExternalFetchDescriptor(
        endpoint = "oracle:9999",
        payload = "test".getBytes,
        signerKeys = Seq(otherKeyDer), // doesn't match responseSignerKey
        maxBytes = 4096,
        timeoutMs = 5000,
        nonce = testNonce,
      )

      an[IllegalArgumentException] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }
    }

    "fail when no pinned data exists for the fetch index" in {
      val resolver = new PinnedDataResolver(Map.empty)

      val descriptor = ExternalFetchDescriptor(
        endpoint = "oracle:9999",
        payload = "test".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 5000,
        nonce = testNonce,
      )

      an[IllegalStateException] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }
    }
  }
}
