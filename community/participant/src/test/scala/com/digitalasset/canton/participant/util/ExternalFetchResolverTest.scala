// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.daml.lf.engine.{ExternalFetchDescriptor, ExternalFetchResponse}
import org.scalatest.wordspec.AnyWordSpec

import java.io.{DataInputStream, DataOutputStream}
import java.net.ServerSocket
import java.security.*
import java.security.spec.{ECGenParameterSpec, X509EncodedKeySpec}
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, KeyAgreement}

/** Tests for TcpExternalFetchResolver and PinnedDataResolver. */
class ExternalFetchResolverTest extends AnyWordSpec with BaseTest {

  private implicit val ec: scala.concurrent.ExecutionContext = directExecutionContext

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

  /** Mock TCP server that speaks the ECIES-encrypted fetchExternal protocol.
    * Reads: nonce (32) || ephPubKeyLen (2) || ephPubKey || iv (12) || encPayload
    * Sends: responseLength (4) || responseIv (12) || encBody || signature
    */
  private def startMockServer(
      responseBody: Array[Byte],
      signResponse: Array[Byte] => Array[Byte],
  ): (Int, Thread) = {
    val server = new ServerSocket(0)
    server.setSoTimeout(10000)
    val port = server.getLocalPort
    val thread = new Thread(() => {
      try {
        val client = server.accept()
        val in = new DataInputStream(client.getInputStream)
        val out = new DataOutputStream(client.getOutputStream)

        // Read nonce
        val nonce = new Array[Byte](32)
        in.readFully(nonce)

        // Read ephemeral public key
        val ephPubLen = in.readUnsignedShort()
        val ephPubBytes = new Array[Byte](ephPubLen)
        in.readFully(ephPubBytes)

        // Read IV
        val iv = new Array[Byte](12)
        in.readFully(iv)

        // Read encrypted payload (rest of stream up to a reasonable limit)
        val encPayload = new Array[Byte](client.getInputStream.available())
        in.readFully(encPayload)

        // Decrypt payload using ECDH (our private key + their ephemeral public key)
        val ephPubKey = KeyFactory.getInstance("EC")
          .generatePublic(new X509EncodedKeySpec(ephPubBytes))
        val ka = KeyAgreement.getInstance("ECDH")
        ka.init(testKeyPair.getPrivate)
        ka.doPhase(ephPubKey, true)
        val sharedSecret = ka.generateSecret()
        val aesKey = new SecretKeySpec(
          MessageDigest.getInstance("SHA-256").digest(sharedSecret), 0, 16, "AES"
        )

        // (We don't need the decrypted payload for the test — just respond)

        // Encrypt response body with same shared secret, different IV
        val responseIv = new Array[Byte](12)
        new SecureRandom().nextBytes(responseIv)
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(128, responseIv))
        val encBody = cipher.doFinal(responseBody)

        // Sign: SHA-256(nonce || body)
        val digest = hashNonceAndBody(nonce, responseBody)
        val signature = signResponse(digest)

        // Send: responseLength (4) || responseIv (12) || encBody || signature
        out.writeInt(encBody.length)
        out.write(responseIv)
        out.write(encBody)
        out.write(signature)
        out.flush()

        client.close()
        server.close()
      } catch {
        case _: Exception => server.close()
      }
    })
    thread.setDaemon(true)
    thread.start()
    (port, thread)
  }

  "TcpExternalFetchResolver" should {

    "successfully fetch and verify a signed response" in {
      val body = "price=42.50".getBytes

      val (port, serverThread) = startMockServer(
        body,
        digest => signData(digest, testKeyPair.getPrivate),
      )

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoint = s"localhost:$port",
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      val response = resolver.resolve(descriptor).futureValueUS
      response.body shouldBe body
      response.signerKey shouldBe testPubKeyDer

      serverThread.join(10000)
    }

    "reject a response with an invalid signature" in {
      val body = "price=42.50".getBytes

      val (port, serverThread) = startMockServer(
        body,
        _ => Array.fill(64)(0xff.toByte), // garbage signature
      )

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoint = s"localhost:$port",
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      serverThread.join(10000)
    }

    "fail on connection timeout" in {
      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoint = "localhost:1",
        payload = "test".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 100,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }
    }
  }

  "PinnedDataResolver" should {

    "supply sequential fetch indices from pinned data" in {
      val body1 = "result1".getBytes
      val body2 = "result2".getBytes
      val sig1 = signData(hashNonceAndBody(testNonce, body1), testKeyPair.getPrivate)
      val sig2 = signData(hashNonceAndBody(testNonce, body2), testKeyPair.getPrivate)

      val pinnedData = Map(
        0 -> ExternalFetchResponse(body1, sig1, testPubKeyDer, System.currentTimeMillis() * 1000),
        1 -> ExternalFetchResponse(body2, sig2, testPubKeyDer, System.currentTimeMillis() * 1000),
      )

      val resolver = new PinnedDataResolver(pinnedData)

      val desc = ExternalFetchDescriptor(
        endpoint = "unused",
        payload = Array.empty,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 1000,
        nonce = testNonce,
      )

      val r1 = resolver.resolve(desc).futureValueUS
      r1.body shouldBe body1

      val r2 = resolver.resolve(desc).futureValueUS
      r2.body shouldBe body2
    }

    "reject pinned data with bad signature" in {
      val body = "result".getBytes
      val badSig = Array.fill(64)(0x00.toByte)

      val pinnedData = Map(
        0 -> ExternalFetchResponse(body, badSig, testPubKeyDer, System.currentTimeMillis() * 1000)
      )

      val resolver = new PinnedDataResolver(pinnedData)
      val desc = ExternalFetchDescriptor(
        endpoint = "unused",
        payload = Array.empty,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 1000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(desc).futureValueUS
      }
    }

    "reject pinned data with unknown signer key" in {
      val body = "result".getBytes
      val sig = signData(hashNonceAndBody(testNonce, body), testKeyPair.getPrivate)
      val otherKey = Array.fill(65)(0xab.toByte) // not in accepted keys

      val pinnedData = Map(
        0 -> ExternalFetchResponse(body, sig, otherKey, System.currentTimeMillis() * 1000)
      )

      val resolver = new PinnedDataResolver(pinnedData)
      val desc = ExternalFetchDescriptor(
        endpoint = "unused",
        payload = Array.empty,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 1000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(desc).futureValueUS
      }
    }

    "fail when no pinned data for index" in {
      val resolver = new PinnedDataResolver(Map.empty)
      val desc = ExternalFetchDescriptor(
        endpoint = "unused",
        payload = Array.empty,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 1000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(desc).futureValueUS
      }
    }
  }
}
