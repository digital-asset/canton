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
        endpoints = Seq(s"localhost:$port"),
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
        endpoints = Seq(s"localhost:$port"),
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
        endpoints = Seq("localhost:1"),
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

    "reject a response signed by a key not in signerKeys" in {
      val body = "price=42.50".getBytes

      // Generate a DIFFERENT keypair — valid signature, but wrong key
      val otherKpg = KeyPairGenerator.getInstance("EC")
      otherKpg.initialize(new ECGenParameterSpec("secp256r1"))
      val otherKeyPair = otherKpg.generateKeyPair()

      val (port, serverThread) = startMockServer(
        body,
        digest => signData(digest, otherKeyPair.getPrivate), // signed by wrong key
      )

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer), // only accepts testKeyPair, not otherKeyPair
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      serverThread.join(10000)
    }

    "reject a response where body was tampered after signing" in {
      val realBody = "price=42.50".getBytes
      val tamperedBody = "price=0.01".getBytes // attacker modifies the price

      // Server signs the real body but we'll have the mock send the tampered one
      // We need a custom mock for this
      val server = new ServerSocket(0)
      server.setSoTimeout(10000)
      val port = server.getLocalPort
      val thread = new Thread(() => {
        try {
          val client = server.accept()
          val in = new DataInputStream(client.getInputStream)
          val out = new DataOutputStream(client.getOutputStream)

          val nonce = new Array[Byte](32)
          in.readFully(nonce)
          val ephPubLen = in.readUnsignedShort()
          val ephPubBytes = new Array[Byte](ephPubLen)
          in.readFully(ephPubBytes)
          val iv = new Array[Byte](12)
          in.readFully(iv)
          Iterator.continually(client.getInputStream.available()).takeWhile(_ > 0).foreach(_ => in.read())

          // ECDH with their ephemeral key
          val ephPubKey = KeyFactory.getInstance("EC")
            .generatePublic(new X509EncodedKeySpec(ephPubBytes))
          val ka = KeyAgreement.getInstance("ECDH")
          ka.init(testKeyPair.getPrivate)
          ka.doPhase(ephPubKey, true)
          val sharedSecret = ka.generateSecret()
          val aesKey = new SecretKeySpec(
            MessageDigest.getInstance("SHA-256").digest(sharedSecret), 0, 16, "AES"
          )

          // Sign the REAL body's digest
          val digest = hashNonceAndBody(nonce, realBody)
          val signature = signData(digest, testKeyPair.getPrivate)

          // But encrypt and send the TAMPERED body
          val responseIv = new Array[Byte](12)
          new SecureRandom().nextBytes(responseIv)
          val cipher = Cipher.getInstance("AES/GCM/NoPadding")
          cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(128, responseIv))
          val encBody = cipher.doFinal(tamperedBody)

          out.writeInt(encBody.length)
          out.write(responseIv)
          out.write(encBody)
          out.write(signature)
          out.flush()
          client.close()
          server.close()
        } catch { case _: Exception => server.close() }
      })
      thread.setDaemon(true)
      thread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      // Signature was over realBody but response contains tamperedBody — must reject
      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      thread.join(10000)
    }

    "reject a replayed response signed with a different nonce" in {
      val body = "price=42.50".getBytes
      val staleNonce = Array.fill(32)(0x99.toByte) // attacker replays with old nonce

      // Custom mock: signs with staleNonce instead of the client's nonce
      val server = new ServerSocket(0)
      server.setSoTimeout(10000)
      val port = server.getLocalPort
      val thread = new Thread(() => {
        try {
          val client = server.accept()
          val in = new DataInputStream(client.getInputStream)
          val out = new DataOutputStream(client.getOutputStream)

          // Read client's nonce (but ignore it)
          in.readFully(new Array[Byte](32))
          val ephPubLen = in.readUnsignedShort()
          val ephPubBytes = new Array[Byte](ephPubLen)
          in.readFully(ephPubBytes)
          val iv = new Array[Byte](12)
          in.readFully(iv)
          Iterator.continually(client.getInputStream.available()).takeWhile(_ > 0).foreach(_ => in.read())

          val ephPubKey = KeyFactory.getInstance("EC")
            .generatePublic(new X509EncodedKeySpec(ephPubBytes))
          val ka = KeyAgreement.getInstance("ECDH")
          ka.init(testKeyPair.getPrivate)
          ka.doPhase(ephPubKey, true)
          val sharedSecret = ka.generateSecret()
          val aesKey = new SecretKeySpec(
            MessageDigest.getInstance("SHA-256").digest(sharedSecret), 0, 16, "AES"
          )

          // Sign with STALE nonce instead of the client's nonce
          val digest = hashNonceAndBody(staleNonce, body)
          val signature = signData(digest, testKeyPair.getPrivate)

          val responseIv = new Array[Byte](12)
          new SecureRandom().nextBytes(responseIv)
          val cipher = Cipher.getInstance("AES/GCM/NoPadding")
          cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(128, responseIv))
          val encBody = cipher.doFinal(body)

          out.writeInt(encBody.length)
          out.write(responseIv)
          out.write(encBody)
          out.write(signature)
          out.flush()
          client.close()
          server.close()
        } catch { case _: Exception => server.close() }
      })
      thread.setDaemon(true)
      thread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce, // client sends testNonce, but server signed with staleNonce
      )

      // Signature was over SHA-256(staleNonce || body) but verifier computes
      // SHA-256(testNonce || body) — mismatch, must reject
      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      thread.join(10000)
    }

    "reject an empty signature" in {
      val body = "price=42.50".getBytes

      val (port, serverThread) = startMockServer(
        body,
        _ => Array.empty[Byte], // empty signature
      )

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
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

    "fail when server closes connection without sending a response" in {
      val server = new ServerSocket(0)
      server.setSoTimeout(10000)
      val port = server.getLocalPort
      val thread = new Thread(() => {
        try {
          val client = server.accept()
          // Read the request but close immediately without responding
          client.getInputStream.read(new Array[Byte](32)) // read nonce
          client.close()
          server.close()
        } catch { case _: Exception => server.close() }
      })
      thread.setDaemon(true)
      thread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      thread.join(10000)
    }

    "fail when server sends incomplete response (partial body)" in {
      val server = new ServerSocket(0)
      server.setSoTimeout(10000)
      val port = server.getLocalPort
      val thread = new Thread(() => {
        try {
          val client = server.accept()
          val in = new DataInputStream(client.getInputStream)
          // Read nonce + ephemeral key + iv + payload
          in.readFully(new Array[Byte](32)) // nonce
          val ephLen = in.readUnsignedShort()
          in.readFully(new Array[Byte](ephLen)) // eph pub key
          in.readFully(new Array[Byte](12)) // iv
          Iterator.continually(client.getInputStream.available()).takeWhile(_ > 0).foreach(_ => in.read())

          // Send response length claiming 1000 bytes, but only send 10
          val out = new DataOutputStream(client.getOutputStream)
          out.writeInt(1000) // claim 1000 bytes
          out.write(new Array[Byte](10)) // only send 10
          out.flush()
          client.close()
          server.close()
        } catch { case _: Exception => server.close() }
      })
      thread.setDaemon(true)
      thread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      thread.join(10000)
    }

    "fail when server sends response exceeding maxBytes" in {
      val server = new ServerSocket(0)
      server.setSoTimeout(10000)
      val port = server.getLocalPort
      val thread = new Thread(() => {
        try {
          val client = server.accept()
          val in = new DataInputStream(client.getInputStream)
          in.readFully(new Array[Byte](32))
          val ephLen = in.readUnsignedShort()
          in.readFully(new Array[Byte](ephLen))
          in.readFully(new Array[Byte](12))
          Iterator.continually(client.getInputStream.available()).takeWhile(_ > 0).foreach(_ => in.read())

          // Claim a response larger than maxBytes
          val out = new DataOutputStream(client.getOutputStream)
          out.writeInt(999999) // way over limit
          out.flush()
          client.close()
          server.close()
        } catch { case _: Exception => server.close() }
      })
      thread.setDaemon(true)
      thread.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 100, // small limit
        timeoutMs = 10000,
        nonce = testNonce,
      )

      an[Exception] shouldBe thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }

      thread.join(10000)
    }

    "fall back to second endpoint when first fails" in {
      // First endpoint: immediately closes
      val badServer = new ServerSocket(0)
      badServer.setSoTimeout(10000)
      val badPort = badServer.getLocalPort
      val badThread = new Thread(() => {
        try {
          val client = badServer.accept()
          client.close() // slam the door
          badServer.close()
        } catch { case _: Exception => badServer.close() }
      })
      badThread.setDaemon(true)
      badThread.start()

      // Second endpoint: works correctly
      val body = "fallback_price=99.99".getBytes
      val (goodPort, goodThread) = startMockServer(
        body,
        digest => signData(digest, testKeyPair.getPrivate),
      )

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$badPort", s"localhost:$goodPort"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      // Should succeed via the second endpoint
      val response = resolver.resolve(descriptor).futureValueUS
      response.body shouldBe body

      badThread.join(10000)
      goodThread.join(10000)
    }

    "fail with all errors when every endpoint in the chain fails" in {
      // Two bad endpoints: both close immediately
      val bad1 = new ServerSocket(0)
      bad1.setSoTimeout(10000)
      val port1 = bad1.getLocalPort
      val t1 = new Thread(() => {
        try { val c = bad1.accept(); c.close(); bad1.close() }
        catch { case _: Exception => bad1.close() }
      })
      t1.setDaemon(true)
      t1.start()

      val bad2 = new ServerSocket(0)
      bad2.setSoTimeout(10000)
      val port2 = bad2.getLocalPort
      val t2 = new Thread(() => {
        try { val c = bad2.accept(); c.close(); bad2.close() }
        catch { case _: Exception => bad2.close() }
      })
      t2.setDaemon(true)
      t2.start()

      val resolver = new TcpExternalFetchResolver(loggerFactory)
      val descriptor = ExternalFetchDescriptor(
        endpoints = Seq(s"localhost:$port1", s"localhost:$port2"),
        payload = "get_price".getBytes,
        signerKeys = Seq(testPubKeyDer),
        maxBytes = 4096,
        timeoutMs = 10000,
        nonce = testNonce,
      )

      val ex = the[Exception] thrownBy {
        resolver.resolve(descriptor).futureValueUS
      }
      ex.getMessage should include("2 endpoints failed")

      t1.join(10000)
      t2.join(10000)
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
        endpoints = Seq("unused"),
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
        endpoints = Seq("unused"),
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
        endpoints = Seq("unused"),
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
        endpoints = Seq("unused"),
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
