// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.{ExternalFetchDescriptor, ExternalFetchResponse}

import java.io.{DataInputStream, DataOutputStream}
import java.net.{InetSocketAddress, Socket}
import java.security.{KeyFactory, KeyPairGenerator, MessageDigest, PublicKey, Signature}
import java.security.spec.{ECGenParameterSpec, X509EncodedKeySpec}
import javax.crypto.{Cipher, KeyAgreement}
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Resolves external data fetch requests.
  * During submission: executes a TCP fetch, verifies the signature, returns the response.
  * During validation: looks up pinned data from the transaction view.
  */
trait ExternalFetchResolver {
  def resolve(descriptor: ExternalFetchDescriptor)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ExternalFetchResponse]
}

/** Executes TCP fetches per the CIP-draft-external-data-pinning protocol.
  *
  * Protocol:
  *   1. Connect to endpoint (TCP)
  *   2. Send: nonce (32 bytes) || payload
  *   3. Receive: length (4 bytes big-endian) || body || signature
  *   4. Verify signature over SHA-256(nonce || body) against accepted signer keys
  *   5. Return FetchResponse
  */
class TcpExternalFetchResolver(
    override val loggerFactory: NamedLoggerFactory
)(implicit ec: ExecutionContext)
    extends ExternalFetchResolver
    with NamedLogging {

  override def resolve(descriptor: ExternalFetchDescriptor)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ExternalFetchResponse] =
    FutureUnlessShutdown.outcomeF(scala.concurrent.Future {
      tryEndpoints(descriptor, descriptor.endpoints.toList, errors = Nil)
    })

  /** Try endpoints left-to-right. First success wins. If all fail, throw with all errors. */
  @scala.annotation.tailrec
  private def tryEndpoints(
      descriptor: ExternalFetchDescriptor,
      remaining: List[String],
      errors: List[(String, Throwable)],
  ): ExternalFetchResponse =
    remaining match {
      case Nil =>
        val msg = errors.map { case (ep, ex) => s"  $ep: ${ex.getMessage}" }.mkString("\n")
        throw new java.io.IOException(
          s"All ${descriptor.endpoints.size} endpoints failed:\n$msg"
        )
      case endpoint :: rest =>
        try {
          logger.debug(s"Trying endpoint $endpoint (${rest.size} fallbacks remaining)")(
            com.digitalasset.canton.tracing.TraceContext.empty
          )
          executeFetchFromEndpoint(descriptor, endpoint)
        } catch {
          case ex: Exception =>
            logger.info(s"Endpoint $endpoint failed: ${ex.getMessage}, trying next")(
              com.digitalasset.canton.tracing.TraceContext.empty
            )
            tryEndpoints(descriptor, rest, errors :+ (endpoint -> ex))
        }
    }

  private def executeFetchFromEndpoint(
      descriptor: ExternalFetchDescriptor,
      endpoint: String,
  ): ExternalFetchResponse = {
    val parts = endpoint.split(":", 2)
    require(parts.length == 2, s"Invalid endpoint format: $endpoint, expected host:port")
    val host = parts(0)
    val port = parts(1).toInt

    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(host, port), descriptor.timeoutMs)
      socket.setSoTimeout(descriptor.timeoutMs)

      val out = new DataOutputStream(socket.getOutputStream)
      val in = new DataInputStream(socket.getInputStream)

      // Encrypt the request payload using the first signer's public key (ECIES-like).
      // This ensures the request content (which may contain account numbers, party
      // identifiers, amounts) is confidential in transit — only the signing service
      // can decrypt it with its private key.
      //
      // Wire format:
      //   nonce (32 bytes)
      //   || ephemeral_pub_key (65 bytes, uncompressed EC point)
      //   || iv (12 bytes)
      //   || encrypted_payload (AES-128-GCM)
      val signerPubKeyBytes = descriptor.signerKeys.headOption.getOrElse(
        throw new IllegalArgumentException("No signer keys provided")
      )
      val signerPubKey = try {
        KeyFactory.getInstance("EC")
          .generatePublic(new X509EncodedKeySpec(signerPubKeyBytes))
      } catch {
        case _: Exception =>
          KeyFactory.getInstance("Ed25519")
            .generatePublic(new X509EncodedKeySpec(signerPubKeyBytes))
      }

      val ephemeralKpg = KeyPairGenerator.getInstance("EC")
      ephemeralKpg.initialize(new ECGenParameterSpec("secp256r1"))
      val ephemeral = ephemeralKpg.generateKeyPair()

      // ECDH key agreement → AES key
      val ka = KeyAgreement.getInstance("ECDH")
      ka.init(ephemeral.getPrivate)
      ka.doPhase(signerPubKey, true)
      val sharedSecret = ka.generateSecret()
      val aesKey = new SecretKeySpec(
        MessageDigest.getInstance("SHA-256").digest(sharedSecret), 0, 16, "AES"
      )

      // AES-GCM encrypt the payload
      val iv = new Array[Byte](12)
      new java.security.SecureRandom().nextBytes(iv)
      val cipher = Cipher.getInstance("AES/GCM/NoPadding")
      cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(128, iv))
      val encryptedPayload = cipher.doFinal(descriptor.payload)

      val ephPubBytes = ephemeral.getPublic.getEncoded // X.509 encoded

      // Send: nonce (32) || ephemeral_pub_key_len (2) || ephemeral_pub_key || iv (12) || encrypted_payload
      out.write(descriptor.nonce)
      out.writeShort(ephPubBytes.length)
      out.write(ephPubBytes)
      out.write(iv)
      out.write(encryptedPayload)
      out.flush()

      // Receive: length (4 bytes big-endian) || encrypted_body || signature
      // The response body is also encrypted with the same shared secret (different IV).
      val responseLength = in.readInt()
      require(
        responseLength >= 0 && responseLength <= descriptor.maxBytes + 128,
        s"Response length $responseLength exceeds limit",
      )
      val responseIv = new Array[Byte](12)
      in.readFully(responseIv)
      val encryptedBody = new Array[Byte](responseLength)
      in.readFully(encryptedBody)
      val sigBytes = in.readAllBytes()
      require(sigBytes.nonEmpty, "No signature in response")

      // Decrypt the response body
      val decCipher = Cipher.getInstance("AES/GCM/NoPadding")
      decCipher.init(Cipher.DECRYPT_MODE, aesKey, new GCMParameterSpec(128, responseIv))
      val body = decCipher.doFinal(encryptedBody)

      // Verify signature over SHA-256(nonce || body) against accepted signer keys
      val hash = MessageDigest.getInstance("SHA-256")
      hash.update(descriptor.nonce)
      hash.update(body)
      val digest = hash.digest()

      val (signerKey, verified) = descriptor.signerKeys
        .map { keyBytes =>
          Try {
            val pubKey = KeyFactory
              .getInstance("EC")
              .generatePublic(new X509EncodedKeySpec(keyBytes))
            val sig = Signature.getInstance("SHA256withECDSA")
            sig.initVerify(pubKey)
            sig.update(digest)
            (keyBytes, sig.verify(sigBytes))
          }.getOrElse {
            // Try Ed25519 if EC fails
            Try {
              val pubKey = KeyFactory
                .getInstance("Ed25519")
                .generatePublic(new X509EncodedKeySpec(keyBytes))
              val sig = Signature.getInstance("Ed25519")
              sig.initVerify(pubKey)
              sig.update(digest)
              (keyBytes, sig.verify(sigBytes))
            }.getOrElse((keyBytes, false))
          }
        }
        .find(_._2)
        .getOrElse(
          throw new SecurityException(
            s"No accepted signer key verified the response signature for ${descriptor.endpoints.mkString(", ")}"
          )
        )

      ExternalFetchResponse(
        body = body,
        signature = sigBytes,
        signerKey = signerKey,
        fetchedAt = System.currentTimeMillis() * 1000, // microseconds
      )
    } finally {
      socket.close()
    }
  }
}

/** Resolver that supplies pinned data from a pre-populated map.
  * Used during transaction validation (reinterpretation) on confirming participants.
  */
class PinnedDataResolver(
    pinnedData: Map[Int, ExternalFetchResponse] // keyed by fetch_index
) extends ExternalFetchResolver {

  private val nextIndex = new java.util.concurrent.atomic.AtomicInteger(0)

  override def resolve(descriptor: ExternalFetchDescriptor)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ExternalFetchResponse] = {
    val index = nextIndex.getAndIncrement()
    pinnedData.get(index) match {
      case Some(response) =>
        // Verify the pinned signature before supplying to the engine
        val hash = MessageDigest.getInstance("SHA-256")
        hash.update(descriptor.nonce)
        hash.update(response.body)
        val digest = hash.digest()

        val keyFound = descriptor.signerKeys.exists(java.util.Arrays.equals(_, response.signerKey))
        require(keyFound, s"Pinned data signer key not in accepted keys for fetch $index")

        // Verify signature
        val verified = Try {
          val pubKey = KeyFactory
            .getInstance("EC")
            .generatePublic(new X509EncodedKeySpec(response.signerKey))
          val sig = Signature.getInstance("SHA256withECDSA")
          sig.initVerify(pubKey)
          sig.update(digest)
          sig.verify(response.signature)
        }.getOrElse {
          Try {
            val pubKey = KeyFactory
              .getInstance("Ed25519")
              .generatePublic(new X509EncodedKeySpec(response.signerKey))
            val sig = Signature.getInstance("Ed25519")
            sig.initVerify(pubKey)
            sig.update(digest)
            sig.verify(response.signature)
          }.getOrElse(false)
        }
        require(verified, s"Pinned data signature verification failed for fetch $index")

        FutureUnlessShutdown.pure(response)
      case None =>
        throw new IllegalStateException(
          s"No pinned data available for external fetch index $index"
        )
    }
  }
}
