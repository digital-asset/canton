// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.engine.{ExternalFetchDescriptor, ExternalFetchResponse}

import java.io.{DataInputStream, DataOutputStream}
import java.net.{InetSocketAddress, Socket}
import java.security.{MessageDigest, PublicKey, Signature, KeyFactory}
import java.security.spec.X509EncodedKeySpec
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
      executeFetch(descriptor)
    })

  private def executeFetch(descriptor: ExternalFetchDescriptor): ExternalFetchResponse = {
    val parts = descriptor.endpoint.split(":", 2)
    require(parts.length == 2, s"Invalid endpoint format: ${descriptor.endpoint}, expected host:port")
    val host = parts(0)
    val port = parts(1).toInt

    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(host, port), descriptor.timeoutMs)
      socket.setSoTimeout(descriptor.timeoutMs)

      val out = new DataOutputStream(socket.getOutputStream)
      val in = new DataInputStream(socket.getInputStream)

      // Send: nonce (32 bytes) || payload
      out.write(descriptor.nonce)
      out.write(descriptor.payload)
      out.flush()

      // Receive: length (4 bytes big-endian) || body || signature
      val bodyLength = in.readInt()
      require(
        bodyLength >= 0 && bodyLength <= descriptor.maxBytes,
        s"Response body length $bodyLength exceeds maxBytes ${descriptor.maxBytes}",
      )

      val body = new Array[Byte](bodyLength)
      in.readFully(body)

      // Read remaining bytes as signature
      val sigBytes = in.readAllBytes()
      require(sigBytes.nonEmpty, "No signature in response")

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
            s"No accepted signer key verified the response signature for ${descriptor.endpoint}"
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

  private var nextIndex = 0

  override def resolve(descriptor: ExternalFetchDescriptor)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ExternalFetchResponse] = {
    val index = nextIndex
    nextIndex += 1
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
