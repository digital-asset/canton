// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.google.protobuf.ByteString

/** A pinned external data fetch recorded in the transaction view.
  *
  * During submission, the submitting participant executes a TCP fetch to the
  * external service and pins the cryptographically signed response here.
  * During validation, confirming participants verify the signature and supply
  * the pinned data to the Daml engine as a fixed input.
  *
  * See CIP-draft-external-data-pinning.
  */
final case class PinnedDataNode(
    endpoint: String,
    payload: ByteString,
    signerKeys: Seq[ByteString],
    maxBytes: Int,
    timeoutMs: Int,
    responseBody: ByteString,
    responseSignature: ByteString,
    responseSignerKey: ByteString,
    responseFetchedAt: Long, // microseconds since epoch
    nonce: ByteString, // 32 bytes: SHA-256(tx_uuid || fetch_index)
    fetchIndex: Int,
) {

  /** Verify the pinned signature over SHA-256(nonce || body) against the accepted signer keys. */
  def verifySignature(): Boolean = {
    val hash = java.security.MessageDigest.getInstance("SHA-256")
    hash.update(nonce.toByteArray)
    hash.update(responseBody.toByteArray)
    val digest = hash.digest()

    signerKeys.exists { keyBytes =>
      val keyArray = keyBytes.toByteArray
      java.util.Arrays.equals(keyArray, responseSignerKey.toByteArray) && {
        try {
          // Try ECDSA first
          val pubKey = java.security.KeyFactory
            .getInstance("EC")
            .generatePublic(new java.security.spec.X509EncodedKeySpec(keyArray))
          val sig = java.security.Signature.getInstance("SHA256withECDSA")
          sig.initVerify(pubKey)
          sig.update(digest)
          sig.verify(responseSignature.toByteArray)
        } catch {
          case _: Exception =>
            try {
              // Try Ed25519
              val pubKey = java.security.KeyFactory
                .getInstance("Ed25519")
                .generatePublic(new java.security.spec.X509EncodedKeySpec(keyArray))
              val sig = java.security.Signature.getInstance("Ed25519")
              sig.initVerify(pubKey)
              sig.update(digest)
              sig.verify(responseSignature.toByteArray)
            } catch {
              case _: Exception => false
            }
        }
      }
    }
  }
}
