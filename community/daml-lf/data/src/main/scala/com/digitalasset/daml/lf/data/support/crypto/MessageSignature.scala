// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package support.crypto

import com.daml.crypto.{MessageSignaturePrototype, Providers}

import java.security.spec.InvalidKeySpecException
import java.security.{
  InvalidKeyException,
  KeyFactory,
  NoSuchAlgorithmException,
  NoSuchProviderException,
  PublicKey,
  SignatureException,
}

object MessageSignature {
  @throws(classOf[NoSuchProviderException])
  @throws(classOf[InvalidKeyException])
  @throws(classOf[SignatureException])
  def verify(signature: Ref.HexString, message: Ref.HexString, publicKey: PublicKey): Boolean =
    MessageSignaturePrototype.Secp256k1.verify(
      Bytes.fromHexString(signature).toByteArray,
      Bytes.fromHexString(message).toByteArray,
      publicKey,
    )

  @throws(classOf[NoSuchProviderException])
  @throws(classOf[InvalidKeyException])
  def validateKey(publicKey: PublicKey): Boolean =
    MessageSignaturePrototype.Secp256k1.validateKey(publicKey)

  // Parse a DER (X.509 SubjectPublicKeyInfo) encoded secp256k1 public key with SunEC,
  // for backward compatibility reason.
  @throws(classOf[NoSuchAlgorithmException])
  @throws(classOf[InvalidKeySpecException])
  def extractPublicKey(hexEncodedPublicKey: Ref.HexString): PublicKey =
    KeyFactory
      // use SunEC for backward compatibity reasons
      .getInstance("EC", Providers.sunEC)
      .generatePublic(
        new java.security.spec.X509EncodedKeySpec(
          Bytes.fromHexString(hexEncodedPublicKey).toByteArray
        )
      )
}
