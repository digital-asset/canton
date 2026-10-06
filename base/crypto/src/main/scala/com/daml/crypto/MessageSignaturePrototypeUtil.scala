// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import java.security.spec.ECGenParameterSpec
import java.security.{KeyPair, PrivateKey, SecureRandom, Signature}

// The following utility methods should only be used within a testing context.
// They have been moved into a main project scope so that daml-script runners may access this code.
class MessageSignaturePrototypeUtil(val algorithm: String) {
  def sign(
      message: Array[Byte],
      privateKey: PrivateKey,
      randomSrc: SecureRandom = new SecureRandom(),
  ): Array[Byte] = {
    val messageSign = Signature.getInstance(algorithm, Providers.bouncyCastle)

    messageSign.initSign(privateKey, randomSrc)
    messageSign.update(message)

    messageSign.sign()
  }

  def generateKeyPair: KeyPair = {
    val keyPairGen = java.security.KeyPairGenerator.getInstance("EC", Providers.bouncyCastle)
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    keyPairGen.generateKeyPair()
  }
}

object MessageSignaturePrototypeUtil {
  val Secp256k1 = new MessageSignaturePrototypeUtil("NONEwithECDSA")
}
