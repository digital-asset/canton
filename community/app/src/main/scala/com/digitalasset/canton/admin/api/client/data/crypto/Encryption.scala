// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

/** Key schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionKeyScheme extends Product with Serializable {
  def name: String
  override def toString: String = name
}

object EncryptionKeyScheme {
  case object EciesP256HkdfHmacSha256Aes128Gcm extends EncryptionKeyScheme {
    override val name: String = "ECIES-P256_HMAC256_AES128-GCM"
  }
}

/** Key schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme extends Product with Serializable {
  def name: String
  override def toString: String = name

  def keySizeInBytes: Int
}

object SymmetricKeyScheme {

  /** AES with 128bit key in GCM */
  case object Aes128Gcm extends SymmetricKeyScheme {
    override def name: String = "AES128-GCM"
    override def keySizeInBytes: Int = 16
  }
}