// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

final case class RequiredEncryptionSpecs(
    algorithms: NonEmpty[Set[EncryptionAlgorithmSpec]],
    keys: NonEmpty[Set[EncryptionKeySpec]],
) extends PrettyPrinting {
  override val pretty: Pretty[this.type] = prettyOfClass(
    param("algorithms", _.algorithms),
    param("keys", _.keys),
  )
}

/** Key schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionKeySpec extends Product with Serializable with PrettyPrinting {
  def name: String
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionKeySpec {
  case object EcP256 extends EncryptionKeySpec {
    override val name: String = "EC-P256"
  }

  case object Rsa2048 extends EncryptionKeySpec {
    override val name: String = "RSA-2048"
  }
}

/** Algorithm schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionAlgorithmSpec extends Product with Serializable with PrettyPrinting {
  def name: String
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionAlgorithmSpec {
  case object EciesHkdfHmacSha256Aes128Gcm extends EncryptionAlgorithmSpec {
    override val name: String = "ECIES_HMAC256_AES128-GCM"
  }

  case object EciesHkdfHmacSha256Aes128Cbc extends EncryptionAlgorithmSpec {
    override val name: String = "ECIES_HMAC256_AES128-CBC"
  }

  case object RsaOaepSha256 extends EncryptionAlgorithmSpec {
    override val name: String = "RSA-OAEP-SHA256"
  }
}

/** Key/algorithm schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  override val pretty: Pretty[this.type] = prettyOfString(_.name)

  def keySizeInBytes: Int
}

object SymmetricKeyScheme {

  /** AES with 128bit key in GCM */
  case object Aes128Gcm extends SymmetricKeyScheme {
    override def name: String = "AES128-GCM"
    override def keySizeInBytes: Int = 16
  }
}
