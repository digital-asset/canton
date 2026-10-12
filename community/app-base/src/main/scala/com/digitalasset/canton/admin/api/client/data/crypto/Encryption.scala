// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.nonempty.NonEmpty

final case class RequiredEncryptionSpecs(
    algorithms: NonEmpty[Set[EncryptionAlgorithmSpec]],
    keys: NonEmpty[Set[EncryptionKeySpec]],
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[RequiredEncryptionSpecs] =
    RequiredEncryptionSpecs
}

object RequiredEncryptionSpecs extends PrettyPrintingCompanion[RequiredEncryptionSpecs] {
  override protected val pretty: Pretty[RequiredEncryptionSpecs] = prettyOfClass(
    param("algorithms", _.algorithms),
    param("keys", _.keys),
  )
}

/** Key schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionKeySpec extends Product with Serializable with PrettyPrintingFromCompanion {
  def name: String
  override def prettyCompanion: PrettyPrintingCompanion[EncryptionKeySpec] = EncryptionKeySpec
}

object EncryptionKeySpec extends PrettyPrintingCompanion[EncryptionKeySpec] {
  override protected val pretty: Pretty[EncryptionKeySpec] = prettyOfString(_.name)
  case object EcP256 extends EncryptionKeySpec {
    override val name: String = "EC-P256"
  }

  case object Rsa2048 extends EncryptionKeySpec {
    override val name: String = "RSA-2048"
  }
}

/** Algorithm schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionAlgorithmSpec
    extends Product
    with Serializable
    with PrettyPrintingFromCompanion {
  def name: String
  override def prettyCompanion: PrettyPrintingCompanion[EncryptionAlgorithmSpec] =
    EncryptionAlgorithmSpec
}

object EncryptionAlgorithmSpec extends PrettyPrintingCompanion[EncryptionAlgorithmSpec] {
  override protected val pretty: Pretty[EncryptionAlgorithmSpec] = prettyOfString(_.name)
  case object EciesHkdfHmacSha256Aes128Cbc extends EncryptionAlgorithmSpec {
    override val name: String = "ECIES_HMAC256_AES128-CBC"
  }

  case object RsaOaepSha256 extends EncryptionAlgorithmSpec {
    override val name: String = "RSA-OAEP-SHA256"
  }
}

/** Key/algorithm schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme extends Product with Serializable with PrettyPrintingFromCompanion {
  def name: String
  override def prettyCompanion: PrettyPrintingCompanion[SymmetricKeyScheme] = SymmetricKeyScheme

  def keySizeInBytes: Int
}

object SymmetricKeyScheme extends PrettyPrintingCompanion[SymmetricKeyScheme] {
  override protected val pretty: Pretty[SymmetricKeyScheme] = prettyOfString(_.name)

  /** AES with 128bit key in GCM */
  case object Aes128Gcm extends SymmetricKeyScheme {
    override def name: String = "AES128-GCM"
    override def keySizeInBytes: Int = 16
  }
}
