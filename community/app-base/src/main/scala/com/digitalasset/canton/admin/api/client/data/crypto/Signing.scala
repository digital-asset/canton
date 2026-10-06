// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.nonempty.NonEmpty

final case class RequiredSigningSpecs(
    algorithms: NonEmpty[Set[SigningAlgorithmSpec]],
    keys: NonEmpty[Set[SigningKeySpec]],
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[RequiredSigningSpecs] =
    RequiredSigningSpecs
}

object RequiredSigningSpecs extends PrettyPrintingCompanion[RequiredSigningSpecs] {
  override protected val pretty: Pretty[RequiredSigningSpecs] = prettyOfClass(
    param("algorithms", _.algorithms),
    param("keys", _.keys),
  )
}

/** Schemes for signature keys. */
sealed trait SigningKeySpec extends Product with Serializable with PrettyPrintingFromCompanion {
  def name: String
  override def prettyCompanion: PrettyPrintingCompanion[SigningKeySpec] = SigningKeySpec
}

object SigningKeySpec extends PrettyPrintingCompanion[SigningKeySpec] {
  override protected val pretty: Pretty[SigningKeySpec] = prettyOfString(_.name)
  case object EcCurve25519 extends SigningKeySpec {
    override val name: String = "EC-Curve25519"
  }

  case object EcP256 extends SigningKeySpec {
    override val name: String = "EC-P256"
  }

  case object EcP384 extends SigningKeySpec {
    override val name: String = "EC-P384"
  }

  case object EcSecp256k1 extends SigningKeySpec {
    override val name: String = "EC-Secp256k1"
  }

  case object MlDsa65 extends SigningKeySpec {
    override val name: String = "ML-DSA-65"
  }
}

/** Algorithm schemes for signing. */
sealed trait SigningAlgorithmSpec
    extends Product
    with Serializable
    with PrettyPrintingFromCompanion {
  def name: String
  override def prettyCompanion: PrettyPrintingCompanion[SigningAlgorithmSpec] =
    SigningAlgorithmSpec
}

object SigningAlgorithmSpec extends PrettyPrintingCompanion[SigningAlgorithmSpec] {
  override protected val pretty: Pretty[SigningAlgorithmSpec] = prettyOfString(_.name)
  case object Ed25519 extends SigningAlgorithmSpec {
    override val name: String = "Ed25519"
  }

  case object EcDsaSha256 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA256"
  }

  case object EcDsaSha384 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA384"
  }

  case object MlDsa65 extends SigningAlgorithmSpec {
    override val name: String = "ML-DSA-65"
  }
}
