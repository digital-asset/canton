// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.Generators
import magnolify.scalacheck.auto.*
import org.scalacheck.*

object GeneratorsCrypto {
  implicit val signingKeySchemeArb: Arbitrary[SigningKeyScheme] = genArbitrary
  implicit val symmetricKeySchemeArb: Arbitrary[SymmetricKeyScheme] = genArbitrary
  implicit val encryptionKeySchemeArb: Arbitrary[EncryptionKeyScheme] = genArbitrary
  implicit val hashAlgorithmArb: Arbitrary[HashAlgorithm] = genArbitrary
  implicit val cryptoKeyFormatArb: Arbitrary[CryptoKeyFormat] = genArbitrary

  implicit val signingKeySchemeNESArb: Arbitrary[NonEmpty[Set[SigningKeyScheme]]] =
    Generators.nonEmptySet[SigningKeyScheme]
  implicit val encryptionKeySchemeNESArb: Arbitrary[NonEmpty[Set[EncryptionKeyScheme]]] =
    Generators.nonEmptySet[EncryptionKeyScheme]
  implicit val symmetricKeySchemeNESArb: Arbitrary[NonEmpty[Set[SymmetricKeyScheme]]] =
    Generators.nonEmptySet[SymmetricKeyScheme]
  implicit val hashAlgorithmNESArb: Arbitrary[NonEmpty[Set[HashAlgorithm]]] =
    Generators.nonEmptySet[HashAlgorithm]
  implicit val cryptoKeyFormatNESArb: Arbitrary[NonEmpty[Set[CryptoKeyFormat]]] =
    Generators.nonEmptySet[CryptoKeyFormat]
}
