// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.SigningKeyUsage.compatibleUsageForSignAndVerify
import com.digitalasset.canton.crypto.provider.jce.JceJavaKeyConverter

import java.security.PublicKey as JPublicKey
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap

object CryptoKeyValidation {

  // TODO(#15632): Make this a real cache with an eviction rule
  // keeps track of the public keys that have been validated
  private lazy val validatedPublicKeys: TrieMap[PublicKey, Either[KeyParseAndValidateError, Unit]] =
    TrieMap.empty

  private[crypto] def parseAndValidateDerKey(
      publicKey: PublicKey
  ): Either[KeyParseAndValidateError, JPublicKey] =
    JceJavaKeyConverter
      .toJava(publicKey)
      .leftMap(err => KeyParseAndValidateError(err.show))

  // TODO(#15634): Verify crypto scheme as part of key validation
  /** Parses and validates a public key. This includes recomputing the fingerprint and verifying
    * that it matches the id of the key, as well as validating its format. We store the validation
    * results in a cache.
    */
  private[crypto] def parseAndValidatePublicKey[E](
      publicKey: PublicKey,
      errFn: String => E,
  ): Either[E, Unit] = {
    @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
    lazy val parseRes = publicKey.format match {
      case CryptoKeyFormat.DerX509Spki =>
        // We check the cache first and if it's not there we convert to Java Key
        // (and consequently check the key format)
        parseAndValidateDerKey(publicKey).map(_ => ())
      case CryptoKeyFormat.Symbolic =>
        Either.unit
      case format @ (CryptoKeyFormat.Der | CryptoKeyFormat.Raw | CryptoKeyFormat.DerPkcs8Pki) =>
        Left(KeyParseAndValidateError(s"Invalid format for public key: $format"))
    }

    // If the result is already in the cache it means the key has already been validated.
    validatedPublicKeys
      .getOrElseUpdate(publicKey, parseRes)
      .leftMap(err => errFn(s"Failed to deserialize ${publicKey.format} public key: $err"))
  }

  private[crypto] def selectEncryptionAlgorithmSpec[E](
      keySpec: EncryptionKeySpec,
      defaultAlgorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpecs: Set[EncryptionAlgorithmSpec],
      errFn: EncryptionAlgorithmSpec => E,
  ): Either[E, EncryptionAlgorithmSpec] =
    if (defaultAlgorithmSpec.supportedEncryptionKeySpecs.contains(keySpec))
      Right(defaultAlgorithmSpec)
    else
      supportedAlgorithmSpecs
        .find(_.supportedEncryptionKeySpecs.contains(keySpec))
        .toRight(errFn(defaultAlgorithmSpec))

  private[crypto] def selectSigningAlgorithmSpec[E](
      keySpec: SigningKeySpec,
      defaultAlgorithmSpec: SigningAlgorithmSpec,
      supportedAlgorithmSpecs: Set[SigningAlgorithmSpec],
      errFn: SigningAlgorithmSpec => E,
  ): Either[E, SigningAlgorithmSpec] =
    if (defaultAlgorithmSpec.supportedSigningKeySpecs.contains(keySpec))
      Right(defaultAlgorithmSpec)
    else
      supportedAlgorithmSpecs
        .find(_.supportedSigningKeySpecs.contains(keySpec))
        .toRight(errFn(defaultAlgorithmSpec))

  private[crypto] def ensureCryptoKeySpec[KeySpec, E](
      keySpec: KeySpec,
      supportedKeySpecs: Set[KeySpec],
      errFnKey: (KeySpec, Set[KeySpec]) => E,
  ): Either[E, Unit] =
    Either.cond(
      supportedKeySpecs.contains(keySpec),
      (),
      errFnKey(keySpec, supportedKeySpecs),
    )

  private[crypto] def ensureCryptoAlgorithmSpec[AlgorithmSpec, E](
      algorithmSpec: AlgorithmSpec,
      supportedAlgorithmSpecs: Set[AlgorithmSpec],
      errFnAlgorithm: (AlgorithmSpec, Set[AlgorithmSpec]) => E,
  ): Either[E, Unit] =
    Either.cond(
      supportedAlgorithmSpecs.contains(algorithmSpec),
      (),
      errFnAlgorithm(algorithmSpec, supportedAlgorithmSpecs),
    )

  /** Ensures that a given key specification is supported by the selected crypto algorithm. It also
    * checks if this crypto algorithm is part of the set of supported algorithms.
    */
  private[crypto] def ensureCryptoSpec[KeySpec, AlgorithmSpec, E](
      keySpec: KeySpec,
      algorithmSpec: AlgorithmSpec,
      supportedKeySpecs: Set[KeySpec],
      supportedAlgorithmSpecs: Set[AlgorithmSpec],
      errFnKey: (KeySpec, Set[KeySpec]) => E,
      errFnAlgorithm: (AlgorithmSpec, Set[AlgorithmSpec]) => E,
  ): Either[E, Unit] =
    for {
      _ <- ensureCryptoKeySpec(keySpec, supportedKeySpecs, errFnKey)
      _ <- ensureCryptoAlgorithmSpec(algorithmSpec, supportedAlgorithmSpecs, errFnAlgorithm)
    } yield ()

  private[crypto] def ensureFormat[E](
      actual: CryptoKeyFormat,
      acceptedFormats: Set[CryptoKeyFormat],
      errFn: (CryptoKeyFormat, Set[CryptoKeyFormat]) => E,
  ): Either[E, Unit] =
    Either.cond(
      acceptedFormats.contains(actual),
      (),
      errFn(actual, acceptedFormats),
    )

  /** @param errFn
    *   An error function that takes the key's fingerprint, its actual key usages, and the expected
    *   usages, and is invoked when these do not match.
    */
  private[crypto] def ensureUsage[E](
      usage: NonEmpty[Set[SigningKeyUsage]],
      keyUsage: NonEmpty[Set[SigningKeyUsage]],
      fingerprint: Fingerprint,
      errFn: (Fingerprint, Set[SigningKeyUsage], Set[SigningKeyUsage]) => E,
  ): Either[E, Unit] =
    Either.cond(
      compatibleUsageForSignAndVerify(keyUsage, usage),
      (),
      errFn(fingerprint, keyUsage.forgetNE, usage.forgetNE),
    )

  private[crypto] def ensureSignatureFormat[E](
      actual: SignatureFormat,
      acceptedFormats: Set[SignatureFormat],
      errFn: (SignatureFormat, Set[SignatureFormat]) => E,
  ): Either[E, Unit] =
    Either.cond(
      acceptedFormats.contains(actual),
      (),
      errFn(actual, acceptedFormats),
    )

}
