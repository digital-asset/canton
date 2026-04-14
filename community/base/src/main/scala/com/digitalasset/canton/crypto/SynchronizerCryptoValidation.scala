// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.protocol.StaticSynchronizerParameters

/** Validates that the crypto schemes and data match the static synchronizer parameters.
  */
trait SynchronizerCryptoValidation {

  protected def staticSynchronizerParameters: StaticSynchronizerParameters

  /** Validates a node's signing key and algorithm spec against the static synchronizer parameters.
    */
  protected def checkVerifySignature(
      hashAlgorithmO: Option[HashAlgorithm],
      signatureFormat: SignatureFormat,
      keyFormat: CryptoKeyFormat,
      keySpec: SigningKeySpec,
      algorithmSpecO: Option[SigningAlgorithmSpec],
  ): Either[SignatureCheckError, Unit] =
    for {
      _ <- hashAlgorithmO match {
        case Some(hashAlgorithm) =>
          Either.cond(
            staticSynchronizerParameters.requiredHashAlgorithms.contains(hashAlgorithm),
            (),
            SignatureCheckError.UnsupportedHashAlgorithm(
              hashAlgorithm,
              staticSynchronizerParameters.requiredHashAlgorithms,
            ),
          )
        case None => Right(())
      }
      _ <- CryptoKeyValidation.ensureSignatureFormat(
        signatureFormat,
        staticSynchronizerParameters.requiredSignatureFormats,
        SignatureCheckError.UnsupportedSignatureFormat.apply,
      )
      _ <- CryptoKeyValidation.ensureFormat(
        keyFormat,
        staticSynchronizerParameters.requiredCryptoKeyFormats,
        SignatureCheckError.UnsupportedKeyFormat.apply,
      )
      _ <- CryptoKeyValidation.ensureCryptoKeySpec(
        keySpec,
        staticSynchronizerParameters.requiredSigningSpecs.keys,
        SignatureCheckError.UnsupportedKeySpec.apply,
      )
      _ <- algorithmSpecO match {
        case Some(algorithmSpec) =>
          CryptoKeyValidation.ensureCryptoAlgorithmSpec(
            algorithmSpec,
            staticSynchronizerParameters.requiredSigningSpecs.algorithms,
            SignatureCheckError.UnsupportedAlgorithmSpec.apply,
          )
        case None =>
          // When the algorithm spec is not provided (legacy signatures), derive it from the key spec
          // and validate that the derived algorithm is allowed by the synchronizer parameters.
          // Without this check, a malicious participant could omit the algorithm spec to bypass
          // the synchronizer's algorithm requirements (cryptographic downgrade attack).
          val derivedAlgoSpecO = staticSynchronizerParameters.requiredSigningSpecs.algorithms
            .find(_.supportedSigningKeySpecs.contains(keySpec))
          derivedAlgoSpecO match {
            case Some(_) => Right(())
            case None =>
              Left(
                SignatureCheckError.NoMatchingAlgorithmSpec(
                  s"No signing algorithm spec in synchronizer's required algorithms " +
                    s"${staticSynchronizerParameters.requiredSigningSpecs.algorithms} " +
                    s"supports key spec $keySpec"
                )
              )
          }
      }
    } yield ()

  /** Validates a node's encryption key and algorithm spec against the static synchronizer
    * parameters.
    */
  protected def checkDecryption(
      keyFormatO: Option[CryptoKeyFormat],
      keySpecO: Option[EncryptionKeySpec],
      algorithmSpec: EncryptionAlgorithmSpec,
  ): Either[DecryptionError, Unit] =
    for {
      _ <- keyFormatO.fold[Either[DecryptionError, Unit]](Right(())) { keyFormat =>
        CryptoKeyValidation.ensureFormat(
          keyFormat,
          staticSynchronizerParameters.requiredCryptoKeyFormats,
          DecryptionError.UnsupportedKeyFormat.apply,
        )
      }
      _ <- keySpecO.fold[Either[DecryptionError, Unit]](Right(())) { keySpec =>
        CryptoKeyValidation.ensureCryptoKeySpec(
          keySpec,
          staticSynchronizerParameters.requiredEncryptionSpecs.keys,
          DecryptionError.UnsupportedKeySpec.apply,
        )
      }
      _ <- CryptoKeyValidation.ensureCryptoAlgorithmSpec(
        algorithmSpec,
        staticSynchronizerParameters.requiredEncryptionSpecs.algorithms,
        DecryptionError.UnsupportedAlgorithmSpec.apply,
      )
    } yield ()

  /** Validates a node's symmetric scheme against the static synchronizer parameters.
    */
  protected def checkSymmetricDecryption(
      keySpec: SymmetricKeyScheme
  ): Either[DecryptionError, Unit] =
    CryptoKeyValidation.ensureCryptoKeySpec(
      keySpec,
      staticSynchronizerParameters.requiredSymmetricKeySchemes,
      DecryptionError.UnsupportedSymmetricKeySpec.apply,
    )

}
