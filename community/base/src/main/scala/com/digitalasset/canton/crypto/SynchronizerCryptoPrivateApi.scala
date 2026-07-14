// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.metrics.{DecryptionMetrics, SigningMetrics}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Wraps the [[CryptoPrivateApi]] to include static synchronizer parameters, ensuring that during
  * asymmetric decryption, the static synchronizer parameters are explicitly checked. This is
  * crucial because a malicious counter participant could potentially use a downgraded scheme. For
  * other methods, such as key generation, or signing by this (honest) participant, we rely on the
  * synchronizer handshake to ensure that only supported schemes within the synchronizer are used.
  *
  * Stateless decorator: it owns no resources and delegates everything to `privateCrypto` (whose
  * lifecycle its owner manages), so it is intentionally not a `FlagCloseable`.
  */
final class SynchronizerCryptoPrivateApi(
    override val staticSynchronizerParameters: StaticSynchronizerParameters,
    privateCrypto: CryptoPrivateApi,
    override val signingMetrics: SigningMetrics,
    override val decryptionMetrics: DecryptionMetrics,
)(implicit executionContext: ExecutionContext)
    extends CryptoPrivateApi
    with SynchronizerCryptoValidation {

  override private[crypto] def decryptInternal[M](
      encrypted: AsymmetricEncrypted[M]
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, DecryptionError, M] =
    for {
      _ <- checkDecryption(
        keyFormatO = None,
        keySpecO = None,
        encrypted.encryptionAlgorithmSpec,
      )
        .toEitherT[FutureUnlessShutdown]
      res <- privateCrypto.decrypt(encrypted)(deserialize)
    } yield res

  override def encryptionSchemes: EncryptionCryptoSchemes = privateCrypto.encryptionSchemes

  override def generateEncryptionKey(
      keySpec: EncryptionKeySpec,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    privateCrypto.generateEncryptionKey(keySpec)

  override def signingSchemes: SigningCryptoSchemes = privateCrypto.signingSchemes

  override private[crypto] def signBytesInternal(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    privateCrypto.signBytes(bytes, signingKeyId, usage, signingAlgorithmSpec)

  override def generateSigningKey(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    privateCrypto.generateSigningKey(keySpec, usage, name)
}
