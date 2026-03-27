// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CacheConfig, CryptoConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{
  Hash,
  KeyPurpose,
  PublicKey,
  Signature,
  SigningKeyUsage,
  SigningPublicKey,
  SyncCryptoError,
  SynchronizerCrypto,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Aggregates all methods related to protocol signing. These methods require a topology snapshot to
  * ensure the correct signing keys are used, based on the current state (i.e., OwnerToKeyMappings).
  */
trait SyncCryptoSigner extends NamedLogging with AutoCloseable {

  protected def cryptoPrivateStore: CryptoPrivateStore

  /** Signs a given hash using the currently active signing keys in the current topology state.
    *
    * @param signingTimestampOverrides
    *   Optional overrides for selecting an approximate signing timestamp and validity end, used to
    *   select the correct session signing key whenever session signing keys are enabled.
    */
  def sign(
      topologySnapshot: TopologySnapshot,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

  protected def findSigningKey(
      member: Member,
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SigningPublicKey] =
    for {
      signingKeys <- EitherT.right(topologySnapshot.signingKeys(member, usage))
      existingKeys <- signingKeys.toList
        .parFilterA(pk => cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError.apply)
      kk <- NonEmpty
        .from(existingKeys)
        .map(PublicKey.getLatestKey)
        .toRight[SyncCryptoError](
          SyncCryptoError
            .KeyNotAvailable(
              member,
              KeyPurpose.Signing,
              topologySnapshot.timestamp,
              signingKeys.map(_.fingerprint),
            )
        )
        .toEitherT[FutureUnlessShutdown]
    } yield kk

}

object SyncCryptoSigner {

  def createWithLongTermKeys(
      member: Member,
      crypto: SynchronizerCrypto,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) =
    new SyncCryptoSignerWithLongTermKeys(
      member,
      crypto.privateCrypto,
      crypto.cryptoPrivateStore,
      loggerFactory,
    )

  def createWithOptionalSessionKeys(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      member: Member,
      crypto: SynchronizerCrypto,
      cryptoConfig: CryptoConfig,
      publicKeyConversionCacheConfig: CacheConfig,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SyncCryptoSigner =
    if (cryptoConfig.sessionSigningKeys.enabled)
      new SyncCryptoSignerWithSessionKeys(
        synchronizerId,
        staticSynchronizerParameters,
        member,
        crypto.privateCrypto,
        crypto.cryptoPrivateStore,
        cryptoConfig.sessionSigningKeys,
        publicKeyConversionCacheConfig,
        futureSupervisor: FutureSupervisor,
        timeouts,
        loggerFactory,
      )
    else
      SyncCryptoSigner.createWithLongTermKeys(
        member,
        crypto,
        loggerFactory,
      )

  /** @param approximateTimestamp
    *   The timestamp used during signing to compute the validity period of session signing keys.
    *   This is used when the topology is not yet fixed (i.e., a topology snapshot approximation is
    *   used), such as for signing submission requests or encrypted view messages. The snapshot used
    *   during signing is still an approximation. The current local clock is often suitable for
    *   `approximateTimestamp`, as it reflects the signer’s current time. On the verifier side, the
    *   current node time (e.g., sequencing time) must also be considered when validating both the
    *   signature and the session signing key.
    * @param validityPeriodEnd
    *   Optional timestamp defining the end of the validity period — the latest time at which a
    *   signature verification is expected to succeed. The chosen session signing key may be valid
    *   for longer.
    */
  final case class SigningTimestampOverrides(
      approximateTimestamp: CantonTimestamp,
      validityPeriodEnd: Option[CantonTimestamp],
  )

  object SigningTimestampOverrides {

    /** Creates timestamps for signing using an approximate timestamp based on `clock.now`, with the
      * default max sequencing time set to `clock.now` + `defaultMaxSequencingTimeOffset`. Should
      * only be used for testing.
      */
    @VisibleForTesting
    def createTimestampsOverrideWithDefaultOffset(
        clock: Clock
    ): Option[SigningTimestampOverrides] = {
      val defaultMaxSequencingTimeOffset = SequencerClientConfig().defaultMaxSequencingTimeOffset
      val now = clock.now
      Some(
        SigningTimestampOverrides(
          approximateTimestamp = now,
          validityPeriodEnd = Some(now.add(defaultMaxSequencingTimeOffset.asJava)),
        )
      )
    }

    def createOption(
        approximateTimestampForSigning: Option[CantonTimestamp],
        validityPeriodEnd: Option[CantonTimestamp],
    ): Option[SigningTimestampOverrides] =
      approximateTimestampForSigning.map { approximateTimestamp =>
        SigningTimestampOverrides(
          approximateTimestamp = approximateTimestamp,
          validityPeriodEnd = validityPeriodEnd,
        )
      }

  }
}
