// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.SymmetricKeyScheme.Aes128Gcm
import com.digitalasset.canton.crypto.provider.jce.{JcePrivateCrypto, JcePureCrypto}
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithSessionKeys.{
  PendingSessionKeyGenerationContext,
  SessionKeyAndDelegation,
}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{CryptoMetrics, SigningHistograms, SigningMetrics}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{EitherTUtil, Mutex}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import com.github.benmanes.caffeine.cache.Scheduler
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Defines the methods for protocol message signing using a session signing key. This requires
  * signatures to include information about which session key is being used, as well as an
  * authorization by a long-term key through an additional signature. This extra signature covers
  * the session key, its validity period, and the synchronizer for which it is valid. This allows us
  * to use the session key, within a specific time frame and synchronizer, to sign protocol messages
  * instead of using the long-term key. Session keys are intended to be used with a KMS/HSM-based
  * provider to reduce the number of signing calls and, consequently, lower the latency costs
  * associated with such external key management services.
  *
  * @param signPrivateApiWithLongTermKeys
  *   The crypto private API used to sign session signing keys, creating a signature delegation with
  *   a long-term key.
  */
class SyncCryptoSignerWithSessionKeys(
    synchronizerId: SynchronizerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    member: Member,
    signPrivateApiWithLongTermKeys: SigningPrivateOps,
    cryptoMetrics: CryptoMetrics,
    override protected val cryptoPrivateStore: CryptoPrivateStore,
    sessionSigningKeysConfig: SessionSigningKeysConfig,
    publicKeyConversionCacheConfig: CacheConfig,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    maxSessionKeyRetries: NonNegativeInt = NonNegativeInt.tryCreate(5),
)(implicit executionContext: ExecutionContext)
    extends SyncCryptoSigner
    with FlagCloseable
    with HasCloseContext {

  // session signing keys can only be used with PV35+
  require(
    staticSynchronizerParameters.protocolVersion >= ProtocolVersion.v35,
    s"Session signing keys are only supported in protocol versions 35 and above. Found PV: ${staticSynchronizerParameters.protocolVersion}",
  )

  private val scheduledExecutorService = Threading.singleThreadScheduledExecutor(
    "session-signing-key-cache",
    noTracingLogger,
  )

  /** The software-based crypto public API that is used to sign protocol messages with a session
    * signing key (generated in software). Except for the signing scheme, when signing with session
    * keys is enabled, all other schemes are not needed. Therefore, we use fixed schemes (i.e.
    * placeholders) for the other crypto parameters.
    */
  private lazy val signPublicApiSoftwareBased: SynchronizerCryptoPureApi = {
    val pureCryptoForSessionKeys = new JcePureCrypto(
      defaultSymmetricKeyScheme = Aes128Gcm, // not used
      signingAlgorithmSpecs = CryptoScheme.tryCreate(
        sessionSigningKeysConfig.signingAlgorithmSpec,
        NonEmpty.mk(Set, sessionSigningKeysConfig.signingAlgorithmSpec),
      ),
      encryptionAlgorithmSpecs =
        CryptoScheme.tryCreate(RsaOaepSha256, NonEmpty.mk(Set, RsaOaepSha256)), // not used
      defaultHashAlgorithm = Sha256, // not used
      defaultPbkdfScheme = PbkdfScheme.Argon2idMode1, // not used
      publicKeyConversionCacheConfig,
      // this `JcePureCrypto` object only holds private key conversions spawned from sign calls
      privateKeyConversionCacheTtl = Some(sessionSigningKeysConfig.keyEvictionPeriod.underlying),
      signatureVerificationParallelism = PositiveInt.one, // not used
      encryptionParallelism = PositiveInt.one, // not used
      signingMetrics = new SigningMetrics(
        new SigningHistograms(MetricName("signing"))(new HistogramInventory()),
        NoOpMetricsFactory,
      )(
        MetricsContext.Empty
      ), // not used since we only want to record latency for KMS signing requests
      decryptionMetrics = cryptoMetrics.decryptionMetrics, // not used
      loggerFactory = loggerFactory,
    )

    new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCryptoForSessionKeys)
  }

  /** The user-configured validity period of a session signing key. */
  private val sessionKeyValidityDuration =
    sessionSigningKeysConfig.keyValidityDuration

  /** The key specification for the session signing keys. */
  private val sessionKeySpec = sessionSigningKeysConfig.signingKeySpec

  /** A value used to shift the validity interval in order to minimize the number of keys created
    * and reduce overlapping intervals.
    */
  @VisibleForTesting
  private[crypto] val toleranceShiftDuration = sessionSigningKeysConfig.toleranceShiftDuration

  /** A cut-off duration that determines when the key should start and stop being used to prevent
    * signature verification failures due to unpredictable sequencing timestamps.
    */
  @VisibleForTesting
  private[crypto] val cutOffDuration =
    sessionSigningKeysConfig.cutOffDuration

  /** The duration a session signing key is retained in memory. It is defined as an AtomicReference
    * only so it can be changed for tests.
    */
  @VisibleForTesting
  private[crypto] val sessionKeyEvictionPeriod = new AtomicReference(
    sessionSigningKeysConfig.keyEvictionPeriod.underlying
  )

  /** Tracks pending new session signing keys. Each session key has an associated validity period
    * and a corresponding long-term key (identified by a fingerprint), both used to generate a
    * signature delegation.
    */
  @VisibleForTesting
  private[crypto] val pendingRequests: TrieMap[
    (SignatureDelegationValidityPeriod, Fingerprint),
    PromiseUnlessShutdown[Option[SessionKeyAndDelegation]],
  ] = TrieMap.empty

  /** Caches the session signing private key and corresponding signature delegation, indexed by the
    * session key ID. The removal of entries from the cache is controlled by a separate parameter,
    * [[sessionKeyEvictionPeriod]]. Given this design, there may be times when multiple valid
    * session keys live in the cache. In such cases, the newest key is always selected to sign new
    * messages.
    */
  @VisibleForTesting
  private[crypto] val sessionSigningKeysCache: Cache[Fingerprint, SessionKeyAndDelegation] =
    Scaffeine()
      .expireAfter[Fingerprint, SessionKeyAndDelegation](
        create = (_, _) => sessionKeyEvictionPeriod.get(),
        update = (_, _, d) => d,
        read = (_, _, d) => d,
      )
      .scheduler(Scheduler.forScheduledExecutorService(scheduledExecutorService))
      .executor(executionContext.execute(_))
      .build()

  override def onClosed(): Unit = {
    LifeCycle.close(
      {
        // Invalidate all cache entries and run pending maintenance tasks
        sessionSigningKeysCache.invalidateAll()
        sessionSigningKeysCache.cleanUp()
        ExecutorServiceExtensions(scheduledExecutorService)(logger, timeouts)
      }
    )(logger)
    super.onClosed()
  }

  /** To control access to the [[sessionSigningKeysCache]] and the [[pendingRequests]]. */
  private val lock = new Mutex()

  /** Creates a delegation signature that authorizes the session key to act on behalf of the
    * long-term key.
    */
  private def createDelegationSignature(
      validityPeriod: SignatureDelegationValidityPeriod,
      activeLongTermKey: SigningPublicKey,
      sessionKey: SigningPublicKey,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SignatureDelegation] =
    for {
      // sign the hash with the long-term key
      signature <- signPrivateApiWithLongTermKeys
        .sign(
          SignatureDelegation.generateHash(
            synchronizerId,
            sessionKey,
            validityPeriod,
          ),
          activeLongTermKey.fingerprint,
          SigningKeyUsage.ProtocolOnly,
        )
        .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoSigningError(err))
      signatureDelegation <- SignatureDelegation
        .create(
          sessionKey,
          validityPeriod,
          signature,
        )
        .leftMap[SyncCryptoError](errMsg =>
          SyncCryptoError.SyncCryptoDelegationSignatureCreationError(errMsg)
        )
        .toEitherT[FutureUnlessShutdown]
    } yield signatureDelegation

  /** Checks whether a session signing key's validity period fully covers the required interval (or
    * point).
    */
  private def isUsableDelegation(
      validityPeriod: SignatureDelegationValidityPeriod,
      validityIntervalToCover: (CantonTimestamp, Option[CantonTimestamp]),
  ): Boolean = {
    val (start, endO) = validityIntervalToCover
    validityPeriod.covers(start) && endO.forall(end => validityPeriod.covers(end))
  }

  /** Generates a new software-based session keypair, signs its public key, validity period, and the
    * synchronizerId with the long-term key to create the signature delegation, and returns both.
    *
    * * NOTE: Concurrency and cache-miss checks are expected to be handled by the caller.
    */
  private def generateNewSessionKeyInternal(
      validityPeriod: SignatureDelegationValidityPeriod,
      activeLongTermKey: SigningPublicKey,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SessionKeyAndDelegation] =
    for {
      // session keys are only used to sign protocol messages
      sessionKeyPair <- JcePrivateCrypto
        .generateSigningKeypair(sessionKeySpec, SigningKeyUsage.ProtocolOnly)
        .leftMap[SyncCryptoError] { err =>
          SyncCryptoError.SyncCryptoSessionKeyGenerationError(err)
        }
        .toEitherT[FutureUnlessShutdown]
      // sign session key + metadata with long-term key to authorize the delegation
      signatureDelegation <- createDelegationSignature(
        validityPeriod,
        activeLongTermKey,
        sessionKeyPair.publicKey,
      )
      sessionKeyAndDelegation = SessionKeyAndDelegation(
        sessionKeyPair.privateKey,
        signatureDelegation,
      )
      _ = sessionSigningKeysCache
        .put(
          sessionKeyPair.publicKey.id,
          sessionKeyAndDelegation,
        )
    } yield sessionKeyAndDelegation

  /** @param validityIntervalToCover
    *   the interval that must be covered by the session signing key. The end is optional; if
    *   `None`, only the start timestamp must be covered.
    * @param signingTs
    *   the timestamp used during signing to determine the validity period for a new session signing
    *   key.
    * @param generateFromTimestampOnly
    *   indicates that only a single timestamp (exact or approximate) is used to compute the
    *   validity interval to cover (i.e., there is no information about the validity period end).
    */
  private case class SigningInfo(
      validityIntervalToCover: (CantonTimestamp, Option[CantonTimestamp]),
      signingTs: CantonTimestamp,
      generateFromTimestampOnly: Boolean,
  )

  /** Determines the validity period for a new session signing key.
    *
    * The validity window of the new key may be shifted backwards to reduce the number of keys
    * created for closely spaced signing requests:
    *   - If centered around a SINGLE exact or approximate ts, the window is shifted by
    *     `toleranceShiftDuration`, leaving a gap behind. This avoids creating a new key for every
    *     slightly earlier timestamp in sequences such as ts, ts-1µs, ts-2µs, ..., ts-n. In that
    *     case, instead of creating n keys, we only create roughly n / (2 * tolerance) keys.
    *     Although this is not perfectly optimal, it reduces key creation compared to simply setting
    *     the validity from ts to ts+l for each request, depending on transaction ordering.
    *   - If we are aware of the full validity interval we need to cover, we do not shift by
    *     `toleranceShiftDuration`. Instead, we generate a new validity window as [ts - cutOff, (ts
    *     - cutOff) + keyValidityDuration]. For this to work, the known validity interval (after
    *       accounting for the cutoff) must be < to `keyValidityDuration`.
    */
  private def determineValidityPeriod(
      signingInfo: SigningInfo
  ): SignatureDelegationValidityPeriod = {
    val validityStart = signingInfo.validityIntervalToCover match {
      // we know the "full" validity interval
      case (intervalStart, Some(_)) if !signingInfo.generateFromTimestampOnly =>
        // Use `validityIntervalToCoverStart` as the starting point without any shift.
        // This timestamp already includes the `cutOff`, giving some margin in the past,
        // and by not shifting further, we extend the key's future margin, increasing
        // the likelihood that the key can be reused more often.
        intervalStart
      // we are either using a SINGLE exact or approximate timestamp
      // (i.e., the validity period end is unknown).
      case _ =>
        signingInfo.signingTs.minus(toleranceShiftDuration.asJava)
    }

    SignatureDelegationValidityPeriod(validityStart, sessionKeyValidityDuration)
  }

  /** Coordinates the retrieval, deduplication, and generation of session signing keys based on
    * temporal validity.
    *
    * If multiple valid options are available in the key cache, the newest key is retrieved. If no
    * session key is available, either a new one is generated and used to populate a created
    * promise, or if an existing promise is found for the validity window, its outcome is waited
    * for.
    */
  private def getSessionKey(
      signingInfo: SigningInfo,
      activeLongTermKey: SigningPublicKey,
      retriesLeft: Int = maxSessionKeyRetries.value,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SessionKeyAndDelegation] = {

    val sessionKeyOrGenerationData = lock.exclusive {
      // get hold of all existing or pending session keys
      val pendingSessionKeys = pendingRequests.toMap
      val keysInCache = sessionSigningKeysCache.asMap().values.toSeq

      // Filter the cache for session keys whose validity period fully covers the required intervaland whose
      // delegation key matches our current activeLongTermKey. Session keys with stale delegation keys are ignored.
      val validUsableSessionKeysInCache = keysInCache.filter { skD =>
        isUsableDelegation(
          skD.signatureDelegation.validityPeriod,
          signingInfo.validityIntervalToCover,
        ) &&
        activeLongTermKey.id == skD.signatureDelegation.delegatingKeyId
      }

      NonEmpty.from(validUsableSessionKeysInCache) match {
        // Case: no cached key is usable
        case None =>
          // Find if there is a pending session key that is valid and can be used
          val validUsablePendingRequests =
            pendingSessionKeys.view.filterKeys { case (existingValidityPeriod, signedBy) =>
              isUsableDelegation(
                existingValidityPeriod,
                signingInfo.validityIntervalToCover,
              ) &&
              activeLongTermKey.id == signedBy
            }.toMap

          val targetValidityPeriod = determineValidityPeriod(signingInfo)

          // If there are no valid and usable pending keys, we add a placeholder promise to the [[pendingRequests]] map,
          // add this to the pendingRequests TrieMap and store this information in the [[PendingSessionKeyGenerationContext]].
          val promiseO = Option
            .when(validUsablePendingRequests.isEmpty) {
              val promise: PromiseUnlessShutdown[Option[SessionKeyAndDelegation]] =
                mkPromise[Option[SessionKeyAndDelegation]](
                  s"sync-crypto-signer-pending-requests-$targetValidityPeriod",
                  futureSupervisor,
                )
              pendingRequests.put((targetValidityPeriod, activeLongTermKey.id), promise).discard
              promise
            }

          // Evaluate the result to return from the lock. The promiseO and validUsablePendingRequests
          // form a binary toggle with exactly one having a non-trivial value.
          Left(
            PendingSessionKeyGenerationContext(
              promiseO,
              validUsablePendingRequests,
              activeLongTermKey,
              targetValidityPeriod,
            )
          )
        // Case: there is a usable and valid session key in the cache
        case Some(validUsableSessionKeysInCacheNE) =>
          // retrieve newest key
          Right(validUsableSessionKeysInCacheNE.maxBy1 { skD =>
            skD.signatureDelegation.validityPeriod.fromInclusive
          })
      }
    }

    // based on result of synchronized block, either return cached key or wait/generate
    sessionKeyOrGenerationData match {
      // CASE 1: No usable session key found; instead we have the context (in Left) for a pending session key promise
      case Left(generationContext: PendingSessionKeyGenerationContext) =>
        generationContext.pendingSessionKeyGenerationPromiseO match {
          // CASE 1A:
          // An empty promise exists in the context (validUsablePendingRequests was evaluated as empty above).
          // This execution path registers a new empty promise to pendingRequests and fulfills it.
          // The use of `lock.exclusive` enforces that only one at a time thread can create
          // a promise deemed usable for a given validity period and active long term key.
          case Some(pendingSessionKeyGenerationPromise) =>
            generateNewSessionKeyInternal(
              generationContext.validityPeriod,
              generationContext.activeLongTermKey,
            ).thereafter { result =>
              // The generation task has terminated. If it was successful, sessionKeysSigningCache has been updated.
              pendingRequests
                .remove(
                  (generationContext.validityPeriod, generationContext.activeLongTermKey.id),
                  pendingSessionKeyGenerationPromise,
                )
                .discard
              result match {
                case Success(Outcome(Right(sessionKeyAndDelegation))) =>
                  pendingSessionKeyGenerationPromise.outcome_(Some(sessionKeyAndDelegation))
                case Success(Outcome(Left(err: SyncCryptoError))) =>
                  logger.debug(
                    s"Failed to generate session key for period ${generationContext.validityPeriod}: $err"
                  )
                  pendingSessionKeyGenerationPromise.outcome_(None)
                // In case of a shutdown, pass the shutdown signal cleanly to the promise. Do not retry.
                case Success(AbortedDueToShutdown) =>
                  logger.debug(
                    s"Session key generation aborted due to node shutdown for period ${generationContext.validityPeriod}"
                  )
                  pendingSessionKeyGenerationPromise.shutdown_()
                // In case of an unhandled exception, pass fail the promise. Do not retry.
                case Failure(exception) =>
                  logger.error(
                    s"Session key generation encountered an unexpected exception for period ${generationContext.validityPeriod}",
                    exception,
                  )
                  pendingSessionKeyGenerationPromise.unwrap.tryFailure(exception).discard
              }
            }
          // CASE 1B:
          // An active request has already created an empty promise, validUsablePendingRequests was evaluated as non-empty
          // This path extracts and awaits the in-flight Futures from the promise.
          case None =>
            // For a single (SignatureDelegationValidityPeriod, Fingerprint) key tuple,
            // there can only be a single in-flight promise.
            // However, multiple distinct validity periods may simultaneously satisfy and cover this request's
            // target interval through `isUsableDelegation`, leading to multiple validUsablePendingRequests.
            val futures =
              generationContext.validUsablePendingRequests.values.map(_.futureUS.unwrap).toSeq

            // wait for the first future to complete
            val first = FutureUnlessShutdown(Future.firstCompletedOf(futures))
            EitherT(first.transformWith {
              case Success(Outcome(Some(sessionKeyAndDelegation))) =>
                FutureUnlessShutdown.pure(Right(sessionKeyAndDelegation))

              // Case where the worker ran but failed due to an expected issue (wrapped in a Left)
              //  (e.g., KMS network drop), which it safely translated into `None`.
              // In this case, we retry a fixed number of times.
              case Success(Outcome(None)) =>
                if (retriesLeft > 0) {
                  logger.debug(
                    s"Awaited session key generation returned None. Retrying getSessionKey for $signingInfo. Attempt ${maxSessionKeyRetries.value - retriesLeft + 1}."
                  )
                  getSessionKey(
                    signingInfo,
                    activeLongTermKey,
                    retriesLeft - 1,
                  ).value
                } else {
                  logger.debug(
                    s"Exhausted concurrency retries of getSessionKey for $signingInfo.."
                  )
                  FutureUnlessShutdown.pure(
                    Left(
                      SyncCryptoError.SyncCryptoSessionKeyGenerationError(
                        SigningKeyGenerationError.GeneralError(
                          new RuntimeException("Exhausted retries for getSessionKey.")
                        )
                      )
                    )
                  )
                }

              // Shutdown case: stop the recursion and pass the shutdown signal up.
              case Success(AbortedDueToShutdown) =>
                logger.info(
                  s"Aborting session key consumer retry loop because the node is shutting down"
                )
                FutureUnlessShutdown(Future.successful(AbortedDueToShutdown))

              // Failure case: The underlying future crashed with an unhandled exception
              // (e.g., NullPointerException) that bypassed the `EitherT` wrapper.
              // Do not retry. Retrying with zero backoff can cause a CPU-burning infinite loop.
              case Failure(exception) =>
                logger.error(s"Awaited session key future chain failed.", exception)
                FutureUnlessShutdown.failed(exception)
            })
        }
      // CASE 2: Found a usable session key
      case Right(sessionKeyAndDelegation) =>
        EitherT.pure[FutureUnlessShutdown, SyncCryptoError](sessionKeyAndDelegation)
    }
  }

  /** Computes the signing timestamp and the validity interval that must be covered when selecting a
    * session signing key.
    *
    * @param topologyTimestamp
    *   the timestamp associated with the topology snapshot used for signing. If an approximate
    *   timestamp is provided via `signingTimestampOverrides`, it is overridden by this timestamp.
    *
    * @return
    *   an optional [[SigningInfo]] describing:
    *   - the signing timestamp used to select the session signing key, and
    *   - the validity interval that the selected key must cover.
    *
    * @return
    *   `None` if the required validity interval cannot be covered by a session signing key, in
    *   which case a long-term key is used for signing instead.
    */
  private def computeSigningInfo(
      topologyTimestamp: CantonTimestamp,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
  ): Option[SigningInfo] = {

    def isCovered(start: CantonTimestamp, end: CantonTimestamp): Boolean = (end - start)
      .compareTo(sessionKeyValidityDuration.asJava) < 0

    signingTimestampOverrides match {
      // if the end period is not defined, and we are using an approximate timestamp (`ts`), then we must choose
      // a session signing key with a validity duration that covers [ts - cutoff, ts + cutoff].
      case Some(SigningTimestampOverrides(ts, None)) =>
        val (start, end) = (
          ts.minus(cutOffDuration.asJava),
          ts.plus(cutOffDuration.asJava),
        )
        // this interval is covered because we enforce that keyValidityDuration > 2 * cutOffDuration
        Some(
          SigningInfo(
            validityIntervalToCover = (start, Some(end)),
            signingTs = ts,
            generateFromTimestampOnly = true,
          )
        )
      // if the end period is defined, and we are using an approximate timestamp (`ts`), then we must choose a
      // session signing key with a validity duration that covers [ts - cutoff, max(ts, validityIntervalToCoverEnd)]
      case Some(SigningTimestampOverrides(ts, Some(periodEnd))) =>
        val (start, end) =
          (ts.minus(cutOffDuration.asJava), ts.max(periodEnd))

        // if any of the validity intervals to cover exceeds the key validity duration
        // for session signing keys, we return `None`, and a long-term key is used for signing instead
        Option.when(isCovered(start, end))(
          SigningInfo(
            validityIntervalToCover = (start, Some(end)),
            signingTs = ts,
            generateFromTimestampOnly = false,
          )
        )
      // if the end period is not defined, and we are using an exact timestamp, then a session signing key
      // is selected whose validity period covers that timestamp.
      case None =>
        Some(
          SigningInfo(
            validityIntervalToCover = (topologyTimestamp, None),
            signingTs = topologyTimestamp,
            generateFromTimestampOnly = true,
          )
        )
    }
  }

  override def sign(
      topologySnapshot: TopologySnapshot,
      signingTimestampOverrides: Option[SigningTimestampOverrides],
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        usage == SigningKeyUsage.ProtocolOnly,
        SyncCryptoError.UnsupportedDelegationSignatureError(
          s"Session signing keys are not supposed to be used for non-protocol messages. Requested usage: $usage"
        ),
      )

      activeLongTermKey <- findSigningKey(member, topologySnapshot, usage)
      // The only exception where we cannot use a session signing key is for the sequencer initialization request,
      // where the timestamp has not yet been assigned and is set with `CantonTimestamp.MinValue` as the reference time
      // (e.g. 0001-01-01T00:00:00.000002Z).
      // If a session signing key was used, its validity would be measured around this reference time,
      // but the verification of that message would be performed using the present time as reference (i.e. now()).
      veryOldTimestampThreshold = CantonTimestamp.MinValue.add(java.time.Duration.ofDays(365))

      signature <-
        if (topologySnapshot.timestamp <= veryOldTimestampThreshold)
          signPrivateApiWithLongTermKeys
            .sign(hash, activeLongTermKey.id, usage)
            .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
        else
          computeSigningInfo(topologySnapshot.timestamp, signingTimestampOverrides) match {
            case Some(signingInfo) =>
              for {
                sessionKeyAndDelegation <- getSessionKey(signingInfo, activeLongTermKey)
                SessionKeyAndDelegation(sessionKey, delegation) = sessionKeyAndDelegation
                signature <- signPublicApiSoftwareBased
                  .sign(hash, sessionKey, usage)
                  .toEitherT[FutureUnlessShutdown]
                  .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
              } yield signature.addSignatureDelegation(delegation)
            case None =>
              // a validity period end of `CantonTimestamp.MaxValue` indicates that we
              // never expected a session signing key to be used, so no need to record it
              if (
                !signingTimestampOverrides.exists(
                  _.validityPeriodEnd.contains(CantonTimestamp.MaxValue)
                )
              )
                cryptoMetrics.kmsMetricsO.foreach(kmsMetrics =>
                  kmsMetrics.sessionSigningKeysFallback.inc()
                )
              signPrivateApiWithLongTermKeys
                .sign(hash, activeLongTermKey.id, usage)
                .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)

          }
    } yield signature

}

object SyncCryptoSignerWithSessionKeys {
  private[crypto] final case class SessionKeyAndDelegation(
      sessionPrivateKey: SigningPrivateKey,
      signatureDelegation: SignatureDelegation,
  )

  private type PendingSessionKeysMap = Map[
    (SignatureDelegationValidityPeriod, Fingerprint),
    PromiseUnlessShutdown[Option[SyncCryptoSignerWithSessionKeys.SessionKeyAndDelegation]],
  ]

  // metadata used to track whether we need to generate a new session key, or wait for a pending one.
  private final case class PendingSessionKeyGenerationContext(
      // if no valid pending session key exists, we will create this promise to notify others
      pendingSessionKeyGenerationPromiseO: Option[
        PromiseUnlessShutdown[Option[SessionKeyAndDelegation]]
      ],
      // valid and usable pending session keys that are already being generated by others
      validUsablePendingRequests: PendingSessionKeysMap,
      activeLongTermKey: SigningPublicKey,
      validityPeriod: SignatureDelegationValidityPeriod,
  )
}
