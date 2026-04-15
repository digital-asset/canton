// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.verifier

import cats.data.EitherT
import cats.implicits.{catsSyntaxAlternativeSeparate, catsSyntaxValidatedId}
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CacheConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.SignatureCheckError.{
  InconsistentSignatureCheckAlarm,
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.SigningAlgorithmSpec.Ed25519
import com.digitalasset.canton.crypto.SymmetricKeyScheme.Aes128Gcm
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.crypto.{
  CryptoScheme,
  Fingerprint,
  Hash,
  PbkdfScheme,
  Signature,
  SignatureCheckError,
  SignatureDelegation,
  SigningKeyUsage,
  SigningPublicKey,
  SynchronizerCryptoPureApi,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, *}

/** Aggregates all methods related to protocol messages' signature verification. If a signature
  * delegation is present, verification uses the session key included in the signature after it has
  * been validated; otherwise, it defaults to the original method and verifies the signature with
  * the long-term key. These methods require a topology snapshot to ensure the correct signing keys
  * are used, based on the current state (i.e., OwnerToKeyMappings).
  *
  * @param verifyPublicApiWithLongTermKeys
  *   The crypto public API used to directly verify messages or validate a signature delegation with
  *   a long-term key.
  */
class SyncCryptoVerifier(
    synchronizerId: SynchronizerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    verifyPublicApiWithLongTermKeys: SynchronizerCryptoPureApi,
    publicKeyConversionCacheConfig: CacheConfig,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with AutoCloseable {

  // The maximum number of concurrent signature verifications allowed.
  private val signatureVerificationParallelism =
    verifyPublicApiWithLongTermKeys.signatureVerificationParallelism

  /** The software-based crypto public API that is used to verify signatures with a session signing
    * key (generated in software). Except for the supported signing schemes, all other schemes are
    * not needed. Therefore, we use fixed schemes (i.e. placeholders) for the other crypto
    * parameters.
    */
  private lazy val verifyPublicApiSoftwareBased: SynchronizerCryptoPureApi = {
    val pureCryptoForSessionKeys = new JcePureCrypto(
      defaultSymmetricKeyScheme = Aes128Gcm, // not used
      signingAlgorithmSpecs =
        CryptoScheme.tryCreate(Ed25519, NonEmpty.mk(Set, Ed25519)), // not used, as this crypto
      // interface is only for signature verification, and the scheme is derived directly from the signature.
      encryptionAlgorithmSpecs =
        CryptoScheme.tryCreate(RsaOaepSha256, NonEmpty.mk(Set, RsaOaepSha256)), // not used
      defaultHashAlgorithm = Sha256, // not used
      defaultPbkdfScheme = PbkdfScheme.Argon2idMode1, // not used
      publicKeyConversionCacheConfig = publicKeyConversionCacheConfig,
      // passing None here is fine because this JcePureCrypto is only used for verifying signatures
      // with a public signing key, and the private key conversion cache is never used.
      privateKeyConversionCacheTtl = None,
      signatureVerificationParallelism = signatureVerificationParallelism,
      loggerFactory = loggerFactory,
    )

    new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCryptoForSessionKeys)
  }

  // Caches the valid signature delegations received as part of the signatures so that we don't need
  // to verify them every time.
  private[canton] val sessionKeysVerificationCache
      : Cache[Fingerprint, (SignatureDelegation, FiniteDuration)] =
    Scaffeine()
      .expireAfter[Fingerprint, (SignatureDelegation, FiniteDuration)](
        create = (_, cacheData) => {
          val (_, retentionTime) = cacheData
          retentionTime
        },
        update = (_, _, d) => d,
        read = (_, _, d) => d,
      )
      // allow the JVM garbage collector to remove entries from it when there is pressure on memory
      .softValues()
      .executor(executionContext.execute(_))
      .build()

  private def loadSigningKeysForMember(
      member: Member,
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Map[Fingerprint, SigningPublicKey]] =
    EitherT.right[SignatureCheckError](
      topologySnapshot
        .signingKeys(member, usage)
        .map(_.map(key => (key.fingerprint, key)).toMap)
    )

  private def loadSigningKeysForMembers(
      members: Seq[Member],
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Map[
    Member,
    Map[Fingerprint, SigningPublicKey],
  ]] =
    EitherT.right[SignatureCheckError](
      topologySnapshot
        .signingKeys(members, usage)
        .map(membersToKeys =>
          members
            .map(member =>
              member -> membersToKeys
                .getOrElse(member, Seq.empty)
                .map(key => (key.fingerprint, key))
                .toMap
            )
            .toMap
        )
    )

  private def getValidKeys(
      topologySnapshot: TopologySnapshot,
      signers: Seq[Member],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Map[Fingerprint, SigningPublicKey]] =
    signers match {
      case Seq(singleSigner) => loadSigningKeysForMember(singleSigner, topologySnapshot, usage)
      case _ =>
        loadSigningKeysForMembers(signers, topologySnapshot, usage)
          .map(_.values.flatMap(_.toSeq).toMap)
    }

  private def determineLongTermKeyForVerification(
      validKeys: Map[Fingerprint, SigningPublicKey],
      authorizingLongTermKey: Fingerprint,
      signerStr: String,
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, SigningPublicKey] =
    (for {
      _ <- Either.cond(
        validKeys.nonEmpty,
        (),
        SignerHasNoValidKeys(
          s"There are no valid keys for $signerStr but received message signed with $authorizingLongTermKey"
        ),
      )
      keyToUse <- validKeys
        .get(authorizingLongTermKey)
        .toRight(
          SignatureWithWrongKey(
            s"Key $authorizingLongTermKey used to generate signature is not a valid key for $signerStr. " +
              s"Valid keys are ${validKeys.values.map(_.fingerprint.unwrap)}"
          )
        )
    } yield keyToUse).toEitherT[FutureUnlessShutdown]

  private def verifySignatureWithSessionKey(
      signatureDelegation: SignatureDelegation,
      topologySnapshot: TopologySnapshot,
      validKeysO: Option[Map[Fingerprint, SigningPublicKey]],
      hash: Hash,
      signature: Signature,
      signers: Seq[Member],
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      _ <- determineSessionKeyIsValid(
        signatureDelegation,
        topologySnapshot,
        validKeysO,
        signers,
        signerStr,
        usage,
      )
      _ <- verifyPublicApiSoftwareBased
        .verifySignature(hash, signatureDelegation.sessionKey, signature, usage)
        .toEitherT[FutureUnlessShutdown]
    } yield ()

  /** Checks if the session key used is still valid
    *
    * For a session key to be valid, we need the following conditions:
    *   - The key must be valid for the given timestamp, according to the validity period
    *   - The key must have been authorized by a long term key which is valid for the given members
    *     and the given time.
    *   - The signatures on the delegation are correct.
    *
    * A valid signatureDelegation (sessionKey, authorizing key, signature) will be cached so that
    * subsequent requests will not require another signature evaluation.
    */
  private def determineSessionKeyIsValid(
      signatureDelegation: SignatureDelegation,
      topologySnapshot: TopologySnapshot,
      validKeysO: Option[Map[Fingerprint, SigningPublicKey]],
      signers: Seq[Member],
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {

    val SignatureDelegation(sessionKey, validityPeriod, _) = signatureDelegation
    val currentTimestamp = topologySnapshot.timestamp
    def invalidSessionKey =
      SignatureCheckError.InvalidSignatureDelegation(
        "The current signature delegation " +
          s"is only valid from ${validityPeriod.fromInclusive} to ${validityPeriod.toExclusive} while the " +
          s"current timestamp is $currentTimestamp"
      ): SignatureCheckError

    def verifySessionKey(
        longTermKey: SigningPublicKey,
        doNotCacheAndWarn: Boolean,
    ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
      for {
        // validate signature of delegation (key validity is tested below)
        _ <- verifyPublicApiWithLongTermKeys
          .verifySignature(
            SignatureDelegation.generateHash(
              synchronizerId,
              signatureDelegation.sessionKey,
              signatureDelegation.validityPeriod,
            ),
            longTermKey,
            signatureDelegation.signature,
            usage,
          )
          .leftMap[SignatureCheckError] { err =>
            SignatureCheckError.InvalidSignatureDelegation(err.show)
          }
          .toEitherT[FutureUnlessShutdown]
        dynamicSynchronizerParameters <- EitherT(
          topologySnapshot.findDynamicSynchronizerParameters()
        )
          .leftMap[SignatureCheckError](err =>
            SignatureCheckError.MissingDynamicSynchronizerParameters(err)
          )

        expirationTime = signatureDelegation.validityPeriod.toExclusive - currentTimestamp
        safetyMargin = (dynamicSynchronizerParameters.parameters.confirmationResponseTimeout +
          dynamicSynchronizerParameters.parameters.mediatorReactionTimeout).duration

        /* The safety margin should be as large as the expected delay in using old topology snapshots.
         * That's by default the sum of the dynamic synchronizer parameters confirmationResponseTimeout and
         * mediatorReactionTimeout (plus the assumed drift - 30 seconds) in the participant's processing
         * speed over sequencer speed).
         */
        retentionTimeMillis = expirationTime.toMillis + safetyMargin.toMillis + 30.seconds.toMillis

      } yield {
        if (doNotCacheAndWarn)
          logger.warn(
            s"Session key ${sessionKey.id} -> ${signatureDelegation.delegatingKeyId} is associated with more than one delegation. This is not wrong, but suspicious."
          )
        else {
          /* The signature delegation is valid, so we can store it for future use. Since
           * the delegation signature is not added at the moment of creation, it may be stored in the cache
           * for longer than its validity period. We can accept this because, even though the key is in the cache,
           * it is invalid and cannot be used.
           */
          sessionKeysVerificationCache.put(
            sessionKey.id,
            (signatureDelegation, FiniteDuration(retentionTimeMillis, MILLISECONDS)),
          )
        }
      }

    // we store the received and validated signature delegation in a cache to avoid re-verifying it
    val cachedSignatureDelegationO =
      sessionKeysVerificationCache.getIfPresent(sessionKey.fingerprint).map { case (sD, _) => sD }
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        usage == SigningKeyUsage.ProtocolOnly,
        (),
        SignatureCheckError.UnsupportedDelegationSignatureError(
          s"Session signing keys are not supposed to be used for non-protocol messages. Requested usage: $usage"
        ),
      )

      // get the current long-term valid keys
      validKeys <- validKeysO.fold(
        getValidKeys(topologySnapshot, signers, usage)
      )(validKeys => EitherT.rightT[FutureUnlessShutdown, SignatureCheckError](validKeys))

      // check if the delegation is no longer valid for this timestamp
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        signatureDelegation.isValidAt(currentTimestamp),
        invalidSessionKey,
      )

      // check if the delegation is using a valid key according to the topology state
      longTermKey <- determineLongTermKeyForVerification(
        validKeys,
        signatureDelegation.signature.authorizingLongTermKey,
        signerStr,
      )
      // if there is no delegation in the cache so we need to validate the session key with the long-term key
      // and store the delegation for future use
      _ <-
        if (!cachedSignatureDelegationO.contains(signatureDelegation)) {
          verifySessionKey(longTermKey, cachedSignatureDelegationO.nonEmpty)
        } else {
          // otherwise we know that the delegation itself is correct in terms of signatures
          // and as we just validated that the keys used in there are still valid, we can skip any further test
          EitherTUtil.unitUS[SignatureCheckError]
        }
    } yield ()
  }

  private def verifySignatureInternal(
      topologySnapshot: TopologySnapshot,
      validKeysO: Option[Map[Fingerprint, SigningPublicKey]],
      hash: Hash,
      signature: Signature,
      signers: Seq[Member],
      signerStr: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    signature.signatureDelegation match {
      case Some(_) if staticSynchronizerParameters.protocolVersion < ProtocolVersion.v35 =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          SignatureCheckError.UnsupportedDelegationSignatureError(
            s"Session signing keys are not supposed to be used with protocol version " +
              s"${staticSynchronizerParameters.protocolVersion} and must be used with protocol version " +
              s"${ProtocolVersion.v35} or higher."
          )
        )
      case Some(signatureDelegation) =>
        verifySignatureWithSessionKey(
          signatureDelegation,
          topologySnapshot,
          validKeysO,
          hash,
          signature,
          signers,
          signerStr,
          usage,
        )
      // a signature with no session key delegation, so we run the verification with the long-term key
      case None =>
        for {
          validKeys <- getValidKeys(topologySnapshot, signers, usage)
          keyToUse <- determineLongTermKeyForVerification(
            validKeys,
            signature.authorizingLongTermKey,
            signerStr,
          )
          _ <- verifyPublicApiWithLongTermKeys
            .verifySignature(hash, keyToUse, signature, usage)
            .toEitherT[FutureUnlessShutdown]
        } yield ()
    }

  /** Verify a given signature using the currently active signing keys in the current topology
    * state.
    */
  def verifySignature(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    verifySignatureInternal(
      topologySnapshot,
      None,
      hash,
      signature,
      Seq(signer),
      signer.toString,
      usage,
    )

  /** Verifies multiple signatures using the currently active signing keys in the current topology
    * state.
    */
  def verifySignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    MonadUtil.parTraverseWithLimit_(signatureVerificationParallelism)(signatures.forgetNE)(
      signature =>
        verifySignatureInternal(
          topologySnapshot,
          None,
          hash,
          signature,
          Seq(signer),
          signer.toString,
          usage,
        )
    )

  /** Only verifies key usage, not the actual signature */
  def verifyKeyUsage(
      topologySnapshot: TopologySnapshot,
      signer: Member,
      signedBy: Fingerprint,
      signatureDelegationO: Option[SignatureDelegation],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    signatureDelegationO match {
      case Some(signatureDelegation) =>
        determineSessionKeyIsValid(
          signatureDelegation,
          topologySnapshot,
          validKeysO = None,
          signers = Seq(signer),
          signerStr = signer.toString,
          usage: NonEmpty[Set[SigningKeyUsage]],
        )
      case None =>
        for {
          validKeys <- getValidKeys(topologySnapshot, Seq(signer), usage)
          _ <- determineLongTermKeyForVerification(
            validKeys,
            Signature.authorizingLongTermKey(signedBy, signatureDelegationO),
            signer.toString,
          )
        } yield ()
    }

  /** Verifies multiple group signatures using the currently active signing keys of the different
    * signers in the current topology state.
    *
    * @param threshold
    *   the number of valid signatures required for the overall verification to be considered
    *   correct.
    */
  def verifyGroupSignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signers: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] =
    for {
      validKeysWithMembers <- loadSigningKeysForMembers(signers, topologySnapshot, usage)
      // Build a map from fingerprint to the set of members that own that key.
      // Using groupMap instead of flatMap+toMap avoids silently dropping entries when
      // two different members share the same key fingerprint.
      keyMembers: Map[Fingerprint, Seq[Member]] = validKeysWithMembers.toSeq
        .flatMap { case (member, keyMap) =>
          keyMap.keys.map(fingerprint => fingerprint -> member)
        }
        .groupMap(_._1)(_._2)
      validKeys = validKeysWithMembers.values.flatMap(_.toSeq).toMap
      validated <- EitherT.right(
        MonadUtil.parTraverseWithLimit(signatureVerificationParallelism)(signatures.forgetNE) {
          signature =>
            verifySignatureInternal(
              topologySnapshot,
              Some(validKeys),
              hash,
              signature,
              signers,
              groupName,
              usage,
            ).fold(
              _.invalid,
              _ => {
                if (staticSynchronizerParameters.protocolVersion >= ProtocolVersion.v35) {
                  val fp = signature.authorizingLongTermKey
                  keyMembers.get(fp) match {
                    case Some(Seq(singleMember)) =>
                      singleMember.valid[SignatureCheckError]
                    case Some(members) =>
                      // The key fingerprint is shared by multiple members in the group.
                      // We cannot reliably attribute the signature to a specific member,
                      // so we reject it to prevent threshold bypass via misattribution.
                      SignatureWithWrongKey(
                        s"Key $fp is shared by multiple group members " +
                          s"(${members.mkString(", ")}); cannot attribute signature unambiguously"
                      ).invalid
                    case None =>
                      // Should not happen since verifySignatureInternal already validated the key,
                      // but handle defensively.
                      SignatureWithWrongKey(
                        s"Key $fp used to sign is not associated with any member in $groupName"
                      ).invalid
                  }
                } else {
                  keyMembers(signature.authorizingLongTermKey).valid[SignatureCheckError]
                }
              },
            )
        }
      )
      _ <- {
        val (signatureCheckErrors, validSigners) = validated.separate
        EitherT.cond[FutureUnlessShutdown](
          validSigners.distinct.sizeIs >= threshold.value, {
            if (signatureCheckErrors.nonEmpty) {
              val errors = SignatureCheckError.MultipleErrors(signatureCheckErrors)
              InconsistentSignatureCheckAlarm
                .Warn(
                  s"Signature check passed for $groupName, although there were errors: $errors"
                )
                .report()
            }
            ()
          },
          SignatureCheckError.MultipleErrors(
            signatureCheckErrors,
            Some(s"$groupName signature threshold not reached"),
          ): SignatureCheckError,
        )
      }
    } yield ()

  override def close(): Unit = {
    sessionKeysVerificationCache.invalidateAll()
    sessionKeysVerificationCache.cleanUp()
  }
}

object SyncCryptoVerifier {

  def create(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      pureCrypto: SynchronizerCryptoPureApi,
      publicKeyConversionCacheConfig: CacheConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) =
    new SyncCryptoVerifier(
      synchronizerId,
      staticSynchronizerParameters,
      pureCrypto,
      publicKeyConversionCacheConfig: CacheConfig,
      loggerFactory,
    )

}
