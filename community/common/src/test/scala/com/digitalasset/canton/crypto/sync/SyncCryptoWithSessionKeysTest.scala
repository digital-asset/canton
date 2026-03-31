// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.sync

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithSessionKeys
import com.digitalasset.canton.crypto.{
  Signature,
  SignatureDelegation,
  SignatureDelegationValidityPeriod,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.ResourceUtil
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

class SyncCryptoWithSessionKeysTest extends AnyWordSpec with SyncCryptoTest {
  override protected lazy val sessionSigningKeysConfig: SessionSigningKeysConfig =
    SessionSigningKeysConfig.default

  private lazy val validityDuration = sessionSigningKeysConfig.keyValidityDuration

  private def sessionKeysCache(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeysSigningCache
      .asMap()

  private def sessionKeysVerificationCache(p: SynchronizerCryptoClient) =
    p.syncCryptoVerifier.sessionKeysVerificationCache.asMap().map { case (id, (sD, _)) => (id, sD) }

  private def cleanUpSessionKeysCache(p: SynchronizerCryptoClient): Unit = {
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeysSigningCache
      .invalidateAll()
    p.syncCryptoSigner.asInstanceOf[SyncCryptoSignerWithSessionKeys].pendingRequests.clear()
  }

  private def cleanUpSessionKeysVerificationCache(p: SynchronizerCryptoClient): Unit =
    p.syncCryptoVerifier.sessionKeysVerificationCache
      .invalidateAll()

  private def cutOffDuration(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner.asInstanceOf[SyncCryptoSignerWithSessionKeys].cutOffDuration

  private def toleranceShiftDuration(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner.asInstanceOf[SyncCryptoSignerWithSessionKeys].toleranceShiftDuration

  private def setSessionKeyEvictionPeriod(
      p: SynchronizerCryptoClient,
      newPeriod: FiniteDuration,
  ): Unit =
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeyEvictionPeriod
      .set(newPeriod)

  /* Verify that a signature delegation is currently stored in the cache and contains the correct
   * information: (1) it is signed by a long-term key, (2) the enclosing signature is correctly listed
   * as being signed by a session key, and (3) the validity period is correct.
   */
  private def checkSignatureDelegation(
      topologySnapshot: TopologySnapshot,
      signature: Signature,
      p: SynchronizerCryptoClient = p1,
      signingTimestampOverrides: Option[SigningTimestampOverrides] = None,
  ): SignatureDelegation = {

    val cache = sessionKeysCache(p)
    val (_, sessionKeyAndDelegation) = cache
      .find { case (_, skD) =>
        signature.signatureDelegation.contains(skD.signatureDelegation)
      }
      .valueOrFail("no signature delegation")

    topologySnapshot
      .signingKeys(p.member, defaultUsage)
      .futureValueUS
      .map(_.id) should contain(
      sessionKeyAndDelegation.signatureDelegation.signature.authorizingLongTermKey
    )

    val sessionKeyId = sessionKeyAndDelegation.signatureDelegation.sessionKey.id
    val validityPeriod = sessionKeyAndDelegation.signatureDelegation.validityPeriod

    // The signature contains the session key in the 'signedBy' field.
    signature.signedBy shouldBe sessionKeyId

    val start = signingTimestampOverrides match {
      // Interval to cover: [ts - cutoff, ts + cutoff]
      // The validity period for a new key is the default [ts - tolerance, (ts - tolerance) + keyValidityDuration]
      case Some(SigningTimestampOverrides(ts, None)) =>
        ts.minus(toleranceShiftDuration(p).asJava)
      // Interval to cover: [ts - cutOff, max(ts, validityPeriodEnd)]
      // The validity period for a new key is
      // [ts - cutoff, (ts - cutoff) + keyValidityDuration].
      case Some(SigningTimestampOverrides(ts, Some(_))) =>
        ts.minus(cutOffDuration(p).asJava)
      // Interval to cover: [ts, None]
      // The validity period for a new key is the default [ts - tolerance, (ts - tolerance) + keyValidityDuration],
      case None =>
        topologySnapshot.timestamp.minus(toleranceShiftDuration(p).asJava)
    }

    // Verify it has the correct validity period
    validityPeriod shouldBe SignatureDelegationValidityPeriod(
      start,
      validityDuration,
    )

    sessionKeyAndDelegation.signatureDelegation
  }

  private def cleanCache(p: SynchronizerCryptoClient) = {
    // make sure we start from a clean state
    cleanUpSessionKeysCache(p)
    cleanUpSessionKeysVerificationCache(p)

    sessionKeysCache(p) shouldBe empty
    sessionKeysVerificationCache(p) shouldBe empty
  }

  "A SyncCrypto with session keys" must {

    behave like syncCryptoSignerTest()

    "use correct sync crypto signer with session keys" in {
      syncCryptoSignerP1 shouldBe a[SyncCryptoSignerWithSessionKeys]
      cleanCache(p1)
    }

    "correctly produce a signature delegation when signing a single message" in {

      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signatureDelegation = checkSignatureDelegation(testSnapshot, signature)

      syncCryptoVerifierP1
        .verifySignature(
          testSnapshot,
          hash,
          participant1.member,
          signature,
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS

      val (_, sDSigningCached) = sessionKeysCache(p1).loneElement
      val (_, sDVerificationCached) = sessionKeysVerificationCache(p1).loneElement

      // make sure that nothing changed with the session key and signature delegation
      sDSigningCached.signatureDelegation shouldBe signatureDelegation
      sDSigningCached.signatureDelegation shouldBe sDVerificationCached

    }

    "sign and verify message with different synchronizers uses different session keys" in {

      ResourceUtil.withResource(
        testingTopology.forOwnerAndSynchronizer(
          owner = participant1,
          synchronizerId = otherSynchronizerId,
        )
      ) { p1OtherSynchronizer =>
        val syncCryptoSignerP1Other = p1OtherSynchronizer.syncCryptoSigner

        val signature = syncCryptoSignerP1Other
          .sign(
            testSnapshot,
            None,
            hash,
            defaultUsage,
          )
          .valueOrFail("sign failed")
          .futureValueUS

        val signatureDelegationOther =
          checkSignatureDelegation(testSnapshot, signature, p1OtherSynchronizer)

        sessionKeysCache(p1OtherSynchronizer).loneElement

        // it's different from the signature delegation for the other synchronizer
        val (_, signatureDelegation) = sessionKeysCache(p1).loneElement
        signatureDelegationOther.validityPeriod shouldBe signatureDelegation.signatureDelegation.validityPeriod
        signatureDelegationOther.sessionKey should not be signatureDelegation.signatureDelegation.sessionKey
        signatureDelegationOther.signature should not be signatureDelegation.signatureDelegation.signature
      }
    }

    "reuse session key when exact timestamp and past cut-off" in {

      val (_, currentSessionKey) = sessionKeysCache(p1).loneElement

      // select a timestamp that is after the "end" cut-off period
      val (_, end) =
        currentSessionKey.signatureDelegation.validityPeriod
          .computeCutOffTimestamp(cutOffDuration(p1).asJava, isUsingAnApproximateTimestamp = true)

      val testSnapshotWithEndTimestamp = testingTopology.topologySnapshot(timestampOfSnapshot = end)

      testSnapshotWithEndTimestamp.timestamp shouldBe end

      // we use an "exact" timestamp to sign
      val signatureEnd = syncCryptoSignerP1
        .sign(
          testSnapshotWithEndTimestamp,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signatureDelegation =
        checkSignatureDelegation(testSnapshot, signatureEnd, p1)

      signatureDelegation.sessionKey shouldBe currentSessionKey.signatureDelegation.sessionKey
      sessionKeysCache(p1).loneElement

    }

    "use new session key when approximate timestamp and past cut-off" in {

      val (_, currentSessionKey) = sessionKeysCache(p1).loneElement
      // select a timestamp that is after the "end" cut-off period
      val (start, end) =
        currentSessionKey.signatureDelegation.validityPeriod
          .computeCutOffTimestamp(cutOffDuration(p1).asJava, isUsingAnApproximateTimestamp = true)

      // we use an "approximate" timestamp to sign
      val signatureEnd = syncCryptoSignerP1
        .sign(
          testSnapshot,
          Some(
            SigningTimestampOverrides(
              approximateTimestamp = end,
              // Use `None` to ignore the validity period end and select the session signing key
              // using only the approximate timestamp
              validityPeriodEnd = None,
            )
          ),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(
        testSnapshot,
        signatureEnd,
        signingTimestampOverrides = Some(
          SigningTimestampOverrides(
            approximateTimestamp = end,
            // Use `None` to ignore the validity period end and select the session signing key
            // using only the approximate timestamp
            validityPeriodEnd = None,
          )
        ),
      )

      // There must be a second key in the cache because we used a different session key for the latest sign call.
      // For the previous key, although still valid, the signing timestamp exceeds its "end" cutoff period.
      sessionKeysCache(p1).size shouldBe 2

      val signatureStart = syncCryptoSignerP1
        .sign(
          testSnapshot,
          Some(
            SigningTimestampOverrides(
              // The immediate predecessor so that the timestamp lies just before the cutOff,
              // because the session signing key's validity period is left-inclusive ([start, end)).
              approximateTimestamp = start.immediatePredecessor,
              // Use `None` to ignore the validity period end and select the session signing key
              // using only the approximate timestamp
              validityPeriodEnd = None,
            )
          ),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(
        testSnapshot,
        signatureStart,
        signingTimestampOverrides = Some(
          SigningTimestampOverrides(
            approximateTimestamp = start.immediatePredecessor,
            // Use `None` to ignore the validity period end and select the session signing key
            // using only the approximate timestamp
            validityPeriodEnd = None,
          )
        ),
      )

      // There must be a third key in the cache because we used a different session key for the latest sign call.
      // For the previous key, although still valid, the signing timestamp does not yet exceed its "start" cutoff period.
      sessionKeysCache(p1).size shouldBe 3

    }

    "use a new session key if the long-term key is no longer active" in {

      val oldLongTermKeyId = testSnapshot
        .signingKeys(participant1.member, defaultUsage)
        .futureValueUS
        .loneElement
        .id

      testingTopology.getTopology().freshKeys.set(true)

      val tsWhenNewKeyIsValid = CantonTimestamp.Epoch.add(validityDuration.underlying)
      val newSnapshotWithFreshKeys =
        testingTopology.topologySnapshot(timestampOfSnapshot = tsWhenNewKeyIsValid)
      val newLongTermKeyId =
        newSnapshotWithFreshKeys
          .signingKeys(participant1.member, defaultUsage)
          .futureValueUS
          .loneElement
          .id

      newLongTermKeyId should not be oldLongTermKeyId

      val signature = syncCryptoSignerP1
        .sign(
          newSnapshotWithFreshKeys,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signatureDelegation = checkSignatureDelegation(newSnapshotWithFreshKeys, signature)

      signatureDelegation.delegatingKeyId shouldBe newLongTermKeyId

      // The current implementation allows the validity period to reach back
      // to before the key was created (i.e., before it became active), since we currently only look
      // whether a key is valid in a given topology snapshot.
      // We could change this behavior in the future if the topology processor ends up exposing
      // future key-deactivation events.
      signatureDelegation.validityPeriod.fromInclusive < tsWhenNewKeyIsValid

    }

    "session signing key is removed from the cache after the eviction period" in {
      cleanUpSessionKeysCache(p1)

      val newEvictionPeriod = PositiveSeconds.tryOfSeconds(5).toFiniteDuration

      setSessionKeyEvictionPeriod(p1, newEvictionPeriod)
      sessionKeysCache(p1) shouldBe empty

      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(testSnapshot, signature)

      Threading.sleep(newEvictionPeriod.toMillis + 100L)

      eventually() {
        sessionKeysCache(p1).toSeq shouldBe empty
      }
    }

    "with decreasing timestamps we still use one session signing key" in {

      cleanCache(p1)
      testingTopology.getTopology().freshKeys.set(false)

      /* This covers a test scenario where we receive a decreasing timestamp order of signing requests
          and verifies that since the validity period of a key is set as ts-cutOff/2 to ts+x+cutOff/2 we
          can still use the same session key created for the first request.
       */

      val signatureDelegation3 = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot = CantonTimestamp.ofEpochSecond(3)),
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      val signatureDelegation2 = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot = CantonTimestamp.ofEpochSecond(2)),
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      val signatureDelegation1 = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot = CantonTimestamp.ofEpochSecond(1)),
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      signatureDelegation1 should equal(signatureDelegation2)
      signatureDelegation2 should equal(signatureDelegation3)

      // enough time has elapsed and the session key is no longer valid

      val signatureDelegationNew = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot =
            CantonTimestamp.Epoch
              .addMicros(validityDuration.unwrap.toMicros)
              .add(cutOffDuration(p1).asJava)
          ),
          None,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      signatureDelegationNew should not equal signatureDelegation1

    }

    def signWithValidityPeriodEnd(
        approximateTimestamp: CantonTimestamp,
        validityPeriodEnd: CantonTimestamp,
        expectSignatureDelegation: Boolean,
    ): Unit = {

      val testingTopologyAtTs =
        testingTopology.topologySnapshot(timestampOfSnapshot = approximateTimestamp)

      val signature = syncCryptoSignerP1
        .sign(
          testingTopologyAtTs,
          Some(
            SigningTimestampOverrides(
              approximateTimestamp = approximateTimestamp,
              validityPeriodEnd = Some(validityPeriodEnd),
            )
          ),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      if (expectSignatureDelegation)
        checkSignatureDelegation(
          testingTopologyAtTs,
          signature,
          signingTimestampOverrides = Some(
            SigningTimestampOverrides(
              approximateTimestamp = approximateTimestamp,
              validityPeriodEnd = Some(validityPeriodEnd),
            )
          ),
        )
      else
        signature.signatureDelegation shouldBe empty

      val testingTopologyAtValidityPeriodEnd =
        testingTopology.topologySnapshot(timestampOfSnapshot = validityPeriodEnd)

      // verify that signature is still valid at `validityPeriodEnd`
      syncCryptoVerifierP1
        .verifySignature(
          testingTopologyAtValidityPeriodEnd,
          hash,
          participant1.member,
          signature,
          defaultUsage,
        )
        .valueOrFail("verification failed")
        .futureValueUS

    }

    "correctly produces a signature delegation with a key validity that covers both timestamp and validity period end" in {

      cleanCache(p1)

      // select an `validityPeriodEnd` that CAN be covered by a session signing key
      signWithValidityPeriodEnd(
        testSnapshot.timestamp,
        testSnapshot.timestamp.immediateSuccessor,
        expectSignatureDelegation = true,
      )

    }

    "uses a different session key when previous one doesn’t cover the validity period end" in {

      val (_, currentSessionKey) = sessionKeysCache(p1).loneElement

      signWithValidityPeriodEnd(
        // The previous session signing key can cover [timestamp - cutOff, (timestamp - cutOff) + keyValidityDuration[
        // so we use the excluded end as our validity period end. However, we use the immediate successor as
        // our signing timestamp due to the constraint that keyValidity - cutOff > maxSequencingTimeOffset.
        testSnapshot.timestamp.immediateSuccessor,
        currentSessionKey.signatureDelegation.validityPeriod.toExclusive,
        expectSignatureDelegation = true,
      )

      sessionKeysCache(p1).size shouldBe 2

    }

    "revert to signing with long-term key if the validity period end cannot be covered" in {

      signWithValidityPeriodEnd(
        testSnapshot.timestamp,
        testSnapshot.timestamp
          .add(
            sessionSigningKeysConfig.keyValidityDuration.asJava
          ),
        expectSignatureDelegation = false,
      )

    }

    "uses a session signing key even if the validity period end is in the past" in {

      cleanCache(p1)

      signWithValidityPeriodEnd(
        testSnapshot.timestamp.immediateSuccessor,
        testSnapshot.timestamp,
        expectSignatureDelegation = true,
      )

    }

  }
}
