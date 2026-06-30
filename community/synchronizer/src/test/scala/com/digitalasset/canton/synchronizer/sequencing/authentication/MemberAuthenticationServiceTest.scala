// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchingConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.Signature.noSignature
import com.digitalasset.canton.crypto.SigningKeyUsage.SequencerAuthenticationOnly
import com.digitalasset.canton.crypto.{
  Nonce,
  SignatureCheckError,
  SigningKeyUsage,
  SigningPublicKey,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  MemberAccessDisabled,
  MissingToken,
  NonMatchingSynchronizerId,
}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.{
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{Member, *}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration as JDuration

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class MemberAuthenticationServiceTest extends AsyncWordSpec with BaseTest with FailOnShutdown {

  import DefaultTestIdentities.*

  private val p1 = participant1

  private val clock: SimClock = new SimClock(loggerFactory = loggerFactory)

  private val topology = TestingTopology().withSimpleParticipants(participant1).build()
  private val syncCrypto = topology.forOwnerAndSynchronizer(participant1, physicalSynchronizerId)

  private def service(
      participantIsActive: Boolean,
      useExponentialRandomTokenExpiration: Boolean = false,
      nonceDuration: JDuration = JDuration.ofMinutes(1),
      tokenDuration: JDuration = JDuration.ofHours(1),
      invalidateMemberCallback: Member => Unit = _ => (),
      store: MemberAuthenticationStore = new MemberAuthenticationStore(
        PositiveInt.tryCreate(10),
        PositiveInt.tryCreate(10),
        loggerFactory,
      ),
  ): MemberAuthenticationService =
    new MemberAuthenticationService(
      physicalSynchronizerId,
      syncCrypto,
      store,
      clock,
      nonceDuration,
      tokenDuration,
      useExponentialRandomTokenExpiration = useExponentialRandomTokenExpiration,
      memberT => invalidateMemberCallback(memberT.value),
      FutureUnlessShutdown.unit,
      DefaultProcessingTimeouts.testing,
      BatchingConfig(),
      loggerFactory,
    ) {
      override def isParticipantActive(participant: ParticipantId)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Boolean] =
        FutureUnlessShutdown.pure(participantIsActive)
    }

  private def serviceImpl(
      store: MemberAuthenticationStore
  ): MemberAuthenticationServiceImpl =
    new MemberAuthenticationServiceImpl(
      physicalSynchronizerId,
      syncCrypto,
      store,
      clock,
      nonceExpirationInterval = JDuration.ofMinutes(1),
      maxTokenExpirationInterval = JDuration.ofHours(1),
      useExponentialRandomTokenExpiration = false,
      _ => (),
      isTopologyInitialized = FutureUnlessShutdown.unit,
      timeouts = DefaultProcessingTimeouts.testing,
      batchingConfig = BatchingConfig(),
      loggerFactory,
    )

  private def getMemberAuthentication(member: Member): MemberAuthentication =
    MemberAuthentication(member).getOrElse(fail("unsupported"))

  "MemberAuthenticationService" should {

    def generateToken(sut: MemberAuthenticationService) =
      for {
        challenge <- sut.generateNonce(p1)
        (nonce, fingerprints) = challenge
        signature <- getMemberAuthentication(p1)
          .signSynchronizerNonce(p1, nonce, physicalSynchronizerId, fingerprints, syncCrypto.crypto)
        tokenAndExpiry <- sut.validateSignature(p1, signature, nonce)
      } yield tokenAndExpiry

    def fetchTokens(
        store: MemberAuthenticationStore,
        members: Seq[Member],
    ): Map[Member, Seq[StoredAuthenticationToken]] =
      members.flatMap(store.fetchTokens).groupBy(_.member)

    "generate a test token with default expiration" in {
      val sut = service(participantIsActive = true)
      val testToken = sut.generateAuthenticationToken(p1, None)
      for {
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          sut.validateToken(physicalSynchronizerId, p1, testToken.token)
        )
      } yield {
        testToken.expireAt should be(clock.now.plus(sut.maxTokenExpirationInterval))
      }
    }

    "generate a test token with explicit expiration" in {
      val sut = service(participantIsActive = true)
      val testToken =
        sut.generateAuthenticationToken(p1, Some(NonNegativeFiniteDuration.tryOfSeconds(5)))
      for {
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          sut.validateToken(physicalSynchronizerId, p1, testToken.token)
        )
      } yield {
        testToken.expireAt should be(clock.now.plus(JDuration.ofSeconds(5)))
      }
    }

    "generate nonce, verify signature, generate token, verify token, and verify expiry" in {
      val sut = service(participantIsActive = true)
      for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          sut.validateToken(physicalSynchronizerId, p1, token)
        )
      } yield {
        expiry should be(clock.now.plus(JDuration.ofHours(1)))
      }
    }

    "generate nonce, verify signature, generate token, verify token, and verify exponential expiry" in {
      val sut = service(participantIsActive = true, useExponentialRandomTokenExpiration = true)
      for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          sut.validateToken(physicalSynchronizerId, p1, token)
        )
      } yield {
        expiry should be >= clock.now.plus(JDuration.ofMinutes(30))
        expiry should be <= clock.now.plus(JDuration.ofHours(1))
      }
    }

    "use random expiry" in {
      val sut = service(participantIsActive = true, useExponentialRandomTokenExpiration = true)
      for {
        expireTimes <- Seq.fill(10)(generateToken(sut).map(_.expiresAt)).sequence
      } yield {
        expireTimes.distinct.size should be > 1
      }
    }

    "fail every method if participant is not active" in {
      val sut = service(participantIsActive = false)
      for {
        generateNonceError <- leftOrFail(sut.generateNonce(p1))("generating nonce")
        validateSignatureError <- leftOrFail(
          sut.validateSignature(p1, null, Nonce.generate(syncCrypto.pureCrypto))
        )(
          "validateSignature"
        )
        validateTokenError = leftOrFail(sut.validateToken(physicalSynchronizerId, p1, null))(
          "token validation should fail"
        )
      } yield {
        generateNonceError shouldBe MemberAccessDisabled(p1)
        validateSignatureError shouldBe MemberAccessDisabled(p1)
        validateTokenError shouldBe MissingToken(p1)
      }
    }

    "check whether the intended synchronizer is the one the participant is connecting to" in {
      val sut = service(participantIsActive = false)
      val wrongSynchronizerId =
        SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("wrong::synchronizer")).toPhysical

      val error =
        leftOrFail(sut.validateToken(wrongSynchronizerId, p1, null))(
          "should fail synchronizer check"
        )
      error shouldBe NonMatchingSynchronizerId(p1, wrongSynchronizerId)
    }

    "invalidate all tokens from a member when logging out" in {
      val store = new MemberAuthenticationStore(
        PositiveInt.tryCreate(10),
        PositiveInt.tryCreate(10),
        loggerFactory,
      )
      val sut = service(participantIsActive = true, store = store)

      for {
        tokenAndExpiry <- generateToken(sut)
        AuthenticationTokenWithExpiry(token, _expiry) = tokenAndExpiry
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          sut.validateToken(physicalSynchronizerId, p1, token)
        )
        // Generate a second token for p1
        _ <- generateToken(sut)

        tokensBefore = fetchTokens(store, Seq(p1))

        // Use the first token to invalidate them all
        _ <- EitherT(sut.invalidateMemberWithToken(token)).leftWiden[AuthenticationError]
        tokensAfter = fetchTokens(store, Seq(p1))
      } yield {
        tokensBefore(p1) should have size 2
        tokensAfter shouldBe empty
      }
    }
  }

  "revoke tokens via the topology processing subscriber when an OwnerToKeyMapping is removed" in {
    val store = new MemberAuthenticationStore(
      PositiveInt.tryCreate(10),
      PositiveInt.tryCreate(10),
      loggerFactory,
    )

    val sutImpl = serviceImpl(store)

    val retainedKey = syncCrypto.crypto
      .generateSigningKey(usage = SigningKeyUsage.All)
      .value
      .futureValueUS
      .getOrElse(fail("Retained key generation failed"))
    val evictedKey = syncCrypto.crypto
      .generateSigningKey(usage = SigningKeyUsage.All)
      .value
      .futureValueUS
      .getOrElse(fail("Evicted key generation failed"))

    val retainedFingerprint = retainedKey.fingerprint
    val evictedFingerprint = evictedKey.fingerprint

    val retainedToken = StoredAuthenticationToken(
      p1,
      clock.now.plusSeconds(100),
      AuthenticationToken.generate(syncCrypto.pureCrypto),
      retainedFingerprint,
    )
    val evictedToken = StoredAuthenticationToken(
      p1,
      clock.now.plusSeconds(100),
      AuthenticationToken.generate(syncCrypto.pureCrypto),
      evictedFingerprint,
    )

    store.saveToken(retainedToken)
    store.saveToken(evictedToken)

    // Create an OTK for the evictedKey
    val evictedOtk = OwnerToKeyMapping.tryCreate(p1, com.daml.nonempty.NonEmpty(Seq, evictedKey))

    // create a topology transaction for the removal of the created OTK
    val removeTopologyTx = TopologyTransaction(
      TopologyChangeOp.Remove,
      PositiveInt.one,
      evictedOtk,
      testedProtocolVersion,
    )

    // create a SignedTopologyTransaction wrapping the removal transaction
    val txElement = SignedTopologyTransaction.withSignature(
      transaction = removeTopologyTx,
      signature = noSignature,
      isProposal = false,
      protocolVersion = testedProtocolVersion,
    )

    val sequencerTime = SequencedTime(clock.now)
    val effectiveTime = EffectiveTime(clock.now)

    val tokensBefore = store.fetchTokens(p1)

    // check that both tokens are active in the store prior to execution
    tokensBefore should contain.only(retainedToken, evictedToken)

    // Trigger the subscriber callback to simulate the observation of a real transaction.
    // Note that the call to observed here is meant to trigger the token eviction, but effectiveTime
    // is not what determines the "old" state in the eviction logic in this test. These tests use the TestingTopology
    // which only support one overall snapshot state, so they have, in fact, only one topology state.
    // Since we only want to test here that the tokens are removed from the stores, this is fine.
    for {
      _ <- sutImpl
        .observed(sequencerTime, effectiveTime, SequencerCounter(1), Seq(txElement))
        .failOnShutdown
    } yield {
      val tokensAfter = store.fetchTokens(p1)

      // Verify the targeted token is revoked and the other token stays
      tokensAfter should contain.only(retainedToken)
    }
  }

  "revoke evicted tokens via the topology processing subscriber when an OwnerToKeyMapping is replaced" in {
    val store = new MemberAuthenticationStore(
      PositiveInt.tryCreate(10),
      PositiveInt.tryCreate(10),
      loggerFactory,
    )

    val sutImpl = serviceImpl(store)

    // For a Replace operation, the transaction payload only contains the new (retained) keys.
    // sutImpl calculates the evicted keys by taking the difference of this new payload against the historical topology snapshot.
    // Thus, we have to ensure that the evicted key exists in the historical state.
    // We do this by fetching the pre-existing default key from the testing topology to act as the evicted key.
    val snapshot = syncCrypto.currentSnapshotApproximation.futureValueUS
    val defaultKeys = snapshot.ipsSnapshot
      .signingKeys(p1, SequencerAuthenticationOnly)
      .futureValueUS
    val evictedKey = defaultKeys.headOption.getOrElse(fail("Evicted key fetch failed"))

    // Generate a new key to act as the retained key that replaces the old mapping
    val retainedKey = syncCrypto.crypto
      .generateSigningKey(usage = SigningKeyUsage.All)
      .value
      .futureValueUS
      .getOrElse(fail("Retained key generation failed"))

    val retainedFingerprint = retainedKey.fingerprint
    val evictedFingerprint = evictedKey.fingerprint

    val retainedToken = StoredAuthenticationToken(
      p1,
      clock.now.plusSeconds(100),
      AuthenticationToken.generate(syncCrypto.pureCrypto),
      retainedFingerprint,
    )
    val evictedToken = StoredAuthenticationToken(
      p1,
      clock.now.plusSeconds(100),
      AuthenticationToken.generate(syncCrypto.pureCrypto),
      evictedFingerprint,
    )

    store.saveToken(retainedToken)
    store.saveToken(evictedToken)

    val retainedOtk = OwnerToKeyMapping.tryCreate(p1, com.daml.nonempty.NonEmpty(Seq, retainedKey))

    // Create an OTK for the retainedKey
    val replaceTopologyTx = TopologyTransaction(
      TopologyChangeOp.Replace,
      PositiveInt.one,
      retainedOtk,
      testedProtocolVersion,
    )

    // create a SignedTopologyTransaction wrapping the removal transaction
    val txElement = SignedTopologyTransaction.withSignature(
      transaction = replaceTopologyTx,
      signature = noSignature,
      isProposal = false,
      protocolVersion = testedProtocolVersion,
    )

    val sequencerTime = SequencedTime(clock.now)
    val effectiveTime = EffectiveTime(clock.now)

    val tokensBefore = store.fetchTokens(p1)

    // check that both tokens are active in the store prior to execution
    tokensBefore should contain.only(retainedToken, evictedToken)

    // Trigger the subscriber callback to simulate the observation of a real transaction
    for {
      _ <- sutImpl
        .observed(sequencerTime, effectiveTime, SequencerCounter(1), Seq(txElement))
        .failOnShutdown
    } yield {
      val tokensAfter = store.fetchTokens(p1)

      // Verify the evicted token is revoked and the retained token persists
      tokensAfter should contain.only(retainedToken)
    }
  }

  "fail token persistence if the signing key is revoked mid-flight (TOCTOU race condition check)" in {
    val store = new MemberAuthenticationStore(
      PositiveInt.tryCreate(10),
      PositiveInt.tryCreate(10),
      loggerFactory,
    )
    val dummySignature = noSignature
    // Prepare a valid nonce in the store to satisfy the challenge check
    val nonce = Nonce.generate(syncCrypto.pureCrypto)
    val storedNonce = StoredNonce(p1, nonce, clock.now, JDuration.ofMinutes(1))
    store.saveNonce(storedNonce)
    val expectedHash = MemberAuthentication
      .hashSynchronizerNonce(nonce, physicalSynchronizerId, syncCrypto.pureCrypto)

    // create mocks of the crypto API and snapshot objects
    val mockCryptoApi = mock[SynchronizerCryptoClient]
    val approxSnapshot = mock[SynchronizerSnapshotSyncCryptoApi]
    val headSnapshot = mock[SynchronizerSnapshotSyncCryptoApi]
    val headIpsSnapshot = mock[TopologySnapshot]

    org.mockito.Mockito.when(mockCryptoApi.pureCrypto).thenReturn(syncCrypto.pureCrypto)
    // Connect the snapshot to its inner IPS snapshot component
    org.mockito.Mockito.when(headSnapshot.ipsSnapshot).thenReturn(headIpsSnapshot)

    // Mock the different snapshot views
    org.mockito.Mockito
      .when(
        mockCryptoApi.currentSnapshotApproximation(org.mockito.ArgumentMatchers.any[TraceContext])
      )
      .thenReturn(FutureUnlessShutdown.pure(approxSnapshot))
    org.mockito.Mockito.when(mockCryptoApi.headSnapshot).thenReturn(headSnapshot)

    // Allow the signature validation to pass for this member, hash, and signature
    org.mockito.Mockito
      .when(
        approxSnapshot.verifySignature(
          org.mockito.ArgumentMatchers.eq(expectedHash),
          org.mockito.ArgumentMatchers.eq(p1: Member),
          org.mockito.ArgumentMatchers.eq(dummySignature),
          org.mockito.ArgumentMatchers
            .any[com.daml.nonempty.NonEmpty[Set[SigningKeyUsage]]](),
        )(org.mockito.ArgumentMatchers.any[TraceContext])
      )
      .thenReturn(
        EitherT.pure[FutureUnlessShutdown, SignatureCheckError](())
      )

    // Simulate the mid-flight key revocation via headSnapshot
    // Mockito will return an empty set of signing keys when queried for p1 in the head snapshot
    org.mockito.Mockito
      .when(
        headIpsSnapshot.signingKeys(
          org.mockito.ArgumentMatchers.eq(p1: Member),
          org.mockito.ArgumentMatchers
            .eq(SequencerAuthenticationOnly),
        )(org.mockito.ArgumentMatchers.any[TraceContext])
      )
      .thenReturn(
        FutureUnlessShutdown.pure(Seq.empty[SigningPublicKey])
      )

    // Instantiate the authService
    val authService = new MemberAuthenticationService(
      physicalSynchronizerId,
      mockCryptoApi,
      store,
      clock,
      JDuration.ofMinutes(1),
      JDuration.ofHours(1),
      useExponentialRandomTokenExpiration = false,
      _ => (),
      FutureUnlessShutdown.unit,
      DefaultProcessingTimeouts.testing,
      BatchingConfig(),
      loggerFactory,
    ) {
      // The real isParticipantActive method queries the topology snapshots to determine if the participant is allowed to connect.
      // Since we mocked headSnapshot to return an empty sequence of signing keys to simulate the revocation, the real isParticipantActive would cause
      // the method to fail early during the initial checking phase. To avoid this, we override it to return true.
      override def isParticipantActive(participant: ParticipantId)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Boolean] =
        FutureUnlessShutdown.pure(true)
    }

    // Execute validateSignature to test the behaviour. This will call headIpsSnapshot.signingKeys, which will return an empty set, which
    // tells the method that the member's key has been revoked and triggers the rollback behaviour.
    for {
      resultError <- leftOrFail(authService.validateSignature(p1, dummySignature, nonce))(
        "should catch mid-flight revocation"
      )
    } yield {
      // Assert that the method correctly returned an InvalidSignature error based on the headSnapshot
      resultError shouldBe a[MemberAuthentication.InvalidSignature]

      // Assert that the rollback successfully removed the newly saved token from the database
      store.fetchTokens(p1) shouldBe empty
    }
  }

  "fail token generation if the member is deactivated mid-flight (TOCTOU race condition check)" in {
    val store = new MemberAuthenticationStore(
      PositiveInt.tryCreate(10),
      PositiveInt.tryCreate(10),
      loggerFactory,
    )
    val dummySignature = noSignature

    // Prepare a valid nonce in the store to satisfy the challenge check
    val nonce = Nonce.generate(syncCrypto.pureCrypto)
    val storedNonce = StoredNonce(p1, nonce, clock.now, JDuration.ofMinutes(1))
    store.saveNonce(storedNonce)
    val expectedHash = MemberAuthentication
      .hashSynchronizerNonce(nonce, physicalSynchronizerId, syncCrypto.pureCrypto)

    // create mocks of the crypto API and snapshot objects
    val mockCryptoApi = mock[SynchronizerCryptoClient]
    val approxSnapshot = mock[SynchronizerSnapshotSyncCryptoApi]
    val headSnapshot = mock[SynchronizerSnapshotSyncCryptoApi]
    val headIpsSnapshot = mock[TopologySnapshot]

    org.mockito.Mockito.when(mockCryptoApi.pureCrypto).thenReturn(syncCrypto.pureCrypto)

    // Connect the snapshot to its inner IPS snapshot component
    org.mockito.Mockito.when(headSnapshot.ipsSnapshot).thenReturn(headIpsSnapshot)

    // Mock the different snapshot views
    org.mockito.Mockito
      .when(
        mockCryptoApi.currentSnapshotApproximation(org.mockito.ArgumentMatchers.any[TraceContext])
      )
      .thenReturn(FutureUnlessShutdown.pure(approxSnapshot))
    org.mockito.Mockito.when(mockCryptoApi.headSnapshot).thenReturn(headSnapshot)

    // Allow the signature validation to pass for this member, hash, and dummySignature
    org.mockito.Mockito
      .when(
        approxSnapshot.verifySignature(
          org.mockito.ArgumentMatchers.eq(expectedHash),
          org.mockito.ArgumentMatchers.eq(p1: Member),
          org.mockito.ArgumentMatchers.eq(dummySignature),
          org.mockito.ArgumentMatchers
            .any[com.daml.nonempty.NonEmpty[Set[SigningKeyUsage]]](),
        )(org.mockito.ArgumentMatchers.any[TraceContext])
      )
      .thenReturn(
        EitherT.pure[FutureUnlessShutdown, SignatureCheckError](())
      )

    // Force the mocked key validation check to pass seamlessly and return proxyActiveKeys when headIpsSnapshot.signingKeys is called
    val proxyActiveKeys =
      new scala.collection.immutable.Seq[SigningPublicKey] {
        override def length: Int = 1
        override def apply(idx: Int): SigningPublicKey =
          throw new UnsupportedOperationException()
        override def iterator: Iterator[SigningPublicKey] =
          Iterator.empty
        override def exists(
            p: SigningPublicKey => Boolean
        ): Boolean = true
      }

    org.mockito.Mockito
      .when(
        headIpsSnapshot.signingKeys(
          org.mockito.ArgumentMatchers.eq(p1: Member),
          org.mockito.ArgumentMatchers
            .eq(SequencerAuthenticationOnly),
        )(org.mockito.ArgumentMatchers.any[TraceContext])
      )
      .thenReturn(FutureUnlessShutdown.pure(proxyActiveKeys))

    // A counter to dynamically track the execution stages
    var activeCheckCount = 0

    // Instantiate the authService with a stateful countdown trap
    val authService = new MemberAuthenticationService(
      physicalSynchronizerId,
      mockCryptoApi,
      store,
      clock,
      JDuration.ofMinutes(1),
      JDuration.ofHours(1),
      useExponentialRandomTokenExpiration = false,
      _ => (),
      FutureUnlessShutdown.unit,
      DefaultProcessingTimeouts.testing,
      BatchingConfig(),
      loggerFactory,
    ) {
      override def isParticipantActive(participant: ParticipantId)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Boolean] = {
        activeCheckCount += 1
        if (activeCheckCount == 1) {
          // pass the check on the first call (initial validation phase)
          FutureUnlessShutdown.pure(true)
        } else {
          // fail the check on subsequent calls to simulate member deactivation
          FutureUnlessShutdown.pure(false)
        }
      }
    }

    // Execute validateSignature to test the behaviour
    for {
      resultError <- leftOrFail(authService.validateSignature(p1, dummySignature, nonce))(
        "should catch mid-flight deactivation"
      )
    } yield {
      // Assert that the method correctly returned a MemberAccessDisabled error
      resultError shouldBe a[MemberAuthentication.MemberAccessDisabled]

      // Assert that the rollback successfully removed the newly saved token from the database
      store.fetchTokens(p1) shouldBe empty
    }
  }
}
