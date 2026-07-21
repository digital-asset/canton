// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import org.scalatest.wordspec.AsyncWordSpec

class MemberAuthenticationStoreTest extends AsyncWordSpec with BaseTest {
  lazy val participant1 = DefaultTestIdentities.participant1
  lazy val participant2 = DefaultTestIdentities.participant2
  lazy val participant3 = DefaultTestIdentities.participant3
  lazy val defaultExpiry = CantonTimestamp.Epoch.plusSeconds(120)
  lazy val crypto = new SymbolicPureCrypto

  "invalidate member" should {
    "work fine if the member has no active token" in {
      val store = mk()

      store.invalidateMember(participant1)
      val tokens = store.fetchTokens(participant1)

      tokens shouldBe empty
    }

    "remove token and nonce for participant" in {
      val store = mk()

      val token = generateToken(participant1)
      store.saveToken(token)
      val _ = store.fetchOrSaveNonce(generateNonce(participant1))

      store.invalidateMember(participant1)
      val tokens = store.fetchTokens(participant1)
      val nonceO = store.fetchNonce(participant1)
      val tokenLookupO =
        store.tokenForMemberAt(participant1, token.token, defaultExpiry.minusSeconds(1))

      tokens shouldBe empty
      nonceO shouldBe empty
      tokenLookupO shouldBe empty
    }
  }

  "fetchOrGenerateNonce" should {
    "generate and store a new nonce if none exists" in {
      val store = mk()
      val fresh = generateNonce(participant1)

      val result = store.fetchOrGenerateNonce(participant1, fresh)

      result shouldBe fresh
      store.fetchNonce(participant1) should contain(fresh)
    }

    "retrieve the existing active nonce if one already exists (sticky behavior)" in {
      val store = mk()
      val original = generateNonce(participant1)
      val second = generateNonce(participant1)

      val _ = store.fetchOrSaveNonce(original)

      val result = store.fetchOrGenerateNonce(participant1, second)

      result shouldBe original
      store.fetchNonce(participant1) should contain(original)
    }
  }

  "fetchOrSaveNonce" should {
    "store and return a new nonce if none exists" in {
      val store = mk()
      val nonce = generateNonce(participant1)

      val result = store.fetchOrSaveNonce(nonce)

      result shouldBe nonce
      store.fetchNonce(participant1) should contain(nonce)
    }

    "retrieve the existing active nonce if one already exists (sticky behavior)" in {
      val store = mk()
      val original = generateNonce(participant1)
      val second = generateNonce(participant1)

      val _ = store.fetchOrSaveNonce(original)

      val result = store.fetchOrSaveNonce(second)

      result shouldBe original
      store.fetchNonce(participant1) should contain(original)
    }
  }

  "fetchNonceIfMatches" should {
    "return the stored nonce if the provided nonce matches" in {
      val store = mk()
      val nonce = generateNonce(participant1)
      val _ = store.fetchOrSaveNonce(nonce)

      val result = store.fetchNonceIfMatches(participant1, nonce.nonce)

      result.value shouldBe nonce
    }

    "return None if the provided nonce does not match" in {
      val store = mk()
      val stored = generateNonce(participant1)
      val wrong = generateNonce(participant1)
      val _ = store.fetchOrSaveNonce(stored)

      val result = store.fetchNonceIfMatches(participant1, wrong.nonce)
      result shouldBe empty
    }

    "return None if no nonce is stored for the member" in {
      val store = mk()
      val queryNonce = generateNonce(participant1)

      val result = store.fetchNonceIfMatches(participant1, queryNonce.nonce)

      result shouldBe empty
    }
  }

  "removeNonceIfMatches" should {
    "remove the nonce and return true if the stored nonce matches" in {
      val store = mk()
      val nonce = generateNonce(participant1)
      val _ = store.fetchOrSaveNonce(nonce)

      val removed = store.removeNonceIfMatches(participant1, nonce.nonce)

      removed shouldBe true
      store.fetchNonce(participant1) shouldBe empty
    }

    "return false and leave the store unchanged if the expected nonce does not match" in {
      val store = mk()
      val stored = generateNonce(participant1)
      val wrong = generateNonce(participant1)
      val _ = store.fetchOrSaveNonce(stored)

      val removed = store.removeNonceIfMatches(participant1, wrong.nonce)

      removed shouldBe false
      store.fetchNonce(participant1) should contain(stored)
    }

    "return false if no nonce is stored for the member" in {
      val store = mk()
      val queryNonce = generateNonce(participant1)

      val removed = store.removeNonceIfMatches(participant1, queryNonce.nonce)

      removed shouldBe false
    }
  }

  "saving tokens" should {
    "support many for a member" in {
      val store = mk()

      val p1t1 = generateToken(participant1)
      val p1t2 = generateToken(participant1)
      val p2t1 = generateToken(participant2)

      List(p1t1, p1t2, p2t1).foreach(store.saveToken)

      val p1Tokens = store.fetchTokens(participant1)
      val p2Tokens = store.fetchTokens(participant2)
      val p3Tokens = store.fetchTokens(participant3)

      p1Tokens should contain.only(p1t1, p1t2)
      p2Tokens should contain.only(p2t1)
      p3Tokens shouldBe empty
    }

    "drop excess token when exceeds the maximumTokensPerMember" in {
      val store = mk(PositiveInt.one)
      val p1t1 = generateToken(participant1)
      val p1t2 = generateToken(participant1)
      List(p1t1, p1t2).foreach(store.saveToken)
      val p1Tokens = store.fetchTokens(participant1)

      // verify that only the last saved token is in the store
      p1Tokens shouldBe Seq(p1t2)

      store.tokenForMemberAt(participant1, p1t1.token, defaultExpiry.minusSeconds(1)) shouldBe empty
      store.tokenForMemberAt(
        participant1,
        p1t2.token,
        defaultExpiry.minusSeconds(1),
      ) should contain(p1t2)
    }

  }

  "tokenForMemberAt" should {
    "return the token if it belongs to the member and is not expired" in {
      val store = mk()
      val token = generateToken(participant1)
      store.saveToken(token)

      store.tokenForMemberAt(
        participant1,
        token.token,
        defaultExpiry.minusSeconds(1),
      ) should contain(token)
    }

    "return None if the query time is equal to the token's expiration time" in {
      val store = mk()
      val token = generateToken(participant1)
      store.saveToken(token)

      store.tokenForMemberAt(participant1, token.token, defaultExpiry) shouldBe empty
    }

    "return None if queried with the wrong member" in {
      val store = mk()
      val token = generateToken(participant1)
      store.saveToken(token)

      // Query with participant2 instead of participant1
      store.tokenForMemberAt(
        participant2,
        token.token,
        defaultExpiry.minusSeconds(1),
      ) shouldBe empty
    }
  }

  "invalidate tokens by fingerprint" should {
    "remove only the tokens matching the fingerprint across all maps" in {
      val store = mk()

      val fpToInvalidate = com.digitalasset.canton.crypto.Fingerprint.tryFromString("target-key")
      val fpSafe = com.digitalasset.canton.crypto.Fingerprint.tryFromString("safe-key")

      val p1TokenToInvalidate =
        generateToken(participant1).copy(signingKeyFingerprint = fpToInvalidate)
      val p1Safe = generateToken(participant1).copy(signingKeyFingerprint = fpSafe)
      val p2Target = generateToken(participant2).copy(signingKeyFingerprint = fpToInvalidate)

      List(p1TokenToInvalidate, p1Safe, p2Target).foreach(store.saveToken)

      store.invalidateTokensByFingerprint(fpToInvalidate)

      val p1Tokens = store.fetchTokens(participant1)
      val p2Tokens = store.fetchTokens(participant2)

      p1Tokens should contain.only(p1Safe)
      p2Tokens shouldBe empty

      store.tokenForMemberAt(
        participant1,
        p1TokenToInvalidate.token,
        defaultExpiry.minusSeconds(1),
      ) shouldBe empty
      store.tokenForMemberAt(
        participant2,
        p2Target.token,
        defaultExpiry.minusSeconds(1),
      ) shouldBe empty
      store.tokenForMemberAt(
        participant1,
        p1Safe.token,
        defaultExpiry.minusSeconds(1),
      ) should contain(p1Safe)
    }
  }

  "expire" should {
    "expire all nonces and tokens at or before the given timestamp" in {
      val store = mk()

      val n1 = generateNonce(participant1, defaultExpiry)
      val n2 = generateNonce(participant2, defaultExpiry.plusSeconds(1))
      val n3 = generateNonce(participant3, defaultExpiry.plusSeconds(-1))

      // generate 2 tokens for participant1
      val p1t1 = generateToken(participant1, defaultExpiry.plusSeconds(1))
      val p1t2 = generateToken(participant1, defaultExpiry.plusSeconds(-1))

      List(n1, n2, n3).foreach(storedNonce => store.fetchOrSaveNonce(storedNonce))
      List(p1t1, p1t2).foreach(store.saveToken)

      store.expireNoncesAndTokens(defaultExpiry)
      val fn1O = store.fetchNonceIfMatches(participant1, n1.nonce)
      val fn2O = store.fetchNonceIfMatches(participant2, n2.nonce)
      val fn3O = store.fetchNonceIfMatches(participant3, n3.nonce)
      val tokensP1 = store.fetchTokens(participant1)

      fn1O shouldBe empty // expired at threshold
      fn2O.value shouldBe n2 // expires in the future
      fn3O shouldBe empty // expired before threshold
      tokensP1 should contain.only(p1t1)
    }
  }

  private def mk(maxItems: PositiveInt = PositiveInt.tryCreate(10)): MemberAuthenticationStore =
    new MemberAuthenticationStore(
      maxItems,
      loggerFactory,
    )

  private def generateToken(
      member: Member,
      expiry: CantonTimestamp = defaultExpiry,
  ): StoredAuthenticationToken =
    StoredAuthenticationToken(
      member,
      expiry,
      AuthenticationToken.generate(crypto),
      member.fingerprint,
    )

  private def generateNonce(member: Member, expiry: CantonTimestamp = defaultExpiry): StoredNonce =
    StoredNonce(member, Nonce.generate(crypto), CantonTimestamp.Epoch, expiry)
}
