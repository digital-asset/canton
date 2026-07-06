// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class TopologyManagerSigningKeyDetectionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  "TopologyManagerSigningKeyDetection" should {

    object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)
    import Factory.*

    def ts(seconds: Long) = CantonTimestamp.Epoch.plusSeconds(seconds)

    def mk() =
      new TopologyManagerSigningKeyDetection(
        new InMemoryTopologyStore(
          SynchronizerStore(Factory.physicalSynchronizerId1),
          predecessor = None,
          testedProtocolVersion,
          loggerFactory,
          timeouts,
        ),
        Factory.syncCryptoClient.crypto.pureCrypto,
        Factory.syncCryptoClient.crypto.cryptoPrivateStore,
        loggerFactory,
      )

    val dtc_uid1a = TopologyTransaction(
      Replace,
      PositiveInt.one,
      SynchronizerTrustCertificate(ParticipantId(uid1a), synchronizerId1),
      testedProtocolVersion,
    )

    "prefer keys furthest from the root certificate" in {
      val detector = mk()

      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions =
            Seq(ns1k1_k1, ns1k2_k1, ns1k3_k2_restrict_nsd).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      detector
        .getValidSigningKeysForTransaction(
          ts(1),
          dtc_uid1a,
          None,
          returnAllValidKeys = false,
          namespacesToSignFor = Seq.empty,
        )
        .map(_._2)
        .futureValueUS shouldBe Right(Seq(SigningKeys.key3.fingerprint))

      // test getting all valid keys
      detector
        .getValidSigningKeysForTransaction(
          ts(1),
          dtc_uid1a,
          None,
          returnAllValidKeys = true,
          namespacesToSignFor = Seq.empty,
        )
        .futureValueUS
        .value
        ._2 should contain theSameElementsAs Seq(
        SigningKeys.key1,
        SigningKeys.key2,
        SigningKeys.key3,
      ).map(_.fingerprint)

      // break the chain by removing the NSD for key2.
      // this prevents key3 from being authorized for new signatures.
      detector.store
        .update(
          SequencedTime(ts(1)),
          EffectiveTime(ts(1)),
          removals = Map(ns1k2_k1.mapping.uniqueKey -> (None, Set(ns1k2_k1.hash))),
          additions = Seq.empty,
        )
        .futureValueUS

      // reset caches so that the namespace delegations are fetched again from the store.
      // normally we would use a separate detector instance per topology manager srequest
      detector.reset()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        detector
          .getValidSigningKeysForTransaction(
            ts(2),
            dtc_uid1a,
            None,
            returnAllValidKeys = false,
            namespacesToSignFor = Seq.empty,
          )
          .map(_._2)
          .futureValueUS shouldBe Right(
          Seq(SigningKeys.key1.fingerprint)
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include(
                s"The following target keys of namespace $ns1 are dangling: ${List(SigningKeys.key3.fingerprint)}"
              ),
              "dangling NSD for key3",
            )
          )
        ),
      )
    }

    "respsect the requested authorization scope" in {
      val detector = mk()

      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions = Seq(ns1k1_k1, ns2k2_k2, ns3k3_k3).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      val ptp = PartyToParticipant.tryCreate(
        PartyId.tryCreate("alice", ns1),
        PositiveInt.one,
        Seq(
          HostingParticipant(ParticipantId(UniqueIdentifier.tryCreate("p2", ns2)), Submission),
          HostingParticipant(ParticipantId(UniqueIdentifier.tryCreate("p3", ns3)), Submission),
        ),
      )

      detector
        .getValidSigningKeysForTransaction(
          ts(1),
          TopologyTransaction(
            TopologyChangeOp.Replace,
            PositiveInt.one,
            ptp,
            testedProtocolVersion,
          ),
          None,
          namespacesToSignFor = Seq(ns2),
          returnAllValidKeys = false,
        )
        .map(_._2)
        .futureValueUS shouldBe Right(Seq(SigningKeys.key2.fingerprint))

      // test getting all valid keys
      detector
        .getValidSigningKeysForTransaction(
          ts(1),
          TopologyTransaction(
            TopologyChangeOp.Replace,
            PositiveInt.one,
            ptp,
            testedProtocolVersion,
          ),
          None,
          namespacesToSignFor = Seq.empty,
          returnAllValidKeys = true,
        )
        .futureValueUS
        .value
        ._2 should contain theSameElementsAs Seq(
        SigningKeys.key1,
        SigningKeys.key2,
        SigningKeys.key3,
      ).map(_.fingerprint)
    }

    "resolves decentralized namespace definitions for finding appropriate signing keys" in {
      val detector = mk()

      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions =
            Seq(ns1k1_k1, ns8k8_k8, ns9k9_k9, ns1k2_k1, dns1).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      val otk = TopologyTransaction(
        Replace,
        PositiveInt.one,
        OwnerToKeyMapping.tryCreate(
          ParticipantId("decentralized-participant", dns1.mapping.namespace),
          NonEmpty(Seq, EncryptionKeys.key1, SigningKeys.key4),
        ),
        testedProtocolVersion,
      )

      syncCryptoClient.crypto.cryptoPrivateStore
        .removePrivateKey(SigningKeys.key8.fingerprint)
        .futureValueUS

      detector
        .getValidSigningKeysForTransaction(
          ts(1),
          otk,
          None,
          returnAllValidKeys = false,
          namespacesToSignFor = Seq.empty,
        )
        .futureValueUS
        .value
        ._2 should contain theSameElementsAs Seq(
        SigningKeys.key2, // the furthest key available for NS1
        SigningKeys.key9, // the root certificate key for NS9
        SigningKeys.key4, // all new signing keys must also sign
        // since we removed key8 from the private key store, it cannot be used to sign something, so it is not suggested
      ).map(_.fingerprint)

      detector
        .getValidSigningKeysForTransaction(
          ts(1),
          otk,
          None,
          returnAllValidKeys = true,
          namespacesToSignFor = Seq.empty,
        )
        .futureValueUS
        .value
        ._2 should contain theSameElementsAs Seq(
        SigningKeys.key1, // the root certificate key for NS1
        SigningKeys.key2, // the key authorized for NS1 by an additional NSD
        SigningKeys.key9, // the root certificate key for NS9
        SigningKeys.key4, // all new signing keys must also sign
        // since we removed key8 from the private key store, it cannot be used to sign something, so it is not suggested
      ).map(_.fingerprint)

    }

    "does not allow self-restrict namespace delegation on an intermediate key" in {
      val detector = mk()

      // Set up the trust chain: k1 is root, k1 authorizes k2
      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions = Seq(ns1k1_k1, ns1k2_k1).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      detector.reset()

      // restricting NSD transaction with k2 is possible with key1 only!!!
      val result = detector
        .getValidSigningKeysForTransaction(
          ts(1),
          ns1k2_k2_restrict_nsd.transaction, // this is in effect querying who can sign key2 on ns1
          inStore = None,
          namespacesToSignFor = Seq.empty,
          returnAllValidKeys = false,
        )
        .futureValueUS

      result.value._2 shouldBe Seq(
        SigningKeys.key1.fingerprint
      )
    }

    "does not allow child keys to sign a parent NSD" in {
      val detector = mk()

      // Set up the trust chain: k1 is root, k1 authorizes k2
      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions = Seq(ns1k1_k1, ns1k2_k1, ns1k3_k2).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      detector.reset()

      // restricting NSD transaction with k2 is possible with key1 only!!!
      val result = detector
        .getValidSigningKeysForTransaction(
          ts(1),
          // in a transaction we don't take into consideration the signing key k3
          // so essentially we ask who can sign k2
          ns1k2_k3.transaction,
          inStore = None,
          namespacesToSignFor = Seq.empty,
          returnAllValidKeys = false,
        )
        .futureValueUS

      result.value._2 shouldBe Seq(
        SigningKeys.key1.fingerprint
      )
    }

    "choose the highest signing key (even from another branch) in the namespace graph" in {
      val detector = mk()

      // Set up the trust chain: k1 is root, k1 authorizes k2, k2 authorized k3
      // k1 -> k2 -> k3
      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions = Seq(ns1k1_k1, ns1k2_k1, ns1k3_k2).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      detector.reset()

      // k1 -> k4 -> k5 -> k6 -> k7
      detector.store
        .update(
          SequencedTime(ts(1)),
          EffectiveTime(ts(1)),
          removals = Map.empty,
          additions =
            Seq(ns1k4_k1, ns1k5_k4, ns1k6_k5, ns1k7_k6).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      // Now we have: k1 -> k2 -> k3
      //              |
      //              k4 -> k5 -> k6 -> k7
      detector.reset()

      val result = detector
        .getValidSigningKeysForTransaction(
          ts(2),
          // in a transaction we don't take into consideration the signing key k2
          // so essentially we ask who can sign k3
          ns1k3_k2.transaction,
          inStore = None,
          namespacesToSignFor = Seq.empty,
          returnAllValidKeys = false,
        )
        .futureValueUS

      result.value._2 shouldBe Seq(
        SigningKeys.key7.fingerprint // k7 which is the "max" on the ns1 path
      )

      val resultAllValidKeys = detector
        .getValidSigningKeysForTransaction(
          ts(2),
          // in a transaction we don't take into consideration the signing key k2
          // so essentially we ask who can sign k3
          ns1k3_k2.transaction,
          inStore = None,
          namespacesToSignFor = Seq.empty,
          returnAllValidKeys = true, // we want to see all the keys
        )
        .futureValueUS

      resultAllValidKeys.value._2 should contain theSameElementsAs Seq(
        SigningKeys.key2.fingerprint,
        SigningKeys.key1.fingerprint,
        SigningKeys.key7.fingerprint,
        SigningKeys.key6.fingerprint,
        SigningKeys.key5.fingerprint,
        SigningKeys.key4.fingerprint,
      )
    }

    "having a bad state in the store selects the proper signing key" in {
      val detector = mk()

      // Set up the trust chain: k1 is root, k1 authorizes k2, and then k2 restricts itself
      detector.store
        .update(
          SequencedTime(ts(0)),
          EffectiveTime(ts(0)),
          removals = Map.empty,
          additions =
            Seq(ns1k1_k1, ns1k2_k1, ns1k2_k2_restrict_nsd).map(ValidatedTopologyTransaction(_)),
        )
        .futureValueUS

      detector.reset()

      // k1 can still delegate to k3
      val result = loggerFactory.assertLogs(
        detector
          .getValidSigningKeysForTransaction(
            ts(1),
            ns1k3_k2.transaction, // the transaction doesn't take into consideration the k2
            inStore = None,
            namespacesToSignFor = Seq.empty,
            returnAllValidKeys = false,
          )
          .futureValueUS,
        _.warningMessage should (include regex s"dangling.*${SigningKeys.key2.fingerprint}"),
      )

      result.value._2 shouldBe Seq(
        SigningKeys.key1.fingerprint
      )
    }

  }
}
