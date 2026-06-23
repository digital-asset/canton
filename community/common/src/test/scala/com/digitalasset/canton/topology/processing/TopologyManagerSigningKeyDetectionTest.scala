// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
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
          additions = Seq(ns1k1_k1, ns1k2_k1, ns1k3_k2).map(ValidatedTopologyTransaction(_)),
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

      // now let's break the chain by removing the NSD for key2.
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

  }
}
