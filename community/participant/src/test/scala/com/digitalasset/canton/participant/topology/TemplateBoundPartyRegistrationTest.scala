// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.config.{BatchAggregatorConfig, NonNegativeFiniteDuration, TopologyConfig}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot
import org.scalatest.wordspec.AsyncWordSpec

class TemplateBoundPartyRegistrationTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  private lazy val topologyConfig = TopologyConfig(
    topologyTransactionObservationTimeout = NonNegativeFiniteDuration.ofSeconds(2),
    broadcastRetryDelay = NonNegativeFiniteDuration.ofSeconds(1),
  )
  private lazy val clock = new WallClock(timeouts, loggerFactory)
  private lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  /** Create a test environment: a signing key, a participant whose namespace
    * matches the key (so both party and participant are in the same namespace),
    * and a topology manager bootstrapped with a root cert.
    */
  private def mkEnv() = {
    val signingKey = crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.NamespaceOnly)
    val namespace = Namespace(signingKey.id)
    val localParticipant = ParticipantId(
      UniqueIdentifier.tryCreate("participant", namespace)
    )

    val store = new InMemoryTopologyStore[TopologyStoreId.AuthorizedStore.type](
      TopologyStoreId.AuthorizedStore,
      testedProtocolVersion,
      loggerFactory.append("store", "test"),
      timeouts,
    )
    val manager = new AuthorizedTopologyManager(
      localParticipant.uid,
      clock,
      crypto,
      BatchAggregatorConfig.defaultsForTesting,
      topologyConfig,
      store,
      exitOnFatalFailures = true,
      timeouts,
      futureSupervisor,
      loggerFactory.append("manager", "test"),
    )

    (signingKey, namespace, localParticipant, store, manager)
  }

  private def mkSnapshot(store: InMemoryTopologyStore[TopologyStoreId.AuthorizedStore.type]) =
    new StoreBasedTopologySnapshot(
      DefaultTestIdentities.physicalSynchronizerId,
      CantonTimestamp.MaxValue,
      store,
      com.digitalasset.canton.topology.store.NoPackageDependencies,
      loggerFactory,
    )

  private def addRootCert(
      manager: AuthorizedTopologyManager,
      key: com.digitalasset.canton.crypto.SigningPublicKey,
  ) =
    manager
      .proposeAndAuthorize(
        TopologyChangeOp.Replace,
        NamespaceDelegation.tryCreate(Namespace(key.id), key, CanSignAllMappings),
        Some(PositiveInt.one),
        Seq(key.fingerprint),
        testedProtocolVersion,
        expectFullAuthorization = false,
        waitToBecomeEffective = None,
      )
      .valueOrFail("root cert")

  "TemplateBoundPartyRegistration" should {

    "allocate + finalize creates mapping and destroys key" in {
      val (signingKey, namespace, localParticipant, store, manager) = mkEnv()
      val partyId = PartyId(UniqueIdentifier.tryCreate("tbp-pool", namespace))

      val registration = new TemplateBoundPartyRegistration(
        localParticipantId = localParticipant,
        topologyManager = manager,
        privateStore = crypto.cryptoPrivateStore,
        hashOps = crypto.pureCrypto,
        protocolVersion = testedProtocolVersion,
        permissionlessTbpHosting = true,
        loggerFactory = loggerFactory,
      )

      for {
        _ <- addRootCert(manager, signingKey)
        _ <- registration.allocate(partyId, signingKey.fingerprint).valueOrFail("allocate")
        result <- registration
          .finalize(
            partyId = partyId,
            hostingParticipantIds = Seq(localParticipant),
            allowedTemplateIds = Set("com.example:AMMPool:1.0", "com.example:Token:1.0"),
            signingKeyFingerprint = signingKey.fingerprint,
          )
          .valueOrFail("finalize")
      } yield {
        result.partyId shouldBe partyId
        result.hostingParticipantIds shouldBe Seq(localParticipant)
        result.allowedTemplateIds shouldBe Set("com.example:AMMPool:1.0", "com.example:Token:1.0")
      }
    }

    "allocate creates PartyToParticipant when permissionless" in {
      val (signingKey, namespace, localParticipant, _, manager) = mkEnv()
      val partyId = PartyId(UniqueIdentifier.tryCreate("tbp-alloc", namespace))

      val registration = new TemplateBoundPartyRegistration(
        localParticipantId = localParticipant,
        topologyManager = manager,
        privateStore = crypto.cryptoPrivateStore,
        hashOps = crypto.pureCrypto,
        protocolVersion = testedProtocolVersion,
        permissionlessTbpHosting = true,
        loggerFactory = loggerFactory,
      )

      for {
        _ <- addRootCert(manager, signingKey)
        pid <- registration.allocate(partyId, signingKey.fingerprint).valueOrFail("allocate")
      } yield {
        pid shouldBe localParticipant
      }
    }

    "allocate rejects when permissionless is disabled" in {
      val (signingKey, namespace, localParticipant, _, manager) = mkEnv()
      val partyId = PartyId(UniqueIdentifier.tryCreate("tbp-reject", namespace))

      val registration = new TemplateBoundPartyRegistration(
        localParticipantId = localParticipant,
        topologyManager = manager,
        privateStore = crypto.cryptoPrivateStore,
        hashOps = crypto.pureCrypto,
        protocolVersion = testedProtocolVersion,
        permissionlessTbpHosting = false,
        loggerFactory = loggerFactory,
      )

      for {
        result <- registration.allocate(partyId, signingKey.fingerprint).value
      } yield {
        result.isLeft shouldBe true
        result.left.getOrElse("") should include("Permissionless")
      }
    }

    "templateBoundPartyConfig returns mapping from topology store" in {
      val (signingKey, namespace, localParticipant, store, manager) = mkEnv()
      val partyId = PartyId(UniqueIdentifier.tryCreate("tbp-lookup", namespace))

      val registration = new TemplateBoundPartyRegistration(
        localParticipantId = localParticipant,
        topologyManager = manager,
        privateStore = crypto.cryptoPrivateStore,
        hashOps = crypto.pureCrypto,
        protocolVersion = testedProtocolVersion,
        permissionlessTbpHosting = true,
        loggerFactory = loggerFactory,
      )

      for {
        _ <- addRootCert(manager, signingKey)
        _ <- registration.allocate(partyId, signingKey.fingerprint).valueOrFail("allocate")
        _ <- registration
          .finalize(
            partyId = partyId,
            hostingParticipantIds = Seq(localParticipant),
            allowedTemplateIds = Set("com.example:Pool:1.0"),
            signingKeyFingerprint = signingKey.fingerprint,
          )
          .valueOrFail("finalize")
        snapshot = mkSnapshot(store)
        config <- snapshot.templateBoundPartyConfig(partyId.toLf)
      } yield {
        config match {
          case Some(mapping) =>
            mapping.partyId shouldBe partyId
            mapping.allowedTemplateIds shouldBe Set("com.example:Pool:1.0")
            mapping.hostingParticipantIds shouldBe Seq(localParticipant)
          case None =>
            fail("Expected Some(TemplateBoundPartyMapping) but got None")
        }
      }
    }

    "templateBoundPartyConfig returns None for regular parties" in {
      val (_, namespace, _, store, _) = mkEnv()
      val snapshot = mkSnapshot(store)
      val partyId = PartyId(UniqueIdentifier.tryCreate("regular-party", namespace))

      for {
        config <- snapshot.templateBoundPartyConfig(partyId.toLf)
      } yield {
        config shouldBe None
      }
    }
  }
}
