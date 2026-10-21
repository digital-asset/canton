// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.{
  S1M1,
  buildBaseEnvironmentDefinition,
}
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{Namespace, StoreBasedSynchronizerOutbox, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.time.Duration
import scala.collection.concurrent.TrieMap

import LsuTopologyVersionChangeOnboardingIntegrationTest.OfflineRoot

/** Onboarding of participants whose root namespace key is offline across an LSU that changes the
  * topology transaction proto version (v30 -> v31).
  *
  * Test scenario:
  *   - participant2, participant3, participant4 and participant5 are initialized with an offline
  *     root namespace key (held by participant1, which acts as the offline key store) and an
  *     intermediate namespace key on the node. Their identity transactions are serialized with the
  *     old protocol version.
  *   - participant2 onboards on the old synchronizer and is upgraded with it (LSU to dev).
  *   - After the upgrade, participant3 connects to the successor by itself, which fails with
  *     TOPOLOGY_CONVERSION_ERROR: re-signing its namespace certificates for the successor's
  *     protocol version requires the offline root key.
  *   - participant4 onboards on the successor with onboarding transactions whose namespace
  *     certificates the operator re-signed with the offline root key for the successor's protocol
  *     version.
  *   - participant5 rotates its intermediate namespace key with the offline root key, and then
  *     registers the successor like participant4. Its outbox also pushes the superseded delegations
  *     of the rotation, which cannot be re-signed without the root key, so it is blocked.
  *
  * Test goal: a participant with an offline root namespace key onboards on a synchronizer whose
  * protocol version uses another topology transaction proto version, provided the operator supplies
  * its namespace certificates re-signed for that version and its authorized store holds no
  * superseded delegations signed with the root key.
  *
  * Test setup: 5 participants, old synchronizer (sequencer1, mediator1) at the tested protocol
  * version, successor (sequencer2, mediator2) at dev.
  */
final class LsuTopologyVersionChangeOnboardingIntegrationTest extends LsuBase {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  override protected def testName: String = "lsu-topology-version-change-onboarding"

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private val offlineRootParticipants =
    Set("participant2", "participant3", "participant4", "participant5")

  private lazy val topologyVersionChanges: Boolean =
    testedProtocolVersion < ProtocolVersion.offPRWithSequencerChannels

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(numParticipants = 5, numSequencers = 2, numMediators = 2)
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs { case (name, config) =>
          if (offlineRootParticipants.contains(name))
            config.focus(_.init.identity).replace(IdentityConfig.Manual)
          else config
        }
      )
      .withSetup { implicit env =>
        defaultEnvironmentSetup(participantsOverride = Some(Seq(env.participant1)))
      }

  private val offlineRoots = TrieMap.empty[String, OfflineRoot]

  /** The root and intermediate namespace delegations, signed with the offline root key for the
    * given protocol version (what the operator does with the offline root key tooling).
    */
  // TODO(#36459): Use the offline-root-key scripts once they support topology proto v31
  private def namespaceCertificates(
      participant: LocalParticipantReference,
      protocolVersion: ProtocolVersion,
  )(implicit env: TestConsoleEnvironment): Seq[GenericSignedTopologyTransaction] = {
    import env.*
    val root = offlineRoots(participant.name)
    val rootKey = root.rootKey
    val intermediateKey = root.intermediateKey
    val offlineStore = participant1.topology.stores.create_temporary_topology_store(
      s"offline-root-${participant.name}-${protocolVersion.toProtoPrimitive}",
      protocolVersion,
    )
    Seq(
      participant1.topology.namespace_delegations
        .propose_delegation(
          Namespace(rootKey.id),
          rootKey,
          CanSignAllMappings,
          store = offlineStore,
        ),
      participant1.topology.namespace_delegations.propose_delegation(
        Namespace(rootKey.id),
        intermediateKey,
        CanSignAllButNamespaceDelegations,
        store = offlineStore,
      ),
    )
  }

  private def registerWithResignedCertificates(participant: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val newPV = ProtocolVersion.dev
    val store = participant.topology.stores.create_temporary_topology_store("onboarding", newPV)
    participant.topology.transactions.load(namespaceCertificates(participant, newPV), store)
    val identity = participant.topology.transactions
      .generate_onboarding_transactions(newPV, temporaryStore = Some(store))
    val trustCertificate = participant.topology.synchronizer_trust_certificates
      .propose(participant.id, daId.logical, store = Some(store))

    participant.synchronizers.register(
      sequencer2,
      alias = daName,
      performHandshake = false,
      onboardingTransactions = identity :+ trustCertificate,
    )
  }

  "Participants with an offline root namespace key" should {
    "be initialized with certificates signed for the old protocol version" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        Seq(participant2, participant3, participant4, participant5).foreach { participant =>
          participant.health.wait_for_ready_for_id()
          val intermediateKey = participant.keys.secret
            .generate_signing_key(
              s"${participant.name}-intermediate",
              SigningKeyUsage.NamespaceOnly,
            )
          // participant1 plays the offline key store
          val rootKey = participant1.keys.secret
            .generate_signing_key(
              s"${participant.name}-offline-root",
              SigningKeyUsage.NamespaceOnly,
            )
          offlineRoots.update(participant.name, OfflineRoot(rootKey, intermediateKey))

          participant.topology.init_id_from_uid(
            UniqueIdentifier.tryCreate(participant.name, Namespace(rootKey.id)),
            delegations = namespaceCertificates(participant, testedProtocolVersion),
          )
          eventually()(participant.is_initialized shouldBe true)
          participant.keys.secret.list(filterFingerprint = rootKey.id.unwrap) shouldBe empty
        }
    }

    "rotate the intermediate namespace key with the offline root key" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        val root = offlineRoots(participant5.name)
        val namespace = Namespace(root.rootKey.id)
        val newIntermediateKey = participant5.keys.secret
          .generate_signing_key(
            s"${participant5.name}-intermediate-2",
            SigningKeyUsage.NamespaceOnly,
          )
        // participant1 plays the offline key store
        val offlineStore = participant1.topology.stores.create_temporary_topology_store(
          s"offline-root-rotation-${participant5.name}",
          testedProtocolVersion,
        )
        participant1.topology.namespace_delegations
          .propose_delegation(namespace, root.rootKey, CanSignAllMappings, store = offlineStore)
        participant1.topology.namespace_delegations.propose_delegation(
          namespace,
          root.intermediateKey,
          CanSignAllButNamespaceDelegations,
          store = offlineStore,
        )
        val rotation = Seq(
          participant1.topology.namespace_delegations.propose_delegation(
            namespace,
            newIntermediateKey,
            CanSignAllButNamespaceDelegations,
            store = offlineStore,
          ),
          participant1.topology.namespace_delegations
            .propose_revocation(namespace, root.intermediateKey, store = offlineStore),
        )
        participant5.topology.transactions.load(rotation, TopologyStoreId.Authorized)
        offlineRoots.update(participant5.name, root.copy(intermediateKey = newIntermediateKey))
    }

    "onboard on a synchronizer with the same protocol version" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        participant2.synchronizers.connect_by_config(defaultSynchronizerConnectionConfig())
        participant2.health.ping(participant1)
    }

    "keep working on the successor when they were connected before the LSU" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        val fixture = fixtureWithDefaults()
        fixture.newPV shouldBe ProtocolVersion.dev
        performSynchronizerNodesLsu(fixture)

        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        transferTraffic()
        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participant1.synchronizers.is_connected(fixture.newPsid) shouldBe true
          participant2.synchronizers.is_connected(fixture.newPsid) shouldBe true
        }
        oldSynchronizerNodes.all.stop()
        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)

        participant2.health.ping(participant1)
    }

    // participant3, participant4 and participant5 were not connected to the old synchronizer:
    // they join the successor for the first time and must present their identity in its protocol version
    "not onboard on the successor without re-signed certificates" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant3.synchronizers.connect(sequencer2, daName),
          _.shouldBeCommandFailure(
            SynchronizerRegistryError.TopologyConversionError,
            "is not available on this node to re-sign it",
          ),
        )
        participant3.synchronizers.list_connected() shouldBe empty
    }

    "onboard on the successor with onboarding transactions re-signed offline" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        registerWithResignedCertificates(participant4)
        participant4.synchronizers.reconnect(daName)
        participant4.health.ping(participant1)
    }

    // TODO(#34022): participant5 onboards once the authorized store is removed
    "be blocked after a key rotation with the offline root key" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        loggerFactory.assertLogsSeq(
          SuppressionRule.forLogger[StoreBasedSynchronizerOutbox] && SuppressionRule.Level(
            Level.WARN
          )
        )(
          {
            registerWithResignedCertificates(participant5)
            participant5.synchronizers.reconnect(daName, synchronize = None)
            // onboarding succeeds: only the outbox is blocked
            participant5.synchronizers.is_connected(daName) shouldBe true
            // the outbox flushes in the background: wait for its first failure before disconnecting
            eventually()(loggerFactory.fetchRecordedLogEntries should not be empty)
            participant5.synchronizers.disconnect(daName)
          },
          forAll(_)(_.warningMessage should include("is not available on this node to re-sign it")),
        )
    }
  }
}

object LsuTopologyVersionChangeOnboardingIntegrationTest {
  private final case class OfflineRoot(rootKey: SigningPublicKey, intermediateKey: SigningPublicKey)
}
