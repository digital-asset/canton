// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, LocalParticipantReference}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyCertificates
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import java.time.Duration

/** Onboarding of participants with an offline root namespace key across an LSU that changes the
  * topology transaction proto version (v30 -> v31).
  *
  * Test scenario:
  *   - participant2 to participant5 have an offline root key and an intermediate key on the node,
  *     with certificates in the old proto version.
  *   - participant2 is connected before the LSU.
  *   - participant3 connects to the successor with its old certificates.
  *   - participant4 connects with certificates re-signed for the successor, then rotates its
  *     intermediate key with a revocation in proto version v30.
  *   - participant5 rotates its intermediate key before connecting, then registers the successor
  *     with onboarding transactions re-signed for it. As its onboarding transactions are provided,
  *     its authorized store, which holds the superseded delegations of the rotation, is not pushed
  *     to the successor.
  *
  * Test goal: the successor admits namespace certificates in both proto versions.
  */
final class LsuTopologyVersionChangeOnboardingIntegrationTest
    extends LsuBase
    with OfflineRootKeyCertificates {
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

  private lazy val oldPV: ProtocolVersion =
    ProtocolVersion.stable
      .filter(pv => protoVersion(pv) != protoVersion(LsuBase.DefaultNewPV))
      .lastOption
      .getOrElse(testedProtocolVersion)

  private lazy val topologyVersionChanges: Boolean =
    testedProtocolVersion == ProtocolVersion.latest &&
      protoVersion(oldPV) != protoVersion(LsuBase.DefaultNewPV)

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(numParticipants = 5, numSequencers = 2, numMediators = 2)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            overrideStaticSynchronizerParameters = Some(
              StaticSynchronizerParameters.initialValues(environment.clock, oldPV)
            ),
          )
        )
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

  private var fixture: LsuBase.Fixture = _

  private def registerWithResignedCertificates(participant: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val newPV = fixture.newPV
    val store = participant.topology.stores.create_temporary_topology_store("onboarding", newPV)
    participant.topology.transactions
      .load_single_from_files(namespaceCertificates(participant, newPV), store)
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

        Seq(participant2, participant3, participant4, participant5)
          .foreach(initializeWithOfflineRoot(_, oldPV))
    }

    "rotate the intermediate namespace key with the offline root key" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        val root = offlineRoots(participant5.name)
        val newIntermediateKey = participant5.keys.secret
          .generate_signing_key(
            s"${participant5.name}-intermediate-2",
            SigningKeyUsage.NamespaceOnly,
          )
        val prefix = s"${certificatePrefix(participant5, oldPV)}-rotation"
        val newIntermediateKeyFile = s"$prefix-intermediate-key.pub"
        participant5.keys.public.download_to(newIntermediateKey.id, newIntermediateKeyFile)
        val rotation = Seq(
          offlineCertificate(
            root.rootKey,
            s"$prefix-intermediate",
            oldPV,
            Seq(
              "--intermediate-delegation",
              "--root-pub-key",
              root.rootKeyFile,
              "--canton-target-pub-key",
              newIntermediateKeyFile,
            ),
          ),
          offlineCertificate(
            root.rootKey,
            s"$prefix-revocation",
            oldPV,
            Seq(
              "--revoke-delegation",
              s"${certificatePrefix(participant5, oldPV)}-intermediate.prep",
            ),
          ),
        )
        participant5.topology.transactions
          .load_single_from_files(rotation, TopologyStoreId.Authorized)
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

        fixture = fixtureWithDefaults()
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
    "onboard on the successor with the certificates signed for the old protocol version" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        participant3.synchronizers.connect(sequencer2, daName)
        participant3.health.ping(participant1)
    }

    "onboard on the successor with onboarding transactions re-signed offline" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        registerWithResignedCertificates(participant4)
        participant4.synchronizers.reconnect(daName)
        participant4.health.ping(participant1)
    }

    "rotate the intermediate namespace key on the successor with a revocation in proto version v30" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        val newPV = fixture.newPV
        val root = offlineRoots(participant4.name)
        val newIntermediateKey = participant4.keys.secret
          .generate_signing_key(
            s"${participant4.name}-intermediate-2",
            SigningKeyUsage.NamespaceOnly,
          )
        val prefix = s"${certificatePrefix(participant4, newPV)}-rotation"
        val newIntermediateKeyFile = s"$prefix-intermediate-key.pub"
        participant4.keys.public.download_to(newIntermediateKey.id, newIntermediateKeyFile)
        val delegationToRevoke = s"$prefix-delegation-to-revoke.tx"
        participant4.topology.namespace_delegations
          .list(TopologyStoreId.Authorized)
          .find(_.item.target == root.intermediateKey)
          .value
          .toTopologyTransaction
          .writeToFile(delegationToRevoke)

        val rotation = Seq(
          offlineCertificate(
            root.rootKey,
            s"$prefix-intermediate",
            newPV,
            Seq(
              "--intermediate-delegation",
              "--root-pub-key",
              root.rootKeyFile,
              "--canton-target-pub-key",
              newIntermediateKeyFile,
            ),
          ),
          offlineCertificate(
            root.rootKey,
            s"$prefix-revocation",
            oldPV,
            Seq("--revoke-delegation", delegationToRevoke),
          ),
        )
        // participant4 registered the successor with onboarding transactions, so its authorized
        // store is not pushed there
        participant4.topology.transactions
          .load_single_from_files(rotation, fixture.newPsid)

        eventually() {
          val delegationTargets = participant4.topology.namespace_delegations
            .list(fixture.newPsid, filterNamespace = participant4.namespace.toProtoPrimitive)
            .map(_.item.target)
          delegationTargets should contain(newIntermediateKey)
          delegationTargets should not contain root.intermediateKey
        }
        offlineRoots.update(participant4.name, root.copy(intermediateKey = newIntermediateKey))
        participant4.health.ping(participant1)
    }

    "onboard on the successor after a key rotation with the offline root key" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        registerWithResignedCertificates(participant5)
        participant5.synchronizers.reconnect(daName)
        participant5.topology.synchronisation.await_idle()
        participant5.health.ping(participant1)
    }
  }
}
