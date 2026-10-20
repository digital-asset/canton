// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import better.files.*
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, LocalParticipantReference}
import com.digitalasset.canton.crypto.{SigningKeySpec, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyScriptsIntegrationTest.{
  assemblePath,
  offlineRootKeyPath,
  preparePath,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.TopologyTransaction
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.collection.concurrent.TrieMap
import scala.sys.process.Process

import LsuTopologyVersionChangeOnboardingIntegrationTest.OfflineRoot

/** Onboarding of participants whose root namespace key is offline across an LSU that changes the
  * topology transaction proto version (v30 -> v31).
  *
  * Test scenario:
  *   - participant2, participant3, participant4 and participant5 are initialized with an offline
  *     root namespace key (whose certificates are made with the offline-root-key scripts) and an
  *     intermediate namespace key on the node. Their identity transactions are serialized with the
  *     old protocol version.
  *   - participant2 onboards on the old synchronizer and is upgraded with it.
  *   - After the upgrade, participant3 connects to the successor by itself, which fails with
  *     TOPOLOGY_CONVERSION_ERROR: re-signing its namespace certificates for the successor's
  *     protocol version requires the offline root key. The operator then re-signs them with the
  *     offline root key, and participant3 onboards with them.
  *   - participant4 onboards on the successor with onboarding transactions whose namespace
  *     certificates the operator re-signed with the offline root key for the successor's protocol
  *     version.
  *   - participant5 rotates its intermediate namespace key with the offline root key, and then
  *     registers the successor like participant4. As its onboarding transactions are provided, its
  *     authorized store, which holds the superseded delegations of the rotation, is not pushed to
  *     the successor.
  *
  * Test goal: a participant with an offline root namespace key onboards on a synchronizer whose
  * protocol version uses another topology transaction proto version, provided the operator supplies
  * its namespace certificates re-signed for that version.
  *
  * Test setup: 5 participants, successor (sequencer2, mediator2) at the default LSU protocol
  * version, old synchronizer (sequencer1, mediator1) at the latest stable protocol version whose
  * topology transactions use another proto version than the successor's. Runs only with the latest
  * protocol version, as it does not depend on the tested one.
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

  private val offlineRoots = TrieMap.empty[String, OfflineRoot]

  private var fixture: LsuBase.Fixture = _

  private lazy val certificatesDir: File = File.newTemporaryDirectory()

  override def afterAll(): Unit = {
    certificatesDir.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  private def protoVersion(protocolVersion: ProtocolVersion): String =
    TopologyTransaction
      .protoVersionFor(protocolVersion)
      .value
      .v
      .toString

  private def runScript(script: File, args: Seq[String]): Unit =
    Process(script.pathAsString +: args, cwd = offlineRootKeyPath.toJava)
      .!(mkProcessLogger()) shouldBe 0

  private def certificatePrefix(
      participant: LocalParticipantReference,
      protocolVersion: ProtocolVersion,
  ): String =
    (certificatesDir / s"${participant.name}-${protoVersion(protocolVersion)}").pathAsString

  /** Prepares a topology transaction for the given protocol version with the offline-root-key
    * scripts, signs it with the offline root key, and assembles the certificate. Returns the path
    * of the certificate.
    */
  private def offlineCertificate(
      rootKey: SigningPublicKey,
      output: String,
      protocolVersion: ProtocolVersion,
      prepareArgs: Seq[String],
  )(implicit env: TestConsoleEnvironment): String = {
    import env.*
    runScript(
      preparePath,
      prepareArgs ++ Seq(
        "--protocol-version",
        protocolVersion.toString,
        "--output",
        output,
      ),
    )
    val signature = File(s"$output.sig")
    signature.writeByteArray(
      global_secret
        .sign(
          ByteString.copyFrom(File(s"$output.hash").byteArray),
          rootKey.fingerprint,
          SigningKeyUsage.NamespaceOnly,
        )
        .toProtoV30
        .signature
        .toByteArray
    )
    runScript(
      assemblePath,
      Seq(
        "--prepared-transaction",
        s"$output.prep",
        "--signature",
        signature.pathAsString,
        "--signature-algorithm",
        "ed25519",
        "--output",
        output,
      ),
    )
    s"$output.cert"
  }

  /** The root and intermediate namespace certificates, signed with the offline root key for the
    * given protocol version (what the operator does with the offline root key tooling).
    */
  private def namespaceCertificates(
      participant: LocalParticipantReference,
      protocolVersion: ProtocolVersion,
  )(implicit env: TestConsoleEnvironment): Seq[String] = {
    val root = offlineRoots(participant.name)
    val prefix = certificatePrefix(participant, protocolVersion)
    val intermediateKeyFile = s"$prefix-intermediate-key.pub"
    participant.keys.public.download_to(root.intermediateKey.id, intermediateKeyFile)
    Seq(
      offlineCertificate(
        root.rootKey,
        s"$prefix-root",
        protocolVersion,
        Seq(
          "--root-delegation",
          "--root-pub-key",
          root.rootKeyFile,
          "--target-pub-key",
          root.rootKeyFile,
        ),
      ),
      offlineCertificate(
        root.rootKey,
        s"$prefix-intermediate",
        protocolVersion,
        Seq(
          "--intermediate-delegation",
          "--root-pub-key",
          root.rootKeyFile,
          "--canton-target-pub-key",
          intermediateKeyFile,
        ),
      ),
    )
  }

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

        Seq(participant2, participant3, participant4, participant5).foreach { participant =>
          participant.health.wait_for_ready_for_id()
          val intermediateKey = participant.keys.secret
            .generate_signing_key(
              s"${participant.name}-intermediate",
              SigningKeyUsage.NamespaceOnly,
            )
          val rootKey = global_secret.keys.secret
            .generate_key(SigningKeySpec.EcCurve25519, SigningKeyUsage.NamespaceOnly)
          val rootKeyFile = certificatesDir / s"${participant.name}-root-key.der"
          rootKeyFile.writeByteArray(
            rootKey.toProtoV30.valueOrFail("serializing public key").publicKey.toByteArray
          )
          offlineRoots.update(
            participant.name,
            OfflineRoot(rootKey, rootKeyFile.pathAsString, intermediateKey),
          )

          participant.topology.init_id_from_uid(
            UniqueIdentifier.tryCreate(participant.name, Namespace(rootKey.id)),
            delegationFiles = namespaceCertificates(participant, oldPV),
          )
          eventually()(participant.is_initialized shouldBe true)
          participant.keys.secret.list(filterFingerprint = rootKey.id.unwrap) shouldBe empty
        }
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
    "onboard on the successor once the operator re-signed the certificates offline" onlyRunWhen (topologyVersionChanges) in {
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

        registerWithResignedCertificates(participant3)
        participant3.synchronizers.reconnect(daName)
        participant3.health.ping(participant1)
    }

    "onboard on the successor with onboarding transactions re-signed offline" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        registerWithResignedCertificates(participant4)
        participant4.synchronizers.reconnect(daName)
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

object LsuTopologyVersionChangeOnboardingIntegrationTest {
  private final case class OfflineRoot(
      rootKey: SigningPublicKey,
      rootKeyFile: String,
      intermediateKey: SigningPublicKey,
  )
}
