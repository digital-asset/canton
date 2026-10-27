// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  StaticSynchronizerParameters,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, LocalParticipantReference}
import com.digitalasset.canton.integration.EnvironmentDefinition.buildBaseEnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.scripts.offlinerootkey.OfflineRootKeyCertificates
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

/** Hard migration of participants with an offline root namespace key to a synchronizer whose
  * topology transactions use another proto version (v30 -> v31).
  *
  * Test scenario:
  *   - participant1, participant2 and participant3 have an offline root namespace key and are
  *     onboarded on the source synchronizer.
  *   - participant1 migrates without preparation, with its certificates in the source proto
  *     version, which the target admits.
  *   - participant2 registers the target without a handshake and with onboarding transactions
  *     re-signed offline, and migrates.
  *   - participant3 registers the target with a handshake and onboarding transactions re-signed
  *     offline, and migrates.
  */
final class HardMigrationOfflineRootKeyOnboardingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OfflineRootKeyCertificates {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  private val sourcePV: ProtocolVersion = ProtocolVersion.v36

  private val targetPV: ProtocolVersion = ProtocolVersion.dev

  private lazy val topologyVersionChanges: Boolean =
    testedProtocolVersion == ProtocolVersion.v36 &&
      protoVersion(sourcePV) != protoVersion(targetPV)

  override lazy val environmentDefinition: EnvironmentDefinition =
    buildBaseEnvironmentDefinition(numParticipants = 3, numSequencers = 2, numMediators = 2)
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
              StaticSynchronizerParameters.initialValues(environment.clock, sourcePV)
            ),
          ),
          NetworkTopologyDescription(
            acmeName,
            synchronizerOwners = Seq[InstanceReference](sequencer2, mediator2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer2),
            mediators = Seq(mediator2),
            overrideStaticSynchronizerParameters = Some(
              StaticSynchronizerParameters.initialValues(environment.clock, targetPV)
            ),
          ),
        )
      }
      .addConfigTransforms(ConfigTransforms.enableDevVersionSupport*)
      // TODO(#34818) Enable the new pipeline
      // During the migration, the digest processor of the new pipeline fails to find an active
      // connection to the target, and this failure can abort the shutdown of the environment.
      .addConfigTransform(ConfigTransforms.disableNewAcsCommitmentProcessorPipeline)
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.init.identity).replace(IdentityConfig.Manual)
        )
      )

  private def targetConfig(implicit env: TestConsoleEnvironment): SynchronizerConnectionConfig = {
    import env.*
    SynchronizerConnectionConfig(
      acmeName,
      SequencerConnections.single(sequencer2.sequencerConnection),
    )
  }

  private def registerTargetWithResignedCertificates(
      participant: LocalParticipantReference,
      performHandshake: Boolean,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    val store =
      participant.topology.stores.create_temporary_topology_store("onboarding", targetPV)
    participant.topology.transactions
      .load_single_from_files(namespaceCertificates(participant, targetPV), store)
    val identity = participant.topology.transactions
      .generate_onboarding_transactions(targetPV, temporaryStore = Some(store))
    val trustCertificate = participant.topology.synchronizer_trust_certificates
      .propose(participant.id, acmeId.logical, store = Some(store))

    participant.synchronizers.register(
      sequencer2,
      alias = acmeName,
      performHandshake = performHandshake,
      onboardingTransactions = identity :+ trustCertificate,
    )
  }

  private def migrateAndPing(participant: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    participant.repair.migrate_synchronizer(source = daName, target = targetConfig)
    participant.synchronizers.reconnect(acmeName)
    participant.topology.synchronisation.await_idle()
    participant.health.ping(participant)
  }

  "Participants with an offline root namespace key" should {
    "be onboarded on the source synchronizer" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        Seq(participant1, participant2, participant3).foreach { participant =>
          initializeWithOfflineRoot(participant, sourcePV)
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.health.ping(participant)
          participant.synchronizers.disconnect_all()
        }
    }

    "migrate with the certificates signed for the source protocol version" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        migrateAndPing(participant1)
    }

    "migrate after registering the target without a handshake and with re-signed certificates" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        registerTargetWithResignedCertificates(participant2, performHandshake = false)
        migrateAndPing(participant2)
    }

    "migrate after registering the target with a handshake and re-signed certificates" onlyRunWhen (topologyVersionChanges) in {
      implicit env =>
        import env.*

        registerTargetWithResignedCertificates(participant3, performHandshake = true)
        migrateAndPing(participant3)
    }
  }
}
