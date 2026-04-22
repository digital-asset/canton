// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  InstanceReference,
  LocalInstanceReference,
  LocalSequencerReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.metrics.MetricValue
import com.digitalasset.canton.metrics.MetricValue.LongPoint
import com.digitalasset.canton.topology.{Member, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerAlias, config}

/** This trait provides helpers for the logical synchronizer upgrade tests. The main goal is to
  * improve readability of each tests by focusing on the behavior we want to test and make it easier
  * to write new tests.
  */
private[lsu] trait LsuBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with LogicalUpgradeUtils
    with TrafficBalanceSupport
    with LsuTrafficManagement {

  registerPlugin(new UsePostgres(loggerFactory))

  protected var oldSynchronizerNodes: SynchronizerNodes = _
  protected var newSynchronizerNodes: SynchronizerNodes = _
  protected def newOldSequencers: Map[String, String]
  protected def newOldMediators: Map[String, String]
  protected def newOldNodesResolution: Map[String, String] =
    newOldSequencers ++ newOldMediators

  protected def upgradeTime: CantonTimestamp

  protected val useStaticTime: Boolean = true

  protected def configTransforms: Seq[ConfigTransform] = List(
    ConfigTransforms.disableAutoInit(newOldNodesResolution.keySet)
  ) ++ ConfigTransforms.enableAlphaVersionSupport
    ++ ConfigTransforms.setTopologyTransactionRegistrationTimeout(
      // As we advance the clock quite a bit, we need to bump this parameter to avoid sequencing timeouts.
      config.NonNegativeFiniteDuration.ofHours(1)
    ) ++
    (if (useStaticTime) Seq(ConfigTransforms.useStaticTime) else Seq.empty)

  /** Prepare the environment for LSU with default values.
    *   - Connect `participants.all` (except if override is used) to synchronizer and upload dar
    *   - Set oldSynchronizerNodes and newSynchronizerNodes
    *   - Set reconciliation interval to 1s
    */
  protected def defaultEnvironmentSetup(
      participantsOverride: Option[Seq[ParticipantReference]] = None,
      hasTrafficControl: Boolean = true,
      changeDynamicSynchronizerParameters: Boolean = true,
      connectParticipants: Boolean = true,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.{participants as _, *}

    if (connectParticipants) {
      val participants = participantsOverride.getOrElse(env.participants.all)

      participants.synchronizers.connect(defaultSynchronizerConnectionConfig())
      participants.dars.upload(CantonExamplesPath)
    }

    if (changeDynamicSynchronizerParameters)
      setDefaultsDynamicSynchronizerParameters(
        daId,
        synchronizerOwners1,
        hasTrafficControl = hasTrafficControl,
      )

    oldSynchronizerNodes =
      SynchronizerNodes(newOldSequencers.values.toSeq.map(ls), newOldMediators.values.toSeq.map(lm))
    newSynchronizerNodes =
      SynchronizerNodes(newOldSequencers.keySet.toSeq.map(ls), newOldMediators.keySet.toSeq.map(lm))
  }

  protected def setDefaultsDynamicSynchronizerParameters(
      psid: PhysicalSynchronizerId,
      synchronizerOwners: Set[InstanceReference],
      hasTrafficControl: Boolean = true,
      reconciliationInterval: config.PositiveDurationSeconds =
        config.PositiveDurationSeconds.ofSeconds(1),
  ): Unit = {
    synchronizerOwners.foreach(
      _.topology.synchronizer_parameters.propose_update(
        psid,
        _.copy(
          // Enable traffic control
          trafficControl = Option.when(hasTrafficControl)(generousTrafficControlParameters),
          // Ensure we have frequent ACS commitments exchange to increase the likelihood of catching issues
          reconciliationInterval = reconciliationInterval,
        ),
      )
    )

    eventually() {
      synchronizerOwners.foreach(
        _.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(psid)
          .reconciliationInterval shouldBe reconciliationInterval
      )
    }
  }

  /** Transfer traffic from old sequencers to new ones.
    *
    * Prerequisite:
    *   - Time is after upgrade time
    *
    * @param fixtureOverride
    *   If defined, use the provided fixture instead of oldSynchronizerNodes and
    *   newSynchronizerNodes.
    * @param suppressLogs
    *   Whether errors in the log (NotAtUpgradeTimeOrBeyond) should be suppressed. Use false if the
    *   call to transferTraffic is already in a suppression logger block (since those cannot be
    *   nested).
    *
    * @param trafficTsOverride
    *   The time used to fetch the traffic state. MUST be empty for regular LSUs. In that case, an
    *   LSU must be announced and sequencer must have reached the upgrade time for this call to
    *   succeed. SHOULD be defined in a disaster recovery scenario when requesting the topology from
    *   a synchronizer without an announced LSU.
    */
  protected def transferTraffic(
      fixtureOverride: Option[Fixture] = None,
      trafficTsOverride: Option[CantonTimestamp] = None,
      suppressLogs: Boolean = true,
  ): Unit = {

    val oldSequencers: Seq[LocalSequencerReference] =
      fixtureOverride.fold(oldSynchronizerNodes.sequencers)(_.oldSynchronizerNodes.sequencers)

    val newSequencers: Seq[LocalSequencerReference] =
      fixtureOverride.fold(newSynchronizerNodes.sequencers)(_.newSynchronizerNodes.sequencers)

    transferTraffic(
      oldSequencers = oldSequencers,
      newSequencers = newSequencers,
      suppressLogs = suppressLogs,
      trafficTsOverride = trafficTsOverride,
    )
  }

  protected def defaultSynchronizerConnectionConfig()(implicit
      env: TestConsoleEnvironment
  ): SynchronizerConnectionConfig = {
    import env.*

    SynchronizerConnectionConfig(
      synchronizerAlias = daName,
      sequencerConnections = sequencer1,
    )
  }

  protected def synchronizerConnectionConfig(
      seq: SequencerReference
  )(implicit env: TestConsoleEnvironment): SynchronizerConnectionConfig = {
    import env.*

    SynchronizerConnectionConfig(
      synchronizerAlias = daName,
      sequencerConnections = seq,
    )
  }

  protected def synchronizerConnectionConfig(
      seqs: Seq[SequencerReference],
      threshold: Int,
  )(implicit env: TestConsoleEnvironment): SynchronizerConnectionConfig = {
    import env.*

    SynchronizerConnectionConfig(
      synchronizerAlias = daName,
      sequencerConnections = SequencerConnections.tryMany(
        connections =
          seqs.map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
        sequencerTrustThreshold = PositiveInt.tryCreate(threshold),
        sequencerLivenessMargin = NonNegativeInt.zero,
        submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
        sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
      ),
    )
  }

  protected def fixtureWithDefaults(
      upgradeTime: CantonTimestamp = upgradeTime,
      newPVOverride: Option[ProtocolVersion] = None,
      newSerialOverride: Option[NonNegativeInt] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Fixture = {
    val currentPsid = env.daId

    Fixture(
      currentPsid = currentPsid,
      upgradeTime = upgradeTime,
      oldSynchronizerNodes = oldSynchronizerNodes,
      newSynchronizerNodes = newSynchronizerNodes,
      newOldNodesResolution = newOldNodesResolution,
      oldSynchronizerOwners = env.synchronizerOwners1,
      newPV = newPVOverride.getOrElse(ProtocolVersion.dev),
      // increasing the serial as well, so that the test also works when running with PV=dev
      newSerial = newSerialOverride.getOrElse(currentPsid.serial.increment.toNonNegative),
    )
  }

  /** Perform synchronizer side of the LSU:
    *
    *   - Upgrade announcement
    *   - Migration of synchronizer nodes
    *   - Sequencer successors announcements
    */
  protected def performSynchronizerNodesLsu(
      fixture: Fixture,
      announceSequencerSuccessors: Boolean = true,
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
    fixture.oldSynchronizerOwners.foreach(
      _.topology.lsu.announcement.propose(fixture.newPsid, fixture.upgradeTime)
    )

    // Ensure all nodes see the announcement
    eventually() {
      forAll(fixture.oldSynchronizerNodes.all)(
        _.topology.lsu.announcement
          .list(store = Some(fixture.currentPsid))
          .filter(_.item.successorSynchronizerId == fixture.newPsid)
          .loneElement
      )
    }

    migrateSynchronizerNodes(fixture)

    if (announceSequencerSuccessors) {
      fixture.oldSynchronizerNodes.sequencers.zip(fixture.newSynchronizerNodes.sequencers).foreach {
        case (oldSequencer, newSequencer) =>
          oldSequencer.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = oldSequencer.id,
            endpoints = newSequencer.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            successorSynchronizerId = fixture.newPsid,
          )
      }
    }
  }

  /** Instantiate the new synchronizer nodes with identity of the previous ones and import topology
    * state.
    * @param fixture
    *   Fixture to be sued
    * @param ignorePsidCheck
    *   Whether the psid check should be ignored on the new synchronizer. Should be used only in the
    *   context of a roll forward when the new synchronizer psid does not correspond to the
    *   successor of an announced LSU.
    * @param sequencerLsuStateTsOverride
    *   Specifies the timestamp to be used for the sequencer state export. Should be used only in
    *   the context of a roll forward when there is no announced LSU.
    */
  protected def migrateSynchronizerNodes(
      fixture: Fixture,
      ignorePsidCheck: Boolean = false,
      sequencerLsuStateTsOverride: Option[CantonTimestamp] = None,
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
    val exportDirectory = exportNodesData(
      SynchronizerNodes(
        sequencers = fixture.oldSynchronizerNodes.sequencers,
        mediators = fixture.oldSynchronizerNodes.mediators,
      ),
      successorPsid = fixture.newPsid,
      sequencerLsuStateTsOverride = sequencerLsuStateTsOverride,
    )

    // Migrate nodes preserving their data (and IDs)
    fixture.newSynchronizerNodes.all.foreach { newNode =>
      migrateNode(
        migratedNode = newNode,
        newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters,
        synchronizerId = fixture.currentPsid,
        newSequencers = fixture.newSynchronizerNodes.sequencers,
        exportDirectory = exportDirectory,
        newNodeToOldNodeName = fixture.newOldNodesResolution,
        ignorePsidCheck = ignorePsidCheck,
      )
    }
  }
}

object LsuBase {
  import org.scalatest.OptionValues.*
  import org.scalatest.EitherValues.*

  // Returns the number of received messages per sender
  def getLsuSequencingTestMetricValues(node: LocalInstanceReference): Map[Member, Long] =
    getMetricValues(node, "daml.received-lsu-sequencing-test-messages").value.collect {
      case metric: MetricValue.LongPoint =>
        Member.fromProtoPrimitive_(metric.attributes.get("sender").value).value -> metric.value
    }.toMap

  // Returns the number of handshakes. Key is (participant id, status) where status is "success" or "failure"
  def getParticipantHandshakesMetricValues(
      node: LocalInstanceReference
  ): Map[(ParticipantId, String), Long] = {
    getMetricValues(node, "daml.sequencer.public-api.handshakes").toList
      .flatMap(_.collect { case l: LongPoint =>
        val member = Member
          .fromProtoPrimitive_(l.attributes.get("member").value)
          .value

        member match {
          case p: ParticipantId =>
            val status = l.attributes.get("status").value

            Some((p, status) -> l.value)
          case _ => None
        }
      })
      .flatten
  }.toMap

  // Return the status of the LSU, per successor psid
  def getLsuStatusMetricValues(
      node: LocalInstanceReference
  ): Map[PhysicalSynchronizerId, NonNegativeInt] = {
    getMetricValues(node, "daml.participant.lsu_status").toList.flatMap(_.collect {
      case l: LongPoint =>
        PhysicalSynchronizerId.tryFromString(
          l.attributes.get("successor_psid").value
        ) -> NonNegativeInt.tryCreate(l.value.toInt)
    })
  }.toMap

  private def getMetricValues(
      node: LocalInstanceReference,
      metricName: String,
  ): Option[Seq[MetricValue]] = node.metrics
    .list(metricName)
    .get(metricName)

  final case class Fixture(
      currentPsid: PhysicalSynchronizerId,
      upgradeTime: CantonTimestamp,
      oldSynchronizerNodes: SynchronizerNodes,
      newSynchronizerNodes: SynchronizerNodes,
      newOldNodesResolution: Map[String, String],
      oldSynchronizerOwners: Set[InstanceReference],
      newPV: ProtocolVersion,
      newSerial: NonNegativeInt,
      overridePsid: Option[PhysicalSynchronizerId] = None,
  ) {
    val newStaticSynchronizerParameters: StaticSynchronizerParameters =
      StaticSynchronizerParameters.defaultsWithoutKMS(
        newPV,
        newSerial,
        topologyChangeDelay = config.NonNegativeFiniteDuration.Zero,
      )

    val newPsid: PhysicalSynchronizerId =
      overridePsid.getOrElse(
        PhysicalSynchronizerId(currentPsid.logical, newStaticSynchronizerParameters.toInternal)
      )

    val synchronizerSuccessor: SynchronizerSuccessor = SynchronizerSuccessor(newPsid, upgradeTime)
  }
}
