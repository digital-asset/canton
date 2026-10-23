// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity

import better.files.File
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{
  CryptoConfig,
  DbConfig,
  PositiveDurationSeconds,
  StorageConfig,
}
import com.digitalasset.canton.console.{InstanceReference, ParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseExternalProcess.ReleasePath
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseExternalConsole,
  UseExternalProcess,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils
import com.digitalasset.canton.topology.{Member, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Chains logical synchronizer upgrades across released Canton binaries, then to the current
  * version, and checks that the contracts created before each upgrade remain usable.
  *
  * Test setup:
  *   - all nodes run as external processes, using released binaries or the current version
  *   - nodes are started only when the step that needs them is reached
  *   - for each release: the binaries of the running nodes are replaced (same protocol version),
  *     then an LSU to the protocol version of the release is performed
  *   - console commands of a release are run by the console of the release, using scripts in
  *     `all-the-lsus/<major>.<minor>-<script>`; console commands of the current version are run by
  *     the test
  */
final class LsuAcrossReleasesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with LogicalUpgradeUtils {

  import LsuAcrossReleasesIntegrationTest.participants

  private val firstProtocolVersion = 34

  /** Time between the LSU announcement and the upgrade time */
  private val upgradeTimeDelay = 90.seconds

  /** Time allowed for the upgrade time to be reached and for the participants to reconnect */
  private val lsuTimeout = 5.minutes

  // First 3.4 build that can discard unknown config keys
  // TODO(#36712): Use the latest stable 3.4 release
  private val release3_4 = ReleaseVersion.tryCreate("3.4.12-snapshot.20260519.17651.0.vb7bec60d")
  private lazy val release3_5 = latestStableRelease(3, 5)
  private lazy val release3_6 = latestStableRelease(3, 6)

  private lazy val stableReleases =
    ReleaseUtils.listAllReleases(includeSnapshot = false).filter(_.isStable)

  private def latestStableRelease(major: Int, minor: Int): ReleaseVersion =
    stableReleases.filter(_.majorMinor == (major, minor)).max

  override protected def testName: String = "lsu-across-releases"

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      // One synchronizer per version: 3.4, 3.5, 3.6 and 3.7 (current)
      // TODO(#36712): Generate the synchronizers dynamically from the tested versions
      MultiSynchronizer.tryCreate(
        Set("sequencer1"),
        Set("sequencer2"),
        Set("sequencer3"),
        Set("sequencer4"),
      ),
    )
  )
  registerPlugin(external)

  private lazy val external =
    new UseExternalProcess(
      loggerFactory,
      externalSequencers = Set("sequencer1", "sequencer2", "sequencer3", "sequencer4"),
      externalMediators = Set("mediator1", "mediator2", "mediator3", "mediator4"),
      externalParticipants = participants.toSet,
      fileNameHint = this.getClass.getSimpleName,
      // Ensure that DB migrations are applied automatically
      configTransforms = Seq(
        ConfigTransforms.updateAllParticipantConfigs_(config =>
          config.focus(_.storage).modify(enableMigrateAndStart)
        ),
        ConfigTransforms.updateAllSequencerConfigs_(config =>
          config.focus(_.storage).modify(enableMigrateAndStart)
        ),
        ConfigTransforms.updateAllMediatorConfigs_(config =>
          config.focus(_.storage).modify(enableMigrateAndStart)
        ),
      ),
      removeConfigPaths = LsuAcrossReleasesIntegrationTest.nodeConfigPathsToRemove,
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = participants.size,
        numSequencers = 4,
        numMediators = 4,
      )
      .withManualStart
      .addConfigTransforms(
        ConfigTransforms.disableAutoInit(
          Set("sequencer2", "mediator2", "sequencer3", "mediator3", "sequencer4", "mediator4")
        ),
        ConfigTransforms.enableNonStandardConfig,
      )
      .addConfigTransforms(ConfigTransforms.clearMinimumProtocolVersion*)
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(participants.toSet)
      )

  private def enableMigrateAndStart(storage: StorageConfig): StorageConfig = storage match {
    case postgres: DbConfig.Postgres => postgres.focus(_.parameters.migrateAndStart).replace(true)
    case other => other
  }

  /** Return the path of the `bin/canton` script of `release`, downloading it if needed */
  private def binary(release: ReleaseVersion)(implicit env: TestConsoleEnvironment): String = {
    import env.environment.executionContext
    ReleaseUtils.retrieve(release).futureValue
  }

  private def startNodes(release: ReleaseVersion, names: String*)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    names.foreach(
      external.start(
        _,
        ReleasePath(binary(release)),
        failOnUnknownConfigKeys = false,
      )
    )

  /** Replace the binaries of the given nodes, keeping their storage */
  private def upgradeBinaries(release: ReleaseVersion, names: String*)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    names.foreach(external.kill(_, force = false))
    startNodes(release, names*)
  }

  /** Run `all-the-lsus/<major>.<minor>-<script>` with the console of `release` */
  private def runScript(
      release: ReleaseVersion,
      script: String,
      properties: Map[String, String] = Map.empty,
  )(implicit env: TestConsoleEnvironment): Unit = {
    val cantonBin = binary(release)
    val darPath = File(cantonBin).parent.parent / "dars" / "CantonExamples.dar"
    val fileNameHint =
      s"${this.getClass.getSimpleName}-$release-${File(script).nameWithoutExtension}"

    val systemProperties =
      (properties + ("all-the-lsus.dar-path" -> darPath.pathAsString)).map { case (key, value) =>
        s"-D$key=$value"
      }

    UseExternalConsole(
      remoteConfigOrConfigFile = Left(env.environment.config),
      cantonBin = cantonBin,
      extraConfigs = Seq(
        "-C canton.parameters.non-standard-config=true",
        "-C canton.features.enable-preview-commands=true",
        "-C canton.parameters.fail-on-unknown-config-keys=false",
      ) ++ systemProperties,
      fileNameHint = fileNameHint,
    ).runScript(s"all-the-lsus/${release.major}.${release.minor}-$script")
  }

  /** Replace the binaries of the old synchronizer nodes and of the participants with `newRelease`,
    * then perform an LSU to `newProtocolVersion` onto the new synchronizer nodes running
    * `newRelease`.
    */
  private def upgradeReleaseAndProtocolVersion(
      newRelease: ReleaseVersion,
      newProtocolVersion: Int,
      oldSequencer: String,
      oldMediator: String,
      newSequencer: String,
      newMediator: String,
  )(implicit env: TestConsoleEnvironment): Unit = {
    val exportDirectory = File.newTemporaryDirectory(s"all-the-lsus-$newRelease-")

    upgradeBinaries(newRelease, (Seq(oldSequencer, oldMediator) ++ participants)*)
    startNodes(newRelease, newSequencer, newMediator)
    waitForInitializationAndReconnect(oldSequencer, oldMediator)

    val upgradeTime = CantonTimestamp.now().add(upgradeTimeDelay)

    runScript(
      newRelease,
      "lsu.canton",
      Map(
        "all-the-lsus.export-dir" -> exportDirectory.pathAsString,
        "all-the-lsus.upgrade-time" -> upgradeTime.toInstant.toString,
        "all-the-lsus.protocol-version" -> newProtocolVersion.toString,
        "all-the-lsus.old-sequencer" -> oldSequencer,
        "all-the-lsus.old-mediator" -> oldMediator,
        "all-the-lsus.new-sequencer" -> newSequencer,
        "all-the-lsus.new-mediator" -> newMediator,
      ),
    )
    val successorPsid = env.s(newSequencer).physical_synchronizer_id
    waitForParticipantsConnected(successorPsid)
    checkExtraTrafficPurchased(newSequencer)
    checkContracts(successorPsid)

    external.stop(oldSequencer)
    external.stop(oldMediator)
  }

  /** pv=37 is not stable yet */
  private def alphaVersionSupport(name: String): Map[String, String] =
    Map("canton.parameters.alpha-version-support" -> "true") ++
      Option.when(participants.contains(name))(
        s"canton.participants.$name.parameters.alpha-version-support" -> "true"
      )

  private def waitForInitializationAndReconnect(sequencer: String, mediator: String)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    (Seq[InstanceReference](env.s(sequencer), env.m(mediator)) ++ participants.map(env.p))
      .foreach(_.health.wait_for_initialized())
    participants.foreach(env.p(_).synchronizers.reconnect_all())
  }

  private val extraTrafficPurchased = NonNegativeLong.tryCreate(500_000L)

  private def participantMembers(implicit env: TestConsoleEnvironment): Seq[Member] =
    participants.map(env.p(_).id.member)

  private def purchaseExtraTraffic(
      sequencer: String
  )(implicit env: TestConsoleEnvironment): Unit = {
    participantMembers.foreach(
      env
        .s(sequencer)
        .traffic_control
        .set_traffic_balance(_, PositiveInt.one, extraTrafficPurchased)
    )
    checkExtraTrafficPurchased(sequencer)
  }

  /** The extra traffic purchased at bootstrap is transferred by each LSU */
  private def checkExtraTrafficPurchased(sequencer: String)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    eventually() {
      val trafficStates = env
        .s(sequencer)
        .traffic_control
        .traffic_state_of_members_approximate(participantMembers)
        .trafficStates
      forAll(participantMembers) { member =>
        trafficStates.get(member).value.extraTrafficPurchased shouldBe extraTrafficPurchased
      }
    }

  private def waitForParticipantsConnected(psid: PhysicalSynchronizerId)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    participants.foreach(env.p(_).synchronizers.reconnect_all())
    eventually(timeUntilSuccess = lsuTimeout) {
      participants.foreach(env.p(_).synchronizers.is_connected(psid) shouldBe true)
    }
  }

  /** Replace the binaries of the old synchronizer nodes and of the participants with the current
    * version, then perform an LSU to `protocolVersion` onto the new synchronizer nodes running the
    * current version.
    */
  private def upgradeToCurrentVersion(
      protocolVersion: ProtocolVersion,
      oldSequencerName: String,
      oldMediatorName: String,
      newSequencerName: String,
      newMediatorName: String,
  )(implicit env: TestConsoleEnvironment): Unit = {
    (Seq(oldSequencerName, oldMediatorName) ++ participants)
      .foreach(external.kill(_, force = false))
    (Seq(oldSequencerName, oldMediatorName, newSequencerName, newMediatorName) ++ participants)
      .foreach(name => external.startCurrentVersion(name, alphaVersionSupport(name)))
    waitForInitializationAndReconnect(oldSequencerName, oldMediatorName)

    val oldSequencer = env.s(oldSequencerName)
    val oldMediator = env.m(oldMediatorName)
    val newSequencer = env.s(newSequencerName)
    val newMediator = env.m(newMediatorName)

    val upgradeTime = CantonTimestamp.now().add(upgradeTimeDelay)
    val currentPsid = oldSequencer.physical_synchronizer_id
    val successorParameters = StaticSynchronizerParameters.defaults(
      CryptoConfig(),
      protocolVersion,
      NonNegativeInt.tryCreate(currentPsid.serial.unwrap + 1),
    )
    val successorPsid =
      PhysicalSynchronizerId(currentPsid.logical, successorParameters.toInternal.value)

    oldSequencer.topology.lsu.announcement.propose(successorPsid, upgradeTime)
    eventually() {
      forAll(Seq[InstanceReference](oldSequencer, oldMediator))(
        _.topology.lsu.announcement
          .list(store = Some(currentPsid))
          .filter(_.item.successorSynchronizerId == successorPsid.opaque)
          .loneElement
      )
    }

    val exportDirectory = exportNodesData(
      Seq(oldSequencer),
      Seq(oldMediator),
      successorPsid,
      sequencerLsuStateTsOverride = None,
    )
    migrateSequencer(
      newSequencer,
      successorParameters,
      exportDirectory,
      oldSequencer.name,
      synchronizerId = currentPsid.logical,
    )
    migrateMediator(
      newMediator,
      successorPsid,
      Seq(newSequencer),
      exportDirectory,
      oldMediator.name,
    )
    Seq[InstanceReference](newSequencer, newMediator).foreach(_.health.wait_for_initialized())

    oldSequencer.topology.lsu.sequencer_successors.propose_successor(
      sequencerId = oldSequencer.id,
      endpoints = newSequencer.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
      successorSynchronizerId = successorPsid,
    )

    eventually(timeUntilSuccess = lsuTimeout) {
      CantonTimestamp.now() should be > upgradeTime
    }
    val trafficState = eventually(timeUntilSuccess = lsuTimeout, retryOnTestFailuresOnly = false) {
      oldSequencer.traffic_control.get_lsu_state()
    }
    newSequencer.traffic_control.set_lsu_state(trafficState)

    waitForParticipantsConnected(successorPsid)
    checkExtraTrafficPurchased(newSequencerName)
    checkContracts(successorPsid)

    external.stop(oldSequencerName)
    external.stop(oldMediatorName)
  }

  /** Check that the Ious of the earlier protocol versions are visible, Bank creates an Iou for
    * Alice, Alice transfers it to Bob. Then the Iou of `protocolVersion` is created.
    */
  private def checkContracts(
      psid: PhysicalSynchronizerId
  )(implicit env: TestConsoleEnvironment): Unit = {
    val protocolVersion = psid.protocolVersion.v
    val participant1 = env.p("participant1")
    val participant2 = env.p("participant2")

    def party(participant: ParticipantReference, name: String): PartyId =
      participant.parties.list(filterParty = name).loneElement.party

    val alice = party(participant1, "Alice")
    val bob = party(participant1, "Bob")
    val bank = party(participant2, "Bank")

    def pvIous(participant: ParticipantReference, party: PartyId): Seq[String] =
      participant.ledger_api.javaapi.state.acs
        .filter(Iou.COMPANION)(party, predicate = _.data.amount.currency.startsWith("pv"))
        .map(_.data.amount.currency)
        .sorted

    forAll(Seq(participant1 -> alice, participant2 -> bank)) { case (participant, party) =>
      pvIous(participant, party) shouldBe
        (firstProtocolVersion until protocolVersion).map(pv => s"pv$pv")
    }

    val aliceUsdIou = IouSyntax.createIou(participant2)(bank, alice)
    eventually() {
      participant1.ledger_api.javaapi.state.acs
        .filter(Iou.COMPANION)(alice, predicate = _.id == aliceUsdIou.id) should not be empty
    }
    participant1.ledger_api.javaapi.commands
      .submit(
        Seq(alice),
        aliceUsdIou.id.exerciseTransfer(bob.toProtoPrimitive).commands.asScala.toSeq,
      )
      .discard
    participant1.ledger_api.javaapi.state.acs
      .filter(Iou.COMPANION)(bob)
      .exists(iou =>
        iou.data.owner == bob.toProtoPrimitive && iou.data.amount.currency == "USD"
      ) shouldBe true

    IouSyntax.createIou(participant2)(bank, alice, currency = s"pv$protocolVersion").discard

    participant1.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(psid)
      .reconciliationInterval shouldBe PositiveDurationSeconds.ofSeconds(1)
    /*
      Ideally, we would like to wait until ACS commitments have been exchanged.
      However, we cannot easily inspect the ACS commitments for remote nodes.
      The pattern sleep+ping+sleep is a poor man's approximation of the check.
     */
    Threading.sleep(2000)
    participant1.health.ping(participant2.id)
    Threading.sleep(2000)
  }

  "Logical synchronizer upgrades across releases" onlyRunWith ProtocolVersion.latest when {
    "bootstrap a synchronizer on 3.4 with pv=34" in { implicit env =>
      startNodes(release3_4, (Seq("sequencer1", "mediator1") ++ participants)*)

      runScript(release3_4, "bootstrap.canton")
      val psid = env.s("sequencer1").physical_synchronizer_id
      psid.protocolVersion shouldBe ProtocolVersion.v34
      psid.serial shouldBe NonNegativeInt.zero
      purchaseExtraTraffic("sequencer1")
    }

    "upgrade the binaries to 3.5 and the synchronizer to pv=35" in { implicit env =>
      upgradeReleaseAndProtocolVersion(
        release3_5,
        newProtocolVersion = 35,
        oldSequencer = "sequencer1",
        oldMediator = "mediator1",
        newSequencer = "sequencer2",
        newMediator = "mediator2",
      )
    }

    "upgrade the binaries to 3.6 and the synchronizer to pv=36" in { implicit env =>
      upgradeReleaseAndProtocolVersion(
        release3_6,
        newProtocolVersion = 36,
        oldSequencer = "sequencer2",
        oldMediator = "mediator2",
        newSequencer = "sequencer3",
        newMediator = "mediator3",
      )
    }

    "upgrade the binaries to the current version and the synchronizer to pv=37" in { implicit env =>
      upgradeToCurrentVersion(
        ProtocolVersion.v37,
        oldSequencerName = "sequencer3",
        oldMediatorName = "mediator3",
        newSequencerName = "sequencer4",
        newMediatorName = "mediator4",
      )
    }
  }
}

private object LsuAcrossReleasesIntegrationTest {

  /** participant1 hosts Alice and Bob, participant2 hosts Bank */
  val participants: Seq[String] = Seq("participant1", "participant2")

  /** Sequencer config keys unknown to the releases that have to be removed explicitly: the releases
    * report them relative to `sequencer.block`, so `fail-on-unknown-config-keys = false` keeps
    * them.
    */
  private val sequencerBlockKeysUnknownToReleases = Seq(
    "circuit-breaker.messages.lsu-sequencing-test",
    "throughput-cap.delayed-activation",
    "throughput-cap.messages.confirmation-response",
  )

  /** Removed from the configs of the sequencers started on a release */
  val nodeConfigPathsToRemove: Set[(String, Option[(String, Any)])] =
    (for {
      sequencer <- Seq("sequencer1", "sequencer2", "sequencer3")
      key <- sequencerBlockKeysUnknownToReleases
    } yield s"sequencers.$sequencer.sequencer.block.$key" -> Option.empty[(String, Any)]).toSet
}
