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
  UseBftSequencer,
  UseExternalConsole,
  UseExternalProcess,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils
import com.digitalasset.canton.topology.{Member, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ReleaseVersion,
  ReleaseVersionToProtocolVersions,
}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Chains logical synchronizer upgrades across released Canton binaries, then to the current
  * version, and checks that the contracts created before each upgrade remain usable.
  *
  * Tested versions:
  *   - the latest stable 3.5 release, on which the synchronizer is bootstrapped with pv=34
  *   - for each minor release from 3.5: its latest stable release, or its latest snapshot if it has
  *     none, with the latest stable protocol version of the minor
  *   - the current version, with its latest protocol version that is neither dev nor almost dev
  *
  * Test setup:
  *   - all nodes run as external processes, using released binaries or the current version
  *   - nodes are started only when the step that needs them is reached
  *   - for each version: if the release changes, the binaries of the running nodes are replaced
  *     (same protocol version), then, if the protocol version changes, an LSU to the protocol
  *     version of the version is performed onto new synchronizer nodes
  *   - console commands of a release are run by the console of the release, using scripts in
  *     `all-the-lsus/<major>.<minor>-<script>`, or `all-the-lsus/<script>` if there is none;
  *     console commands of the current version are run by the test
  */
final class LsuAcrossReleasesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with LogicalUpgradeUtils {

  import LsuAcrossReleasesIntegrationTest.*

  /** Protocol versions overriding the latest stable protocol version of each minor release */
  private val protocolVersionOverrides: Map[(Int, Int), ProtocolVersion] = Map.empty

  /** Releases tested after the minor releases and before the current version */
  private val additionalReleases: Seq[(ReleaseVersion, ProtocolVersion)] = Seq.empty

  private val firstProtocolVersion = 34

  /** Time between the LSU announcement and the upgrade time */
  private val upgradeTimeDelay = 60.seconds

  /** Time allowed for the upgrade time to be reached and for the participants to reconnect */
  private val lsuTimeout = 5.minutes

  private lazy val releases = ReleaseUtils.listAllReleases()

  /** Latest stable release of the minor, or its latest snapshot if it has none */
  private def latestRelease(major: Int, minor: Int): ReleaseVersion = {
    val (stables, snapshots) = releases.filter(_.majorMinor == (major, minor)).partition(_.isStable)
    stables.maxOption
      .orElse(snapshots.maxOption)
      .getOrElse(sys.error(s"No release found for $major.$minor"))
  }

  private lazy val firstRelease = latestRelease(3, 5)

  /** Latest protocol version of the current version, ignoring the dev ones */
  private lazy val latestNonDevProtocolVersion: ProtocolVersion =
    (ProtocolVersion.stable.forgetNE ++ ProtocolVersion.alpha)
      .filterNot(_ == ProtocolVersion.almostDev)
      .maxBy(_.v)

  private lazy val steps: Seq[Step] = {
    val (major, firstMinor) = firstRelease.majorMinor
    val minorReleases = (firstMinor until ReleaseVersion.current.minor).map { minor =>
      latestRelease(major, minor) -> protocolVersionOverrides.getOrElse(
        (major, minor),
        ReleaseVersionToProtocolVersions
          .majorMinorToStableProtocolVersions((major, minor))
          .maxBy(_.v),
      )
    }
    val versions =
      (((firstRelease -> (ProtocolVersion.v34: ProtocolVersion)) +: minorReleases) ++ additionalReleases)
        .map { case (release, protocolVersion) =>
          Option(release) -> protocolVersion
        } :+
        (None -> latestNonDevProtocolVersion)

    val protocolVersions = versions.map { case (_, protocolVersion) => protocolVersion }
    val synchronizerIndices = protocolVersions
      .zip(protocolVersions.drop(1))
      .scanLeft(1) { case (index, (previous, next)) => if (previous == next) index else index + 1 }

    versions.zip(synchronizerIndices).map { case ((release, protocolVersion), index) =>
      Step(release, protocolVersion, index)
    }
  }

  private lazy val sequencers = steps.map(_.sequencer).distinct
  private lazy val mediators = steps.map(_.mediator).distinct

  override protected def testName: String = "lsu-across-releases"

  registerPlugin(new UsePostgres(loggerFactory))
  // One synchronizer per version. The synchronizers created before 3.6 use the reference sequencer,
  // as the BFT orderer of 3.5 cannot read the database of a 3.4 BFT sequencer; the later ones use the
  // BFT sequencer.
  private lazy val (referenceSequencers, bftSequencers) = sequencers.partition { sequencer =>
    steps.find(_.sequencer == sequencer).flatMap(_.release).exists { release =>
      val (major, minor) = release.majorMinor
      major == 3 && minor < 6
    }
  }

  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(referenceSequencers.map(Set(_))*),
    )
  )
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(bftSequencers.map(Set(_))*),
    )
  )
  registerPlugin(external)

  private lazy val external =
    new UseExternalProcess(
      loggerFactory,
      externalSequencers = sequencers.toSet,
      externalMediators = mediators.toSet,
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
      removeConfigPaths = nodeConfigPathsToRemove(
        releaseSequencers = steps.collect { case s if s.release.nonEmpty => s.sequencer }.distinct
      ),
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = participants.size,
        numSequencers = sequencers.size,
        numMediators = mediators.size,
      )
      .withManualStart
      .addConfigTransforms(
        ConfigTransforms.disableAutoInit((sequencers.drop(1) ++ mediators.drop(1)).toSet),
        ConfigTransforms.enableNonStandardConfig,
        _.focus(_.parameters.enableKeyDownload).replace(true),
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

  private def startNodes(step: Step, names: String*)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    names.foreach { name =>
      val configOverrides = alphaVersionSupport(step.protocolVersion, name)
      step.release match {
        case Some(release) =>
          external.start(
            name,
            ReleasePath(binary(release)),
            failOnUnknownConfigKeys = false,
            configOverrides = configOverrides,
          )
        case None => external.startCurrentVersion(name, configOverrides)
      }
    }

  /** Replace the binaries of the given nodes with the ones of `step`, keeping their storage */
  private def upgradeBinaries(step: Step, names: String*)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    names.foreach(external.kill(_, force = false))
    startNodes(step, names*)
  }

  /** Run `all-the-lsus/<major>.<minor>-<script>`, or `all-the-lsus/<script>` if there is none, with
    * the console of `release`
    */
  private def runScript(
      release: ReleaseVersion,
      protocolVersion: ProtocolVersion,
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
      ) ++ Option.when(protocolVersion.isAlpha)(
        "-C canton.parameters.alpha-version-support=true"
      ) ++ systemProperties,
      fileNameHint = fileNameHint,
    ).runScript(s"all-the-lsus/${scriptOf(release, script)}")
  }

  private def scriptOf(release: ReleaseVersion, script: String): String = {
    val releaseScript = s"${release.major}.${release.minor}-$script"
    if (getClass.getResource(s"/all-the-lsus/$releaseScript") != null) releaseScript else script
  }

  /** If the release changes, replace the binaries of the synchronizer nodes of `previous` and of
    * the participants with the ones of `step`. If the protocol version changes, perform an LSU onto
    * the synchronizer nodes of `step`.
    */
  private def upgrade(previous: Step, step: Step)(implicit env: TestConsoleEnvironment): Unit = {
    val oldNodes = Seq(previous.sequencer, previous.mediator)
    if (step.release != previous.release) upgradeBinaries(step, (oldNodes ++ participants)*)

    if (step.hasNewSynchronizer(previous)) {
      startNodes(step, step.sequencer, step.mediator)
      waitForInitializationAndReconnect(previous.sequencer, previous.mediator)

      step.release match {
        case Some(release) => lsuWithReleaseConsole(release, previous, step)
        case None => lsuWithCurrentConsole(previous, step)
      }

      val successorPsid = env.s(step.sequencer).physical_synchronizer_id
      waitForParticipantsConnected(successorPsid)
      checkExtraTrafficPurchased(step.sequencer)
      checkContracts(successorPsid)

      oldNodes.foreach(external.stop)
    } else {
      waitForInitializationAndReconnect(previous.sequencer, previous.mediator)
      waitForParticipantsConnected(env.s(previous.sequencer).physical_synchronizer_id)
      env.p("participant1").health.ping(env.p("participant2").id)
    }
  }

  /** Perform an LSU onto the synchronizer nodes of `step` with the console of `release` */
  private def lsuWithReleaseConsole(release: ReleaseVersion, previous: Step, step: Step)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    val exportDirectory = File.newTemporaryDirectory(s"all-the-lsus-$release-")
    val upgradeTime = CantonTimestamp.now().add(upgradeTimeDelay)

    runScript(
      release,
      step.protocolVersion,
      "lsu.canton",
      Map(
        "all-the-lsus.export-dir" -> exportDirectory.pathAsString,
        "all-the-lsus.upgrade-time" -> upgradeTime.toInstant.toString,
        "all-the-lsus.protocol-version" -> step.protocolVersion.v.toString,
        "all-the-lsus.old-sequencer" -> previous.sequencer,
        "all-the-lsus.old-mediator" -> previous.mediator,
        "all-the-lsus.new-sequencer" -> step.sequencer,
        "all-the-lsus.new-mediator" -> step.mediator,
      ),
    )
  }

  /** Required by the nodes running a synchronizer on an alpha protocol version */
  private def alphaVersionSupport(
      protocolVersion: ProtocolVersion,
      name: String,
  ): Map[String, String] =
    if (!protocolVersion.isAlpha) Map.empty
    else
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

  /** Perform an LSU onto the synchronizer nodes of `step` with the console of the current version
    */
  private def lsuWithCurrentConsole(previous: Step, step: Step)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    val oldSequencer = env.s(previous.sequencer)
    val oldMediator = env.m(previous.mediator)
    val newSequencer = env.s(step.sequencer)
    val newMediator = env.m(step.mediator)

    val upgradeTime = CantonTimestamp.now().add(upgradeTimeDelay)
    val currentPsid = oldSequencer.physical_synchronizer_id
    val successorParameters = StaticSynchronizerParameters.defaults(
      CryptoConfig(),
      step.protocolVersion,
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
    s"bootstrap a synchronizer on 3.5 with pv=$firstProtocolVersion" in { implicit env =>
      val firstStep = steps.head
      startNodes(firstStep, (Seq(firstStep.sequencer, firstStep.mediator) ++ participants)*)

      runScript(firstRelease, firstStep.protocolVersion, "bootstrap.canton")
      val psid = env.s(firstStep.sequencer).physical_synchronizer_id
      psid.protocolVersion shouldBe ProtocolVersion.v34
      psid.serial shouldBe NonNegativeInt.zero
      purchaseExtraTraffic(firstStep.sequencer)
    }

    steps.zip(steps.drop(1)).foreach { case (previous, step) =>
      val pv = step.protocolVersion.v
      val description =
        if (step.release == previous.release) s"upgrade the synchronizer to pv=$pv on ${step.name}"
        else if (step.hasNewSynchronizer(previous))
          s"upgrade the binaries to ${step.name} and the synchronizer to pv=$pv"
        else s"upgrade the binaries to ${step.name} and keep the synchronizer on pv=$pv"

      description in { implicit env =>
        upgrade(previous, step)
      }
    }
  }
}

private object LsuAcrossReleasesIntegrationTest {

  /** participant1 hosts Alice and Bob, participant2 hosts Bank */
  val participants: Seq[String] = Seq("participant1", "participant2")

  /** Nodes running `release` (the current version if empty), with a synchronizer on
    * `protocolVersion` served by the synchronizer nodes of index `synchronizerIndex`
    */
  final case class Step(
      release: Option[ReleaseVersion],
      protocolVersion: ProtocolVersion,
      synchronizerIndex: Int,
  ) {
    def sequencer: String = s"sequencer$synchronizerIndex"
    def mediator: String = s"mediator$synchronizerIndex"

    def name: String = release.fold("the current version")(r => s"${r.major}.${r.minor}")

    def hasNewSynchronizer(previous: Step): Boolean =
      synchronizerIndex != previous.synchronizerIndex
  }

  /** Sequencer config keys unknown to the releases that have to be removed explicitly: the releases
    * report them relative to `sequencer.block`, so `fail-on-unknown-config-keys = false` keeps
    * them.
    */
  private val sequencerBlockKeysUnknownToReleases = Seq(
    "circuit-breaker.messages.lsu-sequencing-test",
    "throughput-cap.delayed-activation",
    "throughput-cap.messages.confirmation-response",
  )

  private def toRemovals(paths: Seq[String]): Set[(String, Option[(String, Any)])] =
    paths.map(_ -> Option.empty[(String, Any)]).toSet

  /** Removed from the configs of the external nodes started on a release */
  def nodeConfigPathsToRemove(
      releaseSequencers: Seq[String]
  ): Set[(String, Option[(String, Any)])] = toRemovals(
    for {
      sequencer <- releaseSequencers
      key <- sequencerBlockKeysUnknownToReleases
    } yield s"sequencers.$sequencer.sequencer.block.$key"
  )
}
