// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.acs

import better.files.*
import com.digitalasset.canton.TempDirectory
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  PostgresDumpRestore,
  UseBftSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

/** Common contract for creating contracts, create - and restore snapshots on the nodes.
  *
  * Since the methods are side effecting (giving back Unit) therefore they are irrespective of what
  * contracts they work on.
  *
  * @tparam F
  *   importing contracts can be an async (eg. Future, IO) or sync (eg. Id Monad) computation
  */
trait LargeAcsSnapshottingBase[F[+_]] {

  protected def createContracts()(implicit
      env: TestConsoleEnvironment
  ): Unit
  protected def createSnapshot(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
  )(implicit
      env: TestConsoleEnvironment,
      executionContext: ExecutionContext,
  ): Unit
  protected def restoreSnapshot(
      node: LocalInstanceReference
  )(implicit
      env: TestConsoleEnvironment,
      executionContext: ExecutionContext,
  ): F[Unit]
}

/** The basic contracts for doing ACS snapshot creation/restore
  *
  * The tests should operate on an instance of AcsTestSet, see
  * [[LargeAcsIntegrationTestBase.AcsTestSet]]
  *
  * It gives some foundation and abstraction for the known implementers:
  *   - [[com.digitalasset.canton.integration.tests.manual.acs.LargeAcsExportAndImportTestBase]]
  *   - [[com.digitalasset.canton.integration.tests.manual.acs.commitment.AcsCommitmentScalabilityTestBase]]
  *
  * Both the above tests rely on dumping PG data into files and reuse them for more test iteration
  * on the same testSet.
  */
private[tests] trait LargeAcsIntegrationTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with LargeAcsSnapshottingBase[Future] {

  /** Test definition, in particular how many active contracts should be used */
  protected def testSet: LargeAcsIntegrationTestBase.AcsTestSet

  // ACS commitment reconciliation Interval between participants
  protected def reconciliationInterval: PositiveSeconds
  protected def baseEnvironmentDefinition: EnvironmentDefinition
  protected def forceLocalPostgres: Boolean
  protected def localSequencerToParticipantRefsMap(implicit
      env: TestConsoleEnvironment
  ): Map[SequencerReference, List[LocalParticipantReference]]
  protected def networkTopologyDescription(implicit
      env: TestConsoleEnvironment
  ): List[NetworkTopologyDescription]

  protected def environmentDefinition: EnvironmentDefinition = baseEnvironmentDefinition

  // Need the persistence for dumping and restoring large ACS
  protected val referenceSequencer = new UseBftSequencer(loggerFactory)
  registerPlugin(referenceSequencer)
  protected val pgPlugin = new UsePostgres(loggerFactory)
  registerPlugin(pgPlugin)
  protected val dumpRestore: PostgresDumpRestore =
    PostgresDumpRestore(pgPlugin, forceLocal = forceLocalPostgres)

  protected def testSetup()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    localSequencerToParticipantRefsMap.values.flatten[LocalParticipantReference].foreach(_.start())
    sequencers.local.start()
    mediators.local.start()

    NetworkBootstrapper(networkTopologyDescription).bootstrap()

    localSequencerToParticipantRefsMap.foreach { case (sequencerRef, participantRefs) =>
      participantRefs.foreach { participant =>
        participant.synchronizers.connect_local(sequencerRef, daName)
        participant.dars.upload(CantonExamplesPath).discard
      }
    }

    initializedSynchronizers.foreach { case (_, initializedSynchronizer) =>
      initializedSynchronizer.synchronizerOwners.foreach(
        _.topology.synchronizer_parameters
          .propose_update(
            initializedSynchronizer.synchronizerId,
            _.update(reconciliationInterval = reconciliationInterval.toConfig),
          )
      )
    }
  }

  protected def grabPartyId(participant: ParticipantReference, name: String): PartyId =
    clue(s"Grabbing party $name on ${participant.name}")(
      participant.parties
        .hosted(filterParty = name)
        .headOption
        .map(_.party)
        .valueOrFail(s"Where is party $name?")
    )

  override protected def createSnapshot(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
  )(implicit
      env: TestConsoleEnvironment,
      executionContext: ExecutionContext,
  ): Unit = {
    val nodes = env.mergeLocalInstances(participants, sequencers, mediators)

    // Stop participants, then synchronizer (order intentional)
    nodes.foreach(_.stop())
    Await.result(
      MonadUtil.sequentialTraverse_(nodes) { node =>
        val filename = s"${node.name}.${testSet.dumpFileExtension}"

        dumpRestore.saveDump(node, testSet.dumpDirectory.toTempFile(filename))
      },
      60.minutes,
    ) // DB dump can be a bit slow
    nodes.foreach(_.start())
    participants.foreach(_.synchronizers.reconnect_all())
  }

  override protected def restoreSnapshot(node: LocalInstanceReference)(implicit
      env: TestConsoleEnvironment,
      executionContext: ExecutionContext,
  ): Future[Unit] = {
    val filename = s"${node.name}.${testSet.dumpFileExtension}"
    dumpRestore
      .restoreDump(node, (testSet.dumpDirectory / filename).path)
  }

  protected def loadOrCreateContracts(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
      networkTopologyDescriptions: Seq[NetworkTopologyDescription],
  )(implicit env: TestConsoleEnvironment, executionContext: ExecutionContext): Unit = if (
    testSet.isEmptyDumpDirectory
  ) {
    testSetup()
    createContracts()
    createSnapshot(
      sequencers = sequencers,
      mediators = mediators,
      participants = participants,
    )
  } else restoreState(sequencers, mediators, participants, networkTopologyDescriptions)

  protected def restoreState(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
      networkTopologyDescriptions: Seq[NetworkTopologyDescription],
  )(implicit env: TestConsoleEnvironment, executionContext: ExecutionContext): Unit =
    handleStartupLogs {
      val nodes = env.mergeLocalInstances(participants, sequencers, mediators)
      nodes.foreach(_.stop())

      clue("Restoring database") {
        Await.result(MonadUtil.sequentialTraverse_(nodes)(restoreSnapshot(_)), 15.minutes)
      }

      clue("Starting all nodes") {
        nodes.foreach(_.start())
        new NetworkBootstrapper(networkTopologyDescriptions*).bootstrap()
        eventually(timeUntilSuccess = 2.minutes, maxPollInterval = 15.seconds) {
          nodes.forall(_.is_initialized)
        }
      }

      participants.foreach(_.synchronizers.reconnect_all())

      clue(
        "Ensure that all participants are up-to-date with the state of the topology " +
          "at the given time as returned by the synchronizer"
      )(
        participants.foreach(_.testing.fetch_synchronizer_times())
      )
      clue("Successfully loaded dumps for all nodes")(
        participants.head.health.ping(participants.head.id)
      )
    }
}

private[tests] object LargeAcsIntegrationTestBase {
  sealed trait AcsTestSet {
    def acsSize: PositiveInt
    def temporaryContracts: NonNegativeInt
    def name: String
    def dumpDirectory: TempDirectory
    def dumpFileExtension: String
    def creationBatchSize: PositiveInt
    def isEmptyDumpDirectory: Boolean
    def exportDirectory: File
    def acsImportDurationBoundMs: Long
  }

  object AcsTestSet {
    // TestSet types
    final case class DefaultAcsTestSet(
        acsSize: PositiveInt,
        temporaryContracts: NonNegativeInt,
        name: String,
        directory: File,
        creationBatchSize: PositiveInt,
    ) extends AcsTestSet {
      override val dumpFileExtension = "pg_dump"

      override def dumpDirectory: TempDirectory =
        TempDirectory((directory / "dump").createDirectoryIfNotExists(createParents = true))

      override def isEmptyDumpDirectory =
        dumpDirectory.directory.list.isEmpty || !dumpDirectory.directory.children.exists { f =>
          f.isRegularFile &&
          f.extension.contains(s".$dumpFileExtension") &&
          f.size > 1024 // Ignore files smaller than 1KB - they might be corrupted
        }

      override def exportDirectory: File =
        (directory / "export").createDirectoryIfNotExists(createParents = true)

      /** bound estimation (linearly fitted); tripling it as a safety margin. For the generic
        * formula see
        * [[com.digitalasset.canton.integration.tests.manual.acs.LargeAcsExportAndImportTestBase]]
        * example time reference table
        */
      override def acsImportDurationBoundMs: Long = (3 * (0.59 * acsSize.value + 18500)).toLong
    }

    /** 1000 -> "1_000", 1000000 -> "1_000_000" */
    def formatWithUnderscores(number: Int): String =
      number.toString.reverse.grouped(3).mkString("_").reverse

    def apply(
        acsSize: PositiveInt,
        temporaryContracts: NonNegativeInt,
        directorySuffix: String = getClass.getSimpleName,
    ): AcsTestSet = {
      val testName = formatWithUnderscores(acsSize.value)
      val creationBatchSize = PositiveInt.tryCreate(400)

      val testDirectory =
        File.currentWorkingDirectory / "tmp" / s"$testName-temporary=${temporaryContracts}_$directorySuffix"
      new DefaultAcsTestSet(
        acsSize = acsSize,
        temporaryContracts = temporaryContracts,
        name = testName,
        directory = testDirectory,
        creationBatchSize = creationBatchSize,
      )
    }
  }
}
