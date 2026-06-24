// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import better.files.File
import cats.Eval
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.ledger.api.v2.commands.Command.toJavaProto
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.javaapi as javab
import com.daml.ledger.javaapi.data.{Command, Transaction}
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  AssignedWrapper,
  ReassignmentWrapper,
  TopologyTransactionWrapper,
  TransactionWrapper,
  UnassignedWrapper,
  UpdateWrapper,
}
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.console.{
  CommandFailure,
  DebuggingHelpers,
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.crypto.{Crypto, SyncCryptoApiParticipantProvider, SynchronizerCrypto}
import com.digitalasset.canton.damltests.java.conflicttest
import com.digitalasset.canton.damltestslf23.java.basickeys.KeyOps
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Dummy
import com.digitalasset.canton.examples.java.{cycle, cycle as C}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.IntegrationTestUtilities.{
  extractSubmissionResult,
  grabCountsRemote,
  poll,
}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseExternalProcess,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.crashrecovery.ParticipantRestartTest.*
import com.digitalasset.canton.integration.util.TestUtils.damlSet
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.SubmissionAlreadyInFlight
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{ErrorLoggingContext, LogEntry}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors
import com.digitalasset.canton.participant.protocol.{RequestJournal, TransactionProcessor}
import com.digitalasset.canton.participant.pruning.PruningProcessor
import com.digitalasset.canton.participant.store.db.DbParticipantPruningStore
import com.digitalasset.canton.participant.store.{
  ParticipantNodePersistentState,
  ParticipantPruningStore,
  StoredSynchronizerConnectionConfig,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncPersistentStateManager,
}
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  LfContractId,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, StorageSingleFactory}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.store.db.DbIndexedStringStore
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.time.{PositiveSeconds, SimClock}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  KnownPhysicalSynchronizerId,
  Party,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  CloseableTest,
  LfTimestamp,
  RequestCounter,
  SequencerAlias,
  SynchronizerAlias,
  config,
}
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalactic.source.Position
import org.scalatest.Assertion
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Minutes, Seconds, Span}

import java.nio.charset.StandardCharsets
import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.annotation.{nowarn, tailrec}
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.math.Ordering.Implicits.*
import scala.util.Random

abstract class ParticipantRestartTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasProgrammableSequencer
    with CloseableTest {

  protected lazy val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set("participant1", "participant2"),
      fileNameHint = this.getClass.getSimpleName,
    )

  registerPlugin(
    new UsePostgres(loggerFactory)
  ) // needs to be before the external process such that we pick up the postgres config changes
  registerPlugin(external)

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  def filterCompletionsByCommandId(commandId: String)(completion: Completion): Boolean =
    completion.commandId == commandId

  /** @param noAutoReconnect
    *   if set synchronizer will be registered, and connected to, but auto-reconnection will not
    *   happen at restart
    */
  def connectToDa(
      participant: ParticipantReference,
      noAutoReconnect: Boolean = false,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participant.synchronizers.connect_local(
      sequencer = sequencer1,
      alias = daName,
      manualConnect = noAutoReconnect,
    )
    if (noAutoReconnect) {
      participant.synchronizers.reconnect(daName, retry = false).discard
    }
    eventually() {
      participant.synchronizers.list_connected() should have length 1
    }
  }

  protected def stopAndRestart(
      participant: RemoteParticipantReference,
      runBeforeReconnect: Option[() => Seq[SynchronizerAlias]] = None,
      runAfterStopBeforeRestart: => Unit = (),
      numSynchronizers: Long = 1L,
  ): Unit = {
    logger.info(s"Stopping participant ${participant.name}")

    restart(participant, runAfterStopBeforeRestart)
    val expectedSynchronizers = runBeforeReconnect match {
      case Some(f) =>
        val errString =
          "runBeforeReconnect was specified, but some synchronizers already connected automatically. If runBeforeReconnect is used, it needs to be ensured for the related participant, that none of the set up synchronizers are auto-connecting, so runBeforeConnect is really running before any connected synchronizers. This can be achieved with like 'connectToDa(participant1, noAutoReconnect = true)'."
        withClue(errString) {
          participant.synchronizers.list_connected() should have length 0
        }
        val toReconnect = f()
        withClue(errString) {
          participant.synchronizers.list_connected() should have length 0
        }
        toReconnect.foreach(
          participant.synchronizers.reconnect(_, retry = false)
        )
        toReconnect.size.toLong

      case None =>
        participant.synchronizers.reconnect_all()
        numSynchronizers
    }

    withClue("id should be set after restart") {
      participant.health.initialized() shouldBe true
    }

    logger.info(s"Restarted participant ${participant.name}")

    eventually() {
      withClue("synchronizer should be connected after restart") {
        participant.synchronizers.list_connected() should have length expectedSynchronizers
      }
    }

    logger.info(s"Participant ${participant.name} is connected again after restart")
  }

  def manyPings(
      pingAttempts: Int,
      proportionSucceeded: Double,
      participant: ParticipantReference,
      timeout: Timeout,
      allowInParallel: Int,
  )(implicit executionContext: ExecutionContext): Unit = {
    require(proportionSucceeded > 0.0)
    require(proportionSucceeded < 1.0)
    require(pingAttempts > 0)
    require(allowInParallel > 0)
    require(timeout.value > Span(0, Seconds))

    // Ping timeout should be lower than the timeout for the futures because we don't need that all pings succeed.
    val pingTimeout =
      config.NonNegativeDuration.tryFromDuration((timeout.value.toMillis / 2).millis)

    def pingFuture(): Future[Option[Duration]] = {
      val future = Future(
        participant.health.maybe_ping(participant.id, timeout = pingTimeout)
      )
      // Try to trigger the participant processing the ping
      Thread.`yield`()
      future
    }

    def parallelPings(num: Int): Int = {
      val pings: Seq[Future[Option[Duration]]] = (1 to num).map(_ => pingFuture())
      val succeeded = pings.map { ping =>
        val duration: Option[Duration] = ping.futureValue(timeout)
        if (duration.isDefined) 1 else 0
      }
      succeeded.sum
    }

    @tailrec def allPings(remaining: Int, succeededSoFar: Int): Int = {
      require(remaining >= 0)
      if (remaining == 0) {
        succeededSoFar
      } else {
        val count = Math.min(allowInParallel, remaining)
        val succeeded = parallelPings(count)
        allPings(remaining - count, succeededSoFar + succeeded)
      }
    }

    val succeeded = allPings(pingAttempts, 0)
    logger.info(s"Generated $pingAttempts attempted pings, of which $succeeded succeeded.")
    assert(succeeded >= pingAttempts * proportionSucceeded)
  }

  def startAndGet(
      instanceName: String
  )(implicit env: TestConsoleEnvironment): RemoteParticipantReference = {
    val participant = env.rp(instanceName)
    external.start(participant.name)
    waitUntilRunning(participant)
    participant
  }

  protected def waitUntilRunning(participant: RemoteParticipantReference): Unit =
    eventually(60.seconds) {
      participant.health.status match {
        case NodeStatus.Success(_) => ()
        case err => fail(s"remote participant ${participant.name} is not starting up: $err")
      }
    }

  protected def restart(participant: RemoteParticipantReference, action: => Unit = ()): Unit = {
    external.kill(participant.name)
    logger.info(s"Stopped external participant ${participant.name}, restarting now")
    action
    external.start(participant.name)
    waitUntilRunning(participant)
  }

  /** Get a storage object to read from the database of the remote participant */
  protected def createStorageFor(
      participant: RemoteParticipantReference
  )(implicit env: TestConsoleEnvironment): DbStorage = {
    import env.*
    val storage =
      new StorageSingleFactory(external.storageConfig(participant.name))
        .create(
          connectionPoolForParticipant = true,
          None,
          new SimClock(CantonTimestamp.Epoch, loggerFactory),
          None,
          ParticipantTestMetrics.dbStorage,
          timeouts,
          loggerFactory,
        )
        .valueOrFailShutdown("Failed to create DbStorage")
    env.environment.addUserCloseable(storage)
    val dbStorage = storage match {
      case dbStorage: DbStorage => dbStorage
      case _: MemoryStorage => fail("Storage must be DbStorage")
    }
    dbStorage
  }

  private def stateManagerFor(
      participant: RemoteParticipantReference,
      storage: DbStorage,
  )(implicit
      env: TestConsoleEnvironment
  ): (ParticipantNodePersistentState, SyncPersistentStateManager) = {
    import env.*
    val cryptoConfig = CryptoConfig()
    val connectionConfigStore = mock[SynchronizerConnectionConfigStore]

    val crypto =
      timeouts.default.await("Build test crypto")(
        Crypto
          .create(
            cryptoConfig,
            CachingConfigs.defaultKmsMetadataCache,
            SessionEncryptionKeyCacheConfig(),
            CachingConfigs.defaultPublicKeyConversionCache,
            storage,
            Option.empty[ReplicaManager],
            testedReleaseProtocolVersion,
            futureSupervisor,
            wallClock,
            CommonMockMetrics.cryptoMetrics,
            executionContext,
            timeouts,
            BatchingConfig(),
            loggerFactory,
            NoReportingTracerProvider,
            new ExecutorServiceMetrics(NoOpMetricsFactory),
          )
          .valueOrFailShutdown("create pure crypto")
      )
    val parameters = ParticipantNodeParameters.forTestingOnly(testedProtocolVersion)
    val indexedStringStore = new DbIndexedStringStore(storage, timeouts, loggerFactory)
    // need to fetch synchronizers once, assuming that everything is connected when the state inspection object is created
    val mapping = participant.synchronizers.list_connected()

    when(connectionConfigStore.get(any[PhysicalSynchronizerId]))
      .thenAnswer((psid: PhysicalSynchronizerId) =>
        mapping
          .find(_.physicalSynchronizerId == psid)
          .map { m =>
            StoredSynchronizerConnectionConfig(
              SynchronizerConnectionConfig(
                synchronizerAlias = m.synchronizerAlias,
                sequencerConnections = SequencerConnections.single(
                  GrpcSequencerConnection(
                    NonEmpty(Set, Endpoint("not-relevant", Port.tryCreate(1))),
                    transportSecurity = false,
                    customTrustCertificates = None,
                    sequencerAlias = SequencerAlias.tryCreate("not used"),
                    sequencerId = None,
                  )
                ),
              ),
              status = SynchronizerConnectionConfigStore.Active,
              configuredPsid = KnownPhysicalSynchronizerId(psid),
              predecessor = None,
            )
          }
          .toRight(SynchronizerConnectionConfigStore.UnknownPsid(psid))
      )
    val aliasResolution = new SynchronizerAliasResolution() {
      override def synchronizerIdForAlias(alias: SynchronizerAlias): Option[SynchronizerId] =
        mapping.find(_.synchronizerAlias == alias).map(_.physicalSynchronizerId)

      override def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias] =
        mapping.find(_.synchronizerId == id).map(_.synchronizerAlias)

      override def synchronizerIdsForAlias(
          alias: SynchronizerAlias
      ): Option[NonEmpty[Set[PhysicalSynchronizerId]]] = NonEmpty.from(mapping.collect {
        case m if m.synchronizerAlias == alias => m.physicalSynchronizerId
      }.toSet)

      override def physicalSynchronizerIds(id: SynchronizerId): Set[PhysicalSynchronizerId] =
        mapping.collect { case m if m.synchronizerId == id => m.physicalSynchronizerId }.toSet

      override def close(): Unit = ()

      override def aliases: Set[SynchronizerAlias] = mapping.map(_.synchronizerAlias).toSet

      override def physicalSynchronizerIds: Set[PhysicalSynchronizerId] =
        mapping.map(_.physicalSynchronizerId).toSet

      override def logicalSynchronizerIds: Set[SynchronizerId] =
        physicalSynchronizerIds.map(_.logical)
    }

    Await
      .result(
        for {
          pnps <- ParticipantNodePersistentState
            .create(
              storage,
              external.storageConfig(participant.name),
              None,
              parameters,
              testedReleaseProtocolVersion,
              ParticipantTestMetrics,
              participant.id.toLf,
              LedgerApiServerConfig(),
              futureSupervisor,
              loggerFactory,
            )
            .failOnShutdown
          stateManager = new SyncPersistentStateManager(
            participant.id,
            aliasResolution,
            storage,
            indexedStringStore,
            pnps.acsCounterParticipantConfigStore,
            parameters,
            TopologyConfig(),
            connectionConfigStore,
            (staticSynchronizerParameters: StaticSynchronizerParameters) =>
              SynchronizerCrypto(crypto, staticSynchronizerParameters),
            env.environment.clock,
            Eval.now(pnps.ledgerApiStore),
            Eval.now(pnps.contractStore),
            ParticipantTestMetrics,
            futureSupervisor,
            loggerFactory,
          )
          _ = env.environment.addUserCloseable(pnps)
          _ <- stateManager.initializePersistentStates().failOnShutdown
        } yield (pnps, stateManager),
        100.seconds,
      )
  }

  protected def pruningProcessorFor(
      participant: RemoteParticipantReference,
      storage: Option[DbStorage] = None,
  )(implicit env: TestConsoleEnvironment): PruningProcessor = {
    import env.*
    val dbStorage = storage.getOrElse(createStorageFor(participant))
    val (participantNodePersistentState, stateManager) = stateManagerFor(participant, dbStorage)

    val synchronizerConnectionConfigStore = mock[SynchronizerConnectionConfigStore]
    when(synchronizerConnectionConfigStore.getAllStatusesFor(any[SynchronizerId]))
      .thenReturn(Right(NonEmpty.mk(Seq, SynchronizerConnectionConfigStore.Active)))

    // StoredSynchronizerConnectionConfig is final and cannot be mocked.
    // Therefore, we need to construct the full object, just to "mock" configuredPsid.
    val activeSynchronizer =
      StoredSynchronizerConnectionConfig(
        SynchronizerConnectionConfig(
          synchronizerAlias = daName,
          sequencerConnections = SequencerConnections.single(
            GrpcSequencerConnection(
              NonEmpty(Set, Endpoint("not-relevant", Port.tryCreate(1))),
              transportSecurity = false,
              customTrustCertificates = None,
              sequencerAlias = SequencerAlias.tryCreate("not used"),
              sequencerId = None,
            )
          ),
        ),
        status = SynchronizerConnectionConfigStore.Active,
        configuredPsid = KnownPhysicalSynchronizerId(daId), // this is the relevant field
        predecessor = None,
      )
    when(synchronizerConnectionConfigStore.getActive(any[SynchronizerId]))
      .thenReturn(Right(activeSynchronizer))
    when(synchronizerConnectionConfigStore.getAllFor(any[SynchronizerId]))
      .thenReturn(Right(NonEmpty.mk(Seq, activeSynchronizer)))

    val aliasResolution = mock[SynchronizerAliasResolution]
    when(aliasResolution.logicalSynchronizerIds).thenReturn(Set(daId.logical))
    when(synchronizerConnectionConfigStore.aliasResolution).thenReturn(aliasResolution)

    new PruningProcessor(
      Eval.now(participantNodePersistentState),
      stateManager,
      maxPruningBatchSize = PositiveInt.tryCreate(100),
      ParticipantTestMetrics.pruning,
      exitOnFatalFailures = true,
      synchronizerConnectionConfigStore,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
  }

  protected def pruneParticipant(
      participant: ParticipantReference,
      pruneOffset: Long,
  ): Unit =
    participant.pruning.prune(pruneOffset)

  /** get state inspection object for a remote participant (invoke only when the participant is
    * properly initialised and connected)
    */
  protected def stateInspectionFor(
      participant: RemoteParticipantReference,
      storage: Option[DbStorage] = None,
  )(implicit env: TestConsoleEnvironment): SyncStateInspection = {
    import env.*
    val dbStorage = storage.getOrElse(createStorageFor(participant))
    val (participantNodePersistentState, stateManager) = stateManagerFor(participant, dbStorage)
    new SyncStateInspection(
      stateManager,
      Eval.now(participantNodePersistentState),
      mock[SynchronizerConnectionConfigStore],
      timeouts,
      mock[ConnectedSynchronizersLookup],
      mock[SyncCryptoApiParticipantProvider],
      participant.id,
      futureSupervisor,
      loggerFactory,
    )
  }

  protected def assertActiveContractsMatchBetweenCantonAndLedgerApiServer(
      participant: RemoteParticipantReference,
      state: SyncStateInspection,
  ): Assertion =
    eventually(timeUntilSuccess = 120.seconds) {
      val (syncAcs, lapiAcs) =
        DebuggingHelpers.get_active_contracts_from_internal_db_state(participant, state)

      val missingFromLapi = syncAcs.keySet.diff(lapiAcs.keySet)
      val missingFromCanton = lapiAcs.keySet.diff(syncAcs.keySet)

      missingFromLapi shouldBe Set.empty
      missingFromCanton shouldBe Set.empty
    }

  protected def assertActiveContractsMatchBetweenCantonAndLedgerApiServer(
      participant: RemoteParticipantReference
  )(implicit env: TestConsoleEnvironment): Assertion = {
    val state = stateInspectionFor(participant)
    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant, state)
  }
}

class ParticipantRestartCausalityIntegrationTest extends ParticipantRestartTest with EntitySyntax {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S2M2_Manual
      .addConfigTransforms(
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag
      )
      .withSetup { implicit env =>
        NetworkBootstrapper(EnvironmentDefinition.S1M1_S1M1)
          .bootstrap()
      }

  val module = "ConflictTest"

  override val defaultParticipant = "participant3"

  /** Checks that for each create or assign there is an archive or un-assign */
  def assertUpdatesAreBalanced(
      updates: Seq[UpdateWrapper],
      participantName: String,
      // TODO(#16192): Figure out why remote participants don't emit UnassignEvents until restart
      transactionsOnly: Boolean = false,
  ): Assertion = {

    logger.debug(s"$participantName updates:\n\n${updates.mkString("\n\n")}")

    val creates =
      updates.toList.flatMap {
        case TransactionWrapper(tx) => tx.events.toList.map(_.event.created.map(_.contractId))
        case AssignedWrapper(_, ae) if !transactionsOnly =>
          ae.iterator.map(e => e.createdEvent.map(_.contractId)).toList
        case UnassignedWrapper(_, ue) if !transactionsOnly => ue.iterator.map(_ => None).toList
        case _: ReassignmentWrapper => List.empty
        case _: TopologyTransactionWrapper => List.empty
      }
    val archives =
      updates.toList.flatMap {
        case TransactionWrapper(tx) => tx.events.toList.map(_.event.archived.map(_.contractId))
        case UnassignedWrapper(_, ue) if !transactionsOnly =>
          ue.iterator.map(e => Some(e.contractId)).toList
        case AssignedWrapper(_, ae) if !transactionsOnly => ae.iterator.map(_ => None).toList
        case _: ReassignmentWrapper => List.empty
        case _: TopologyTransactionWrapper => List.empty
      }

    logger.debug(s"$participantName has \n creates: $creates \narchives: $archives")

    /** Take a list of Some(contractId) for every create operation, and None for every archive. Swap
      * adjacent (Some(contractId), None) pairs to get a list of expected archive operations.
      */
    def expectedArchives(c: List[Option[String]]): List[Option[String]] =
      c match {
        case Nil => Nil
        case h1 :: h2 :: tail => h2 :: h1 :: expectedArchives(tail)
        case _ =>
          fail(
            s"Updates for $participantName were unbalanced. Got creates: $creates, and archives: $archives, from updates: $updates"
          )
      }

    val expected = expectedArchives(creates)
    // in a multi synchronizer world the order is only maintained per synchronizer
    archives should contain theSameElementsAs expected
  }

  def reassign(
      bob: Party,
      obs: List[PartyId],
      pkg: String,
      participantBob: ParticipantReference,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val bobAlice =
      ledger_api_utils.create(pkg, module, "Many", Map[String, Any]("sig" -> bob, "obs" -> obs))
    val submitted =
      participantBob.ledger_api.javaapi.commands
        .submit(
          Seq(bob),
          Seq(Command.fromProtoCommand(toJavaProto(bobAlice))),
          synchronizerId = Some(daId),
        )

    val id: Seq[conflicttest.Many.Contract] =
      JavaDecodeUtil.decodeAllCreated(conflicttest.Many.COMPANION)(submitted)

    val manyId = id.headOption.value.id
    val unassignedEvent =
      participantBob.ledger_api.commands
        .submit_unassign(bob, Seq(manyId.toLf), daId, acmeId)

    participantBob.ledger_api.commands.submit_assign(
      bob,
      unassignedEvent.reassignmentId,
      daId,
      acmeId,
    )

    logger.info(s"Archive")
    val archiveCmd = manyId.exerciseArchive().commands.asScala.toSeq
    participantBob.ledger_api.javaapi.commands.submit(Seq(bob), archiveCmd)
  }

  private def txAndReassignmentsFor(partyIds: Set[PartyId]): UpdateFormat = {
    val eventFormat = EventFormat(
      filtersByParty = partyIds.map(_.toLf -> Filters(Nil)).toMap,
      filtersForAnyParty = None,
      verbose = false,
    )
    UpdateFormat(
      includeTransactions = Some(
        TransactionFormat(
          eventFormat = Some(eventFormat),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        )
      ),
      includeReassignments = Some(eventFormat),
      includeTopologyEvents = None,
    )
  }

  "successfully reassign before and after restart" in { implicit env =>
    import env.*

    val participant1 = startAndGet("participant1")
    val participant2 = startAndGet("participant2")

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = daId)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = acmeId)

    val participantAlice = participant1
    val participantBob = participant2

    val alice = participantAlice.parties.testing.enable(
      "Alice",
      synchronizeParticipants = Seq(participantBob),
      synchronizer = daName,
    )
    participantAlice.parties.testing.also_enable(
      alice,
      synchronizeParticipants = Seq(participantBob),
      synchronizer = acmeName,
    )

    val bob = participantBob.parties.testing.enable(
      "Bob",
      synchronizeParticipants = Seq(participantAlice),
      synchronizer = daName,
    )
    participantBob.parties.testing.also_enable(
      bob,
      synchronizeParticipants = Seq(participantAlice),
      synchronizer = acmeName,
    )

    val pkg = participant1.packages.find_by_module(module).headOption.value.packageId

    awaitTopologyUpToDate(acmeId, staticSynchronizerParameters2, participant1, participant2)

    reassign(bob, List(alice), pkg, participantBob)

    logger.info(s"Restart $participantBob")
    stopAndRestart(participantBob, numSynchronizers = 2)

    val duration = participant1.health.ping(participant2)
    logger.info(s"p1 pings p2 in $duration")

    reassign(bob, List(alice), pkg, participantBob)

    val updatesP1 = Future(
      participant1.ledger_api.updates
        .updates(
          txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )
    val updatesP2 = Future(
      participant2.ledger_api.updates
        .updates(
          txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )

    assertUpdatesAreBalanced(updatesP1.futureValue, "participant1")
    assertUpdatesAreBalanced(updatesP2.futureValue, "participant2", transactionsOnly = true)
  }

  "continue to make progress after replaying reassignments" in { implicit env =>
    import env.*

    val participant1 = startAndGet("participant1")
    val participant2 = startAndGet("participant2")

    connectToDa(participant1)
    connectToDa(participant2)
    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = daId)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = acmeId)

    val stateInspection2 = stateInspectionFor(participant2)

    val participantAlice = participant1
    val participantBob = participant2

    val alice =
      participantAlice.parties.testing.enable(
        "Alice",
        synchronizeParticipants = Seq(participantBob),
        synchronizer = daName,
      )
    participantAlice.parties.testing.also_enable(
      alice,
      synchronizeParticipants = Seq(participantBob),
      synchronizer = acmeName,
    )

    val bob = participantBob.parties.testing.enable(
      "Bob",
      synchronizeParticipants = Seq(participantAlice),
      synchronizer = daName,
    )
    participantBob.parties.testing.also_enable(
      bob,
      synchronizeParticipants = Seq(participantAlice),
      synchronizer = acmeName,
    )

    val pkg = participant1.packages.find_by_module(module).headOption.value.packageId

    awaitTopologyUpToDate(acmeId, staticSynchronizerParameters2, participant1, participant2)

    reassign(bob, List(alice), pkg, participantBob)
    reassign(bob, List(alice), pkg, participantBob)

    stopAndRestart(
      participantBob,
      // Force the dirty replay of all deliver events
      runAfterStopBeforeRestart = stateInspection2.moveLedgerEndBackToScratch(),
      numSynchronizers = 2,
    )

    participant1.health.ping(participant2)
    logger.info(s"Reassignment after restart p2")
    reassign(bob, List(alice), pkg, participantBob)

    val updatesP1 = Future(
      participant1.ledger_api.updates
        .updates(
          updateFormat = txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )
    val updatesP2 = Future(
      participant2.ledger_api.updates
        .updates(
          updateFormat = txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )

    assertUpdatesAreBalanced(updatesP1.futureValue, "participant1")
    assertUpdatesAreBalanced(updatesP2.futureValue, "participant2", transactionsOnly = true)
  }

  "continue to make progress after replaying assignments without unassignments" in { implicit env =>
    import env.*

    val participant1 = startAndGet("participant1")
    participant3.start()
    participant4.start()

    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)

    Seq(participant3, participant4).foreach { p =>
      p.synchronizers.connect_local(sequencer1, alias = daName)
      p.synchronizers.connect_local(sequencer2, alias = acmeName)
    }
    Seq(participant3, participant4).dars.upload(CantonTestsPath, synchronizerId = daId)
    Seq(participant1, participant3, participant4).dars
      .upload(CantonTestsPath, synchronizerId = acmeId)

    val participantAlice = participant3
    val participantBob = participant4

    PartiesAllocator(Set(participant1, participantAlice, participantBob))(
      newParties = Seq("Alice" -> participantAlice, "Bob" -> participantBob),
      targetTopology = Map(
        "Alice" -> Map(
          daId -> (PositiveInt.one, Set(participantAlice.id -> ParticipantPermission.Submission)),
          acmeId -> (PositiveInt.one, Set(
            participantAlice.id -> ParticipantPermission.Submission,
            participant1.id -> ParticipantPermission.Submission,
          )),
        ),
        "Bob" -> Map(
          daId -> (PositiveInt.one, Set(participantBob.id -> ParticipantPermission.Submission)),
          acmeId -> (PositiveInt.one, Set(participantBob.id -> ParticipantPermission.Submission)),
        ),
      ),
    )
    val alice = "Alice".toPartyId(participantAlice)
    val bob = "Bob".toPartyId(participantBob)

    val pkg = participant1.packages.find_by_module(module).headOption.value.packageId

    reassign(bob, List(alice), pkg, participantBob)
    reassign(bob, List(alice), pkg, participantBob)

    val stateInspection1 = stateInspectionFor(participant1)

    stopAndRestart(
      participant1,
      runAfterStopBeforeRestart = stateInspection1.moveLedgerEndBackToScratch(),
      numSynchronizers = 1,
    )

    logger.info(s"Reassignment after restart p1")
    reassign(bob, List(alice), pkg, participantBob)

    val updatesP1 = Future(
      participant1.ledger_api.updates
        .updates(
          updateFormat = txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )
    val updatesP3 = Future(
      participant3.ledger_api.updates
        .updates(
          updateFormat = txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )
    val updatesP4 = Future(
      participant4.ledger_api.updates
        .updates(
          updateFormat = txAndReassignmentsFor(Set(alice, bob)),
          completeAfter = Int.MaxValue,
          timeout = config.NonNegativeDuration.ofSeconds(19),
        )
    )

    assertUpdatesAreBalanced(updatesP1.futureValue, "participant1")
    assertUpdatesAreBalanced(updatesP3.futureValue, "participant3")
    assertUpdatesAreBalanced(updatesP4.futureValue, "participant4")
  }

}

@nowarn("msg=match may not be exhaustive")
class ParticipantRestartRealClockIntegrationTest extends ParticipantRestartTest {
  private lazy val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Manual
      .addConfigTransforms(
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
        ProgrammableSequencer.configOverride(getClass.toString, loggerFactory),
      )

  private def startSynchronizers(synchronizers: Seq[NetworkTopologyDescription])(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    NetworkBootstrapper(synchronizers).bootstrap()
    synchronizers.foreach { synchronizer =>
      synchronizer.sequencers.headOption.foreach { sequencer =>
        import env.*
        sequencer.topology.synchronizer_parameters
          .propose_update(
            env.getInitializedSynchronizer(synchronizer.synchronizerName).synchronizerId,
            _.copy(
              confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(40),
              reconciliationInterval = reconciliationInterval.toConfig,
            ),
          )
      }
    }
  }

  "successfully ping after restart" in { implicit env =>
    startSynchronizers(Seq(EnvironmentDefinition.S1M1))

    val participant1 = startAndGet("participant1")

    connectToDa(participant1)

    stopAndRestart(participant1)

    assertPingSucceeds(participant1, participant1)
    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
  }

  "successfully restart after ping" in { implicit env =>
    startSynchronizers(Seq(EnvironmentDefinition.S1M1))

    val participant1 = startAndGet("participant1")

    connectToDa(participant1)

    withClue("ping before restarting") {
      assertPingSucceeds(participant1, participant1)
    }

    stopAndRestart(participant1)

    withClue("ping should succeed after restart") {
      assertPingSucceeds(participant1, participant1)
    }

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
  }

  "restart during transaction submission" in { implicit env =>
    import env.*

    startSynchronizers(Seq(EnvironmentDefinition.S1M1))

    val participant1 = startAndGet("participant1")

    connectToDa(participant1)

    // Use Alice instead of the P1 admin party at least until #16073 is fixed and the ledger api sees
    // participant admin parties.
    val alice = participant1.parties.testing.enable(
      "Alice",
      synchronizeParticipants = Nil,
      synchronizer = daName,
    )

    val participant1Id =
      participant1.id // Do not inline into policy because this internally performs a GRPC call
    val p1Id = alice

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val submissionDelay = Promise[Unit]()
    val submissionHappened = Promise[Unit]()

    // Shut down the participant when it sends the first message
    sequencer.setPolicy_("delay until after shutdown") {
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (submissionRequest.sender == participant1Id && submissionRequest.isConfirmationRequest) {
          submissionHappened.trySuccess(())
          SendDecision.HoldBack(submissionDelay.future)
        } else SendDecision.Process
      }
    }

    val participantOffset = participant1.ledger_api.state.end()

    val pingId = "participant1-ping-shutdown"
    val commandId = "participant1-ping-shutdown-command"
    val pingCommand =
      new Ping(
        pingId,
        p1Id.toLf,
        p1Id.toLf,
      ).create.commands.asScala.toSeq

    val submissionF = Future {
      participant1.ledger_api.javaapi.commands.submit_async(
        Seq(p1Id),
        pingCommand,
        commandId = commandId,
      )
    }
    val shutdownF = submissionHappened.future.map { _ =>
      restart(participant1, submissionDelay.success(()))
      participant1.synchronizers.reconnect_all()
    }
    Await.result(shutdownF, 2.minutes)

    val timeout = 30.seconds
    val completions = participant1.ledger_api.completions.list(
      p1Id,
      1,
      participantOffset,
      timeout = timeout,
      filter = filterCompletionsByCommandId(commandId),
    )
    val transactions =
      participant1.ledger_api.updates.transactions(
        partyIds = Set(p1Id),
        completeAfter = 1,
        beginOffsetExclusive = participantOffset,
        timeout = timeout,
      )
    assert(completions.size == 1)
    val completion = completions.head
    assert(completion.status.forall(_.code == Status.OK.getCode.value))
    assert(transactions.size == 1)
    val transaction = transactions.head
    assert(completion.updateId == transaction.updateId)
    submissionF.futureValue

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
  }

  "successfully restart during a bong" taggedAs
    ReliabilityTest(
      Component("Bong application", "connected to single non-replicated participant"),
      AdverseScenario(
        dependency = "Participant",
        details = "participant process is forcefully restarted while running a bong",
      ),
      Remediation(
        remediator = "bong application",
        action = "retries on timeouts and connection issues",
      ),
      outcome = "bong can progress whenever the participant is running",
    ) in { implicit env =>
      import env.*

      console.set_command_timeout(
        NonNegativeDuration.tryFromDuration(console.command_timeout.duration.mul(2))
      )

      startSynchronizers(Seq(EnvironmentDefinition.S1M1))

      val participant1 = startAndGet("participant1")
      val participant2 = startAndGet("participant2")

      connectToDa(participant1)
      connectToDa(participant2)

      val stateInspection1 = stateInspectionFor(participant1)
      val stateInspection2 = stateInspectionFor(participant2)

      val p1_count = grabCountsRemote(daName, stateInspection1)
      val p2_count = grabCountsRemote(daName, stateInspection2)

      val levels: Int = 6
      val restartF = Future {
        val before = poll(40.seconds, 10.milliseconds) {
          val p1_count2 = grabCountsRemote(daName, stateInspection1)
          // wait until a quarter of the levels have been dealt with
          assert(
            p1_count2.acceptedTransactionCount - p1_count.acceptedTransactionCount >= math
              .pow(2, (levels + 1d) / 4),
            s"A quarter of levels have not yet been dealt with",
          )
          // make sure the bong hasn't finished
          assert(
            p1_count2.acceptedTransactionCount - p1_count.acceptedTransactionCount <= 3 * math
              .pow(2, (levels + 1d) / 4),
            s"Bong almost finished",
          )
          p1_count2.pcsCount
        }

        logger.info(s"Restarting $participant1 at pcs count $before")
        stopAndRestart(participant1)
        val afterRestartTs = Instant.now()
        logger.info(s"After restart time chosen: $afterRestartTs")
        val after = grabCountsRemote(daName, stateInspection1).pcsCount

        logger.info(s"After restart, $participant1 has pcs count $after")

        (before, after, afterRestartTs)
      }

      val bongF = Future {
        participant2.testing.bong(
          targets = Set(participant1.id, participant2.id),
          validators = Set(participant1.id),
          levels = levels,
          timeout = 120.seconds,
        )
      }

      val patience = defaultPatience.copy(timeout = 180.seconds)
      bongF.futureValue(patience, Position.here)
      val (contractsBeforeRestart, contractsAfterRestart, afterRestartTs) =
        restartF.futureValue(patience, Position.here)

      val bongCounts = IntegrationTestUtilities.expectedGrabbedCountsForBong(levels, validators = 1)
      assert(
        contractsBeforeRestart <= contractsAfterRestart,
        s"More contracts found before restart than after restart",
      )
      assert(
        contractsAfterRestart - p1_count.pcsCount <= bongCounts.pcsCount - 10,
        s"Restart did not happen in the middle of the bong",
      )

      eventually(2.minutes) {
        val limit = (math.pow(2, levels + 2d) + 200).toInt
        val p1_count2 = grabCountsRemote(daName, stateInspection1, limit)
        val p2_count2 = grabCountsRemote(daName, stateInspection2, limit)
        assertResult(
          p1_count.plus(bongCounts),
          s"For p1: Initial count + bong count should be the final count",
        )(p1_count2)
        assertResult(
          p2_count.plus(bongCounts),
          s"For p2: Initial count + bong count should be the final count",
        )(p2_count2)
        // and final bong got archived
        stateInspection1
          .findContracts(daName, None, None, filterTemplate = Some("^Canton.Internal.Bong"), 100)
          .filter(_._1) shouldBe empty
      }

      eventually(20.seconds) {
        val optSafeTs = stateInspection1.noOutstandingCommitmentsTs(daName, CantonTimestamp.now())
        if (!optSafeTs.exists(_.toInstant > afterRestartTs))
          fail(s"Safe pruning point $optSafeTs before $afterRestartTs")
        else {
          logger.info(s"Safe pruning point $optSafeTs moved after $afterRestartTs")
        }
      }

      assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection1)
      assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant2, stateInspection2)
    }

  "restart with inflight validation requests" in { implicit env =>
    import env.*

    NetworkBootstrapper(Seq(EnvironmentDefinition.S1M1)).bootstrap()
    sequencer1.topology.synchronizer_parameters.propose_update(
      daId,
      _.update(confirmationResponseTimeout = NonNegativeFiniteDuration.ofSeconds(40)),
    )

    val participant1 = startAndGet("participant1")
    val participant2 = startAndGet("participant2")
    participant3.start()

    connectToDa(participant1, noAutoReconnect = true)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant3.synchronizers.connect_local(sequencer1, alias = daName)

    // Do not inline into policy because this internally performs a GRPC call
    val participant2Id = participant2.id

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val firstTransactionRespondedPromise = Promise[Unit]()
    val secondTransactionCompletePromise = Promise[Unit]()

    // Hold back all confirmation responses from participant 2 until participant 3 has pinged participant 1
    sequencer.setPolicy("hold back participant2 confirmation responses") {
      SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
        if (
          submissionRequest.sender == participant2Id && isConfirmationResponse(submissionRequest)
        ) {
          logger.info("Holding back mediator message.")
          firstTransactionRespondedPromise.trySuccess(())
          SendDecision.HoldBack(secondTransactionCompletePromise.future)
        } else SendDecision.Process
      }
    }
    val stateInspection1 = this.stateInspectionFor(participant1)
    val stateInspection2 = this.stateInspectionFor(participant2)
    val requestCounterOfFirstPing = RequestCounter.Genesis
    valueOrFail(stateInspection1.requestStateInJournal(requestCounterOfFirstPing, daId))(
      "Not find request counter for first ping"
    ).futureValueUS shouldBe None

    val firstPing = Future {
      // This ping may succeed even if the first ping transaction times out
      // because the ping service automatically retries
      assertPingSucceeds(participant2, participant1, timeoutMillis = 60000, id = "firstPing")
    }
    val secondPing = firstTransactionRespondedPromise.future.flatMap { _ =>
      // We can not wait for the second ping to finish because the TransactionAccepted updates will be stuck
      // in the event scheduler until the first ping's request is resolved.
      // So we wait for the second ping to become clean in the request journal (even though the clean cursor has not yet advanced).
      val requestCounterOfSecondPing = requestCounterOfFirstPing + 1L
      valueOrFail(stateInspection1.requestStateInJournal(requestCounterOfSecondPing, daId))(
        "Not find request counter for second ping"
      ).futureValueUS shouldBe None
      val secondPingF = Future {
        // We use participant 3 for the second ping because we are going to kill participant 1 and we're blocking participant 2.
        assertPingSucceeds(participant3, participant1, timeoutMillis = 60000, id = "secondPing")
      }
      eventually(timeUntilSuccess = 10.seconds) {
        val resultF =
          stateInspection1.requestStateInJournal(requestCounterOfSecondPing, daId).value
        resultF.futureValueUS.value.state shouldBe RequestJournal.RequestState.Clean
      }

      stopAndRestart(
        participant1,
        runBeforeReconnect = Some { () =>
          // Abort the test if the first ping has already timed out
          val resultF =
            stateInspection1.requestStateInJournal(requestCounterOfFirstPing, daId).value
          if (resultF.futureValueUS.value.state == RequestJournal.RequestState.Clean)
            cancel()
          Seq(daName)
        },
      )
      secondTransactionCompletePromise.success(())
      secondPingF
    }

    val patience = defaultPatience.copy(timeout = 90.seconds)
    firstPing.futureValue(patience, Position.here)
    secondPing.futureValue(patience, Position.here)
    stateInspection1.verifyLapiStoreIntegrity()
    stateInspection2.verifyLapiStoreIntegrity()
    participant3.testing.state_inspection.verifyLapiStoreIntegrity()

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection1)
    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant2, stateInspection2)
  }

  "replay reassignments" in { implicit env =>
    import env.*

    startSynchronizers(EnvironmentDefinition.S1M1_S1M1)

    val participant1 = startAndGet("participant1")
    val participant2 = startAndGet("participant2")

    connectToDa(participant1)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = daId)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = acmeId)

    val stateInspection1 = stateInspectionFor(participant1)
    val participantBob = participant2

    val alice = participant1.parties.testing.enable(
      "Alice",
      synchronizeParticipants = Seq(participantBob),
      synchronizer = daName,
    )
    participant1.parties.testing.also_enable(
      alice,
      synchronizeParticipants = Seq(participantBob),
      synchronizer = acmeName,
    )
    val bob = participantBob.parties.testing.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
      synchronizer = daName,
    )
    participantBob.parties.testing.also_enable(
      bob,
      synchronizeParticipants = Seq(participant1),
      synchronizer = acmeName,
    )

    val module = "ConflictTest"

    val Some(pkg) = participant1.packages.find_by_module(module).headOption.map(_.packageId)

    def reassign(): Unit = {
      val aliceBob = ledger_api_utils.create(
        pkg,
        module,
        "Many",
        Map[String, Any]("sig" -> alice, "obs" -> List(bob)),
      )
      val Value.Sum.ContractId(manyId) =
        extractSubmissionResult(
          participant1.ledger_api.commands
            .submit(
              actAs = Seq(alice),
              commands = Seq(aliceBob),
              synchronizerId = Some(daId),
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )
        )
      participant1.ledger_api.commands
        .submit_reassign(
          alice,
          Seq(LfContractId.assertFromString(manyId)),
          daId,
          acmeId,
        )
    }

    awaitTopologyUpToDate(acmeId, staticSynchronizerParameters2, participant1, participant2)
    reassign()

    clue("Perform restart") {
      stopAndRestart(
        participant1,
        runAfterStopBeforeRestart = {
          // Participant 1 replays the unassignments and assignments
          stateInspection1.moveLedgerEndBackToScratch()
        },
        numSynchronizers = 2,
      )
    }

    clue("Ping after restart") {
      // Following pings should prevent flaking on by advancing the watermark on both participant before the reassign:
      // `Failed to observe update on all nodes: Alice::12201b2f11b1...@Participant 'participant1': observed, Bob::1220d079e1c0...@Participant 'participant2': not observed`
      Seq(daId, acmeId).foreach(id => participant1.health.ping(participant2, synchronizerId = id))
    }

    reassign()

  }

  "restart while submitting an unassignment" in { implicit env =>
    import env.*

    startSynchronizers(EnvironmentDefinition.S1M1_S1M1)

    val participant1 = startAndGet("participant1")
    val participant2 = startAndGet("participant2")

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
    participant2.synchronizers.connect_local(sequencer2, alias = acmeName)

    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = daId)
    Seq(participant1, participant2).dars.upload(CantonTestsPath, synchronizerId = acmeId)

    val participantAlice = participant1
    val participantBob = participant2

    val alice = participantAlice.parties.testing.enable(
      "Alice",
      synchronizeParticipants = Seq(participantBob),
      synchronizer = daName,
    )
    participantAlice.parties.testing.also_enable(
      alice,
      synchronizeParticipants = Seq(participantBob),
      synchronizer = acmeName,
    )
    val participantAliceId =
      participantAlice.id // Do not inline into policy because this internally performs a GRPC call

    val bob = participantBob.parties.testing.enable(
      "Bob",
      synchronizeParticipants = Seq(participantAlice),
      synchronizer = daName,
    )
    participantBob.parties.testing.also_enable(
      bob,
      synchronizeParticipants = Seq(participantAlice),
      synchronizer = acmeName,
    )

    val module = "ConflictTest"
    val pkg = participant1.packages.find_by_module(module).headOption.map(_.packageId).value

    val aliceBob = ledger_api_utils.create(
      pkg,
      module,
      "Many",
      Map[String, Any]("sig" -> alice, "obs" -> List(bob)),
    )
    val Value.Sum.ContractId(manyId) =
      extractSubmissionResult(
        participantAlice.ledger_api.commands
          .submit(
            actAs = Seq(alice),
            commands = Seq(aliceBob),
            synchronizerId = Some(daId),
            transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
          )
      )

    val sequencerDa = getProgrammableSequencer(sequencer1.name)

    val unassignmentRequestHold = Promise[Unit]()
    val unassignmentRequestReady = Promise[Unit]()

    sequencerDa.setPolicy_("hold back Alice's unassignment confirmation") {
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (
          submissionRequest.sender == participantAliceId &&
          isConfirmationResponse(submissionRequest)
        ) {
          unassignmentRequestReady.success(())
          SendDecision.HoldBack(unassignmentRequestHold.future)
        } else SendDecision.Process
      }
    }

    // wait until the topology has been observed, as otherwise, unassignment will fail
    eventually() {
      participantBob.topology.party_to_participant_mappings.are_known(
        daId,
        Set(alice.partyId -> participantAlice, bob.partyId -> participantBob),
      ) shouldBe true
      participantBob.topology.party_to_participant_mappings.are_known(
        acmeId,
        Set(alice.partyId -> participantAlice, bob.partyId -> participantBob),
      ) shouldBe true
    }

    val unassignedEventF = Future {
      assertThrowsAndLogsCommandFailures(
        participantBob.ledger_api.commands
          .submit_unassign(bob, Seq(LfContractId.assertFromString(manyId)), daId, acmeId),
        entry => entry.commandFailureMessage should include("Request failed for participant2"),
      )
    }

    val restartF = unassignmentRequestReady.future.map { _ =>
      restart(participantBob, unassignmentRequestHold.success(()))
      participantBob.synchronizers.reconnect_all()

      // wait until unassignment is done
      withClue("unassignment is done, ready for assignment") {
        eventually() {
          participantBob.ledger_api.state.acs
            .incomplete_unassigned_of_party(bob) should not be empty
        }
      }

      val incompleteUnassigned = participantBob.ledger_api.state.acs
        .incomplete_unassigned_of_party(bob)

      withClue("there should be exactly one pending reassignment") {
        incompleteUnassigned should have length 1
      }

      withClue("assignment should succeed") {
        incompleteUnassigned.headOption.foreach { item =>
          participantBob.ledger_api.commands.submit_assign(
            bob,
            item.reassignmentId,
            daId,
            acmeId,
          )
        }
      }
    }

    val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(10))
    restartF.futureValue(patience, Position.here)
    unassignedEventF.futureValue(patience, Position.here)

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant2)
  }

  "replay many messages" in { implicit env =>
    import env.*

    startSynchronizers(Seq(EnvironmentDefinition.S1M1))

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)
    val stateInspection1 = this.stateInspectionFor(participant1)

    manyPings(100, 3.0 / 4.0, participant1, Timeout(Span(5, Minutes)), 10)

    stopAndRestart(
      participant1,
      // Force the dirty replay of all deliver events
      runAfterStopBeforeRestart = stateInspection1.moveLedgerEndBackToScratch(),
    )

    eventually() {
      withClue("synchronizer should be connected after restart") {
        participant1.synchronizers.list_connected() should have length 1
      }
    }

    eventually(30.seconds) {
      assertPingSucceeds(participant1, participant1)
    }

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection1)
  }

}

abstract class ParticipantRestartStaticTimeIntegrationTestBase(
    enableAllLedgerApiReassignments: Boolean = false
) extends ParticipantRestartTest {

  private val overrideMaxRequestSize = NonNegativeInt.tryCreate(100 * 1024)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        // enabling retries to be on the safe side
        ConfigTransforms.setPingRetries(true),
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
        // Set a small request size for the participant so that the participant's sequencer client refuses to
        // send big requests to the sequencer and thus the participant can produce a rejection event
        // without having to wait for the max sequencing time elapsing.
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.sequencerClient.overrideMaxRequestSize).replace(Some(overrideMaxRequestSize))
        ),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.enableAllLedgerApiReassignments)
            .replace(enableAllLedgerApiReassignments)
        ),
      )
      .withSetup { implicit env =>
        NetworkBootstrapper(Seq(EnvironmentDefinition.S1M1)).bootstrap()
      }

  /** Test Goal: Verify that a participant node correctly recovers and assigns record order/time to
    * internally generated events (like offline repairs and local rejections) if it crashes in the
    * middle of processing a massive batch of expired timeouts.
    *
    * Test Design:
    *   1. The test drops participant requests at the sequencer and submits 250 commands.
    *   1. The clock is advanced, forcing all 250 commands to time out simultaneously.
    *   1. The participant node is violently crashed *during* this flood of local timeout
    *      rejections.
    *   1. While offline, a manual `repair.purge` is executed. The test asserts that the resulting
    *      repair event correctly inherits the record time of the interrupted timeout batch.
    *   1. The participant node reconnects with sequencer reads blocked. The test verifies the
    *      remaining timeouts finish processing, and then forces an immediate local rejection (via a
    *      payload size limit). It asserts this new rejection also adopts the safely recovered
    *      record time.
    */
  "repair and timely reject due to errors are correctly created in record order after restart, if crash before publishing next sequencer counter which is preceded by published floating events after the last published sequencer counter" in {
    implicit env =>
      import env.*
      val participant1 = startAndGet("participant1")

      // Disable auto-reconnect so we can keep the node offline after the restart
      // to perform manual repair operations before it receives new network traffic.
      connectToDa(participant1, noAutoReconnect = true)

      participant1.dars.upload(CantonExamplesPath)
      val stateInspection1 = this.stateInspectionFor(participant1)
      val party = participant1.adminParty

      def createCycleCommands(
          prefix: String,
          small: Boolean,
      ): Seq[javab.data.Command] = {
        import scala.jdk.CollectionConverters.*
        // Large payloads will be rejected by the sequencer client because of the request size limit
        val cyclePayload = if (small) { prefix + "-small" }
        else {
          prefix + "-big" + Seq
            // Use a random ASCII string so that it cannot be compressed easily beyond a factor of 2.
            // Generating a payload definitively larger than overrideMaxRequestSize forces the local sequencer client
            // to reject the command immediately, bypassing the network.
            .fill(overrideMaxRequestSize.value * 2)(Random.nextPrintableChar().toString)
            .mkString("")
        }
        new cycle.Cycle(cyclePayload, party.toProtoPrimitive).create.commands.asScala.toSeq
      }

      def createCycleContractSync(
          prefix: String,
          small: Boolean,
      ): Transaction =
        participant1.ledger_api.javaapi.commands.submit(
          Seq(party),
          createCycleCommands(prefix, small),
          commandId = s"$prefix-command-id",
          submissionId = s"$prefix-submission-id",
        )

      def createCycleContractAsync(
          prefix: String,
          small: Boolean,
      ): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(party),
          createCycleCommands(prefix, small),
          commandId = s"$prefix-command-id",
          submissionId = s"$prefix-submission-id",
        )

      val (baselineOffset, _baselineRecordTime, baselineContractId) =
        clue("Create a single contract to set the baseline") {
          val baselineTransaction = createCycleContractSync("baseline", small = true)
          val offset = baselineTransaction.getOffset
          val recordTime = CantonTimestamp.assertFromInstant(baselineTransaction.getRecordTime)
          val baseLineContractIds = baselineTransaction.getEventsById.asScala.collect {
            case (_, event) => event.getContractId
          }
          baseLineContractIds should have size 1
          val contractId = LfContractId.assertFromString(baseLineContractIds.headOption.value)

          logger.info(
            s"Created a single contract to set the baseline with offset: $offset record time: $recordTime contractID: $contractId"
          )
          (offset, recordTime, contractId)
        }

      val p1id = participant1.id
      val programmableSequencer1 = getProgrammableSequencer(sequencer1.name)
      val droppedRequestsCounter = new AtomicInteger(0)

      // Silently dropping confirmation requests ensures the submitted commands will sit in the in-flight tracker
      // until the simulated clock is advanced past their max decision time.
      programmableSequencer1.setPolicy("drop confirmation requests by participant1")(
        SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
          if (submissionRequest.isConfirmationRequest && submissionRequest.sender == p1id) {
            val counter = droppedRequestsCounter.incrementAndGet()
            logger.debug(s"Dropping confirmation request with counter $counter")
            SendDecision.Drop
          } else SendDecision.Process
        }
      )

      val timeoutCommandCount = 250
      logger.info(s"Started creating $timeoutCommandCount commands which supposed to timeout")

      1 to timeoutCommandCount foreach { i =>
        createCycleContractAsync(s"sequencing-timeout-batch-$i", small = true)
      }

      clue("Wait for all async submissions to be fully processed and reach the sequencer") {
        eventually() {
          droppedRequestsCounter.get() shouldBe timeoutCommandCount
        }
      }

      val ledgerEndBeforeTriggeringTimeoutRejections = participant1.ledger_api.state.end()
      logger.info(s"Finished creating $timeoutCommandCount commands which supposed to timeout")

      val firstTimeoutRejection =
        clue("Advancing time to trigger timeouts and waiting for the first rejection") {
          environment.simClock.value.advance(java.time.Duration.ofDays(365))

          eventually(timeUntilSuccess = 60.seconds) {
            participant1.ledger_api.completions
              .list(
                partyId = party,
                atLeastNumCompletions = 1,
                beginOffsetExclusive = ledgerEndBeforeTriggeringTimeoutRejections,
                timeout = config.NonNegativeDuration.ofSeconds(2), // fast fail to allow retries
              )
              .headOption
              .value
          }
        }

      firstTimeoutRejection.status.value.message should include(
        TransactionProcessor.SubmissionErrors.TimeoutError.id
      )
      val firstTimeoutOffset = firstTimeoutRejection.offset
      val firstTimeoutRejectionRecordTime = CantonTimestamp
        .fromProtoTimestamp(firstTimeoutRejection.synchronizerTime.value.recordTime.value)
        .toOption
        .value
      logger.info(
        s"First timeout rejection arrived (offset: $firstTimeoutOffset record-time:$firstTimeoutRejectionRecordTime), crashing participant before the SequencerIndex moves ahead"
      )

      stopAndRestart(
        participant1,
        runAfterStopBeforeRestart = {
          clue(
            "Cleaning SequencedEventStore: all elements above the SynchronizerIndex will be removed"
          ) {
            // This simulates a hard crash where recently processed events (like our flood of timeouts)
            // were not fully committed and must be recovered during the node's startup sequence.
            stateInspection1.cleanSequencedEventStoreAboveCleanSynchronizerIndex(daId)
          }
        },
        runBeforeReconnect = Some { () =>
          val ledgerEndAfterRestart = participant1.ledger_api.state.end()
          // Approximating received timeout rejections with the number of bumps the ledger end received:
          // We need to make sure we did not receive all, ensuring that shutdown happens before ingesting the timeproof
          // event (which is actually triggering all the timeout tasks to be published).
          // If this condition is failing (maybe in a flaky test), then increasing timeoutCommandCount might help.
          ledgerEndAfterRestart should be < firstTimeoutOffset + timeoutCommandCount
          logger.info(
            s"Participant restarted, synchronizers not connected yet, offset: $ledgerEndAfterRestart"
          )

          // Perform an offline repair operation before the node connects to the network
          participant1.repair.purge(daName, Seq(baselineContractId), ignoreAlreadyPurged = false)

          val (repairOffset, repairRecordTime) = if (enableAllLedgerApiReassignments) {
            participant1.ledger_api.updates
              .reassignments(
                Set(party),
                completeAfter = 1,
                beginOffsetExclusive = ledgerEndAfterRestart,
              )
              .collectFirst { case unassignedWrapper: UnassignedWrapper =>
                (
                  unassignedWrapper.reassignment.offset,
                  CantonTimestamp
                    .fromProtoTimestamp(
                      unassignedWrapper.reassignment.recordTime.value
                    )
                    .toOption
                    .value,
                )
              }
              .value
          } else {
            participant1.ledger_api.updates
              .transactions(Set(party), 1, ledgerEndAfterRestart)
              .collectFirst { case txWrapper: TransactionWrapper =>
                (
                  txWrapper.transaction.offset,
                  CantonTimestamp
                    .fromProtoTimestamp(
                      txWrapper.transaction.recordTime.value
                    )
                    .toOption
                    .value,
                )
              }
              .value
          }

          logger.info(
            s"Repair finished, one update is committed at offset: $repairOffset record-time: $repairRecordTime"
          )
          repairOffset shouldBe ledgerEndAfterRestart + 1

          // Proves that the offline repair operation correctly inherited the participant's
          // last known record time from the floating timeout events being recovered.
          firstTimeoutRejectionRecordTime shouldBe repairRecordTime

          // Muting the synchronizer isolates the participant from the network. This guarantees
          // that the completions we observe next are purely from the local recovery mechanism.
          programmableSequencer1.blockFutureMemberRead(participant1.member)
          logger.info(
            s"Connecting participant1 to $daName, the underlying sequencer connection should be blocked"
          )
          Seq(daName)
        },
      )

      // After the synchronizer is connected we see all the timeout events flowing in, as the timeout is already reached before.
      val completions =
        clue(s"Waiting for all $timeoutCommandCount timeout rejections after restart") {
          eventually(timeUntilSuccess = 60.seconds) {
            val res = participant1.ledger_api.completions
              .list(
                partyId = party,
                atLeastNumCompletions = timeoutCommandCount,
                beginOffsetExclusive = baselineOffset,
                timeout = config.NonNegativeDuration.ofSeconds(2), // fast fail to allow retries
                filter = _.commandId.startsWith("sequencing-timeout-batch"),
              )
            res should have size timeoutCommandCount.toLong
            res
          }
        }

      completions.foreach(
        _.status.value.message should include(TransactionProcessor.SubmissionErrors.TimeoutError.id)
      )
      logger.info("All scheduled rejections observed")

      val ledgerEndAfterAllTimeoutRejections = participant1.ledger_api.state.end()

      // After all rejections received, there should not be any traffic on the ledger, as the synchronizer is not connected.
      clue("Ensuring no unexpected traffic occurs while synchronizer is disconnected") {
        Threading.sleep(2000)
        participant1.ledger_api.state.end() shouldBe ledgerEndAfterAllTimeoutRejections
      }

      val immediatelyRejectedCompletion =
        clue("Sending a command which should be immediately rejected and waiting for completion") {
          createCycleContractAsync("immediately-rejected", small = false)

          eventually(timeUntilSuccess = 60.seconds) {
            participant1.ledger_api.completions
              .list(
                partyId = party,
                atLeastNumCompletions = 1,
                beginOffsetExclusive = ledgerEndAfterAllTimeoutRejections,
                timeout = config.NonNegativeDuration.ofSeconds(2), // fast fail
              )
              .headOption
              .value
          }
        }

      val immediatelyRejectedOffset = immediatelyRejectedCompletion.offset
      val immediatelyRejectedRecordTime = CantonTimestamp
        .fromProtoTimestamp(
          immediatelyRejectedCompletion.synchronizerTime.value.recordTime.value
        )
        .toOption
        .value

      logger.info(
        s"Immediate rejection received with offset: $immediatelyRejectedOffset, record-time: $immediatelyRejectedRecordTime"
      )

      immediatelyRejectedCompletion.commandId shouldBe "immediately-rejected-command-id"
      immediatelyRejectedCompletion.status.value.message should include(
        TransactionProcessor.SubmissionErrors.SequencerRequest.id
      )
      immediatelyRejectedOffset shouldBe ledgerEndAfterAllTimeoutRejections + 1

      // Proves that synchronously rejected commands generated *after* recovery
      // correctly align with the frozen recovered record time.
      immediatelyRejectedRecordTime shouldBe firstTimeoutRejectionRecordTime

      logger.info(
        s"Test conditions succeeded, now verifying that synchronizer is functional"
      )
      programmableSequencer1.resetPolicy()
      programmableSequencer1.unBlockMemberRead(participant1.member)
      assertPingSucceeds(participant1, participant1)
      assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
  }

  "ping succeeds after restart" in { implicit env =>
    val participant1 = startAndGet("participant1")
    connectToDa(participant1)
    stopAndRestart(participant1)

    participant1.testing.fetch_synchronizer_times()
    eventually() {
      withClue("synchronizer should be connected after restart") {
        participant1.synchronizers.list_connected() should have length 1
      }
    }
    assertPingSucceeds(participant1, participant1)

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
  }

  "perform a clean replay of several messages" in { implicit env =>
    import env.*
    val parameters =
      DynamicSynchronizerParameters.initialValues(testedProtocolVersion)
    val clock = environment.simClock.value

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)

    manyPings(30, 3.0 / 4.0, participant1, Timeout(Span(5, Minutes)), 5)
    clock.advance(
      parameters.confirmationResponseTimeout.unwrap.plus(parameters.mediatorReactionTimeout.unwrap)
    )
    manyPings(30, 3.0 / 4.0, participant1, Timeout(Span(5, Minutes)), 5)

    stopAndRestart(participant1)

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1)
  }

  "obtain a rejection for commands lost during a restart" in { implicit env =>
    import env.*

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)
    participant1.dars.upload(CantonExamplesPath)
    val alice = participant1.parties.testing.enable("Alice", synchronizeParticipants = Nil)
    val participant1Id =
      participant1.id // Do not inline into the sequencer policy because this call has side effects

    val tolerance = DynamicSynchronizerParameters
      .initialValues(testedProtocolVersion)
      .ledgerTimeRecordTimeTolerance
      .unwrap

    // Advance the ledger time sufficiently into the future so that
    // we can be sure that the participant will not pick a higher one.
    val minLedgerTime = env.environment.clock.now.plusSeconds(60)
    val tooLate = minLedgerTime.add(tolerance).add(tolerance)

    // Kill participant1 once we get the first message from participant1
    val droppedMessagePromise = Promise[Unit]()
    val sequencer = getProgrammableSequencer(sequencer1.name)
    sequencer.setPolicy_("drop first message from participant1") {
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (!droppedMessagePromise.isCompleted && submissionRequest.sender == participant1Id) {
          external.kill(participant1.name)
          droppedMessagePromise.success(())
          SendDecision.Reject()
        } else SendDecision.Process
      }
    }

    val participantEndOffset = participant1.ledger_api.state.end()
    val command = new Dummy(alice.toProtoPrimitive).create.commands.loneElement
    val commandId = "Request-to-be-dropped"
    participant1.ledger_api.javaapi.commands.submit_async(
      Seq(alice),
      Seq(command),
      commandId = commandId,
      minLedgerTimeAbs = Some(minLedgerTime.toInstant),
    )
    droppedMessagePromise.future.futureValue // Wait until the request has been dropped and the participant killed

    // Advance the clock beyond the allowed record times.
    // Restart participant1 and send another command which should succeed.
    env.environment.simClock.value.advanceTo(tooLate)

    external.start(participant1.name)
    waitUntilRunning(participant1)
    participant1.synchronizers.reconnect_all()

    // Since the participant has observed a timestamp after the max record time determined by the ledger time tolerance,
    // we should see a rejection for the command that was being submitted when the crash happened.
    participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      Seq(command),
      commandId = "Request-to-succeed",
    )

    val completions = participant1.ledger_api.completions.list(alice, 2, participantEndOffset)
    completions should have size 2
    val rejection = completions.find { completion =>
      completion.commandId == "Request-to-be-dropped"
    }
    rejection shouldNot equal(None)
    rejection.foreach(
      _.status.value.message should include(SubmissionErrors.TimeoutError.id)
    )
  }

  "recover after crash during a dirty replay" in { implicit env =>
    import env.*

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)
    val stateInspection1 = this.stateInspectionFor(participant1)

    val wantedSucceedPingsProportion = 1.0 / 4.0
    val pingCount = 50
    val minSucceedPings = Math.ceil(pingCount * wantedSucceedPingsProportion).toLong
    val totalRequests = pingCount * 3
    val minSucceedRequests = minSucceedPings * 3
    // Each ping corresponds to three requests: ping, pong, and ACK
    // Crash when at least the first `lowerBoundCrashPoint` requests have been replayed
    val lowerBoundCrashPoint = RequestCounter(minSucceedRequests)
    // Crash before `upperBoundCrashPoint` requests have been replayed
    val upperBoundCrashPoint = RequestCounter(totalRequests - 2)
    logger.debug("RestartTest: starting many pings")
    // Don't run all pings in parallel as otherwise it's likely that the clean request index leaps forward over the zone when we want to crash the participant
    manyPings(
      pingCount,
      wantedSucceedPingsProportion,
      participant1,
      Timeout(Span(5, Minutes)),
      allowInParallel = 20,
    )

    logger.debug("RestartTest: restart participant1")
    // Make the participant perform a "dirty replay" of all messages
    restart(participant1, stateInspection1.moveLedgerEndBackToScratch())

    logger.debug("RestartTest: reconnect participant1")
    participant1.synchronizers.reconnect_all()

    final case class MissedTimeWindow(after: RequestCounter)
        extends RuntimeException(
          s"Missed the time window $lowerBoundCrashPoint-$upperBoundCrashPoint to crash the participant again: counter is $after"
        )

    try {
      poll(30.seconds, 10.milliseconds) {
        logger.info("Requesting clean request index")

        val counterBefore =
          stateInspection1.lookupCleanTimeOfRequest(daId).value.futureValueUS.value.rc

        logger.info(s"Found clean request counter: $counterBefore")

        assert(counterBefore > lowerBoundCrashPoint)
        // Trigger a crash
        external.kill(participant1.name)

        val counterAfter =
          stateInspection1
            .lookupCleanTimeOfRequest(daId)
            .fold(
              s => throw new Exception(s),
              _.futureValueUS.value.rc,
            )

        logger.info(
          s"Triggered crash between counters $counterBefore and $counterAfter, with bounds ($lowerBoundCrashPoint, $upperBoundCrashPoint)"
        )

        // Fail the test if we do not catch the middle of the replay
        // Do not cancel it because otherwise we won't notice when we're not testing this at all any more.
        // Do not use `fail` directly because we're inside a `poll`.
        if (counterAfter > upperBoundCrashPoint) throw MissedTimeWindow(counterAfter)
      }
    } catch {
      case e: MissedTimeWindow => fail(e)
    }

    // Begin replay after the crash during replay
    external.start(participant1.name)
    waitUntilRunning(participant1)
    participant1.synchronizers.reconnect_all()

    // Check that the system is still live
    assertPingSucceeds(participant1, participant1)

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection1)
  }

  /** Test Goal: Verify that the participant node's command deduplication and in-flight tracking
    * mechanisms remain completely consistent across multiple hard participant node crashes and
    * restarts.
    *
    * Test Design: The suite deterministically interleaves 6 command submissions (using the same
    * `commandId` but different `submissionId`s to simulate application retries) with 3 hard
    * participant restarts. It evaluates the deduplication boundaries across two distinct scenarios:
    *
    *   1. Crash AFTER reaching sequencer: The initial command successfully reaches the network
    *      before the first crash. Verifies that subsequent retries correctly bounce (synchronously
    *      or asynchronously), resulting in exactly 1 contract created.
    *
    *   1. Crash BEFORE reaching sequencer: The node crashes while the command is still trapped in
    *      the local in-flight tracker. Verifies that the initial command is safely aborted, retries
    *      are handled cleanly upon recovery, resulting in exactly 0 contracts created.
    */
  class DedupFixture(implicit env: TestConsoleEnvironment) {
    import env.*

    val commandId = "restart-command-dedup-id"
    val submissionIdPrefix = "restart-command-dedup-submission-id"

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)
    participant1.dars.upload(CantonExamplesPath)

    val sequencer = getProgrammableSequencer(sequencer1.name)
    val participant1Id = participant1.id // Do not inline because requires the participant to run
    val requestCounter = new AtomicInteger(0)

    val severin = participant1.parties.testing.enable("Severin", synchronizeParticipants = Nil)
    val totalSubmissions = 6

    val tolerance = DynamicSynchronizerParameters
      .initialValues(testedProtocolVersion)
      .ledgerTimeRecordTimeTolerance
    val timeTrackerPatience = sequencer1.config.timeTracker.patienceDuration

    // Ensure the time tracker triggers timeouts well before the ledger time tolerance is exceeded.
    // This prevents retries from failing with unrelated ledger-time bounds errors instead of the expected
    // deduplication/timeout behaviors.
    timeTrackerPatience.asJava.multipliedBy(5) should be < tolerance.unwrap

    val participantOffsetEnd = participant1.ledger_api.state.end()
    val simClock = env.environment.simClock.value

    // Tracks synchronous rejections to accurately calculate how many Ledger API completions to wait for:
    // - If `submit_async` bounces a command immediately "at the door" (e.g., throwing SUBMISSION_ALREADY_IN_FLIGHT
    //   because the previous retry is still pending), the Ledger API does NOT emit an asynchronous
    //   completion event to the stream.
    // - By subtracting these synchronous bounces from the total submissions, we prevent the test from hanging and
    //   timing out while waiting for completion events that will never arrive.
    val synchronousRejectionCount = new AtomicInteger(0)

    def doRestart(): Future[Unit] = Future {
      stopAndRestart(participant1)
    }

    /** Submits a command and executes the provided `waitLogic` closure. This abstracts the
      * submission mechanics while leaving the timing control to the caller.
      */
    def submit(index: Int)(waitLogic: (Boolean, Int, String) => Unit): Future[Unit] = Future {
      val createCycleContract = new C.Cycle(
        s"restart-command-dedup-$index",
        severin.toProtoPrimitive,
      ).create.commands.loneElement
      val submissionId = s"$submissionIdPrefix-$index"

      logger.debug(s"Attempting command submission $index")
      val requestCounterBefore = requestCounter.get()

      // Dynamically assert the logs based on the outcome of the submission
      val submissionResult = loggerFactory.assertLogsUnorderedOptionalFromResult(
        Either.catchOnly[CommandFailure] {
          participant1.ledger_api.javaapi.commands.submit_async(
            Seq(severin),
            Seq(createCycleContract),
            commandId = commandId, // Same commandId forces deduplication
            submissionId = submissionId, // Different submissionId tracks retries

            // Explicitly set a massive deduplication window (1 day) to guarantee
            // that all 6 retries across all 3 node restarts are caught by the same
            // deduplication boundary, rather than being treated as fresh commands.
            deduplicationPeriod = Some(DeduplicationDuration(java.time.Duration.ofDays(1))),
          )
        },
        { (result: Either[CommandFailure, ?]) =>
          val assertion: LogEntry => Assertion =
            _.shouldBeCantonErrorCode(SubmissionAlreadyInFlight)

          // If the command failed synchronously, we REQUIRE the in-flight bounce log.
          // If it succeeded, the log is Optional (we don't expect it, but don't strictly forbid it).
          if (result.isLeft) Seq(LogEntryOptionality.Required -> assertion)
          else Seq(LogEntryOptionality.Optional -> assertion)
        },
      )

      val expectSequencingOrCompletion = submissionResult.isRight

      if (submissionResult.isLeft) {
        synchronousRejectionCount.incrementAndGet().discard
      }

      // Delegate to the test-specific wait behavior (e.g., waiting for sequencer vs. fire-and-forget)
      waitLogic(expectSequencingOrCompletion, requestCounterBefore, submissionId)
    }

    /** Deterministically executes the submit and restart sequence, guaranteeing the exact
      * interleaving of submissions and node crashes to avoid flaky race conditions.
      */
    def runSequence(submitFn: Int => Future[Unit]): Unit = {
      val testExecutionSequence = for {
        _ <- submitFn(1)
        _ <- submitFn(2)
        _ <- doRestart()
        _ <- submitFn(3)
        _ <- submitFn(4)
        _ <- doRestart()
        _ <- submitFn(5)
        _ <- submitFn(6)
        _ <- doRestart()
      } yield ()

      val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(10))
      testExecutionSequence.futureValue(patience, Position.here)
    }

    def getExpectedCompletionsCount: Int = totalSubmissions - synchronousRejectionCount.get()

    /** Polls the Ledger API for the dynamically calculated number of expected completions */
    def fetchCompletions(): (Seq[Completion], Seq[Completion]) = {
      val expected = getExpectedCompletionsCount
      val completions = clue(
        s"Waiting for $expected completions ($totalSubmissions total - ${synchronousRejectionCount.get()} synchronous rejections)"
      ) {
        eventually(timeUntilSuccess = 60.seconds) {
          val res = participant1.ledger_api.completions.list(
            partyId = severin,
            atLeastNumCompletions =
              expected, // Query exactly the remaining amount so the stream returns promptly
            beginOffsetExclusive = participantOffsetEnd,
            timeout =
              config.NonNegativeDuration.ofSeconds(2), // Fast-fail to avoid hanging the test thread
            filter = completion => completion.commandId == commandId,
          )
          res.size shouldBe expected
          res
        }
      }
      completions.partition(_.status.exists(_.code == com.google.rpc.Code.OK_VALUE))
    }
  }

  "deduplicate commands across multiple restarts (crash AFTER reaching sequencer)" in {
    implicit env =>
      val fixture = new DedupFixture
      import fixture.*

      // Arrange
      // Sequencer policy: Count requests to allow us to wait until the message hits the network
      sequencer.setPolicy("Count requests by participant1") {
        SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
          if (
            submissionRequest.isConfirmationRequest && submissionRequest.sender == participant1Id
          ) {
            val newCount = requestCounter.incrementAndGet()
            logger.debug(s"Advanced request count to $newCount")
          }
          SendDecision.Process
        }
      }

      // Arrange
      // Wait behavior: Block until the request reaches the sequencer BEFORE continuing to the next step (the crash)
      def submitAndWait(index: Int): Future[Unit] = submit(index) {
        (expectSequencingOrCompletion, requestCounterBefore, submissionId) =>
          if (expectSequencingOrCompletion) {
            eventually(timeUntilSuccess = 60.seconds) {
              val requestCounterAfter = requestCounter.get()
              val aRequestReachedTheSequencer = requestCounterAfter > requestCounterBefore
              val allRequestsReachedTheSequencer = requestCounterAfter >= totalSubmissions

              if (!(aRequestReachedTheSequencer || allRequestsReachedTheSequencer)) {
                val completions = participant1.ledger_api.completions.list(
                  partyId = severin,
                  atLeastNumCompletions = 1,
                  beginOffsetExclusive = participantOffsetEnd,
                  timeout = config.NonNegativeDuration.ofMillis(500),
                  filter = c => c.commandId == commandId && c.submissionId == submissionId,
                )

                // If there are no completions either, advance the time to trigger timely rejections and go looking again
                if (completions.isEmpty) {
                  // If the message is genuinely stuck in the pipeline without reaching the sequencer,
                  // manually advancing the simulated clock past the patience threshold forces the in-flight
                  // tracker to finally issue a timeout rejection, breaking the wait loop.
                  simClock.advance(timeTrackerPatience.asJava.plusMillis(1))
                  fail("Found neither the sequenced message nor a completion. Go look again")
                } else succeed
              }
            }
          }
      }

      // Act
      runSequence(submitAndWait)

      // Assert
      val (accepts, rejects) = fetchCompletions()

      clue(
        "Since the first command was guaranteed to reach the sequencer, exactly 1 command must be accepted"
      )(accepts should have size 1)
      clue("All other asynchronous completions must be rejections")(
        rejects should have size (getExpectedCompletionsCount - 1).toLong
      )
  }

  "deduplicate commands across multiple restarts (crash BEFORE reaching sequencer)" in {
    implicit env =>
      val fixture = new DedupFixture
      import fixture.*

      // Arrange
      // Sequencer policy: Drop requests entirely to simulate a node crashing before it can emit to the network
      sequencer.setPolicy("Drop requests by participant1 to simulate crash before sequencing") {
        SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
          if (
            submissionRequest.isConfirmationRequest && submissionRequest.sender == participant1Id
          ) {
            logger.debug(s"Dropping request to simulate crash before reaching sequencer")
            SendDecision.Drop
          } else {
            SendDecision.Process
          }
        }
      }

      // Arrange
      // Wait behavior: Fire-and-forget. We do not wait for the sequencer, meaning the subsequent crash happens immediately.
      def submitAsyncOnly(index: Int): Future[Unit] = submit(index)((_, _, _) => ())

      // Act
      runSequence(submitAsyncOnly)

      // Teardown: Restore normal sequencer behavior and advance clock to forcefully timeout the dropped, in-flight messages
      sequencer.resetPolicy()
      simClock.advance(java.time.Duration.ofDays(365))

      // Assert
      val (accepts, rejects) = fetchCompletions()

      clue(
        "Since the commands were dropped before sequencing, exactly 0 commands must be accepted"
      )(accepts shouldBe empty)
      clue("All asynchronous completions must be rejections")(
        rejects should have size getExpectedCompletionsCount.toLong
      )
  }

}

@UnstableTest // TODO(#19922)
class ParticipantRestartStaticTimeIntegrationTest
    extends ParticipantRestartStaticTimeIntegrationTestBase

@UnstableTest // TODO(#30408)
class ParticipantRestartStaticTimeReassignmentIntegrationTest
    extends ParticipantRestartStaticTimeIntegrationTestBase(enableAllLedgerApiReassignments = true)

@nowarn("msg=match may not be exhaustive")
class ParticipantRestartContractKeyIntegrationTest extends ParticipantRestartTest {
  private val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransforms(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory)
      )
      .withSetup { implicit env =>
        NetworkBootstrapper(Seq(EnvironmentDefinition.S1M1)).bootstrap()
      }

  "restart with a inflight validation request with contract keys" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
    implicit env =>
      import env.*

      sequencer1.topology.synchronizer_parameters.propose_update(
        daId,
        _.update(
          confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofMinutes(10),
          reconciliationInterval = reconciliationInterval.toConfig,
        ),
      )

      val participant1 = startAndGet("participant1")
      val participant2 = startAndGet("participant2")

      connectToDa(participant1, noAutoReconnect = true)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)

      participants.all.dars.upload(CantonTestsLF23Path)

      val alice = participant1.parties.testing.enable(
        "Alice",
        synchronizeParticipants = Seq(participant2),
      )
      val bob = participant2.parties.testing.enable(
        "Bob",
        synchronizeParticipants = Seq(participant1),
      )

      val keyOpsProposalCreate = new KeyOps(
        damlSet(Set(alice.toProtoPrimitive)),
        bob.toProtoPrimitive,
      ).create.commands.asScala.toSeq
      val keyProposalTx =
        participant1.ledger_api.javaapi.commands.submit(Seq(alice), keyOpsProposalCreate)
      val Seq(keyOpsProposal) = JavaDecodeUtil.decodeAllCreated(KeyOps.COMPANION)(keyProposalTx)
      val acceptanceCmd = keyOpsProposal.id.exerciseAccept().commands.asScala.toSeq
      val acceptanceTx =
        participant2.ledger_api.javaapi.commands.submit(Seq(bob), acceptanceCmd)
      val Seq(keyOpsContract) = JavaDecodeUtil.decodeAllCreated(KeyOps.COMPANION)(acceptanceTx)

      // Do not inline into policy because this internally performs a GRPC call
      val participant1Id = participant1.id

      val sequencer = getProgrammableSequencer(sequencer1.name)

      val dropAliceConfirmationResponse = new AtomicBoolean(true)
      val firstConfirmationResponseDropped = Promise[Unit]()

      // drop the first confirmation response sent from participant 1.
      sequencer.setPolicy("drop first participant1 confirmation responses") {
        SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
          if (
            submissionRequest.sender == participant1Id && isConfirmationResponse(submissionRequest)
          ) {
            logger.info("Dropping mediator message.")
            val dropIt = dropAliceConfirmationResponse.getAndSet(false)
            if (dropIt) {
              firstConfirmationResponseDropped.success(())
              SendDecision.Reject()
            } else SendDecision.Process
          } else SendDecision.Process
        }
      }

      val stateInspection1 = this.stateInspectionFor(participant1)
      val lastCleanRequest =
        stateInspection1.lookupCleanTimeOfRequest(daId).value.futureValueUS.value
      val firstRequestCounter = lastCleanRequest.rc + 1L
      valueOrFail(stateInspection1.requestStateInJournal(firstRequestCounter, daId))(
        "Not find request counter for request"
      ).futureValueUS shouldBe None
      valueOrFail(stateInspection1.requestStateInJournal(firstRequestCounter - 1, daId))(
        "Not find request counter for request"
      ).futureValueUS shouldBe defined

      // 1. Bob checks that Alice's key is unallocated; Alice confirms this, but her confirmation response is dropped.
      // 2. Bob creates the key; we must wait for this request to become clean on Alice's participant
      // 3. Alice's participant crashes. Replay starts with replaying the first request as dirty.
      //    This should resend Alice's confirmation response and so the request should succeed.

      console.set_command_timeout(
        config.NonNegativeDuration.tryFromDuration(console.command_timeout.duration.mul(2))
      )
      val firstRequest = Future {
        val lookupCmd =
          keyOpsContract.id
            .exerciseLookupGivenKey(bob.toProtoPrimitive, damlSet(Set(alice.toProtoPrimitive)))
            .commands
            .asScala
            .toSeq
        participant2.ledger_api.javaapi.commands.submit(Seq(bob), lookupCmd)
      }
      val secondRequest = firstConfirmationResponseDropped.future.flatMap { _ =>
        // We can not wait for the second ping to finish because the TransactionAccepted updates will be stuck
        // in the event scheduler until the first request is resolved.
        // So we wait for the second request to become clean in the request journal (even though the clean cursor has not yet advanced).
        val secondRequestCounter = firstRequestCounter + 1L
        val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(10))
        logger.debug("Checking second request counter")
        valueOrFail(stateInspection1.requestStateInJournal(secondRequestCounter, daId))(
          "Not find request counter for second request"
        ).failOnShutdown.futureValue(patience, Position.here) shouldBe None
        logger.debug("About to send second request")
        val secondRequestF = Future {
          val createCmd =
            keyOpsContract.id
              .exerciseCreateKey(bob.toProtoPrimitive, damlSet(Set(alice.toProtoPrimitive)))
              .commands
              .asScala
              .toSeq
          logger.info("Sending second request")
          participant2.ledger_api.javaapi.commands.submit(Seq(bob), createCmd)
        }
        eventually(timeUntilSuccess = 60.seconds) {
          val resultF = stateInspection1.requestStateInJournal(secondRequestCounter, daId).value
          resultF.futureValueUS.value.state shouldBe RequestJournal.RequestState.Clean
        }
        logger.info("restarting participant 1")
        stopAndRestart(
          participant1,
          runBeforeReconnect = Some { () =>
            logger.info("before reconnecting participant 1")
            // Abort the test if the first request has already timed out
            val resultF = stateInspection1.requestStateInJournal(firstRequestCounter, daId).value
            if (resultF.futureValueUS.value.state == RequestJournal.RequestState.Clean)
              cancel()
            Seq(daName)
          },
        )
        logger.info("reconnected participant 1")
        secondRequestF
      }

      val patience =
        defaultPatience.copy(timeout = console.command_timeout.duration.plus(10.seconds))
      secondRequest.futureValue(patience, Position.here)
      firstRequest.futureValue(patience, Position.here)

      assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection1)
  }
}

class ParticipantRestartPruningIntegrationTest extends ParticipantRestartTest {
  private val reconciliationInterval = PositiveSeconds.tryOfSeconds(2)
  private val transactionTolerance = reconciliationInterval.unwrap

  private val internalPruningBatchSize =
    PositiveInt.tryCreate(200) // large enough to give each pruning batch enough work to do

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        // enabling retries to be on the safe side
        ConfigTransforms.setPingRetries(true),
        ConfigTransforms.updatePruningBatchSize(internalPruningBatchSize),
        ConfigTransforms.updateMaxDeduplicationDurations(transactionTolerance),
      )
      .withSetup { implicit env =>
        NetworkBootstrapper(
          Seq(EnvironmentDefinition.S1M1)
        ).bootstrap()

      }

  "successfully restart if participant crashes during pruning" in { implicit env =>
    import env.*

    sequencer1.topology.synchronizer_parameters.propose_update(
      daId,
      _.update(
        confirmationResponseTimeout =
          config.NonNegativeFiniteDuration.tryFromJavaDuration(transactionTolerance),
        mediatorReactionTimeout =
          config.NonNegativeFiniteDuration.tryFromJavaDuration(transactionTolerance),
        reconciliationInterval = reconciliationInterval.toConfig,
        confirmationRequestsMaxRate = NonNegativeInt.tryCreate(Int.MaxValue),
      ),
    )

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)

    val updateFormat = UpdateFormat(
      includeTransactions = Some(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(participant1.adminParty.toLf -> Filters(Nil)),
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        )
      ),
      includeReassignments = None,
      includeTopologyEvents = None,
    )

    val clock = environment.simClock.value
    clock.advance(JDuration.ofSeconds(1))

    val pingF = Future.traverse((1 to 200).toList) { _ =>
      Future {
        assertPingSucceeds(participant1, participant1, timeoutMillis = 60000)
      }
    }

    eventually(timeUntilSuccess = 1.minute) {
      pingF.isCompleted shouldBe true
    }
    pingF.futureValue

    logger.info("Participant has been populated with transactions.")

    val storageP1 = createStorageFor(participant1)
    val stateInspection = stateInspectionFor(participant1, storageP1.some)
    val pruneOffset = participant1.ledger_api.state.end()

    // Advance clock until it's safe to prune.
    eventually() {
      clock.advance(transactionTolerance)
      participant1.health.ping(participant1)
      val ledgerEnd = participant1.ledger_api.state.end()
      val ledgerEndOffset = Offset.fromLong(ledgerEnd).value

      // Create a fresh pruning processor object so we get a fresh publication time lower bound
      val pruningProcessor = pruningProcessorFor(participant1, storageP1.some)
      val safeOffset =
        pruningProcessor.safeToPrune(clock.now, ledgerEndOffset, None).futureValueUS.value.value
      assert(safeOffset.unwrap >= pruneOffset)
    }

    val firstTx = participant1.ledger_api.updates
      .transactions(
        Set(participant1.adminParty),
        completeAfter = 1,
        endOffsetInclusive = Some(pruneOffset),
      )
      .headOption
      .value
    val firstUpdateId = firstTx.updateId
    val firstContractId = firstTx.transaction.events
      .map(_.event)
      .collectFirst {
        case Event.Created(created) => created.contractId
        case Event.Archived(archived) => archived.contractId
      }
      .value
    // Look up last archived ping as a sign that pruning has finished.
    val lastUpdateId = participant1.ledger_api.updates
      .transactions(
        Set(participant1.adminParty),
        completeAfter = 1000000,
        endOffsetInclusive = Some(pruneOffset),
      )
      .lastOption
      .value
      .updateId

    // Make sure that the first update can be queried before pruning
    participant1.ledger_api.updates
      .update_by_id(firstUpdateId, updateFormat) should not be empty

    // Make sure that the last update can be queried before pruning
    participant1.ledger_api.updates
      .update_by_id(lastUpdateId, updateFormat) should not be empty

    val pruningStoreForPolling = new DbParticipantPruningStore(
      name = ParticipantPruningStore.dbStoreName,
      storage = storageP1,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
    val statusBeforePruning = pruningStoreForPolling.pruningStatus().futureValueUS

    logger.info(s"Pruning at $pruneOffset, current pruning status: $statusBeforePruning ...")

    // Start pruning in the background
    val pruneF = loggerFactory.assertThrowsAndLogsAsync[CommandFailure](
      Future(pruneParticipant(participant1, pruneOffset)),
      _ => succeed,
      _.commandFailureMessage should include("UNAVAILABLE/Network closed for unknown reason"),
    )

    // Wait until we are sure that ledger-level pruning has started
    eventually(maxPollInterval = 2.millis) { // low poll interval so that we don't flake because of exponential backoff and missing the prune
      logger.info(s"Loading pruning status")
      // Using Await as futureValue might evaluate via polling / resulting longer wait times here
      val status = Await.result(pruningStoreForPolling.pruningStatus(), 20.millis).toOption.value
      logger.info(s"Loaded pruning status: $status")
      status.startedO.isDefined shouldBe true
    }

    // Restart
    logger.info("Restarting...")
    stopAndRestart(participant1)

    // Wait until pruning command has terminated
    pruneF.futureValue

    // Make sure the external participant has logged the unfinished pruning.
    val participant1LogFile = File(external.logFile(participant1.name))

    // Read warnings in the participant's log file.
    // Use ISO_8859_1, because that can also deal with incomplete characters.
    // Incomplete characters may occur because the participant is killed.
    // Incomplete characters would crash decoding with UTF8.
    // Credits to: https://stackoverflow.com/a/44233948
    val participant1Warnings =
      participant1LogFile.lines(StandardCharsets.ISO_8859_1).filter(_.contains("WARN"))

    // Tolerate several warnings so the test succeeds even on an old log file.
    forEvery(participant1Warnings) {
      _ should (
        // Occasionally a later pruning batch is interrupted, so don't check offsets specific to the first batch
        include("Unfinished pruning operation. The participant has been partially pruned up to")
        // Occasionally the ledger api command service manages to cancel its CommandTrackerFlow causing this warning:
          or include(
            "CommandTrackerFlow$ - Completion Stream failed with an error. Trying to recover"
          )
      )
    }

    // But at least one error needs to be about unfinished pruning.
    forAtLeast(1, participant1Warnings) {
      _ should include(
        s"Unfinished pruning operation. The participant has been partially pruned up to"
      )
    }

    // Make sure that pruning has not completed
    stateInspection.contractCount.futureValueUS should be > 0 withClue
      s"The new first offset is not smaller than pruning offset $pruneOffset. Did we miss restarting the participant before prune finished?"

    // Make sure that the first update can still be queried, as the ledger api server index
    // has not been pruned (only canton stores were partially pruned before the restart)
    participant1.ledger_api.updates
      .update_by_id(firstUpdateId, updateFormat) should not be empty withClue
      "Since we have interrupted pruning and ledger api server index has not been pruned, first transaction must still be readable"

    participant1.ledger_api.updates
      .update_by_offset(firstTx.transaction.offset, updateFormat) should not be empty withClue
      "Since we have interrupted pruning and ledger api server index has not been pruned, first transaction must still be readable"

    participant1.ledger_api.event_query
      .by_contract_id(firstContractId, Seq(participant1.adminParty.toLf))

    participant1.ledger_api.updates
      .updates(
        updateFormat = updateFormat,
        completeAfter = PositiveInt.tryCreate(1000000),
        endOffsetInclusive = Some(pruneOffset),
      )

    participant1.ledger_api.state.acs
      .of_party(participant1.adminParty.toLf, activeAtOffsetO = Some(firstTx.transaction.offset))

    // Make sure also that the last update can still be queried
    participant1.ledger_api.updates
      .update_by_id(lastUpdateId, updateFormat) should not be empty withClue
      "Since we have interrupted pruning and ledger api server index has not been pruned, last transaction must still be readable"

    // Make sure the participant is still functional despite partial pruning
    assertPingSucceeds(participant1, participant1)
    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection)

    // Now complete the pruning
    pruneParticipant(participant1, pruneOffset)

    // Check consistency between observable events and the sequencer messages.
    val firstSynchronizerEvent = participant1.ledger_api.updates
      .transactions(
        partyIds = Set(participant1.adminParty),
        completeAfter = 1,
        beginOffsetExclusive = pruneOffset,
      )
      .headOption
      .value
      .transaction
    firstSynchronizerEvent.synchronizerId shouldBe daId.logical.toProtoPrimitive
    val firstSequencerMessage = stateInspection
      .findMessages(daId, from = None, to = None, limit = None, warnOnDiscardedEnvelopes = true)
      .minBy(_.counter)
    assert(
      firstSequencerMessage.timestamp.underlying <= LfTimestamp.assertFromInstant(
        firstSynchronizerEvent.recordTime.value.asJavaInstant
      ),
      s"After consistent pruning, no event should be older than the oldest sequencer message, but $firstSynchronizerEvent is older than $firstSequencerMessage",
    )

    // Now the last update should have been pruned
    participant1.ledger_api.updates
      .update_by_id(lastUpdateId, updateFormat) shouldBe empty withClue
      "After pruning not interrupted, ledger api server index should indicate pruning went through"

    assertActiveContractsMatchBetweenCantonAndLedgerApiServer(participant1, stateInspection)

    // Advance the clock once more such that the current time is indeed `maxDedupDuration` after the pruning offset's publication time.
    clock.advance(java.time.Duration.ofNanos(1000))

    assertPingSucceeds(participant1, participant1)
  }
}

private object ParticipantRestartTest {
  def awaitTopologyUpToDate(
      targetSynchronizerId: PhysicalSynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      participant1: ParticipantReference,
      participant2: ParticipantReference,
  )(implicit elc: ErrorLoggingContext): Unit = {
    elc.info(s"Perform ping to ensure participant is running")
    participant1.health.ping(participant2.id, synchronizerId = Some(targetSynchronizerId.logical))

    // Await the time when the participant should have processed any topology transactions that are sequenced before the ping
    // This is used to make sure participants pick a target synchronizer timestamp on `synchronizer` where the identity state makes sense
    val safeTs =
      CantonTimestamp.now().plus(staticSynchronizerParameters.topologyChangeDelay.toConfig.asJava)
    participant1.testing.await_synchronizer_time(targetSynchronizerId, safeTs)
    participant2.testing.await_synchronizer_time(targetSynchronizerId, safeTs)
  }
}
