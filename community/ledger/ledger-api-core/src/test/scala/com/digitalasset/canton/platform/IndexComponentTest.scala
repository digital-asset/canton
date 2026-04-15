// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries, Offset}
import com.digitalasset.canton.ledger.api.{CumulativeFilter, EventFormat, TemplateWildcardFilter}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.Onboarding
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.SameAsContractPackageId
import com.digitalasset.canton.ledger.participant.state.Update.{
  ContractInfo,
  OnPRReassignmentAccepted,
  RepairReassignmentAccepted,
  RepairTransactionAccepted,
  SequencedReassignmentAccepted,
  SequencedTransactionAccepted,
  TopologyTransactionEffective,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.{CommonMockMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.participant.ledger.api.LedgerApiJdbcUrl
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.IndexComponentTest.TestServices
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.config.{IndexServiceConfig, ServerRole}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.parallel.NoOpReassignmentOffsetPersistence
import com.digitalasset.canton.platform.indexer.{IndexerConfig, JdbcIndexer}
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.{ContractLoader, LfValueTranslation}
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.{
  DbSupport,
  FlywayMigrations,
  LedgerApiContractStoreImpl,
  PruningOffsetService,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ExampleContractFactory,
  LfContractId,
  ReassignmentId,
  TestUpdateId,
  UpdateId,
}
import com.digitalasset.canton.resource.DbStorageSingle
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.{SimClock, WallClock}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.PekkoUtil.{FutureQueue, IndexingFutureQueue}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{BaseTest, HasExecutorService, RepairCounter, platform}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, Node}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import org.scalatest.Suite
import org.scalatest.concurrent.PatienceConfiguration

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

trait IndexComponentTest
    extends PekkoBeforeAndAfterAll
    with BaseTest
    with HasExecutorService
    with HasCloseContext
    with FlagCloseable {
  self: Suite =>

  private val clock = new WallClock(ProcessingTimeout(), loggerFactory)

  implicit val ec: ExecutionContext = system.dispatcher

  protected implicit val loggingContextWithTrace: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private val dbName: String = getClass.getSimpleName.toLowerCase

  protected val dbConfig: com.digitalasset.canton.config.DbConfig =
    DbBasicConfig(username = "", password = "", dbName = dbName, host = "", port = 0).toH2DbConfig

  protected def jdbcUrl: String = LedgerApiJdbcUrl.fromDbConfig(dbConfig).value.url

  protected val indexerConfig: IndexerConfig = IndexerConfig()

  protected val indexServiceConfig: IndexServiceConfig = IndexServiceConfig()

  protected val indexReadConnectionPoolSize: Int = 10

  private val testServicesRef: AtomicReference[TestServices] = new AtomicReference()

  private def testServices: TestServices =
    Option(testServicesRef.get())
      .getOrElse(throw new Exception("TestServices not initialized. Not accessing from a test?"))

  private def ledgerEndOffset = testServices.index.currentLedgerEnd().futureValue

  protected def ingestUpdates(updates: (Update, Vector[ContractInstance])*): Offset = {
    val ledgerEndLongBefore = ledgerEndOffset.map(_.positive).getOrElse(0L)
    val ingestionTimeout = 60.minutes
    // contracts should be stored in participant contract store before ingesting the updates to get the internal contract ids mapping
    MonadUtil
      .sequentialTraverse_(updates) { case (update, contracts) =>
        storeContracts(update, contracts).flatMap(testServices.indexer.offer)
      }
      .futureValue(timeout = PatienceConfiguration.Timeout(ingestionTimeout))
    val expectedOffset = Offset.tryFromLong(updates.size + ledgerEndLongBefore)
    eventually(timeUntilSuccess = ingestionTimeout) {
      ledgerEndOffset shouldBe Some(expectedOffset)
      expectedOffset
    }
  }

  protected def ingestUpdateAsync(update: Update): Future[Unit] =
    testServices.indexer.offer(update).map(_ => ())

  protected def storeContracts(
      update: Update,
      contracts: Vector[ContractInstance],
  ): Future[Update] =
    // this mimics protocol processing that stores contracts and retrieves their internal contract ids afterward
    testServices.participantContractStore.participantContractStore
      .storeContracts(contracts)
      .failOnShutdownToAbortException("storeContracts")
      .flatMap(_ =>
        testServices.participantContractStore
          .lookupBatchedInternalIdsNonReadThrough(
            contracts.map(_.contractId)
          )
      )
      .map { internalContractIds =>
        update match {
          case txAccepted: SequencedTransactionAccepted =>
            txAccepted.copy(contractInfos =
              injectInternalContractIds(
                txAccepted.contractInfos,
                internalContractIds,
              )
            )
          case txAccepted: RepairTransactionAccepted =>
            txAccepted.copy(contractInfos =
              injectInternalContractIds(
                txAccepted.contractInfos,
                internalContractIds,
              )
            )
          case reassignment: SequencedReassignmentAccepted =>
            reassignment.copy(
              reassignment =
                injectInternalContractIds(reassignment.reassignment, internalContractIds)
            )
          case reassignment: RepairReassignmentAccepted =>
            reassignment.copy(
              reassignment =
                injectInternalContractIds(reassignment.reassignment, internalContractIds)
            )
          case reassignment: OnPRReassignmentAccepted =>
            reassignment.copy(
              reassignment =
                injectInternalContractIds(reassignment.reassignment, internalContractIds)
            )
          case other => other
        }
      }

  private def injectInternalContractIds(
      contractInfo: Map[LfContractId, ContractInfo],
      internalContractIds: Map[LfContractId, Long],
  ): Map[LfContractId, ContractInfo] =
    internalContractIds.foldLeft(contractInfo) { case (acc, (coid, internalContractId)) =>
      acc.updated(
        coid,
        acc.get(coid) match {
          case Some(contractInfo) => contractInfo.copy(internalContractId = internalContractId)
          case None =>
            ContractInfo(
              internalContractId = internalContractId,
              contractAuthenticationData = Bytes.Empty,
              representativePackageId = SameAsContractPackageId,
            )
        },
      )
    }

  private def injectInternalContractIds(
      reassignmentBatch: Reassignment.Batch,
      internalContractIds: Map[LfContractId, Long],
  ): Reassignment.Batch = Reassignment.Batch(
    reassignmentBatch.reassignments.map {
      case assign: Reassignment.Assign =>
        assign.copy(internalContractId = internalContractIds.get(assign.createNode.coid).value)
      case unassign: Reassignment.Unassign => unassign: Reassignment
    }
  )

  private val wildcardTemplates = CumulativeFilter(
    templateFilters = Set.empty,
    interfaceFilters = Set.empty,
    templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
  )
  protected def eventFormat(party: Ref.Party) = EventFormat(
    filtersByParty = Map(
      party -> wildcardTemplates
    ),
    filtersForAnyParty = None,
    verbose = false,
  )
  protected val allPartyEventFormat = EventFormat(
    filtersByParty = Map.empty,
    filtersForAnyParty = Some(wildcardTemplates),
    verbose = false,
  )

  protected def index: IndexService = testServices.index

  protected def dbSupport: DbSupport = testServices.dbSupport

  protected def sequentialPostProcessor: Update => Unit = _ => ()

  @scala.annotation.unused
  protected def incompleteOffsets(
      _o: Offset,
      _p: Option[Set[Ref.Party]],
      _tc: TraceContext,
  ): FutureUnlessShutdown[Vector[Offset]] = FutureUnlessShutdown.pure(Vector.empty)

  private lazy val engine =
    new Engine(EngineConfig(LanguageVersion.stableLfVersionsRange), loggerFactory)
  private lazy val participantId =
    Ref.ParticipantId.assertFromString("index-component-test-participant-id")
  private lazy val pruningOffsetService = new PruningOffsetService {
    override def pruningOffset(implicit
        traceContext: TraceContext
    ): Future[Option[Offset]] = Future.successful(None)
  }

  implicit val scheduler: ScheduledExecutorService = scheduledExecutor()

  private def indexResourceOwner(
      config: IndexerConfig,
      serviceConfig: IndexServiceConfig,
      repairMode: Boolean,
  ): ResourceOwner[(IndexService, FutureQueue[Update], LedgerApiContractStoreImpl, DbSupport)] =
    for {
      dbStorage <- ResourceOwner
        .forCloseable(() =>
          DbStorageSingle
            .tryCreate(
              config = dbConfig,
              connectionPoolForParticipant = false,
              logQueryCost = None,
              clock = new SimClock(CantonTimestamp.Epoch, loggerFactory),
              scheduler = None,
              metrics = CommonMockMetrics.dbStorage,
              timeouts = timeouts,
              loggerFactory = loggerFactory,
            )
        )
      contractStore <-
        ResourceOwner
          .forCloseable(() =>
            ContractStore.create(
              storage = dbStorage,
              processingTimeouts = timeouts,
              cachingConfigs = CachingConfigs(),
              batchingConfig = BatchingConfig(),
              loggerFactory = loggerFactory,
            )
          )
      participantContractStore = LedgerApiContractStoreImpl(
        contractStore,
        loggerFactory,
        LedgerApiServerMetrics.ForTesting,
      )
      mutableLedgerEndCache = MutableLedgerEndCache()
      stringInterningView = new StringInterningView(loggerFactory)
      (inMemoryState, updaterFlow) <- LedgerApiServerInternals.createInMemoryStateAndUpdater(
        participantId = participantId,
        commandProgressTracker = CommandProgressTracker.NoOp,
        indexServiceConfig = serviceConfig,
        maxCommandsInFlight = 1, // not used
        metrics = LedgerApiServerMetrics.ForTesting,
        executionContext = ec,
        tracer = NoReportingTracerProvider.tracer,
        loggerFactory = loggerFactory,
      )(mutableLedgerEndCache, stringInterningView)
      _ <- ResourceOwner.forFuture(() => new FlywayMigrations(jdbcUrl, loggerFactory).migrate())
      dbSupport <- DbSupport
        .owner(
          serverRole = ServerRole.ApiServer,
          metrics = LedgerApiServerMetrics.ForTesting,
          dbConfig = DbConfig(
            jdbcUrl = jdbcUrl,
            connectionPool = ConnectionPoolConfig(
              connectionPoolSize = indexReadConnectionPoolSize,
              connectionTimeout = 250.millis,
            ),
          ),
          loggerFactory = loggerFactory,
        )
      indexerF <- new JdbcIndexer.Factory(
        participantId = participantId,
        participantDataSourceConfig = DbSupport.ParticipantDataSourceConfig(jdbcUrl),
        config = config,
        metrics = LedgerApiServerMetrics.ForTesting,
        inMemoryState = inMemoryState,
        apiUpdaterFlow = updaterFlow,
        executionContext = ec,
        tracer = NoReportingTracerProvider.tracer,
        loggerFactory = loggerFactory,
        dataSourceProperties =
          IndexerConfig.createDataSourcePropertiesForTesting(config, loggerFactory),
        highAvailability = HaConfig(),
        indexServiceDbDispatcher = Some(dbSupport.dbDispatcher),
        clock = clock,
        reassignmentOffsetPersistence = NoOpReassignmentOffsetPersistence,
        postProcessor = (_, _) => Future.unit,
        sequentialPostProcessor = sequentialPostProcessor,
        contractStore = participantContractStore,
      ).initialized()
      indexerFutureQueueConsumer <- ResourceOwner.forFuture(() => indexerF(repairMode)(_ => ()))
      indexer <- ResourceOwner.forReleasable(() =>
        new IndexingFutureQueue(indexerFutureQueueConsumer)
      ) { indexer =>
        indexer.shutdown()
        indexer.done.map(_ => ())
      }
      contractLoader <- ContractLoader.create(
        participantContractStore = participantContractStore,
        contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
          inMemoryState.stringInterningView,
          inMemoryState.ledgerEndCache,
        ),
        dbDispatcher = dbSupport.dbDispatcher,
        metrics = LedgerApiServerMetrics.ForTesting,
        maxQueueSize = 10000,
        maxBatchSize = 50,
        parallelism = 5,
        loggerFactory = loggerFactory,
      )
      indexService <- new IndexServiceOwner(
        dbSupport = dbSupport,
        config = serviceConfig,
        participantId = Ref.ParticipantId.assertFromString(IndexComponentTest.TestParticipantId),
        metrics = LedgerApiServerMetrics.ForTesting,
        inMemoryState = inMemoryState,
        tracer = NoReportingTracerProvider.tracer,
        loggerFactory = loggerFactory,
        incompleteOffsets = incompleteOffsets,
        contractLoader = contractLoader,
        getPackageMetadataSnapshot = _ => PackageMetadata(),
        lfValueTranslation = new LfValueTranslation(
          metrics = LedgerApiServerMetrics.ForTesting,
          engineO = Some(engine),
          // Not used
          loadPackage = (_, _) => Future(None),
          loggerFactory = loggerFactory,
        ),
        queryExecutionContext = executorService,
        commandExecutionContext = executorService,
        getPackagePreference = (
            _: PackageName,
            _: Set[PackageId],
            _: String,
            _: LoggingContextWithTrace,
        ) => FutureUnlessShutdown.pure(Left("not used")),
        participantContractStore = participantContractStore,
        pruningOffsetService = pruningOffsetService,
      )
    } yield (indexService, indexer, participantContractStore, dbSupport)

  private def acquireServices(
      config: IndexerConfig,
      serviceConfig: IndexServiceConfig,
      repairMode: Boolean,
  )(implicit resourceContext: ResourceContext): Unit = {
    val indexResource = indexResourceOwner(config, serviceConfig, repairMode).acquire()
    val (index, indexer, participantContractStore, dbSupport) =
      indexResource.asFuture.futureValue(timeout = PatienceConfiguration.Timeout(60.seconds))

    testServicesRef.set(
      TestServices(
        indexResource = indexResource,
        index = index,
        indexer = indexer,
        participantContractStore = participantContractStore,
        dbSupport = dbSupport,
      )
    )
  }

  /** Restarts the indexer and all related services by releasing and re-acquiring all resources.
    * Optionally accepts a new IndexerConfig to change the configuration on restart.
    */
  protected def restartIndexer(
      config: IndexerConfig = indexerConfig,
      serviceConfig: IndexServiceConfig = indexServiceConfig,
      repairMode: Boolean = false,
  ): Unit = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    testServices.indexResource.release().futureValue
    acquireServices(config, serviceConfig, repairMode)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We use the dispatcher here because the default Scalatest execution context is too slow.
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    acquireServices(config = indexerConfig, serviceConfig = indexServiceConfig, repairMode = false)
  }

  override def afterAll(): Unit = {
    testServices.indexResource.release().futureValue
    super.afterAll()
  }

  protected object TxBuilder {
    def apply(): NodeIdTransactionBuilder & TestNodeBuilder = new NodeIdTransactionBuilder
      with TestNodeBuilder
  }

  protected val synchronizer1: SynchronizerId = SynchronizerId.tryFromString("x::synchronizer1")
  protected val synchronizer2: SynchronizerId = SynchronizerId.tryFromString("x::synchronizer2")
  protected val packageName: Ref.PackageName = Ref.PackageName.assertFromString("-package-name-")
  protected val dsoParty: Party = Ref.Party.assertFromString("dsoParty") // sees all
  private lazy val parties =
    (1 to 10000).view.map(index => Ref.Party.assertFromString(s"party$index")).toVector
  protected lazy val templates: Seq[Ref.FullReference[PackageId]] =
    (1 to 300).view.map(index => Ref.Identifier.assertFromString(s"P:M:T$index")).toVector

  private val someLFHash = com.digitalasset.daml.lf.crypto.Hash
    .assertFromString("01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086")

  private val random = new scala.util.Random
  private def randomString(length: Int) = {
    val sb = new mutable.StringBuilder()
    for (_ <- 1 to length) {
      sb.append(random.alphanumeric.head)
    }
    sb.toString
  }

  private def randomTemplate = templates(random.nextInt(templates.size))
  private def randomParty = parties(random.nextInt(parties.size))
  private def randomHash: Hash = Hash.digest(
    HashPurpose.PreparedSubmission,
    ByteString.copyFromUtf8(s"${random.nextLong()}"),
    Sha256,
  )
  protected def randomUpdateId: UpdateId = TestUpdateId(randomHash.toHexString)
  private def randomLength(lengthFromToInclusive: (Int, Int)) = {
    val (from, to) = lengthFromToInclusive
    val randomDistance = to - from + 1
    assert(randomDistance > 1, s"random range ($from, $to) must have length of at least 1")
    from + random.nextInt(randomDistance)
  }
  private val builder = TxBuilder()
  private val testAcsChangeFactory = TestAcsChangeFactory()

  protected def createsAndArchives(
      nextRecordTime: () => CantonTimestamp,
      txSize: Int,
      txsCreatedThenArchived: Int,
      txsCreatedNotArchived: Int,
      createPayloadLength: Int,
      archiveArgumentPayloadLengthFromTo: (Int, Int),
      archiveResultPayloadLengthFromTo: (Int, Int),
  ): Vector[(Update.SequencedTransactionAccepted, Vector[ContractInstance])] = {
    val (createTxs, contracts) =
      (1 to txsCreatedThenArchived + txsCreatedNotArchived).iterator
        .map(_ =>
          creates(
            recordTime = nextRecordTime,
            payloadLength = createPayloadLength,
          )(txSize)
        )
        .toVector
        .unzip
    val archivingTxs = contracts.iterator
      .take(txsCreatedThenArchived)
      .map(_.map(_.inst.toCreateNode))
      .map(
        archives(
          recordTime = nextRecordTime,
          argumentLength = randomLength(archiveArgumentPayloadLengthFromTo),
          resultLength = randomLength(archiveResultPayloadLengthFromTo),
        )
      )
      .toVector
    createTxs.zip(contracts) ++ archivingTxs.map(_ -> Vector.empty)
  }

  protected def creates(recordTime: () => CantonTimestamp, payloadLength: Int)(
      size: Int
  ): (Update.SequencedTransactionAccepted, Vector[ContractInstance]) = {
    val txBuilder = TxBuilder()
    val contracts = createContracts(payloadLength, size)
    contracts.map(_.inst.toCreateNode).foreach(txBuilder.add)
    val tx = txBuilder.buildCommitted()
    val contractAuthenticationData = contracts
      .map(
        _.contractId -> Bytes.fromByteString(ByteString.copyFromUtf8(randomString(42)))
      )
      .toMap
    transaction(
      synchronizerId = synchronizer1,
      recordTime = recordTime(),
    )(tx, contractAuthenticationData) -> contracts
  }

  private def createContracts(payloadLength: Int, size: Int) =
    (1 to size)
      .map(_ =>
        genContract(
          argumentPayload = randomString(payloadLength),
          template = randomTemplate,
          signatories = Set(
            dsoParty,
            randomParty,
            randomParty,
            randomParty,
          ),
        )
      )
      .toVector

  protected def ingestPartyOnboarding(parties: Set[String], recordTime: CantonTimestamp): Offset = {
    val topologyTransaction = TopologyTransactionEffective(
      updateId = randomUpdateId,
      events = parties.map(party =>
        PartyToParticipantAuthorization(
          party = Ref.Party.assertFromString(party),
          participant = Ref.ParticipantId.assertFromString("participant"),
          authorizationEvent = Onboarding(AuthorizationLevel.Observation),
        )
      ),
      synchronizerId = synchronizer1,
      effectiveTime = recordTime,
    )
    val ledgerEndBeforeTopology = index.currentLedgerEnd().futureValue
    ingestUpdateAsync(topologyTransaction).futureValue
    eventually() {
      val ledgerEndAfterTopology = index.currentLedgerEnd().futureValue
      ledgerEndAfterTopology should be > ledgerEndBeforeTopology
      ledgerEndAfterTopology.value
    }
  }

  protected def repairCreates(recordTime: () => CantonTimestamp, payloadLength: Int)(
      size: Int
  ): (Update.RepairTransactionAccepted, Vector[ContractInstance]) = {
    val (sequenced, contracts) = creates(recordTime, payloadLength)(size)
    repairTransaction(sequenced) -> contracts
  }

  protected def archives(
      recordTime: () => CantonTimestamp,
      argumentLength: Int,
      resultLength: Int,
  )(
      creates: Seq[Node.Create]
  ): Update.SequencedTransactionAccepted = {
    val txBuilder = TxBuilder()
    val archives = creates.iterator
      .map(archiveCreatedContract(argumentLength, resultLength))
      .toVector
    archives.foreach(txBuilder.add)
    val tx = txBuilder.buildCommitted()
    transaction(
      synchronizerId = synchronizer1,
      recordTime = recordTime(),
    )(tx)
  }

  private def archiveCreatedContract(argumentLength: Int, resultLength: Int)(
      create: Node.Create
  ): Node.Exercise =
    archive(
      create = create,
      actingParties = Set(
        randomParty,
        randomParty,
        randomParty,
      ),
      argumentPayload = randomString(argumentLength),
      resultPayload = randomString(resultLength),
    )

  def genContract(
      argumentPayload: String,
      template: Ref.Identifier,
      signatories: Set[Party],
  ): ContractInstance =
    ExampleContractFactory
      .build(
        templateId = template,
        argument = Value.ValueRecord(
          tycon = None,
          fields = ImmArray(None -> Value.ValueText(argumentPayload)),
        ),
        signatories = signatories,
        stakeholders = signatories,
        packageName = packageName,
      )

  private def archive(
      create: Node.Create,
      actingParties: Set[Ref.Party],
      argumentPayload: String,
      resultPayload: String,
  ): platform.Exercise =
    builder.exercise(
      contract = create,
      choice = Ref.Name.assertFromString("archivingarchivingarchivingarchivingarchivingarchiving"),
      consuming = true,
      actingParties = actingParties,
      argument = Value.ValueRecord(
        tycon = None,
        fields = ImmArray(None -> Value.ValueText(argumentPayload)),
      ),
      byKey = false,
      interfaceId = None,
      result = Some(
        Value.ValueRecord(
          tycon = None,
          fields = ImmArray(None -> Value.ValueText(resultPayload)),
        )
      ),
    )

  protected def transaction(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(
      transaction: CommittedTransaction,
      contractAuthenticationData: Map[ContractId, Bytes] = Map.empty,
  ): Update.SequencedTransactionAccepted =
    Update.SequencedTransactionAccepted(
      completionInfoO = None,
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = recordTime.underlying,
        workflowId = None,
        preparationTime = recordTime.underlying,
        submissionSeed = someLFHash,
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transactionInfo = Update.TransactionAccepted.TransactionInfo(transaction),
      updateId = randomUpdateId,
      synchronizerId = synchronizerId,
      recordTime = recordTime,
      acsChangeFactory = testAcsChangeFactory,
      externalTransactionHash = None,
      contractInfos = contractAuthenticationData.map { case (cid, authData) =>
        cid -> ContractInfo(
          internalContractId = 0L,
          contractAuthenticationData = authData,
          representativePackageId = SameAsContractPackageId,
        )
      },
    )

  protected def mkReassignmentAccepted(
      party: Ref.Party,
      updateIdS: String,
      withAcsChange: Boolean,
      createNode: Node.Create,
  ): Update.ReassignmentAccepted = {
    val synchronizer1 = SynchronizerId.tryFromString("x::synchronizer1")
    val synchronizer2 = SynchronizerId.tryFromString("x::synchronizer2")
    val updateId = TestUpdateId(updateIdS)
    val recordTime = Time.Timestamp.now()
    if (withAcsChange)
      Update.OnPRReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizer1),
          targetSynchronizer = Target(synchronizer2),
          submitter = Option(party),
          reassignmentId = ReassignmentId.tryCreate("00"),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.now(),
            createNode = createNode,
            contractAuthenticationData = Bytes.Empty,
            reassignmentCounter = 15L,
            nodeId = 0,
            internalContractId =
              -1, // will be filled when contracts are stored in the participant contract store
          )
        ),
        repairCounter = RepairCounter.Genesis,
        recordTime = CantonTimestamp(recordTime),
        synchronizerId = synchronizer2,
        acsChangeFactory = TestAcsChangeFactory(),
      )
    else
      Update.RepairReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizer1),
          targetSynchronizer = Target(synchronizer2),
          submitter = Option(party),
          reassignmentId = ReassignmentId.tryCreate("00"),
          isReassigningParticipant = true,
        ),
        reassignment = Reassignment.Batch(
          Reassignment.Assign(
            ledgerEffectiveTime = Time.Timestamp.now(),
            createNode = createNode,
            contractAuthenticationData = Bytes.Empty,
            reassignmentCounter = 15L,
            nodeId = 0,
            internalContractId =
              -1, // will be filled when contracts are stored in the participant contract store
          )
        ),
        repairCounter = RepairCounter.Genesis,
        recordTime = CantonTimestamp(recordTime),
        synchronizerId = synchronizer2,
      )
  }

  protected def ingestUpdateSync(update: Update): Offset = {
    val ledgerEndBefore = index.currentLedgerEnd().futureValue
    ingestUpdateAsync(update).futureValue

    eventually() {
      val ledgerEndAfter = index.currentLedgerEnd().futureValue
      ledgerEndAfter should be > ledgerEndBefore
      ledgerEndAfter.value
    }
  }
  protected def repairTransaction(
      sequenced: Update.SequencedTransactionAccepted
  ): Update.RepairTransactionAccepted =
    Update.RepairTransactionAccepted(
      transactionMeta = sequenced.transactionMeta,
      transactionInfo = sequenced.transactionInfo,
      updateId = sequenced.updateId,
      synchronizerId = sequenced.synchronizerId,
      repairCounter = RepairCounter.Genesis,
      recordTime = sequenced.recordTime,
      contractInfos = sequenced.contractInfos,
    )

}

object IndexComponentTest {

  val TestParticipantId = "index-component-test-participant-id"

  final case class TestServices(
      indexResource: Resource[Any],
      index: IndexService,
      indexer: FutureQueue[Update],
      participantContractStore: LedgerApiContractStoreImpl,
      dbSupport: DbSupport,
  )
}
