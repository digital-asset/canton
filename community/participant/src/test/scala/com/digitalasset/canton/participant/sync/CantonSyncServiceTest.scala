// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import akka.stream.Materializer
import cats.Id
import cats.data.EitherT
import cats.implicits.*
import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.lf.CantonOnly
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.ValueRecord
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric, String255}
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  BatchAggregatorConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
  LoggingConfig,
}
import com.digitalasset.canton.crypto.{Fingerprint, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.{
  AdminWorkflowConfig,
  PackageService,
  ResourceManagementService,
  workflows,
}
import com.digitalasset.canton.participant.config.{
  LedgerApiServerParametersConfig,
  ParticipantProtocolConfig,
  ParticipantStoreConfig,
  PartyNotificationConfig,
}
import com.digitalasset.canton.participant.domain.{DomainAliasManager, DomainRegistry}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.pruning.NoOpPruningProcessor
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.ParticipantEventLog.ProductionParticipantEventLogId
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.{
  InMemoryParticipantEventLog,
  InMemoryParticipantSettingsStore,
}
import com.digitalasset.canton.participant.sync.LedgerSyncEvent.TransactionAccepted
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  ParticipantTopologyManager,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreFactory
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  PartyToParticipant,
  RequestSide,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyElementId,
  TopologyStateUpdate,
  TopologyStateUpdateElement,
  TopologyTransaction,
}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  DefaultDamlValues,
  HasExecutionContext,
  LedgerSubmissionId,
  LfPartyId,
}
import org.mockito.ArgumentMatchers
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.Future
import scala.jdk.FutureConverters.*

class CantonSyncServiceTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {

  private val LocalNodeParameters = ParticipantNodeParameters(
    general = CantonNodeParameters.General.Impl(
      tracing = TracingConfig(TracingConfig.Propagation.Disabled),
      delayLoggingThreshold = NonNegativeFiniteDuration.ofMillis(5000),
      enableAdditionalConsistencyChecks = true,
      loggingConfig = LoggingConfig(api = ApiLoggingConfig(messagePayloads = Some(true))),
      logQueryCost = None,
      processingTimeouts = DefaultProcessingTimeouts.testing,
      enablePreviewFeatures = false,
      nonStandardConfig = false,
      cachingConfigs = CachingConfigs(),
      sequencerClient = SequencerClientConfig(),
    ),
    partyChangeNotification = PartyNotificationConfig.Eager,
    adminWorkflow = AdminWorkflowConfig(
      bongTestMaxLevel = 10,
      retries = 10,
      submissionTimeout = NonNegativeFiniteDuration.ofHours(1),
    ),
    maxUnzippedDarSize = 10,
    stores = ParticipantStoreConfig(
      maxItemsInSqlClause = PositiveNumeric.tryCreate(10),
      maxPruningBatchSize = PositiveNumeric.tryCreate(10),
      acsPruningInterval = NonNegativeFiniteDuration.ofSeconds(30),
      dbBatchAggregationConfig = BatchAggregatorConfig.defaultsForTesting,
    ),
    transferTimeProofFreshnessProportion = NonNegativeInt.tryCreate(3),
    protocolConfig = ParticipantProtocolConfig(
      Some(testedProtocolVersion),
      devVersionSupport = false,
      dontWarnOnDeprecatedPV = false,
      initialProtocolVersion = testedProtocolVersion,
    ),
    uniqueContractKeys = false,
    enableCausalityTracking = true,
    unsafeEnableDamlLfDevVersion = false,
    ledgerApiServerParameters = LedgerApiServerParametersConfig(),
    maxDbConnections = 10,
    excludeInfrastructureTransactions = true,
  )

  case class Fixture() {

    val participantId: ParticipantId = ParticipantId("CantonSyncServiceTest")
    private val domainRegistry = mock[DomainRegistry]
    private val aliasManager = mock[DomainAliasManager]
    private val syncDomainPersistentStateManager = mock[SyncDomainPersistentStateManager]
    private val syncDomainPersistentStateFactory = mock[SyncDomainPersistentStateFactory]
    private val domainConnectionConfigStore = mock[DomainConnectionConfigStore]
    private val packageService = mock[PackageService]
    private val topologyStoreFactory = mock[TopologyStoreFactory]
    private val causalityLookup = mock[MultiDomainCausalityStore]
    val topologyManager: ParticipantTopologyManager = mock[ParticipantTopologyManager]
    private val identityPusher = mock[ParticipantTopologyDispatcher]
    val partyNotifier = mock[LedgerServerPartyNotifier]
    private val syncCrypto = mock[SyncCryptoApiProvider]
    private val multiDomainEventLog = mock[MultiDomainEventLog]
    val participantNodePersistentState = mock[ParticipantNodePersistentState]
    private val participantSettingsStore = new InMemoryParticipantSettingsStore(loggerFactory)
    val participantEventPublisher = mock[ParticipantEventPublisher]
    private val participantEventLog =
      new InMemoryParticipantEventLog(ProductionParticipantEventLogId, loggerFactory)
    private val indexedStringStore = InMemoryIndexedStringStore()
    private val participantNodeEphemeralState = mock[ParticipantNodeEphemeralState]
    private val pruningProcessor = NoOpPruningProcessor
    private val commandDeduplicationStore = mock[CommandDeduplicationStore]
    private val inFlightSubmissionStore = mock[InFlightSubmissionStore]

    private val ledgerId = participantId.uid.id.unwrap
    private implicit val mat: Materializer = mock[Materializer] // not used
    private val syncDomainStateFactory: SyncDomainEphemeralStateFactory =
      mock[SyncDomainEphemeralStateFactoryImpl]

    participantSettingsStore
      .insertMaxDeduplicationDuration(NonNegativeFiniteDuration.Zero)
      .futureValue

    when(participantNodePersistentState.participantEventLog).thenReturn(participantEventLog)
    when(participantNodePersistentState.multiDomainEventLog).thenReturn(multiDomainEventLog)
    when(participantNodePersistentState.settingsStore).thenReturn(participantSettingsStore)
    when(participantNodePersistentState.commandDeduplicationStore).thenReturn(
      commandDeduplicationStore
    )
    when(participantNodePersistentState.inFlightSubmissionStore).thenReturn(inFlightSubmissionStore)
    when(partyNotifier.resumePending()).thenReturn(Future.unit)

    when(
      multiDomainEventLog.fetchUnpublished(
        ArgumentMatchers.eq(ProductionParticipantEventLogId),
        ArgumentMatchers.eq(None),
      )(anyTraceContext)
    )
      .thenReturn(Future.successful(List.empty))
    when(multiDomainEventLog.lookupByEventIds(any[Seq[EventId]])(anyTraceContext))
      .thenReturn(Future.successful(Map.empty))

    when(participantNodeEphemeralState.participantEventPublisher).thenReturn(
      participantEventPublisher
    )
    when(participantEventPublisher.publishTimeModelConfigNeededUpstreamOnlyIfFirst(anyTraceContext))
      .thenReturn(Future.unit)
    when(domainConnectionConfigStore.getAll()).thenReturn(Seq.empty)
    when(aliasManager.ids).thenReturn(Set.empty)

    when(
      commandDeduplicationStore.storeDefiniteAnswers(
        any[Seq[(ChangeId, DefiniteAnswerEvent, Boolean)]]
      )(anyTraceContext)
    ).thenReturn(Future.unit)
    when(inFlightSubmissionStore.delete(any[Seq[InFlightReference]])(anyTraceContext))
      .thenReturn(Future.unit)

    val sync = new CantonSyncService(
      participantId,
      domainRegistry,
      domainConnectionConfigStore,
      aliasManager,
      participantNodePersistentState,
      participantNodeEphemeralState,
      syncDomainPersistentStateManager,
      syncDomainPersistentStateFactory,
      packageService,
      topologyStoreFactory,
      causalityLookup,
      topologyManager,
      identityPusher,
      partyNotifier,
      syncCrypto,
      pruningProcessor,
      ledgerId,
      CantonOnly.newDamlEngine(uniqueContractKeys = false, enableLfDev = false),
      syncDomainStateFactory,
      new SimClock(loggerFactory = loggerFactory),
      new ResourceManagementService.CommunityResourceManagementService(
        None,
        ParticipantTestMetrics,
      ),
      LocalNodeParameters,
      SyncDomain.DefaultFactory,
      indexedStringStore,
      ParticipantTestMetrics,
      () => true,
      FutureSupervisor.Noop,
      SuppressingLogger(getClass),
    )
  }

  override type FixtureParam = Fixture

  override def withFixture(test: OneArgTest): Outcome =
    test(Fixture())

  "Canton sync service" should {
    "emit add party event" in { f =>
      when(
        f.topologyManager.authorize(
          any[TopologyTransaction[TopologyChangeOp]],
          any[Option[Fingerprint]],
          any[ProtocolVersion],
          anyBoolean,
          anyBoolean,
        )(anyTraceContext)
      )
        .thenReturn(EitherT.rightT(mock[SignedTopologyTransaction[TopologyChangeOp]]))

      when(f.participantEventPublisher.publish(any[LedgerSyncEvent])(anyTraceContext))
        .thenReturn(Future.unit)

      val lfInputPartyId = LfPartyId.assertFromString("desiredPartyName")
      val partyId =
        PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"${lfInputPartyId.toString}::default"))
      when(
        f.partyNotifier.setDisplayName(
          ArgumentMatchers.eq(partyId),
          ArgumentMatchers.eq(String255.tryCreate("displayName")),
        )(anyTraceContext)
      )
        .thenReturn(Future.successful(()))

      val submissionId = LedgerSubmissionId.assertFromString("CantonSyncServiceTest submission")

      val fut = f.sync
        .allocateParty(Some(lfInputPartyId), Some("displayName"), submissionId)(
          com.daml.logging.LoggingContext.ForTesting,
          com.daml.telemetry.NoOpTelemetryContext,
        )
        .asScala

      val result = fut.map(_ => {
        verify(f.topologyManager).authorize(
          eqTo(
            TopologyStateUpdate(
              TopologyChangeOp.Add,
              TopologyStateUpdateElement(
                TopologyElementId.tryCreate(submissionId),
                PartyToParticipant(
                  RequestSide.Both,
                  partyId,
                  f.participantId,
                  ParticipantPermission.Submission,
                ),
              ),
              testedProtocolVersion,
            )
          ),
          eqTo(None),
          eqTo(testedProtocolVersion),
          eqTo(false),
          eqTo(false),
        )(anyTraceContext)
        succeed
      })

      result.futureValue
    }

    def stats(sync: CantonSyncService, packageId: String): Option[Int] = {

      import TransactionBuilder.Implicits.*

      val builder = TransactionBuilder()
      val createNode = builder.create(
        id = builder.newCid,
        templateId = Ref.Identifier(packageId, Ref.QualifiedName("M", "D")),
        argument = ValueRecord(None, ImmArray.Empty),
        signatories = Seq("Alice"),
        observers = Seq.empty,
        key = None,
      )
      builder.add(createNode)

      lazy val event = TransactionAccepted(
        optCompletionInfo = DefaultDamlValues.completionInfo(List.empty).some,
        transactionMeta = DefaultDamlValues.transactionMeta(),
        transaction = CommittedTransaction.subst[Id](builder.build()),
        transactionId = DefaultDamlValues.lfTransactionId(1),
        recordTime = CantonTimestamp.Epoch.toLf,
        divulgedContracts = List.empty,
        blindingInfo = None,
        contractMetadata = Map(),
      )

      Option(sync.augmentTransactionStatistics(event))
        .collect({ case ta: TransactionAccepted => ta })
        .flatMap(_.optCompletionInfo)
        .flatMap(_.statistics)
        .map(_.committed.actions)

    }

    "populate metering" in { f =>
      stats(f.sync, "packageX") shouldBe Some(1)
    }

    "not include ping-pong packages in metering" in { f =>
      stats(f.sync, workflows.PackageID.PingPong) shouldBe Some(0)
    }

    "not include dar-distribution packages in metering" in { f =>
      stats(f.sync, workflows.PackageID.DarDistribution) shouldBe Some(0)
    }

  }
}
