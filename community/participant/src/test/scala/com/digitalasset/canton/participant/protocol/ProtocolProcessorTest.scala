// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Eval
import cats.data.{EitherT, Nested}
import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.concurrent.DirectExecutionContext.directExecutionContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{
  CachingConfigs,
  ProcessingTimeout,
  SessionEncryptionKeyCacheConfig,
  StorageConfig,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration
import com.digitalasset.canton.data.PeanoQueue.{BeforeHead, NotInserted}
import com.digitalasset.canton.data.{CantonTimestamp, Offset, SubmissionTrackerData}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{
  ChangeId,
  CompletionInfo,
  SequencedEventUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  DefaultPromiseUnlessShutdownFactory,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrintingCompanion}
import com.digitalasset.canton.participant.admin.party.{
  FreezingToOnboardingScheduler,
  OnboardingClearanceScheduler,
}
import com.digitalasset.canton.participant.commitment.AcsCommitmentSender
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.RequestOutcome
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{CleanReplayData, Wrapped}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.*
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
  TestProcessorError,
  TestViewType,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.*
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.UnsequencedSubmissionMap
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.sync.{SyncEphemeralState, SynchronizerConnectionsManager}
import com.digitalasset.canton.participant.topology.FailingOfflineTopologyLookup
import com.digitalasset.canton.participant.{
  DefaultParticipantStateValues,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend.TrafficRegistration
import com.digitalasset.canton.platform.indexer.parallel.PostPublishData
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.Phase37Processor.PublishUpdateViaRecordOrderPublisher
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendAsyncResult,
  SendCallback,
  SendResult,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.{TrafficControlErrors, TrafficReceipt}
import com.digitalasset.canton.sequencing.{AsyncResult, UnthrottledAsync}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{IndexedPhysicalSynchronizer, IndexedSynchronizer}
import com.digitalasset.canton.time.{SynchronizerTimeTracker, WallClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{
  BaseTest,
  DefaultDamlValues,
  HasExecutionContext,
  RepairCounter,
  RequestCounter,
  SequencerCounter,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.eq as isEq
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, Tag}

import java.time.Duration
import java.util.UUID
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise
import scala.language.implicitConversions

class ProtocolProcessorTest
    extends AnyWordSpec
    with BaseTest
    with SecurityTestSuite
    with HasExecutionContext
    with HasTestCloseContext {
  import PublishUpdateViaRecordOrderPublisher.noop as publishNoop

  // Workaround to avoid false errors reported by IDEA.
  private implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  private lazy val authenticity: SecurityTest =
    SecurityTest(SecurityTest.Property.Authenticity, "virtual shared ledger")

  private def authenticityAttack(threat: String, mitigation: String)(implicit
      lineNo: sourcecode.Line,
      fileName: sourcecode.File,
  ): SecurityTest =
    authenticity.setAttack(Attack("a malicious network participant", threat, mitigation))

  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::participant")
  )
  private val otherParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::other-participant")
  )
  private val party = PartyId(UniqueIdentifier.tryFromProtoPrimitive("party::participant"))
  private val psid = DefaultTestIdentities.physicalSynchronizerId
  private val topology: TestingTopology = TestingTopology.from(
    Set(psid),
    Map(
      party.toLf -> Map(
        participant -> ParticipantPermission.Submission
      )
    ),
    Set(
      MediatorGroup(
        NonNegativeInt.zero,
        NonEmpty(Seq, DefaultTestIdentities.daMediator),
        Seq(),
        PositiveInt.one,
      )
    ),
  )
  private val crypto =
    TestingIdentityFactory(topology, loggerFactory, TestSynchronizerParameters.defaultDynamic)
      .forOwnerAndSynchronizer(participant, psid)
  private type Send = SendCallback => EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit]
  private type Prepare =
    EitherT[FutureUnlessShutdown, SendAsyncClientError, SequencerClientSend.PreparedSubmission]
  // With None, the test fails if the processor registers.
  private type Registration = Option[UnlessShutdown[Either[RpcError, Unit]]]

  private val registers: Registration = Some(UnlessShutdown.Outcome(Right(())))

  private lazy val delivered = Deliver.create(
    None,
    CantonTimestamp.Epoch,
    psid,
    Some(MessageId.tryCreate("delivered")),
    Batch.empty[Envelope[?]](testedProtocolVersion),
    Option.empty[TrafficReceipt],
    testedProtocolVersion,
  )

  private def callsBack(result: UnlessShutdown[SendResult]): Send = { callback =>
    callback(result)
    EitherTUtil.unitUS
  }

  // With a cost, the processor registers and has a correlation to release.
  private def prepared(send: Send, cost: Option[Long] = Some(1L)): Prepare =
    EitherT.rightT[FutureUnlessShutdown, SendAsyncClientError](
      new SequencerClientSend.PreparedSubmission {
        override def trafficCost: Option[Long] = cost

        override def sendAsync(
            callback: SendCallback,
            amplify: Boolean,
            useConfirmationResponseAmplificationParameters: Boolean,
        ): SendAsyncResult =
          Nested(EitherT.rightT[FutureUnlessShutdown, SendAsyncClientError](send(callback)))
      }
    )

  private def sendReturns(result: SendResult, cost: Option[Long] = Some(1L)): Prepare =
    prepared(callsBack(UnlessShutdown.Outcome(result)), cost)

  private def preparingClient(prepare: Prepare): SequencerClientSend = {
    val client = mock[SequencerClientSend]
    when(client.psid).thenAnswer(psid)
    when(client.protocolVersion).thenAnswer(psid.protocolVersion)
    when(
      client.prepareSend(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[SynchronizerSnapshotSyncCryptoApi]],
        any[SendRequestTimestamps],
        any[MessageId],
        any[Option[AggregationRule]],
      )(anyTraceContext, any[MetricsContext])
    ).thenReturn(prepare)
    client
  }

  private val mockSequencerClient =
    preparingClient(sendReturns(SendResult.Success(delivered), cost = None))

  private val trm = mock[ConfirmationResultMessage]
  when(trm.prettyCompanion).thenAnswer(
    new PrettyPrintingCompanion[ConfirmationResultMessage] {
      override protected val pretty: Pretty[ConfirmationResultMessage] =
        Pretty.adHocPrettyInstance[ConfirmationResultMessage]
    }
  )
  when(trm.verdict).thenAnswer(Verdict.Approve.create(testedProtocolVersion))
  when(trm.rootHash).thenAnswer(rootHash)
  when(trm.psid).thenAnswer(DefaultTestIdentities.physicalSynchronizerId)

  private val requestId = RequestId(CantonTimestamp.Epoch)
  private val requestSc = SequencerCounter(0)
  private val resultSc = SequencerCounter(1)
  private val rc = RequestCounter(0)
  private val parameters = DynamicSynchronizerParametersWithValidity(
    DynamicSynchronizerParameters.initialValues(testedProtocolVersion),
    CantonTimestamp.MinValue,
    None,
  )

  private val sessionKeyMapTest = NonEmpty(
    Seq,
    new AsymmetricEncrypted[SecureRandomness](
      ByteString.EMPTY,
      // this is only a placeholder, the data is not encrypted
      crypto.pureCrypto.encryptionAlgorithmSpecs.default,
      Fingerprint.tryFromString("dummy"),
    ),
  )

  private type TestInstance =
    ProtocolProcessor[
      Int,
      Unit,
      TestViewType,
      TestProcessingSteps.TestProcessingError,
    ]

  private def waitForAsyncResult(asyncResult: AsyncResult[UnthrottledAsync]) =
    asyncResult.unwrap.flatMap(_.future).futureValueUS

  private def checkMismatchedMediatorSubmissionCleanup(isReceipt: Boolean): Assertion = {
    val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)
    val publishedPostPublishDataP = Promise[Option[PostPublishData]]()

    // Capture the completion emitted when the invalid request is terminated, then feed it through
    //  the same post-publication tracker that deletes completed in-flight submissions.
    val publishUpdate = new PublishUpdateViaRecordOrderPublisher[SequencedEventUpdate] {
      override def apply(eventO: Option[SequencedEventUpdate]): Unit =
        publishedPostPublishDataP.trySuccess(
          eventO.flatMap(
            PostPublishData.from(_, Offset.tryFromLong(1), requestId.unwrap)
          )
        )
    }

    // viewMessage's mediator defaults to group 0.
    //  The mediator argument below is the root-hash message recipient, group 1,
    //  so processRequestInternal takes the mediator-mismatch branch.
    val otherMediatorGroup = MediatorGroupRecipient(MediatorGroupIndex.one)

    // TestProcessingSteps does not construct transaction rejections, so provide the completion
    //  that the production transaction steps would create for this local submitter.
    val rejectionError =
      TransactionProcessor.SubmissionErrors.NoViewWithValidRecipients.Error(requestId.unwrap)
    val rejectionEvent = Update.SequencedCommandRejected(
      completionInfo = CompletionInfo(
        List.empty,
        changeId.userId,
        changeId.commandId,
        None,
        Some(subId),
        paidTrafficCost = NonNegativeLong.zero,
      ),
      reasonTemplate = Update.CommandRejected.FinalReason(rejectionError.rpcStatus()),
      synchronizerId = psid.logical,
      recordTime = requestId.unwrap,
      isTransaction = true,
      transactionHash = None,
      traceContext = TraceContext.empty,
    )

    testProcessingSteps(
      submissionDataForTrackerO = submissionTrackerData(),
      overrideInFlightSubmissionStoreO = Some(inFlightSubmissionStore),
      rejectedCommandEventO = Some(rejectionEvent),
    ) { (sut, _, ephemeral) =>
      val synchronizerTracker = ephemeral.inFlightSubmissionSynchronizerTracker
      val publicationTracker = new InFlightSubmissionTracker(
        Eval.now(inFlightSubmissionStore),
        new NoCommandDeduplicator(),
        loggerFactory,
      )
      val deduplicationPeriod = DeduplicationDuration.create(Duration.ofSeconds(10)).value

      // Seed the tracked submission before delivering its receipt or preplay.
      synchronizerTracker
        .register(unsequencedSubmission, deduplicationPeriod)
        .valueOrFailShutdown("register submission")
        .futureValue
        .isRight shouldBe true

      // Receipts are observed by message ID here; processRequest observes preplays by root hash.
      if (isReceipt) {
        synchronizerTracker
          .observeSequencing(
            Map(unsequencedSubmission.messageId -> SequencedSubmission(requestId.unwrap))
          )
          .futureValueUS
      }

      loggerFactory.assertLogs(
        waitForAsyncResult(
          processRequest(
            sut,
            requestBatch(mediator = otherMediatorGroup, isReceipt = isReceipt),
            publishUpdate = publishUpdate,
          )
        ),
        _.errorMessage should include(
          s"Mediator ${MediatorGroupRecipient(MediatorGroupIndex.zero)} declared in views is not the recipient $otherMediatorGroup of the root hash message"
        ),
      )

      // Both paths must leave the row sequenced until its rejection completion is published.
      inFlightSubmissionStore
        .lookup(changeIdHash)
        .value
        .futureValueUS
        .value
        .sequencingInfo shouldBe SequencedSubmission(requestId.unwrap)
      inFlightSubmissionStore
        .lookupEarliest(psid.logical)
        .futureValueUS shouldBe Some(requestId.unwrap)

      // The bad request is terminated at its immediate successor, publishing the rejection above.
      ephemeral.requestTracker.tick(requestSc + 1, requestId.unwrap.immediateSuccessor)
      val postPublishData = publishedPostPublishDataP.future.futureValue.getOrElse(
        fail("Expected the local submitter rejection to publish a completion")
      )
      publicationTracker.processPublications(Seq(postPublishData)).futureValueUS

      // Completion publication releases the pruning watermark
      //  and permits resubmission with the same change ID.
      inFlightSubmissionStore.lookup(changeIdHash).value.futureValueUS shouldBe None
      inFlightSubmissionStore.lookupEarliest(psid.logical).futureValueUS shouldBe None

      synchronizerTracker
        .register(
          unsequencedSubmission.copy(messageUuid = UUID.randomUUID()),
          deduplicationPeriod,
        )
        .valueOrFailShutdown("resubmit change ID")
        .futureValue
        .isRight shouldBe true
    }
    succeed
  }

  private def testProcessingSteps(
      overrideConstructedPendingRequestDataO: Option[TestPendingRequestData] = None,
      startingPoints: ProcessingStartingPoints = ProcessingStartingPoints.default,
      pendingSubmissionMap: concurrent.Map[Int, Unit] = TrieMap[Int, Unit](),
      sequencerClient: SequencerClientSend = mockSequencerClient,
      crypto: SynchronizerCryptoClient = crypto,
      overrideInFlightSubmissionSynchronizerTrackerO: Option[
        InFlightSubmissionSynchronizerTracker
      ] = None,
      submissionDataForTrackerO: Option[SubmissionTrackerData] = None,
      overrideInFlightSubmissionStoreO: Option[InFlightSubmissionStore] = None,
      rejectedCommandEventO: Option[SequencedEventUpdate] = None,
      trafficEnforcementBackend: TrafficEnforcementBackend = releasingTrafficEnforcementBackend(),
      trafficRegistration: Registration = registers,
  )(test: (TestInstance, SyncPersistentState, SyncEphemeralState) => Assertion): Unit = {
    val clock = new WallClock(timeouts, loggerFactory)
    val testParameters = ParticipantNodeParameters.forTestingOnly(testedProtocolVersion)

    val nodePersistentState = timeouts.default.await("creating node persistent state")(
      ParticipantNodePersistentState
        .create(
          new MemoryStorage(loggerFactory, timeouts),
          StorageConfig.Memory(),
          None,
          testParameters,
          testedReleaseProtocolVersion,
          ParticipantTestMetrics,
          participant.toLf,
          LedgerApiServerConfig(),
          futureSupervisor,
          loggerFactory,
          None,
        )
        .failOnShutdown
    )

    val contractStore = new InMemoryContractStore(timeouts, loggerFactory)

    val logical = new InMemoryLogicalSyncPersistentState(
      IndexedSynchronizer.tryCreate(psid.logical, 1),
      testParameters,
      new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1), // only one synchronizer needed
      contractStore,
      nodePersistentState.acsCounterParticipantConfigStore,
      Eval.now(nodePersistentState.ledgerApiStore),
      participant,
      new FailingOfflineTopologyLookup(),
      loggerFactory,
      FutureSupervisor.Noop,
    )
    val physical =
      new InMemoryPhysicalSyncPersistentState(
        crypto.crypto,
        IndexedPhysicalSynchronizer.tryCreate(psid, 1),
        defaultStaticSynchronizerParameters,
        predecessor = None,
        loggerFactory,
        timeouts,
        futureSupervisor,
      )

    val persistentState = new SyncPersistentState(logical, physical, loggerFactory)

    val ledgerApiIndexer = mock[LedgerApiIndexer]

    when(ledgerApiIndexer.enqueue).thenAnswer((_: Update) => FutureUnlessShutdown.unit)
    when(ledgerApiIndexer.onlyForTestingTransactionInMemoryStore).thenAnswer(None)

    val timeTracker = mock[SynchronizerTimeTracker]
    when(timeTracker.requestTick(any[CantonTimestamp], any[Boolean])(any[TraceContext]))
      .thenReturn(SynchronizerTimeTracker.DummyTickRequest)
    val recordOrderPublisher = RecordOrderPublisher(
      psid = psid,
      synchronizerSuccessor = None,
      initSc = SequencerCounter.Genesis,
      initTimestamp = CantonTimestamp.MinValue,
      ledgerApiIndexer = ledgerApiIndexer,
      metrics = ParticipantTestMetrics.synchronizer.recordOrderPublisher,
      exitOnFatalFailures = true,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
      futureSupervisor = futureSupervisor,
      clock = clock,
      performLsu = SynchronizerConnectionsManager.NoAutomaticLsuHandler(logger),
    )
    val unsequencedSubmissionMap = new UnsequencedSubmissionMap[SubmissionTrackingData](
      psid,
      1000,
      ParticipantTestMetrics.synchronizer.inFlightSubmissionSynchronizerTracker.unsequencedInFlight,
      loggerFactory,
    )
    val inFlightSubmissionSynchronizerTracker =
      overrideInFlightSubmissionSynchronizerTrackerO.getOrElse {
        new InFlightSubmissionSynchronizerTracker(
          psid,
          Eval.now(
            overrideInFlightSubmissionStoreO.getOrElse(nodePersistentState.inFlightSubmissionStore)
          ),
          new NoCommandDeduplicator(),
          recordOrderPublisher,
          timeTracker,
          unsequencedSubmissionMap,
          loggerFactory,
        )
      }
    val ephemeralState =
      new SyncEphemeralState(
        participant,
        recordOrderPublisher,
        timeTracker,
        inFlightSubmissionSynchronizerTracker,
        mock[AcsCommitmentSender],
        mock[OnboardingClearanceScheduler],
        mock[FreezingToOnboardingScheduler],
        persistentState,
        ledgerApiIndexer,
        contractStore,
        new DefaultPromiseUnlessShutdownFactory(timeouts, loggerFactory),
        startingPoints,
        ParticipantTestMetrics.synchronizer,
        exitOnFatalFailures = true,
        // Disable the session encryption key cache: it starts a scheduler that must be closed properly,
        // otherwise we see RejectedExecutionException warnings during shutdown.
        SessionEncryptionKeyCacheConfig(enabled = false),
        CachingConfigs.defaultTransparencyCheckCache,
        timeouts,
        loggerFactory,
        FutureSupervisor.Noop,
        clock,
      )

    val steps = new TestProcessingSteps(
      pendingSubmissionMap = pendingSubmissionMap,
      overrideConstructedPendingRequestDataO,
      informeesOfView = _ => Set(party.toLf),
      submissionDataForTrackerO = submissionDataForTrackerO,
      rejectedCommandEventO = rejectedCommandEventO,
    )

    val sut: TestInstance =
      new ProtocolProcessor(
        steps,
        inFlightSubmissionSynchronizerTracker,
        ephemeralState,
        crypto,
        sequencerClient,
        clock,
        loggerFactory,
        FutureSupervisor.Noop,
        FlagCloseable.withCloseContext(logger, timeouts),
      )(directExecutionContext) {
        override def testingConfig: TestingConfigInternal =
          TestingConfigInternal()

        override def participantId: ParticipantId = participant

        override def timeouts: ProcessingTimeout = ProtocolProcessorTest.this.timeouts

        override protected def metricsContextForSubmissionParam(
            submissionParam: Int
        ): MetricsContext = MetricsContext.Empty

        override protected def registerTrafficCost(
            submissionParam: Int,
            trafficCost: Long,
        )(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, RpcError, Option[TrafficRegistration]] =
          trafficRegistration match {
            case None => fail("The processor must not register this submission")
            case Some(outcome) =>
              EitherT(
                FutureUnlessShutdown.lift(
                  outcome.map(
                    _.map(_ => Option(TrafficRegistration(changeId, subId, trafficRegisteredAt)))
                  )
                )
              )
          }

        override protected def releaseTrafficRegistration(registration: TrafficRegistration)(
            implicit traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] =
          trafficEnforcementBackend.releaseSubmission(
            registration.changeId,
            registration.submissionId,
            registration.registeredAt,
          )
      }

    try test(sut, persistentState, ephemeralState).discard
    finally LifeCycle.close(sut)(logger)
  }

  private def releasingTrafficEnforcementBackend(): TrafficEnforcementBackend = {
    val backend = mock[TrafficEnforcementBackend]
    when(
      backend.releaseSubmission(any[ChangeId], any[Ref.SubmissionId], any[CantonTimestamp])(
        anyTraceContext
      )
    ).thenReturn(FutureUnlessShutdown.unit)
    backend
  }

  private lazy val rootHash = RootHash(TestHash.digest(1))
  private lazy val trafficRegisteredAt = CantonTimestamp.Epoch.plusSeconds(1)
  private lazy val testTopologyTimestamp = CantonTimestamp.Epoch
  private lazy val viewHash = ViewHash(TestHash.digest(2))

  private def singleView(encryptedViewBytes: ByteString): EncryptedViewMessage[TestViewType] =
    EncryptedViewMessageUtils.exampleEncryptedSingleView(TestViewType)(
      submittingParticipantSignature = None,
      viewHash = viewHash,
      viewEncryptionKeyRandomness = sessionKeyMapTest,
      encryptedViewBytes = encryptedViewBytes,
      synchronizerId = DefaultTestIdentities.physicalSynchronizerId,
      viewEncryptionScheme = SymmetricKeyScheme.Aes128Gcm,
      protocolVersion = testedProtocolVersion,
    )

  private lazy val viewMessage = singleView(rootHash.toProtoPrimitive)

  private lazy val rootHashMessage = RootHashMessage(
    rootHash,
    DefaultTestIdentities.physicalSynchronizerId,
    TestViewType,
    testTopologyTimestamp,
    SerializedRootHashMessagePayload.empty,
    testedProtocolVersion,
  )
  private lazy val someRecipients = Recipients.cc(participant)

  private type TestRequestBatch =
    RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[TestViewType]]]

  private def requestBatch(
      views: Seq[EncryptedViewMessage[TestViewType]] = Seq(viewMessage),
      rootHashMsg: RootHashMessage[SerializedRootHashMessagePayload] = rootHashMessage,
      mediator: MediatorGroupRecipient = MediatorGroupRecipient(MediatorGroupIndex.zero),
      isReceipt: Boolean = false,
  ): TestRequestBatch =
    RequestAndRootHashMessage(
      NonEmpty
        .from(views.map(view => OpenEnvelope(view, someRecipients)(testedProtocolVersion)))
        .value,
      rootHashMsg,
      mediator,
      isReceipt,
    )

  private lazy val someRequestBatch = requestBatch()

  private def processRequest(
      sut: TestInstance,
      batch: TestRequestBatch = someRequestBatch,
      ts: CantonTimestamp = requestId.unwrap,
      publishUpdate: PublishUpdateViaRecordOrderPublisher[SequencedEventUpdate] = publishNoop,
  ): AsyncResult[UnthrottledAsync] =
    sut
      .processRequest(ts, rc, requestSc, batch, publishUpdate, NonNegativeLong.zero)
      .onShutdown(fail())
      .futureValue

  private lazy val pendingRequestData = TestPendingRequestData(
    rc,
    requestSc,
    MediatorGroupRecipient(MediatorGroupIndex.one),
    locallyRejectedF = FutureUnlessShutdown.pure(false),
    abortEngine = _ => (),
    engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
    publishNoop,
  )

  private def submissionTrackerData(
      submitter: ParticipantId = participant
  ): Option[SubmissionTrackerData] =
    Some(
      SubmissionTrackerData(
        submittingParticipant = submitter,
        maxSequencingTime = requestId.unwrap.plusSeconds(10),
      )
    )

  private lazy val topologySnapshot = topology.build().topologySnapshot()

  private lazy val subId = DefaultDamlValues.submissionId()
  private lazy val changeId = DefaultParticipantStateValues.changeId(Set.empty)
  private lazy val changeIdHash = ChangeIdHash(changeId)

  private lazy val unsequencedSubmission = InFlightSubmission(
    changeIdHash = changeIdHash,
    submissionId = Some(subId),
    submissionSynchronizerId = psid,
    messageUuid = UUID.randomUUID(),
    rootHashO = Some(rootHash),
    sequencingInfo = UnsequencedSubmission(
      requestId.unwrap,
      TransactionSubmissionTrackingData.create(
        CompletionInfo(
          List.empty,
          changeId.userId,
          changeId.commandId,
          None,
          Some(subId),
          paidTrafficCost = NonNegativeLong.zero,
        ),
        SubmissionTrackingData.TimeoutCause,
        psid,
        transactionHash = None,
      ),
    ),
    TraceContext.empty,
  )

  "submit" should {
    import com.digitalasset.canton.sequencing.protocol.SequencerDeliverError as DeliverErrorReason

    type SubmitResult = UnlessShutdown[Either[TestProcessingSteps.TestProcessingError, Unit]]

    final case class SubmitCase(
        name: String,
        prepare: Prepare,
        expected: SubmitResult,
        registration: Registration = registers,
        releases: Boolean = false,
    )

    def sendFails(error: SendAsyncClientError): Prepare = prepared(_ => EitherT.leftT(error))
    val sendNotCalled = prepared(_ => fail("The processor must not send this submission"))
    def submitFails(error: ProcessorError): SubmitResult =
      UnlessShutdown.Outcome(Left(TestProcessorError(error)))
    def deliverError(reason: DeliverErrorReason): DeliverError =
      DeliverError.create(
        None,
        CantonTimestamp.Epoch,
        psid,
        MessageId.tryCreate("error"),
        reason,
        None,
        testedProtocolVersion,
      )

    val succeeded: SubmitResult = UnlessShutdown.Outcome(Right(()))
    val aborted: SubmitResult = UnlessShutdown.AbortedDueToShutdown
    val requestFailed = SendAsyncClientError.RequestFailed("no thank you")
    val alreadyExists = SendAsyncClientError.RequestAlreadyExists("dup")
    val requestInvalid = SendAsyncClientError.RequestInvalid("invalid")
    val refused = deliverError(SequencerErrors.SubmissionRequestRefused("refused"))
    val aggregateSent = deliverError(SequencerErrors.AggregateSubmissionAlreadySent("already sent"))
    val teaRejection: RpcError = TrafficControlErrors.TrafficStateNotFound.Error()(
      ErrorLoggingContext.fromTracedLogger(logger)
    )

    val submitCases = Seq(
      SubmitCase("the send succeeds", sendReturns(SendResult.Success(delivered)), succeeded),
      SubmitCase(
        "there is no traffic cost",
        sendReturns(SendResult.Success(delivered), cost = None),
        succeeded,
        registration = None,
      ),
      SubmitCase(
        "the request fails",
        sendFails(requestFailed),
        submitFails(SequencerRequestError(requestFailed)),
        releases = true,
      ),
      SubmitCase(
        "the request already exists",
        sendFails(alreadyExists),
        submitFails(SequencerRequestError(alreadyExists)),
      ),
      SubmitCase(
        "the send times out",
        sendReturns(SendResult.Timeout(CantonTimestamp.Epoch)),
        submitFails(SequencerTimeoutError),
        releases = true,
      ),
      SubmitCase(
        "the sequencer refuses the request",
        sendReturns(SendResult.Error(refused)),
        submitFails(ProtocolProcessor.SequencerDeliverError(refused)),
        releases = true,
      ),
      SubmitCase(
        "the aggregate submission was already sent",
        sendReturns(SendResult.Error(aggregateSent)),
        submitFails(ProtocolProcessor.SequencerDeliverError(aggregateSent)),
      ),
      SubmitCase(
        "sending aborts",
        prepared(_ =>
          EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit](
            FutureUnlessShutdown.abortedDueToShutdown
          )
        ),
        aborted,
      ),
      SubmitCase(
        "the result aborts",
        prepared(callsBack(UnlessShutdown.AbortedDueToShutdown)),
        aborted,
      ),
      SubmitCase(
        "preparing fails",
        EitherT.leftT[FutureUnlessShutdown, SequencerClientSend.PreparedSubmission](
          requestInvalid
        ),
        submitFails(SequencerRequestError(requestInvalid)),
        registration = None,
      ),
      SubmitCase(
        "TEA rejects",
        sendNotCalled,
        submitFails(TrafficEnforcementRejected(teaRejection)),
        registration = Some(UnlessShutdown.Outcome(Left(teaRejection))),
      ),
      SubmitCase(
        "registration aborts",
        sendNotCalled,
        aborted,
        registration = Some(UnlessShutdown.AbortedDueToShutdown),
      ),
    )

    "keep, clean up, or release the pending submission" when {
      submitCases.foreach { submitCase =>
        submitCase.name in {
          val submissionMap = TrieMap[Int, Unit]()
          val backend = releasingTrafficEnforcementBackend()

          testProcessingSteps(
            sequencerClient = preparingClient(submitCase.prepare),
            pendingSubmissionMap = submissionMap,
            trafficEnforcementBackend = backend,
            trafficRegistration = submitCase.registration,
          ) { (sut, _, _) =>
            val result = loggerFactory.suppressWarnings(
              sut.submit(0, topologySnapshot).value.unwrap.futureValue
            )

            result.map(_.map(_.futureValueUS)) shouldBe submitCase.expected
            submissionMap.contains(0) shouldBe (submitCase.expected == succeeded)
            if (submitCase.releases)
              verify(backend)
                .releaseSubmission(isEq(changeId), isEq(subId), isEq(trafficRegisteredAt))(
                  anyTraceContext
                )
                .discard
            else verifyZeroInteractions(backend)
            succeed
          }
        }
      }
    }

    val exception = new RuntimeException("boom")
    def throws[A]: EitherT[FutureUnlessShutdown, SendAsyncClientError, A] =
      EitherT.liftF(FutureUnlessShutdown.failed[A](exception))
    val decisionTime = parameters.decisionTimeFor(CantonTimestamp.Epoch).value

    "clean up the pending submission without releasing it" when {
      Seq[(String, Prepare, Registration, Boolean)](
        ("preparing", throws, None, false),
        ("sending", prepared(_ => throws), registers, false),
        ("scheduling the removal", sendReturns(SendResult.Success(delivered)), registers, true),
      ).foreach { case (step, prepare, registration, decisionTickThrows) =>
        s"$step throws" in {
          val submissionMap = TrieMap[Int, Unit]()
          val backend = releasingTrafficEnforcementBackend()

          testProcessingSteps(
            sequencerClient = preparingClient(prepare),
            pendingSubmissionMap = submissionMap,
            trafficEnforcementBackend = backend,
            trafficRegistration = registration,
          ) { (sut, _, ephemeral) =>
            if (decisionTickThrows)
              when(
                ephemeral.timeTracker
                  .requestTick(isEq(decisionTime), any[Boolean])(any[TraceContext])
              ).thenThrow(exception).discard
            sut.submit(0, topologySnapshot).value.unwrap.failed.futureValue shouldBe exception
            submissionMap.contains(0) shouldBe false
            // We can't tell whether the request was sequenced, so we keep the correlation.
            verifyZeroInteractions(backend)
            succeed
          }
        }
      }
    }

    "clean up the pending submissions when no request is received" in {
      val submissionMap = TrieMap[Int, Unit]()

      testProcessingSteps(pendingSubmissionMap = submissionMap) { (sut, _, _) =>
        sut
          .submit(1, topologySnapshot)
          .valueOrFailShutdown("submission")
          .futureValue
          .failOnShutdown("shutting down while test is running")
          .futureValue shouldBe ()
        submissionMap.get(1) shouldBe Some(())
        val afterDecisionTime =
          parameters.decisionTimeFor(CantonTimestamp.Epoch).value.plusMillis(1)
        waitForAsyncResult(processRequest(sut, ts = afterDecisionTime))
        eventually() {
          submissionMap.get(1) shouldBe None
        }
      }
    }

    "fail if there is no active mediator" in {
      val crypto2 = TestingIdentityFactory(
        TestingTopology(mediatorGroups = Set.empty),
        loggerFactory,
        parameters.parameters,
      ).forOwnerAndSynchronizer(participant, psid)
      testProcessingSteps(crypto = crypto2) { (sut, _, _) =>
        val topo2 = crypto2.currentSnapshotApproximation.futureValueUS.ipsSnapshot
        val res = sut.submit(1, topo2).onShutdown(fail("submission shutdown")).value.futureValue
        res shouldBe Left(TestProcessorError(NoMediatorError(CantonTimestamp.Epoch)))
      }
    }
  }

  "process request" should {

    "transit to confirmed" in {
      testProcessingSteps(overrideConstructedPendingRequestDataO = Some(pendingRequestData)) {
        (sut, _, ephemeral) =>
          ephemeral.requestJournal.query(rc).value.futureValueUS shouldEqual None

          waitForAsyncResult(processRequest(sut))
          val requestState = ephemeral.requestJournal.query(rc).value.futureValueUS
          requestState.value.state shouldEqual RequestState.Pending
          ephemeral.phase37Synchronizer
            .awaitConfirmed(TestPendingRequestDataType)(requestId)
            .futureValueUS shouldBe RequestOutcome.Success(Wrapped(pendingRequestData))
      }
    }

    "leave the request state unchanged when doing a clean replay" in {
      testProcessingSteps(
        overrideConstructedPendingRequestDataO = Some(pendingRequestData),
        startingPoints = ProcessingStartingPoints.tryCreate(
          cleanReplay = MessageCleanReplayStartingPoint(
            rc,
            requestSc,
            CantonTimestamp.Epoch.minusSeconds(20),
          ),
          processing = MessageProcessingStartingPoint(
            rc + 1,
            requestSc + 1,
            CantonTimestamp.Epoch.minusSeconds(10),
            CantonTimestamp.Epoch.minusSeconds(10),
            RepairCounter.Genesis,
          ),
        ),
      ) { (sut, _, ephemeral) =>
        ephemeral.requestJournal.query(rc).value.futureValueUS shouldEqual None

        waitForAsyncResult(processRequest(sut))
        ephemeral.requestJournal.query(rc).value.futureValueUS shouldEqual None
      }
    }

    "trigger a timeout when the result doesn't arrive" in {
      testProcessingSteps(overrideConstructedPendingRequestDataO = Some(pendingRequestData)) {
        (sut, _, ephemeral) =>
          val journal = ephemeral.requestJournal
          journal.query(rc).value.futureValueUS shouldEqual None

          // Process a request but never a corresponding response
          waitForAsyncResult(processRequest(sut))

          ephemeral.requestTracker.taskScheduler.readSequencerCounterQueue(
            requestSc
          ) shouldBe BeforeHead

          // The request remains at Pending until the timeout is triggered
          always() {
            journal.query(rc).value.futureValueUS.value.state shouldEqual RequestState.Pending
          }

          // Trigger the timeout for the request
          ephemeral.requestTracker.tick(
            requestSc + 1,
            parameters.decisionTimeFor(requestId.unwrap).value,
          )

          eventually() {
            val state = journal.query(rc).value.futureValueUS
            state.value.state shouldEqual RequestState.Clean
          }
      }
    }

    Seq(
      (
        "log wrong root hashes",
        Seq(viewMessage, singleView(RootHash(TestHash.digest(3)).toProtoPrimitive)),
        s"Request $rc: Found malformed payload: WrongRootHash",
      ),
      (
        "log decryption errors",
        Seq(singleView(ByteString.EMPTY)),
        s"Request $rc: Decryption error: SyncCryptoDecryptError(",
      ),
    ).foreach { case (name, views, warning) =>
      name in {
        testProcessingSteps() { (sut, _, _) =>
          waitForAsyncResult(
            loggerFactory.assertLogs(
              processRequest(sut, requestBatch(views)),
              _.warningMessage should include(warning),
            )
          )
          succeed
        }
      }
    }

    "check the declared mediator ID against the root hash message mediator" taggedAs {
      authenticityAttack(
        threat =
          "the mediator in the common metadata is not the recipient of the root hash message",
        mitigation = "all honest participants roll back the request",
      )
    } in {
      // Instead of rolling back the request in Phase 7, it is discarded in Phase 3. This has the same effect.

      val otherMediatorGroup = MediatorGroupRecipient(MediatorGroupIndex.one)

      testProcessingSteps() { (sut, _, _) =>
        loggerFactory.assertLogs(
          waitForAsyncResult(processRequest(sut, requestBatch(mediator = otherMediatorGroup))),
          _.errorMessage should include(
            s"Mediator ${MediatorGroupRecipient(MediatorGroupIndex.zero)} declared in views is not the recipient $otherMediatorGroup of the root hash message"
          ),
        )
        succeed
      }
    }

    "clear the in-flight submission after a mismatched-mediator receipt" in {
      checkMismatchedMediatorSubmissionCleanup(isReceipt = true)
    }

    "clear the in-flight submission after a mismatched-mediator preplay" in {
      checkMismatchedMediatorSubmissionCleanup(isReceipt = false)
    }

    "check that the mediator is active" taggedAs {
      authenticityAttack(
        threat = "the mediator in the common metadata is not active",
        mitigation = "all honest participants roll back the request",
      )
    } in {
      // Instead of rolling back the request in Phase 7, it is discarded in Phase 3. This has the same effect.

      val testCrypto = TestingIdentityFactory(
        topology.copy(mediatorGroups = Set.empty), // Topology without any mediator active
        loggerFactory,
        parameters.parameters,
      ).forOwnerAndSynchronizer(participant, psid)

      testProcessingSteps(crypto = testCrypto) { (sut, _, _) =>
        loggerFactory.assertLogs(
          waitForAsyncResult(processRequest(sut)),
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"Request $rc: Chosen mediator ${MediatorGroupRecipient(MediatorGroupIndex.zero)} is inactive at ${requestId.unwrap}. Skipping this request.",
          ),
        )
        succeed
      }
    }

    final case class TrackerCase(
        name: String,
        submitter: ParticipantId,
        isReceipt: Boolean,
        notifies: Boolean,
    )

    Seq(
      TrackerCase(
        "notify the in-flight submission tracker with the root hash when necessary",
        submitter = participant,
        isReceipt = false,
        notifies = true,
      ),
      TrackerCase(
        "not notify the in-flight submission tracker when the message is a receipt",
        submitter = participant,
        isReceipt = true,
        notifies = false,
      ),
      TrackerCase(
        "not notify the in-flight submission tracker when not submitting participant",
        submitter = otherParticipant,
        isReceipt = false,
        notifies = false,
      ),
    ).foreach { case TrackerCase(name, submitter, isReceipt, notifies) =>
      name in {
        val tracker = mock[InFlightSubmissionSynchronizerTracker]
        when(
          tracker.observeSequencedRootHash(any[RootHash], any[SequencedSubmission])(anyTraceContext)
        ).thenAnswer(FutureUnlessShutdown.unit)

        testProcessingSteps(
          overrideInFlightSubmissionSynchronizerTrackerO = Some(tracker),
          submissionDataForTrackerO = submissionTrackerData(submitter),
        ) { (sut, _, _) =>
          waitForAsyncResult(processRequest(sut, requestBatch(isReceipt = isReceipt)))

          if (notifies)
            verify(tracker).observeSequencedRootHash(
              isEq(rootHash),
              isEq(SequencedSubmission(requestId.unwrap)),
            )(anyTraceContext)
          else verifyZeroInteractions(tracker)
          succeed
        }
      }
    }

    "override the sequencing time when a preplay loses the race with the normal notification" in {
      // This test reproduces the scenario of a submission request preplayed without a messageId, i.e.
      // the participant receives first the preplay without messageId, and later the actual submission receipt.
      //
      // Background:
      // -----------
      // The submission tracker ensures only the first sequenced request produces a command completion, and
      // accordingly the in-flight submission tracker must track the preplay as sequenced.
      // The in-flight submission tracker is normally notified of the sequencing when it receives a request
      // with the messageId corresponding to the submission.
      // However, to handle replays, it is also notified when receiving a matching request (based on the
      // root hash), without messageId, but for which the participant is the submitter.
      // The normal notification happens in the synchronous request processing, while the other takes place
      // during the asynchronous part. Consequently, a race happens as to which notification happens first.
      // To handle this, the non-normal notification is allowed to override the in-flight submission tracker
      // information if the sequencing time of its request is earlier.
      //
      // This test checks this situation when the non-normal notification loses this race.

      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)
      testProcessingSteps(
        submissionDataForTrackerO = submissionTrackerData(),
        overrideInFlightSubmissionStoreO = Some(inFlightSubmissionStore),
      ) { (sut, _, ephemeral) =>
        val ifst = ephemeral.inFlightSubmissionSynchronizerTracker
        val subF = for {
          // The participant registers the submission in the in-flight submission tracker
          _ <- ifst
            .register(
              unsequencedSubmission,
              DeduplicationDuration.create(Duration.ofSeconds(10)).value,
            )
            .valueOrFailShutdown("register submission")

          // Even though sequenced later, the normal notification wins the race
          _ <- ifst
            .observeSequencing(
              Map(
                unsequencedSubmission.messageId -> SequencedSubmission(
                  requestId.unwrap.plusSeconds(1)
                )
              )
            )
            .failOnShutdown

          // The preplay gets processed and notifies the in-flight submission tracker
          _ <- processRequest(sut).unwrap.unwrap

          // Retrieve the submission from the in-flight submission tracker store to check its info
          sub <- inFlightSubmissionStore.lookup(changeIdHash).getOrElse(fail()).failOnShutdown
        } yield sub

        val sub = subF.futureValue
        sub.sequencingInfo.isSequenced shouldBe true
        sub.sequencingInfo match {
          // The information corresponds to the preplay
          case SequencedSubmission(ts) => ts shouldBe requestId.unwrap
          case _ => fail(s"Bad information in in-flight submission tracker:\n$sub")
        }
      }
    }

    "log a warning for future topology timestamps and replace them with request timestamps" in {
      testProcessingSteps() { (sut, _, ephemeral) =>
        val submissionTopologyTimestamp = CantonTimestamp.MaxValue

        val maliciousRootHashMessage = RootHashMessage(
          rootHash,
          DefaultTestIdentities.physicalSynchronizerId,
          TestViewType,
          submissionTopologyTimestamp, // Crafted submission timestamp (far) in the future
          SerializedRootHashMessagePayload.empty,
          testedProtocolVersion,
        )

        val asyncResult = loggerFactory
          .assertLogs(
            processRequest(sut, requestBatch(rootHashMsg = maliciousRootHashMessage)),
            _.shouldBeCantonError(
              SubmissionTopologyHelper.SubmissionTopologyErrors.TopologyAlarm,
              _ shouldBe s"Received future-dated submission timestamp $submissionTopologyTimestamp. Falling back to request timestamp ${CantonTimestamp.Epoch}.",
            ),
          )
        // This awaits the full asynchronous completion of the request processing.
        // If the future timestamp was not being replaced, this call would hang indefinitely.
        waitForAsyncResult(asyncResult)

        // BeforeHead confirms the TaskScheduler has successfully processed the event and advanced the queue pointer
        ephemeral.requestTracker.taskScheduler.readSequencerCounterQueue(
          requestSc
        ) shouldBe BeforeHead

        // Verify the request transitioned to the Pending state.
        ephemeral.requestJournal
          .query(rc)
          .value
          .futureValueUS
          .value
          .state shouldBe RequestState.Pending
      }
    }
  }

  "perform result processing" should {

    val requestTimestamp = requestId.unwrap
    val activenessTimestamp = CantonTimestamp.Epoch
    val activenessSet = mkActivenessSet()

    def addRequestState(
        ephemeral: SyncEphemeralState,
        decisionTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(60),
    ): Unit =
      ephemeral.requestTracker
        .addRequest(
          rc,
          requestSc,
          requestTimestamp,
          activenessTimestamp,
          decisionTime,
          activenessSet,
        )
        .value
        .failOnShutdown
        .futureValue

    def setUpOrFail(
        persistent: SyncPersistentState,
        ephemeral: SyncEphemeralState,
    ): Unit = {

      val setupF = for {
        _ <- persistent.connectivityStatusStore.setParameters(defaultStaticSynchronizerParameters)

        _ <- ephemeral.requestJournal.insert(rc, CantonTimestamp.Epoch)
      } yield ephemeral.phase37Synchronizer
        .registerRequest(TestPendingRequestDataType)(requestId)
        .complete(Some(Wrapped(pendingRequestData)))
      setupF.futureValueUS
    }

    def performResultProcessing(
        timestamp: CantonTimestamp,
        sut: TestInstance,
    ): EitherT[FutureUnlessShutdown, sut.steps.ResultError, Unit] = {
      val mockSignedProtocolMessage = mock[SignedProtocolMessage[ConfirmationResultMessage]]
      when(mockSignedProtocolMessage.message).thenReturn(trm)
      when(
        mockSignedProtocolMessage
          .verifyMediatorSignatures(any[SyncCryptoApi], any[MediatorGroupIndex])(anyTraceContext)
      )
        .thenReturn(EitherT.rightT(()))
      sut
        .processResultInternal1(
          NoOpeningErrors(
            AggregatedSignedContent(
              mock[Deliver[Batch[DefaultOpenEnvelope]]],
              Signature.noSignature,
              testedProtocolVersion,
            )
          ),
          mockSignedProtocolMessage,
          requestId,
          timestamp,
          resultSc,
        )
        .flatMap(identity)
    }

    "succeed without errors and transit to clean" in {
      testProcessingSteps() { (sut, persistent, ephemeral) =>
        addRequestState(ephemeral)

        val taskScheduler = ephemeral.requestTracker.taskScheduler
        // Check the initial state is clean
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

        setUpOrFail(persistent, ephemeral)
        valueOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut))(
          "result processing failed"
        ).futureValueUS

        val finalState = ephemeral.requestJournal.query(rc).value.futureValueUS
        finalState.value.state shouldEqual RequestState.Clean

        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
      }
    }

    "wait for request processing to finish" in {
      testProcessingSteps() { (sut, _, ephemeral) =>
        val taskScheduler = ephemeral.requestTracker.taskScheduler
        val requestJournal = ephemeral.requestJournal

        // Register request is called before request processing is triggered
        val handle =
          ephemeral.phase37Synchronizer
            .registerRequest(sut.steps.requestType)(
              requestId
            )
        ephemeral.submissionTracker
          .register(rootHash, requestId)
          .discard

        // Process the result message before the request
        val processF = performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut)

        // Processing should not complete as the request processing has not finished
        always()(processF.value.isCompleted shouldEqual false)

        // Check the result processing has not modified the request state
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)
        requestJournal.query(rc).value.futureValueUS shouldBe None

        // Now process the request message. This should trigger the completion of the result processing.
        sut
          .processRequestInternal(
            requestId.unwrap,
            rc,
            requestSc,
            someRequestBatch,
            handle,
            freshOwnTimelyTxF = FutureUnlessShutdown.pure(true),
            publishNoop,
            trafficCost = NonNegativeLong.zero,
          )
          .onShutdown(fail())
          .futureValue

        eventually() {
          processF.value.futureValueUS shouldEqual Either.unit
          taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
          requestJournal.query(rc).value.futureValueUS.value.state shouldBe RequestState.Clean
        }
      }
    }

    "succeed without errors on clean replay, not changing the request state" in {

      testProcessingSteps(
        startingPoints = ProcessingStartingPoints.tryCreate(
          MessageCleanReplayStartingPoint(rc, requestSc, CantonTimestamp.Epoch.minusSeconds(1)),
          MessageProcessingStartingPoint(
            rc + 5,
            requestSc + 10,
            CantonTimestamp.Epoch.plusSeconds(30),
            CantonTimestamp.Epoch.plusSeconds(30),
            RepairCounter.Genesis,
          ),
        )
      ) { (sut, _, ephemeral) =>
        addRequestState(ephemeral)
        ephemeral.phase37Synchronizer
          .registerRequest(TestPendingRequestDataType)(requestId)
          .complete(
            Some(
              CleanReplayData(
                rc,
                requestSc,
                MediatorGroupRecipient(MediatorGroupIndex.one),
                locallyRejectedF = FutureUnlessShutdown.pure(false),
                abortEngine = _ => (),
                engineAbortStatusF = FutureUnlessShutdown.pure(EngineAbortStatus.notAborted),
              )
            )
          )

        val before = ephemeral.requestJournal.query(rc).value.futureValueUS
        before shouldEqual None
        val taskScheduler = ephemeral.requestTracker.taskScheduler
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

        valueOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut))(
          "result processing failed"
        ).futureValueUS

        val requestState = ephemeral.requestJournal.query(rc).value.futureValueUS
        requestState shouldEqual None
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
      }
    }

    "give an error when decision time has elapsed" in {
      testProcessingSteps() { (sut, persistent, ephemeral) =>
        addRequestState(ephemeral, decisionTime = CantonTimestamp.Epoch.plusSeconds(5))

        val taskScheduler = ephemeral.requestTracker.taskScheduler
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

        setUpOrFail(persistent, ephemeral)
        val result = loggerFactory.suppressWarningsAndErrors(
          leftOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(5 * 60), sut))(
            "result processing did not return a left"
          ).futureValueUS
        )

        result match {
          case TestProcessorError(DecisionTimeElapsed(_, _)) =>
            taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead

          case _ => fail()
        }
      }
    }
  }

}
