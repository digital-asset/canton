// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import akka.stream.Materializer
import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Encrypted, SyncCryptoApi, TestHash}
import com.digitalasset.canton.data.PeanoQueue.{BeforeHead, NotInserted}
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, PeanoQueue}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.*
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.{Confirmed, Pending}
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
  TestProcessorError,
  TestViewType,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.*
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerDomainState
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  NoCommandDeduplicator,
}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  MultiDomainEventLog,
  ParticipantNodePersistentState,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.{
  ParticipantEventPublisher,
  SyncDomainPersistentStateLookup,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.sequencing.client.SendResult.Success
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendType,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{CursorPrehead, IndexedDomain}
import com.digitalasset.canton.time.{DomainTimeTracker, NonNegativeFiniteDuration, WallClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{
  ParticipantAttributes,
  ParticipantPermission,
  TrustLevel,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, RequestCounter, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class ProtocolProcessorTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("participant::participant")
  )
  private val party = PartyId(UniqueIdentifier.tryFromProtoPrimitive("party::participant"))
  private val domain = DefaultTestIdentities.domainId
  private val topology: TestingTopology = TestingTopology(
    Set(domain),
    Map(
      party.toLf -> Map(
        participant -> ParticipantAttributes(ParticipantPermission.Submission, TrustLevel.Ordinary)
      )
    ),
  )
  private val crypto =
    TestingIdentityFactory(topology, loggerFactory, TestDomainParameters.defaultDynamic)
      .forOwnerAndDomain(participant, domain)
  private val mockSequencerClient = mock[SequencerClient]
  when(
    mockSequencerClient.sendAsync(
      any[Batch[DefaultOpenEnvelope]],
      any[SendType],
      any[Option[CantonTimestamp]],
      any[CantonTimestamp],
      any[MessageId],
      any[Option[AggregationRule]],
      any[SendCallback],
    )(anyTraceContext)
  )
    .thenAnswer(
      (
          batch: Batch[DefaultOpenEnvelope],
          _: SendType,
          _: Option[CantonTimestamp],
          _: CantonTimestamp,
          messageId: MessageId,
          _: Option[AggregationRule],
          callback: SendCallback,
      ) => {
        callback(
          UnlessShutdown.Outcome(
            Success(
              Deliver.create(
                SequencerCounter(0),
                CantonTimestamp.Epoch,
                domain,
                Some(messageId),
                Batch.filterOpenEnvelopesFor(batch, participant),
                testedProtocolVersion,
              )
            )
          )
        )
        EitherT.pure[Future, SendAsyncClientError](())
      }
    )

  private val trm = mock[TransactionResultMessage]
  when(trm.pretty).thenAnswer(Pretty.adHocPrettyInstance[TransactionResultMessage])
  when(trm.verdict).thenAnswer(Verdict.Approve(testedProtocolVersion))
  when(trm.rootHash).thenAnswer(rootHash)
  when(trm.domainId).thenAnswer(DefaultTestIdentities.domainId)

  private val requestId = RequestId(CantonTimestamp.Epoch)
  private val requestSc = SequencerCounter(0)
  private val resultSc = SequencerCounter(1)
  private val rc = RequestCounter(0)
  private val parameters = DynamicDomainParametersWithValidity(
    DynamicDomainParameters.initialValues(NonNegativeFiniteDuration.Zero, testedProtocolVersion),
    CantonTimestamp.MinValue,
    None,
    domain,
  )

  private type TestInstance =
    ProtocolProcessor[
      Int,
      Unit,
      TestViewType,
      TransactionResultMessage,
      TestProcessingSteps.TestProcessingError,
    ]

  private def waitForAsyncResult(asyncResult: AsyncResult) = asyncResult.unwrap.unwrap.futureValue

  private def testProcessingSteps(
      overrideConstructedPendingRequestData: Option[TestPendingRequestData] = None,
      startingPoints: ProcessingStartingPoints = ProcessingStartingPoints.default,
      pendingSubmissionMap: concurrent.Map[Int, Unit] = TrieMap[Int, Unit](),
      sequencerClient: SequencerClient = mockSequencerClient,
      crypto: DomainSyncCryptoClient = crypto,
  ): (TestInstance, SyncDomainPersistentState, SyncDomainEphemeralState) = {

    val multiDomainEventLog = mock[MultiDomainEventLog]
    val persistentState =
      new InMemorySyncDomainPersistentStateOld(
        IndexedDomain.tryCreate(domain, 1),
        testedProtocolVersion,
        crypto.crypto.pureCrypto,
        enableAdditionalConsistencyChecks = true,
        loggerFactory,
        timeouts,
        futureSupervisor,
      )
    val syncDomainPersistentStates: SyncDomainPersistentStateLookup =
      new SyncDomainPersistentStateLookup {
        override val getAll: Map[DomainId, SyncDomainPersistentState] = Map(
          domain -> persistentState
        )
      }
    val indexedStringStore = InMemoryIndexedStringStore()
    val clock = new WallClock(timeouts, loggerFactory)
    implicit val mat = mock[Materializer]
    val nodePersistentState = timeouts.default.await("creating node persistent state")(
      ParticipantNodePersistentState
        .create(
          syncDomainPersistentStates,
          new MemoryStorage(loggerFactory, timeouts),
          clock,
          None,
          uniqueContractKeysO = Some(false),
          ParticipantStoreConfig(),
          testedReleaseProtocolVersion,
          ParticipantTestMetrics,
          indexedStringStore,
          timeouts,
          futureSupervisor,
          loggerFactory,
        )
        .failOnShutdown
    )
    val globalTracker =
      new GlobalCausalOrderer(
        participant,
        _ => true,
        DefaultProcessingTimeouts.testing,
        new InMemoryMultiDomainCausalityStore(loggerFactory),
        futureSupervisor,
        loggerFactory,
      )
    val domainCausalTracker =
      new SingleDomainCausalTracker(
        globalTracker,
        persistentState.causalDependencyStore,
        loggerFactory,
      )

    val mdel = InMemoryMultiDomainEventLog(
      syncDomainPersistentStates,
      nodePersistentState.participantEventLog,
      clock,
      timeouts,
      indexedStringStore,
      ParticipantTestMetrics,
      futureSupervisor,
      loggerFactory,
    )

    val ephemeralState = new AtomicReference[SyncDomainEphemeralState]()

    val eventPublisher = new ParticipantEventPublisher(
      participant,
      Eval.now(nodePersistentState.participantEventLog),
      Eval.now(mdel),
      clock,
      Eval.now(Duration.ofDays(1L)),
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    val inFlightSubmissionTracker = new InFlightSubmissionTracker(
      Eval.now(nodePersistentState.inFlightSubmissionStore),
      eventPublisher,
      new NoCommandDeduplicator(),
      Eval.now(mdel),
      _ =>
        Option(ephemeralState.get())
          .map(InFlightSubmissionTrackerDomainState.fromSyncDomainState(persistentState, _)),
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    val timeTracker = mock[DomainTimeTracker]

    ephemeralState.set(
      new SyncDomainEphemeralState(
        persistentState,
        Eval.now(multiDomainEventLog),
        domainCausalTracker,
        inFlightSubmissionTracker,
        startingPoints,
        _ => timeTracker,
        ParticipantTestMetrics.domain,
        timeouts,
        useCausalityTracking = true,
        loggerFactory,
        FutureSupervisor.Noop,
      )
    )

    val steps = new TestProcessingSteps(
      pendingSubmissionMap = pendingSubmissionMap,
      overrideConstructedPendingRequestData,
      informeesOfView = _ => Set(ConfirmingParty(party.toLf, 1)),
    )

    val sut: ProtocolProcessor[
      Int,
      Unit,
      TestViewType,
      TransactionResultMessage,
      TestProcessingSteps.TestProcessingError,
    ] =
      new ProtocolProcessor(
        steps,
        inFlightSubmissionTracker,
        ephemeralState.get(),
        crypto,
        sequencerClient,
        domainId = DefaultTestIdentities.domainId,
        testedProtocolVersion,
        loggerFactory,
        FutureSupervisor.Noop,
        skipRecipientsCheck = false,
      )(
        directExecutionContext: ExecutionContext,
        TransactionResultMessage.transactionResultMessageCast,
      ) {
        override def participantId: ParticipantId = participant

        override def timeouts = ProtocolProcessorTest.this.timeouts
      }

    ephemeralState.get().recordOrderPublisher.scheduleRecoveries(List.empty)
    (sut, persistentState, ephemeralState.get())
  }

  lazy val rootHash = RootHash(TestHash.digest(1))
  lazy val viewHash = ViewHash(TestHash.digest(2))
  lazy val encryptedView =
    EncryptedView(TestViewType)(Encrypted.fromByteString(rootHash.toProtoPrimitive).value)
  lazy val viewMessage: EncryptedViewMessage[TestViewType] = EncryptedViewMessageV0(
    submitterParticipantSignature = None,
    viewHash = viewHash,
    randomnessMap = Map.empty,
    encryptedView = encryptedView,
    domainId = DefaultTestIdentities.domainId,
  )
  lazy val rootHashMessage = RootHashMessage(
    rootHash,
    DefaultTestIdentities.domainId,
    testedProtocolVersion,
    TestViewType,
    SerializedRootHashMessagePayload.empty,
  )
  lazy val someRecipients = Recipients.cc(participant)
  lazy val someRequestBatch = RequestAndRootHashMessage(
    NonEmpty(Seq, OpenEnvelope(viewMessage, someRecipients)(testedProtocolVersion)),
    rootHashMessage,
    DefaultTestIdentities.mediator,
  )

  "submit" should {

    "succeed without errors" in {
      val submissionMap = TrieMap[Int, Unit]()
      val (sut, _persistent, _ephemeral) = testProcessingSteps(pendingSubmissionMap = submissionMap)
      sut
        .submit(0)
        .valueOrFailShutdown("submission")
        .futureValue
        .failOnShutdown("shutting down while test is running")
        .futureValue shouldBe (())
      submissionMap.get(0) shouldBe Some(()) // store the pending submission
    }

    "clean up the pending submissions when send request fails" in {
      val submissionMap = TrieMap[Int, Unit]()
      val failingSequencerClient = mock[SequencerClient]
      val sendError = SendAsyncClientError.RequestFailed("no thank you")
      when(
        failingSequencerClient.sendAsync(
          any[Batch[DefaultOpenEnvelope]],
          any[SendType],
          any[Option[CantonTimestamp]],
          any[CantonTimestamp],
          any[MessageId],
          any[Option[AggregationRule]],
          any[SendCallback],
        )(anyTraceContext)
      )
        .thenReturn(EitherT.leftT[Future, Unit](sendError))
      val (sut, persistent, ephemeral) =
        testProcessingSteps(
          sequencerClient = failingSequencerClient,
          pendingSubmissionMap = submissionMap,
        )
      val submissionResult = sut.submit(0).onShutdown(fail("submission shutdown")).value.futureValue
      submissionResult shouldEqual Left(TestProcessorError(SequencerRequestError(sendError)))
      submissionMap.get(0) shouldBe None // remove the pending submission
    }

    "clean up the pending submissions when no request is received" in {
      val submissionMap = TrieMap[Int, Unit]()
      val (sut, _persistent, _ephemeral) = testProcessingSteps(pendingSubmissionMap = submissionMap)

      sut
        .submit(1)
        .valueOrFailShutdown("submission")
        .futureValue
        .failOnShutdown("shutting down while test is running")
        .futureValue shouldBe (())
      submissionMap.get(1) shouldBe Some(())
      val afterDecisionTime = parameters.decisionTimeFor(CantonTimestamp.Epoch).value.plusMillis(1)
      val asyncRes = sut
        .processRequest(afterDecisionTime, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      eventually() {
        submissionMap.get(1) shouldBe None
      }
    }

    "fail if there is no active mediator" in {
      val crypto2 = TestingIdentityFactory(
        TestingTopology(mediators = Set.empty),
        loggerFactory,
        parameters.parameters,
      ).forOwnerAndDomain(participant, domain)
      val (sut, persistent, ephemeral) = testProcessingSteps(crypto = crypto2)
      val res = sut.submit(1).onShutdown(fail("submission shutdown")).value.futureValue
      res shouldBe Left(TestProcessorError(NoMediatorError(CantonTimestamp.Epoch)))
    }
  }

  "process request" should {

    "succeed without errors" in {
      val (sut, _persistent, _ephemeral) = testProcessingSteps()
      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      succeed
    }

    "transit to confirmed" in {
      val pd = TestPendingRequestData(
        rc,
        requestSc,
        Set.empty,
        MediatorId(UniqueIdentifier.tryCreate("another", "mediator")),
      )
      val (sut, _persistent, ephemeral) =
        testProcessingSteps(overrideConstructedPendingRequestData = Some(pd))
      val before = ephemeral.requestJournal.query(rc).value.futureValue
      before shouldEqual None

      val asyncRes = sut
        .processRequest(CantonTimestamp.Epoch, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      val requestState = ephemeral.requestJournal.query(rc).value.futureValue
      requestState.value.state shouldEqual RequestState.Confirmed
    }

    "leave the request state unchanged when doing a clean replay" in {
      val pendingData = TestPendingRequestData(
        rc,
        requestSc,
        Set.empty,
        MediatorId(UniqueIdentifier.tryCreate("another", "mediator")),
      )
      val (sut, _persistent, ephemeral) =
        testProcessingSteps(
          overrideConstructedPendingRequestData = Some(pendingData),
          startingPoints = ProcessingStartingPoints.tryCreate(
            MessageProcessingStartingPoint(rc, requestSc, CantonTimestamp.Epoch.minusSeconds(20)),
            MessageProcessingStartingPoint(
              rc + 1,
              requestSc + 1,
              CantonTimestamp.Epoch.minusSeconds(10),
            ),
            RequestCounter.Genesis.asLocalOffset,
            None,
          ),
        )

      val before = ephemeral.requestJournal.query(rc).value.futureValue
      before shouldEqual None

      val asyncRes = sut
        .processRequest(requestId.unwrap, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)
      val requestState = ephemeral.requestJournal.query(rc).value.futureValue
      requestState shouldEqual None
    }

    "trigger a timeout when the result doesn't arrive" in {
      val pd = TestPendingRequestData(
        rc,
        requestSc,
        Set.empty,
        MediatorId(UniqueIdentifier.tryCreate("another", "mediator")),
      )
      val (sut, _persistent, ephemeral) =
        testProcessingSteps(overrideConstructedPendingRequestData = Some(pd))

      val journal = ephemeral.requestJournal

      val initialSTate = journal.query(rc).value.futureValue
      initialSTate shouldEqual None

      // Process a request but never a corresponding response
      val asyncRes = sut
        .processRequest(CantonTimestamp.Epoch, rc, requestSc, someRequestBatch)
        .onShutdown(fail())
        .futureValue
      waitForAsyncResult(asyncRes)

      ephemeral.requestTracker.taskScheduler.readSequencerCounterQueue(
        requestSc
      ) shouldBe BeforeHead

      // The request remains at Confirmed until the timeout is triggered
      always() {
        journal.query(rc).value.futureValue.value.state shouldEqual RequestState.Confirmed
      }

      // Trigger the timeout for the request
      ephemeral.requestTracker.tick(
        requestSc + 1,
        parameters.decisionTimeFor(requestId.unwrap).value,
      )

      eventually() {
        val state = journal.query(rc).value.futureValue
        state.value.state shouldEqual RequestState.Clean
      }
    }

    "log wrong root hashes" in {
      val wrongRootHash = RootHash(TestHash.digest(3))
      val viewHash1 = ViewHash(TestHash.digest(2))
      val encryptedViewWrongRH =
        EncryptedView(TestViewType)(Encrypted.fromByteString(wrongRootHash.toProtoPrimitive).value)
      val viewMessageWrongRH = EncryptedViewMessageV0(
        submitterParticipantSignature = None,
        viewHash = viewHash1,
        randomnessMap = Map.empty,
        encryptedView = encryptedViewWrongRH,
        domainId = DefaultTestIdentities.domainId,
      )
      val requestBatchWrongRH = RequestAndRootHashMessage(
        NonEmpty(
          Seq,
          OpenEnvelope(viewMessage, someRecipients)(testedProtocolVersion),
          OpenEnvelope(viewMessageWrongRH, someRecipients)(testedProtocolVersion),
        ),
        rootHashMessage,
        DefaultTestIdentities.mediator,
      )

      val (sut, _persistent, _ephemeral) = testProcessingSteps()
      val asyncRes = loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, requestBatchWrongRH)
            .onShutdown(fail()),
          _.warningMessage should include(
            s"Request ${rc}: Found malformed payload: WrongRootHash"
          ),
        )
        .futureValue
      waitForAsyncResult(asyncRes)
    }

    "log decryption errors" in {
      val viewMessageDecryptError: EncryptedViewMessage[TestViewType] = EncryptedViewMessageV0(
        submitterParticipantSignature = None,
        viewHash = viewHash,
        randomnessMap = Map.empty,
        encryptedView =
          EncryptedView(TestViewType)(Encrypted.fromByteString(ByteString.EMPTY).value),
        domainId = DefaultTestIdentities.domainId,
      )
      val requestBatchDecryptError = RequestAndRootHashMessage(
        NonEmpty(
          Seq,
          OpenEnvelope(viewMessageDecryptError, someRecipients)(testedProtocolVersion),
        ),
        rootHashMessage,
        DefaultTestIdentities.mediator,
      )

      val (sut, _persistent, _ephemeral) = testProcessingSteps()
      val asyncRes = loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, requestBatchDecryptError)
            .onShutdown(fail()),
          _.warningMessage should include(
            s"Request ${rc}: Found malformed payload: SyncCryptoDecryptError("
          ),
        )
        .futureValue
      waitForAsyncResult(asyncRes)
      succeed
    }

    "check the declared mediator ID against the root hash message mediator" in {
      val otherMediatorId = MediatorId(UniqueIdentifier.tryCreate("mediator", "other"))
      val requestBatch = RequestAndRootHashMessage(
        NonEmpty(Seq, OpenEnvelope(viewMessage, someRecipients)(testedProtocolVersion)),
        rootHashMessage,
        otherMediatorId,
      )

      val (sut, _persistent, _ephemeral) = testProcessingSteps()
      loggerFactory
        .assertLogs(
          sut
            .processRequest(requestId.unwrap, rc, requestSc, requestBatch)
            .onShutdown(fail()),
          _.errorMessage should include(
            s"Mediator ${DefaultTestIdentities.mediator} declared in views is not the recipient $otherMediatorId of the root hash message"
          ),
        )
        .futureValue

    }
  }

  "perform result processing" should {

    val requestTimestamp = requestId.unwrap
    val activenessTimestamp = CantonTimestamp.Epoch
    val activenessSet = mkActivenessSet()

    def addRequestState(
        ephemeral: SyncDomainEphemeralState,
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
        persistent: SyncDomainPersistentState,
        ephemeral: SyncDomainEphemeralState,
    ): Unit = {

      val setupF = for {
        _ <- persistent.parameterStore.setParameters(defaultStaticDomainParameters)

        _ <- ephemeral.requestJournal.insert(rc, CantonTimestamp.Epoch)
        _ <- ephemeral.requestJournal.transit(rc, CantonTimestamp.Epoch, Pending, Confirmed)
      } yield ephemeral.phase37Synchronizer
        .registerRequest(TestPendingRequestDataType)(requestId)
        .complete(
          Some(
            WrappedPendingRequestData(
              TestPendingRequestData(
                rc,
                requestSc,
                Set.empty,
                MediatorId(UniqueIdentifier.tryCreate("another", "mediator")),
              )
            )
          )
        )
      setupF.futureValue
    }

    def performResultProcessing(
        timestamp: CantonTimestamp,
        sut: TestInstance,
    ): EitherT[Future, sut.steps.ResultError, Unit] = {
      val mockSignedProtocolMessage = mock[SignedProtocolMessage[TransactionResultMessage]]
      when(mockSignedProtocolMessage.message).thenReturn(trm)
      when(
        mockSignedProtocolMessage
          .verifySignature(any[SyncCryptoApi], any[Member])
      )
        .thenReturn(EitherT.rightT(()))
      sut
        .performResultProcessing(
          mock[Either[
            EventWithErrors[Deliver[DefaultOpenEnvelope]],
            SignedContent[Deliver[DefaultOpenEnvelope]],
          ]],
          Right(mockSignedProtocolMessage),
          requestId,
          timestamp,
          resultSc,
        )
        .map(_.onShutdown(fail("Unexpected shutdown")))
        .flatMap(identity)
    }

    "succeed without errors and transit to clean" in {
      val (sut, persistent, ephemeral) = testProcessingSteps()
      addRequestState(ephemeral)

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      // Check the initial state is clean
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

      setUpOrFail(persistent, ephemeral)
      valueOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut))(
        "result processing failed"
      ).futureValue

      val finalState = ephemeral.requestJournal.query(rc).value.futureValue
      finalState.value.state shouldEqual RequestState.Clean

      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
    }

    "wait for request processing to finish" in {
      val (sut, _persistent, ephemeral) = testProcessingSteps()

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      val requestJournal = ephemeral.requestJournal

      // Register request is called before request processing is triggered
      val handle =
        ephemeral.phase37Synchronizer
          .registerRequest(sut.steps.requestType)(
            requestId
          )

      // Process the result message before the request
      val processF = performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut)

      // Processing should not complete as the request processing has not finished
      always() { processF.value.isCompleted shouldEqual false }

      // Check the result processing has not modified the request state
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)
      requestJournal.query(rc).value.futureValue shouldBe None

      // Now process the request message. This should trigger the completion of the result processing.
      sut
        .performRequestProcessing(
          requestId.unwrap,
          rc,
          requestSc,
          handle,
          someRequestBatch,
        )
        .onShutdown(fail())
        .futureValue

      eventually() {
        processF.value.futureValue shouldEqual Right(())
        taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
        requestJournal.query(rc).value.futureValue.value.state shouldBe RequestState.Clean
      }
    }

    "succeed without errors on clean replay, not changing the request state" in {

      val (sut, _persistent, ephemeral) = testProcessingSteps(
        startingPoints = ProcessingStartingPoints.tryCreate(
          MessageProcessingStartingPoint(rc, requestSc, CantonTimestamp.Epoch.minusSeconds(1)),
          MessageProcessingStartingPoint(
            rc + 5,
            requestSc + 10,
            CantonTimestamp.Epoch.plusSeconds(30),
          ),
          RequestCounter.Genesis.asLocalOffset,
          None,
        )
      )

      addRequestState(ephemeral)
      ephemeral.phase37Synchronizer
        .registerRequest(TestPendingRequestDataType)(requestId)
        .complete(
          Some(
            CleanReplayData(
              rc,
              requestSc,
              Set.empty,
              MediatorId(UniqueIdentifier.tryCreate("another", "mediator")),
            )
          )
        )

      val before = ephemeral.requestJournal.query(rc).value.futureValue
      before shouldEqual None
      val taskScheduler = ephemeral.requestTracker.taskScheduler
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

      valueOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(10), sut))(
        "result processing failed"
      ).futureValue

      val requestState = ephemeral.requestJournal.query(rc).value.futureValue
      requestState shouldEqual None
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead
    }

    "give an error when decision time has elapsed" in {
      val (sut, persistent, ephemeral) = testProcessingSteps()

      addRequestState(ephemeral, decisionTime = CantonTimestamp.Epoch.plusSeconds(5))

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      taskScheduler.readSequencerCounterQueue(resultSc) shouldBe NotInserted(None, None)

      setUpOrFail(persistent, ephemeral)
      val result = loggerFactory.suppressWarningsAndErrors(
        leftOrFail(performResultProcessing(CantonTimestamp.Epoch.plusSeconds(5 * 60), sut))(
          "result processing did not return a left"
        ).futureValue
      )

      result match {
        case TestProcessorError(DecisionTimeElapsed(_, _)) =>
          taskScheduler.readSequencerCounterQueue(resultSc) shouldBe BeforeHead

        case _ => fail()
      }
    }

    "tick the record order publisher only after the clean request prehead has advanced" in {
      val (sut, persistent, ephemeral) = testProcessingSteps(
        startingPoints = ProcessingStartingPoints.tryCreate(
          MessageProcessingStartingPoint(
            rc - 1L,
            requestSc - 1L,
            CantonTimestamp.Epoch.minusSeconds(11),
          ),
          MessageProcessingStartingPoint(
            rc - 1L,
            requestSc - 1L,
            CantonTimestamp.Epoch.minusSeconds(11),
          ),
          rc.asLocalOffset - 1L,
          Some(CursorPrehead(requestSc, requestTimestamp)),
        )
      )

      val taskScheduler = ephemeral.requestTracker.taskScheduler
      val tsBefore = CantonTimestamp.Epoch.minusSeconds(10)
      taskScheduler.addTick(requestSc - 1L, tsBefore)
      addRequestState(ephemeral)
      setUpOrFail(persistent, ephemeral)

      val resultTs = CantonTimestamp.Epoch.plusSeconds(1)
      valueOrFail(performResultProcessing(resultTs, sut))("result processing failed").futureValue

      val finalState = ephemeral.requestJournal.query(rc).value.futureValue
      finalState.value.state shouldEqual RequestState.Clean
      val prehead = persistent.requestJournalStore.preheadClean.futureValue
      prehead shouldBe None

      // So if the protocol processor ticks the record order publisher without waiting for the request journal cursor,
      // we'd eventually see the record order publisher being ticked for `requestSc`.
      always() {
        ephemeral.recordOrderPublisher.readSequencerCounterQueue(requestSc) shouldBe
          PeanoQueue.NotInserted(None, Some(resultTs))
      }
    }

  }

}
