// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.flatMap._
import cats.syntax.option._
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.crypto.{Encrypted, HashPurpose, HashPurposeTest, TestHash}
import com.digitalasset.canton.data._
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.MessageDispatcher.{
  DoNotExpectMediatorResult,
  ExpectMalformedMediatorRequestResult,
  MalformedMediatorRequestMessage,
  SendMalformedAndExpectMediatorResult,
  _,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.protocol.messages.EncryptedView.CompressedView
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{
  RequestAndRootHashMessage,
  RequestId,
  RequestProcessor,
  RootHash,
  TransferId,
  ViewHash,
  v0 => protocolv0,
}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.sequencing.{
  HandlerResult,
  PossiblyIgnoredProtocolEvent,
  RawProtocolEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{MediatorId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, DiscardOps, DomainId, LfPartyId, SequencerCounter}
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.{eq => isEq}
import org.scalatest.Assertion
import org.scalatest.wordspec.{AsyncWordSpec, AsyncWordSpecLike}

import java.util.UUID
import scala.concurrent.Future

trait MessageDispatcherTest { this: AsyncWordSpecLike with BaseTest =>

  import MessageDispatcherTest._

  val domainId = DomainId.tryFromString("messageDispatcher::domain")
  val originDomain = DomainId.tryFromString("originDomain::originDomain")
  val participantId = ParticipantId.tryFromProtoPrimitive("messageDispatcher::participant")
  val mediatorId = MediatorId(domainId)
  val mediatorId2 = MediatorId(UniqueIdentifier.tryCreate("another", "mediator"))

  case class Fixture(
      messageDispatcher: MessageDispatcher,
      requestTracker: RequestTracker,
      testProcessor: RequestProcessor[TestViewType],
      otherTestProcessor: RequestProcessor[OtherTestViewType],
      identityProcessor: (
          SequencerCounter,
          CantonTimestamp,
          Traced[List[DefaultOpenEnvelope]],
      ) => HandlerResult,
      acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
      requestCounterAllocator: RequestCounterAllocator,
      recordOrderPublisher: RecordOrderPublisher,
      badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
      repairProcessor: RepairProcessor,
      inFlightSubmissionTracker: InFlightSubmissionTracker,
      causalityTracker: SingleDomainCausalTracker,
  )

  object Fixture {
    def mk(
        mkMd: (
            DomainId,
            ParticipantId,
            RequestTracker,
            RequestProcessors,
            SingleDomainCausalTracker,
            (SequencerCounter, CantonTimestamp, Traced[List[DefaultOpenEnvelope]]) => HandlerResult,
            AcsCommitmentProcessor.ProcessorType,
            RequestCounterAllocator,
            RecordOrderPublisher,
            BadRootHashMessagesRequestProcessor,
            RepairProcessor,
            InFlightSubmissionTracker,
            NamedLoggerFactory,
        ) => MessageDispatcher,
        initRc: RequestCounter = 0L,
        cleanReplaySequencerCounter: SequencerCounter = 0L,
        processingRequestF: => FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit,
        processingResultF: => FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit,
    ): Fixture = {
      val requestTracker = mock[RequestTracker]

      def mockMethods[VT <: ViewType](processor: RequestProcessor[VT]): Unit = {
        when(
          processor.processRequest(
            any[CantonTimestamp],
            any[RequestCounter],
            any[SequencerCounter],
            any[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]],
          )(anyTraceContext)
        )
          .thenReturn(processingRequestF)
        when(
          processor.processResult(any[SignedContent[Deliver[DefaultOpenEnvelope]]])(anyTraceContext)
        )
          .thenReturn(processingResultF)
        when(
          processor.processMalformedMediatorRequestResult(
            any[CantonTimestamp],
            any[SequencerCounter],
            any[SignedContent[Deliver[DefaultOpenEnvelope]]],
          )(anyTraceContext)
        )
          .thenReturn(processingResultF)
      }

      val testViewProcessor = mock[RequestProcessor[TestViewType]]
      mockMethods(testViewProcessor)

      val otherTestViewProcessor = mock[RequestProcessor[OtherTestViewType]]
      mockMethods(otherTestViewProcessor)

      val identityProcessor =
        mock[
          (SequencerCounter, CantonTimestamp, Traced[List[DefaultOpenEnvelope]]) => HandlerResult
        ]
      when(
        identityProcessor.apply(
          any[SequencerCounter],
          any[CantonTimestamp],
          any[Traced[List[DefaultOpenEnvelope]]],
        )
      )
        .thenReturn(HandlerResult.done)

      val acsCommitmentProcessor = mock[AcsCommitmentProcessor.ProcessorType]
      when(
        acsCommitmentProcessor.apply(
          any[CantonTimestamp],
          any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]],
        )
      )
        .thenReturn(FutureUnlessShutdown.unit)

      val tracker = mock[SingleDomainCausalTracker]
      when(tracker.registerCausalityMessages(any[List[CausalityMessage]])(anyTraceContext))
        .thenReturn(Future.unit)

      val requestCounterAllocator =
        new RequestCounterAllocatorImpl(initRc, cleanReplaySequencerCounter, loggerFactory)
      val recordOrderPublisher = mock[RecordOrderPublisher]

      val badRootHashMessagesRequestProcessor = mock[BadRootHashMessagesRequestProcessor]
      when(
        badRootHashMessagesRequestProcessor
          .handleBadRequestWithExpectedMalformedMediatorRequest(
            any[RequestCounter],
            any[SequencerCounter],
            any[CantonTimestamp],
            any[MediatorId],
          )(anyTraceContext)
      )
        .thenReturn(processingRequestF)
      when(
        badRootHashMessagesRequestProcessor.sendRejectionAndExpectMediatorResult(
          any[RequestCounter],
          any[SequencerCounter],
          any[CantonTimestamp],
          any[RootHash],
          any[MediatorId],
          any[String],
        )(anyTraceContext)
      )
        .thenReturn(processingRequestF)

      val repairProcessor = mock[RepairProcessor]

      val inFlightSubmissionTracker = mock[InFlightSubmissionTracker]
      when(
        inFlightSubmissionTracker.observeSequencing(
          any[DomainId],
          any[Map[MessageId, SequencedSubmission]],
        )(anyTraceContext)
      )
        .thenReturn(Future.unit)
      when(inFlightSubmissionTracker.observeDeliverError(any[DeliverError])(anyTraceContext))
        .thenReturn(Future.unit)

      val protocolProcessors = new RequestProcessors {
        override protected def getInternal[P](
            viewType: ViewType { type Processor = P }
        ): Option[P] = viewType match {
          case TestViewType => Some(testViewProcessor)
          case OtherTestViewType => Some(otherTestViewProcessor)
          case _ => None
        }
      }

      val messageDispatcher = mkMd(
        domainId,
        participantId,
        requestTracker,
        protocolProcessors,
        tracker,
        identityProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        loggerFactory,
      )

      Fixture(
        messageDispatcher,
        requestTracker,
        testViewProcessor,
        otherTestViewProcessor,
        identityProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        tracker,
      )
    }
  }

  def mkDeliver(
      batch: Batch[DefaultOpenEnvelope],
      sc: SequencerCounter = 0L,
      ts: CantonTimestamp = CantonTimestamp.Epoch,
      messageId: Option[MessageId] = None,
  ): Deliver[DefaultOpenEnvelope] =
    Deliver.create(sc, ts, domainId, messageId, batch)

  def rootHash(index: Int): RootHash = RootHash(TestHash.digest(index))

  def signEvent[Env](event: SequencedEvent[Env]): SignedContent[SequencedEvent[Env]] =
    SequencerTestUtils.sign(event)

  val dummySignature = SymbolicCrypto.emptySignature

  def mockEncryptedViewTree = mock[Encrypted[CompressedView[MockViewTree]]]
  val encryptedTestView = EncryptedView(TestViewType)(mockEncryptedViewTree)
  val encryptedTestViewMessage =
    EncryptedViewMessage(
      None,
      ViewHash(TestHash.digest(9000)),
      Map.empty,
      encryptedTestView,
      domainId,
    )

  val encryptedOtherTestView = EncryptedView(OtherTestViewType)(mockEncryptedViewTree)
  val encryptedOtherTestViewMessage =
    EncryptedViewMessage(
      None,
      ViewHash(TestHash.digest(9001)),
      Map.empty,
      encryptedOtherTestView,
      domainId,
    )

  val requestId = RequestId(CantonTimestamp.Epoch)
  val testMediatorResult =
    SignedProtocolMessage(
      TestRegularMediatorResult(TestViewType, domainId, Verdict.Approve, requestId),
      dummySignature,
    )
  val otherTestMediatorResult =
    SignedProtocolMessage(
      TestRegularMediatorResult(OtherTestViewType, domainId, Verdict.Approve, requestId),
      dummySignature,
    )

  val causalityMessage = CausalityMessage(
    domainId,
    TransferId(originDomain, CantonTimestamp.Epoch),
    VectorClock(
      originDomain,
      CantonTimestamp.Epoch,
      LfPartyId.assertFromString("Alice::domain"),
      Map.empty,
    ),
  )

  def messageDispatcher(
      mkMd: (
          DomainId,
          ParticipantId,
          RequestTracker,
          RequestProcessors,
          SingleDomainCausalTracker,
          (SequencerCounter, CantonTimestamp, Traced[List[DefaultOpenEnvelope]]) => HandlerResult,
          AcsCommitmentProcessor.ProcessorType,
          RequestCounterAllocator,
          RecordOrderPublisher,
          BadRootHashMessagesRequestProcessor,
          RepairProcessor,
          InFlightSubmissionTracker,
          NamedLoggerFactory,
      ) => MessageDispatcher
  ) = {

    type AnyProcessor = RequestProcessor[_ <: ViewType]
    type ProcessorOfFixture = Fixture => AnyProcessor

    def mk(
        initRc: RequestCounter = 0L,
        cleanReplaySequencerCounter: SequencerCounter = 0L,
    ): Fixture =
      Fixture.mk(mkMd, initRc, cleanReplaySequencerCounter)

    val idTx = mock[DomainTopologyTransactionMessage]
    when(idTx.domainId).thenReturn(domainId)

    val rawCommitment = mock[AcsCommitment]
    when(rawCommitment.domainId).thenReturn(domainId)
    val commitment = SignedProtocolMessage(rawCommitment, dummySignature)

    val reject = MediatorReject.Timeout.Reject()
    val malformedMediatorRequestResult =
      SignedProtocolMessage(
        MalformedMediatorRequestResult(
          RequestId(CantonTimestamp.MinValue),
          domainId,
          TestViewType,
          reject,
        ),
        dummySignature,
      )

    def checkTickIdentityProcessor(
        sut: Fixture,
        sc: SequencerCounter = 0L,
        ts: CantonTimestamp = CantonTimestamp.Epoch,
    ): Assertion = {
      verify(sut.identityProcessor).apply(
        isEq(sc),
        isEq(ts),
        any[Traced[List[DefaultOpenEnvelope]]],
      )
      succeed
    }

    def checkTickRequestTracker(
        sut: Fixture,
        sc: SequencerCounter = 0L,
        ts: CantonTimestamp = CantonTimestamp.Epoch,
    ): Assertion = {
      verify(sut.requestTracker).tick(isEq(sc), isEq(ts))(anyTraceContext)
      succeed
    }

    def checkTickRecordOrderPublisher(
        sut: Fixture,
        sc: SequencerCounter,
        ts: CantonTimestamp,
    ): Assertion = {
      verify(sut.recordOrderPublisher).tick(isEq(sc), isEq(ts))(anyTraceContext)
      succeed
    }

    def checkObserveSequencing(
        sut: Fixture,
        expected: Map[MessageId, SequencedSubmission],
    ): Assertion = {
      verify(sut.inFlightSubmissionTracker).observeSequencing(isEq(domainId), isEq(expected))(
        anyTraceContext
      )
      succeed
    }

    def checkObserveDeliverError(sut: Fixture, expected: DeliverError): Assertion = {
      verify(sut.inFlightSubmissionTracker).observeDeliverError(isEq(expected))(anyTraceContext)
      succeed
    }

    def checkTicks(
        sut: Fixture,
        sc: SequencerCounter = 0L,
        ts: CantonTimestamp = CantonTimestamp.Epoch,
    ): Assertion = {
      checkTickIdentityProcessor(sut, sc, ts)
      checkTickRequestTracker(sut, sc, ts)
      checkTickRecordOrderPublisher(sut, sc, ts)
    }

    def checkProcessRequest[VT <: ViewType](
        processor: RequestProcessor[VT],
        ts: CantonTimestamp,
        rc: RequestCounter,
        sc: SequencerCounter,
    ): Assertion = {
      verify(processor).processRequest(
        isEq(ts),
        isEq(rc),
        isEq(sc),
        any[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]],
      )(anyTraceContext)
      succeed
    }

    def checkNotProcessRequest[VT <: ViewType](processor: RequestProcessor[VT]): Assertion = {
      verify(processor, never).processRequest(
        any[CantonTimestamp],
        any[RequestCounter],
        any[SequencerCounter],
        any[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]],
      )(anyTraceContext)
      succeed
    }

    def checkProcessResult(processor: AnyProcessor): Assertion = {
      verify(processor).processResult(any[SignedContent[Deliver[DefaultOpenEnvelope]]])(
        anyTraceContext
      )
      succeed
    }

    def signAndTrace(event: RawProtocolEvent): Traced[Seq[PossiblyIgnoredProtocolEvent]] =
      Traced(Seq(OrdinarySequencedEvent(signEvent(event))(traceContext)))

    def handle(sut: Fixture, event: RawProtocolEvent)(checks: => Assertion): Future[Assertion] = {
      for {
        _ <- sut.messageDispatcher
          .handleAll(signAndTrace(event))
          .onShutdown(fail(s"Encountered shutdown while handling $event"))
        _ <- sut.messageDispatcher.flush()
      } yield { checks }
    }

    "handling a deliver event" should {
      "call the transaction processor after having informed the identity processor and tick the request tracker" in {
        val sut = mk()
        val sc = 1L
        val ts = CantonTimestamp.Epoch
        val prefix = TimeProof.timeEventMessageIdPrefix
        val deliver = SequencerTestUtils.mockDeliver(
          sc,
          ts,
          domainId,
          messageId = Some(
            MessageId
              .tryCreate(s"$prefix testing")
          ),
        )
        // Check that we're calling the topology manager before we're publishing the deliver event and ticking the
        // request tracker
        when(
          sut.recordOrderPublisher.scheduleEmptyAcsChangePublication(
            any[SequencerCounter],
            any[CantonTimestamp],
          )(anyTraceContext)
        )
          .thenAnswer {
            checkTickIdentityProcessor(sut, sc, ts).discard
          }
        when(sut.requestTracker.tick(any[SequencerCounter], any[CantonTimestamp])(anyTraceContext))
          .thenAnswer {
            checkTickIdentityProcessor(sut, sc, ts).discard
          }

        handle(sut, deliver) {
          verify(sut.recordOrderPublisher).scheduleEmptyAcsChangePublication(isEq(sc), isEq(ts))(
            anyTraceContext
          )
          checkTicks(sut, sc, ts)
        }
      }
    }

    "topology transactions" should {
      "be passed to the identity processor" in {
        val sut = mk()
        val sc = 1L
        val ts = CantonTimestamp.ofEpochSecond(1)
        val event = mkDeliver(Batch.of(idTx -> Recipients.cc(participantId)), sc, ts)
        handle(sut, event) {
          checkTicks(sut, sc, ts)
        }
      }
    }

    "causality messages" should {
      "be passed to the causality tracker" in {
        val sut = mk()
        val sc = 1L
        val ts = CantonTimestamp.ofEpochSecond(1)
        val event = mkDeliver(Batch.of(causalityMessage -> Recipients.cc(participantId)), sc, ts)
        handle(sut, event) {
          verify(sut.causalityTracker)
            .registerCausalityMessages(isEq[List[CausalityMessage]](List(causalityMessage)))(
              anyTraceContext
            )
          checkTicks(sut, sc, ts)
        }
      }
    }

    "ACS commitments" should {
      "be passed to the ACS commitment processor" in {
        val sut = mk()
        val sc = 2L
        val ts = CantonTimestamp.ofEpochSecond(2)
        val event = mkDeliver(Batch.of(commitment -> Recipients.cc(participantId)), sc, ts)
        handle(sut, event) {
          verify(sut.acsCommitmentProcessor)
            .apply(isEq(ts), any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]])
          checkTicks(sut, sc, ts)
        }
      }
    }

    "synchronous shutdown propagates" in {
      val sut = mk()
      val sc = 3L
      val ts = CantonTimestamp.ofEpochSecond(3)

      // Overwrite the mocked identity processor so that it aborts synchronously
      when(
        sut.identityProcessor
          .apply(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Traced[List[DefaultOpenEnvelope]]],
          )
      )
        .thenReturn(HandlerResult.synchronous(FutureUnlessShutdown.abortedDueToShutdown))
      when(
        sut.acsCommitmentProcessor.apply(
          any[CantonTimestamp],
          any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]],
        )
      )
        .thenReturn(FutureUnlessShutdown.unit)

      val event = mkDeliver(Batch.of[ProtocolMessage](idTx -> Recipients.cc(participantId)), sc, ts)
      for {
        result <- sut.messageDispatcher.handleAll(signAndTrace(event)).unwrap
        _ <- sut.messageDispatcher.flush()
      } yield {
        result shouldBe UnlessShutdown.AbortedDueToShutdown
        verify(sut.acsCommitmentProcessor, never)
          .apply(
            any[CantonTimestamp],
            any[Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]]],
          )
        succeed
      }
    }

    "asynchronous shutdown propagates" in {
      val sut = mk()
      val sc = 3L
      val ts = CantonTimestamp.ofEpochSecond(3)

      // Overwrite the mocked identity processor so that it aborts asynchronously
      when(
        sut.identityProcessor
          .apply(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Traced[List[DefaultOpenEnvelope]]],
          )
      )
        .thenReturn(HandlerResult.asynchronous(FutureUnlessShutdown.abortedDueToShutdown))

      val event = mkDeliver(Batch.of[ProtocolMessage](idTx -> Recipients.cc(participantId)), sc, ts)
      for {
        result <- sut.messageDispatcher.handleAll(signAndTrace(event)).unwrap
        _ <- sut.messageDispatcher.flush()
        abort <- result.traverse(_.unwrap).unwrap
      } yield {
        abort.flatten shouldBe UnlessShutdown.AbortedDueToShutdown
        // Since the shutdown happened asynchronously, we cannot enforce whether the other handlers are called.
      }
    }

    "complain about unknown view types in a request" in {
      val sut = mk(initRc = -12L)
      val encryptedUnknownTestView = EncryptedView(UnknownTestViewType)(mockEncryptedViewTree)
      val encryptedUnknownTestViewMessage =
        EncryptedViewMessage(
          None,
          ViewHash(TestHash.digest(9002)),
          Map.empty,
          encryptedUnknownTestView,
          domainId,
        )
      val rootHashMessage =
        RootHashMessage(
          rootHash(1),
          domainId,
          UnknownTestViewType,
          SerializedRootHashMessagePayload.empty,
        )
      val event = mkDeliver(
        Batch.of[ProtocolMessage](
          encryptedUnknownTestViewMessage -> Recipients.cc(participantId),
          rootHashMessage -> Recipients.cc(participantId, mediatorId),
        ),
        11L,
        CantonTimestamp.ofEpochSecond(11),
      )
      for {
        error <- loggerFactory.assertLogs(
          sut.messageDispatcher.handleAll(signAndTrace(event)).failed,
          loggerFactory.checkLogsInternalError[IllegalArgumentException](
            _.getMessage should include(show"No processor for view type $UnknownTestViewType")
          ),
          _.errorMessage should include("event processing failed."),
        )
      } yield {
        error shouldBe a[IllegalArgumentException]
        error.getMessage should include(show"No processor for view type $UnknownTestViewType")
      }
    }

    "complain about unknown view types in a result" in {
      val sut = mk(initRc = -11L)
      val unknownTestMediatorResult =
        SignedProtocolMessage(
          TestRegularMediatorResult(UnknownTestViewType, domainId, Verdict.Approve, requestId),
          dummySignature,
        )
      val event =
        mkDeliver(
          Batch.of[ProtocolMessage](unknownTestMediatorResult -> Recipients.cc(participantId)),
          12L,
          CantonTimestamp.ofEpochSecond(11),
        )
      for {
        error <- loggerFactory.assertLogs(
          sut.messageDispatcher.handleAll(signAndTrace(event)).failed,
          loggerFactory.checkLogsInternalError[IllegalArgumentException](
            _.getMessage should include(show"No processor for view type $UnknownTestViewType")
          ),
          _.errorMessage should include("processing failed"),
        )
      } yield {
        error shouldBe a[IllegalArgumentException]
        error.getMessage should include(show"No processor for view type $UnknownTestViewType")
      }
    }

    def request(
        view: EncryptedViewMessage[ViewType],
        processor: ProcessorOfFixture,
        wrongView: EncryptedViewMessage[ViewType],
    ): Unit = {
      val viewType = view.viewType
      val wrongViewType = wrongView.viewType

      s"be passed to the $viewType processor" in {
        val initRc = 2L
        val sut = mk(initRc = initRc)
        val sc = 2L
        val ts = CantonTimestamp.ofEpochSecond(2)
        val rootHashMessage =
          RootHashMessage(rootHash(1), domainId, viewType, SerializedRootHashMessagePayload.empty)
        val event =
          mkDeliver(
            Batch.of[ProtocolMessage](
              view -> Recipients.cc(participantId),
              rootHashMessage -> Recipients.cc(participantId, mediatorId),
            ),
            sc,
            ts,
          )
        handle(sut, event) {
          checkProcessRequest(processor(sut), ts, initRc, sc)
          checkTickIdentityProcessor(sut, sc, ts)
          checkTickRequestTracker(sut, sc, ts)
          sut.requestCounterAllocator.peek shouldBe initRc + 1
        }
      }

      "expect a valid root hash message" in {
        val rootHashMessage =
          RootHashMessage(rootHash(1), domainId, viewType, SerializedRootHashMessagePayload.empty)
        val otherParticipant = ParticipantId.tryFromProtoPrimitive("other::participant")
        // Batch -> expected alarms -> expected reaction
        val badBatches = List(
          Batch.of[ProtocolMessage](view -> Recipients.cc(participantId)) ->
            Seq("No valid root hash message in batch") -> DoNotExpectMediatorResult,
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId),
          ) -> Seq(
            "Received root hash messages that were not sent to a mediator",
            "No valid root hash message in batch",
          ) -> DoNotExpectMediatorResult,
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, otherParticipant, mediatorId2),
          ) -> Seq(
            "Received root hash message with invalid recipients"
          ) -> ExpectMalformedMediatorRequestResult(mediatorId2),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, otherParticipant, mediatorId2),
            rootHashMessage -> Recipients.cc(participantId, mediatorId2),
          ) -> Seq("Multiple root hash messages in batch") -> ExpectMalformedMediatorRequestResult(
            (mediatorId2)
          ),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage
              .copy(viewType = wrongViewType) -> Recipients.cc(participantId, mediatorId),
          ) -> Seq(
            show"Received no encrypted view message of type $wrongViewType",
            show"Expected view type $wrongViewType, but received view types $viewType",
          ) -> SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediatorId,
            show"Received no encrypted view message of type $wrongViewType",
          ),
          Batch.of[ProtocolMessage](
            rootHashMessage -> Recipients.cc(participantId, mediatorId)
          ) -> Seq(
            show"Received no encrypted view message of type $viewType"
          ) -> SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediatorId,
            show"Received no encrypted view message of type $viewType",
          ),
          Batch.of[ProtocolMessage](
            wrongView -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId),
          ) -> Seq(
            show"Expected view type $viewType, but received view types $wrongViewType",
            show"Received no encrypted view message of type $viewType",
          ) -> SendMalformedAndExpectMediatorResult(
            rootHashMessage.rootHash,
            mediatorId,
            show"Received no encrypted view message of type $viewType",
          ),
        )

        // sequentially process the test cases so that the log messages don't interfere
        MonadUtil
          .sequentialTraverse_(badBatches.zipWithIndex) {
            case (((batch, alarms), reaction), index) =>
              val initRc = index.toLong
              val sut = mk(initRc = initRc)
              val sc = index.toLong
              val ts = CantonTimestamp.ofEpochSecond(index.toLong)
              withClueF(s"at batch $index") {
                loggerFactory.assertLogsUnordered(
                  handle(sut, mkDeliver(batch, sc, ts)) {
                    // tick the request counter only if we expect a mediator result
                    sut.requestCounterAllocator.peek shouldBe
                      (if (reaction == DoNotExpectMediatorResult) initRc else initRc + 1)
                    checkNotProcessRequest(processor(sut))
                    reaction match {
                      case DoNotExpectMediatorResult => checkTicks(sut, sc, ts)
                      case ExpectMalformedMediatorRequestResult(mediatorId) =>
                        verify(sut.badRootHashMessagesRequestProcessor)
                          .handleBadRequestWithExpectedMalformedMediatorRequest(
                            eqTo(initRc),
                            eqTo(sc),
                            eqTo(ts),
                            eqTo(mediatorId),
                          )(anyTraceContext)
                        checkTickIdentityProcessor(sut, sc, ts)
                        checkTickRequestTracker(sut, sc, ts)
                      case SendMalformedAndExpectMediatorResult(rootHash, mediatorId, reason) =>
                        verify(sut.badRootHashMessagesRequestProcessor)
                          .sendRejectionAndExpectMediatorResult(
                            eqTo(initRc),
                            eqTo(sc),
                            eqTo(ts),
                            eqTo(rootHash),
                            eqTo(mediatorId),
                            eqTo(reason),
                          )(anyTraceContext)
                        checkTickIdentityProcessor(sut, sc, ts)
                        checkTickRequestTracker(sut, sc, ts)
                    }
                    succeed
                  },
                  alarms.map(alarm =>
                    (logEntry: LogEntry) => logEntry.errorMessage should include(alarm)
                  ): _*
                )
              }
          }
          .map(_ => succeed)
      }

      "crash upon root hash messages for multiple mediators" in {
        val rootHashMessage =
          RootHashMessage(rootHash(1), domainId, viewType, SerializedRootHashMessagePayload.empty)
        val fatalBatches = List(
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId2),
          ),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId, mediatorId2),
          ),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.groups(
              NonEmptyList.of(
                NonEmptySet.of(participantId, mediatorId),
                NonEmptySet.of(participantId, mediatorId2),
              )
            ),
          ),
        )

        // sequentially process the test cases so that the log messages don't interfere
        MonadUtil
          .sequentialTraverse_(fatalBatches.zipWithIndex) { case (batch, index) =>
            val initRc = index.toLong
            val sut = mk(initRc = initRc)
            val sc = index.toLong
            val ts = CantonTimestamp.ofEpochSecond(index.toLong)
            withClueF(s"at batch $index") {
              loggerFactory.assertThrowsAndLogsAsync[IllegalArgumentException](
                handle(sut, mkDeliver(batch, sc, ts))(succeed),
                _.getMessage should include(
                  "Received batch with encrypted views and root hash messages addressed to multiple mediators"
                ),
                _.errorMessage should include(ErrorUtil.internalErrorMessage),
                _.errorMessage should include("event processing failed."),
              )
            }
          }
          .map(_ => succeed)
      }

      "not get confused about additional envelopes" in {
        val rootHashMessage =
          RootHashMessage(rootHash(1), domainId, viewType, SerializedRootHashMessagePayload.empty)
        val badBatches = List(
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId),
          ) -> Seq("Received root hash messages that were not sent to a mediator"),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId),
            commitment -> Recipients.cc(participantId),
            idTx -> Recipients.cc(participantId),
          ) -> Seq(),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId),
            wrongView -> Recipients.cc(participantId),
          ) -> Seq(show"Expected view type $viewType, but received view types $wrongViewType"),
          Batch.of[ProtocolMessage](
            view -> Recipients.cc(participantId),
            rootHashMessage -> Recipients.cc(participantId, mediatorId),
            malformedMediatorRequestResult -> Recipients.cc(participantId),
          ) -> Seq(
            show"Received unexpected $MalformedMediatorRequestMessage for ${malformedMediatorRequestResult.message.requestId}"
          ),
        )

        // sequentially process the test cases so that the log messages don't interfere
        MonadUtil
          .sequentialTraverse_(badBatches.zipWithIndex) { case ((batch, alarms), index) =>
            val initRc = index.toLong
            val sut = mk(initRc = initRc)
            val sc = index.toLong
            val ts = CantonTimestamp.ofEpochSecond(index.toLong)
            withClueF(s"at batch $index") {
              loggerFactory.assertLogsUnordered(
                handle(sut, mkDeliver(batch, sc, ts)) {
                  checkProcessRequest(processor(sut), ts, initRc, sc)
                  checkTickIdentityProcessor(sut, sc, ts)
                  checkTickRequestTracker(sut, sc, ts)
                  // do tick the request counter
                  sut.requestCounterAllocator.peek shouldBe initRc + 1
                },
                alarms.map(alarm =>
                  (logEntry: LogEntry) => logEntry.errorMessage should include(alarm)
                ): _*
              )
            }
          }
          .map(_ => succeed)
      }

      "be skipped if they precede the clean replay starting point" in {
        val initRc = 2L
        val initSc = 50L
        val sut = mk(initRc = initRc, cleanReplaySequencerCounter = initSc)
        val sc = initSc - 1L
        val ts = CantonTimestamp.ofEpochSecond(2)
        val rootHashMessage =
          RootHashMessage(rootHash(1), domainId, viewType, SerializedRootHashMessagePayload.empty)
        val event =
          mkDeliver(
            Batch.of[ProtocolMessage](
              view -> Recipients.cc(participantId),
              rootHashMessage -> Recipients.cc(participantId, mediatorId),
            ),
            sc,
            ts,
          )
        handle(sut, event) {
          checkNotProcessRequest(processor(sut))
          checkTickIdentityProcessor(sut, sc, ts)
          checkTickRequestTracker(sut, sc, ts)
          sut.requestCounterAllocator.peek shouldBe initRc
        }
      }
    }

    "Test requests" should {
      behave like request(encryptedTestViewMessage, _.testProcessor, encryptedOtherTestViewMessage)
    }

    "Other test requests" should {
      behave like request(
        encryptedOtherTestViewMessage,
        _.otherTestProcessor,
        encryptedTestViewMessage,
      )
    }

    "Mediator results" should {
      "be sent to the right processor" in {
        def check(result: ProtocolMessage, processor: ProcessorOfFixture): Future[Assertion] = {
          val sut = mk()
          val batch = Batch.of(result -> Recipients.cc(participantId))
          handle(sut, mkDeliver(batch)) {
            checkTickIdentityProcessor(sut)
            checkTickRequestTracker(sut)
            checkProcessResult(processor(sut))
          }
        }

        for {
          _ <- check(testMediatorResult, _.testProcessor)
          _ <- check(otherTestMediatorResult, _.otherTestProcessor)
        } yield succeed
      }

      "come one at a time" in {
        val batch = Batch.of[ProtocolMessage](
          testMediatorResult -> Recipients.cc(participantId),
          otherTestMediatorResult -> Recipients.cc(participantId),
        )
        val sut = mk()
        loggerFactory.assertLogsUnordered(
          handle(sut, mkDeliver(batch)) {
            checkTicks(sut)
          },
          _.errorMessage should include(
            show"Received unexpected ${RequestKind(TestViewType)} for $requestId"
          ),
          _.errorMessage should include(
            show"Received unexpected ${RequestKind(OtherTestViewType)} for $requestId"
          ),
        )
      }

      "malformed mediator requests be sent to the right processor" in {
        def malformed(viewType: ViewType, processor: ProcessorOfFixture): Future[Assertion] = {
          val reject = MediatorReject.Topology.InvalidRootHashMessages.Reject("")
          val result =
            SignedProtocolMessage(
              MalformedMediatorRequestResult(
                RequestId(CantonTimestamp.MinValue),
                domainId,
                viewType,
                reject,
              ),
              dummySignature,
            )
          val batch = Batch.of(result -> Recipients.cc(participantId))
          val sut = mk()
          withClueF(show"for $viewType") {
            handle(sut, mkDeliver(batch)) {
              verify(processor(sut)).processMalformedMediatorRequestResult(
                isEq(CantonTimestamp.Epoch),
                isEq(0L),
                any[SignedContent[Deliver[DefaultOpenEnvelope]]],
              )(anyTraceContext)
              checkTickIdentityProcessor(sut)
              checkTickRequestTracker(sut)
            }
          }
        }

        for {
          _ <- malformed(TestViewType, _.testProcessor)
          _ <- malformed(OtherTestViewType, _.otherTestProcessor)
        } yield succeed
      }
    }

    "receipts and deliver errors" should {
      "trigger in-flight submission tracking" in {
        val sut = mk()
        val messageId1 = MessageId.fromUuid(new UUID(0, 1))
        val messageId2 = MessageId.fromUuid(new UUID(0, 2))
        val messageId3 = MessageId.fromUuid(new UUID(0, 3))

        val dummyBatch = Batch.of(malformedMediatorRequestResult -> Recipients.cc(participantId))
        val deliver1 = mkDeliver(dummyBatch, 0L, CantonTimestamp.Epoch, messageId1.some)
        val deliver2 = mkDeliver(dummyBatch, 1L, CantonTimestamp.ofEpochSecond(1), messageId2.some)
        val deliver3 = mkDeliver(dummyBatch, 2L, CantonTimestamp.ofEpochSecond(2))
        val deliverError4 = DeliverError.create(
          3L,
          CantonTimestamp.ofEpochSecond(3),
          domainId,
          messageId3,
          DeliverErrorReason.BatchInvalid("invalid batch"),
        )

        val sequencedEvents = Seq(deliver1, deliver2, deliver3, deliverError4).map(event =>
          OrdinarySequencedEvent(signEvent(event))(traceContext)
        )
        for {
          _ <- sut.messageDispatcher
            .handleAll(Traced(sequencedEvents))
            .onShutdown(fail("Encountered shutdown while handling batch of sequenced events"))
        } yield {
          checkObserveSequencing(
            sut,
            Map(
              messageId1 -> SequencedSubmission(0L, CantonTimestamp.Epoch),
              messageId2 -> SequencedSubmission(1L, CantonTimestamp.ofEpochSecond(1)),
            ),
          )
          checkObserveDeliverError(sut, deliverError4)
        }
      }
    }
  }
}

object MessageDispatcherTest {

  // The message dispatcher only sees encrypted view trees, so there's no point in implementing the methods.
  sealed trait MockViewTree extends ViewTree with HasCryptographicEvidence

  trait AbstractTestViewType extends ViewType {
    override type View = MockViewTree

    override def toProtoEnum: protocolv0.ViewType =
      throw new UnsupportedOperationException(
        s"${this.getClass.getSimpleName} cannot be serialized"
      )
  }

  case object TestViewType extends AbstractTestViewType
  type TestViewType = TestViewType.type

  case object OtherTestViewType extends AbstractTestViewType
  type OtherTestViewType = OtherTestViewType.type

  case object UnknownTestViewType extends AbstractTestViewType
  type UnknownTestViewType = OtherTestViewType.type

  case class TestRegularMediatorResult(
      override val viewType: ViewType,
      override val domainId: DomainId,
      override val verdict: Verdict,
      override val requestId: RequestId,
  ) extends RegularMediatorResult {
    override def toProtoSomeSignedProtocolMessage
        : protocolv0.SignedProtocolMessage.SomeSignedProtocolMessage =
      throw new UnsupportedOperationException(
        s"${this.getClass.getSimpleName} cannot be serialized"
      )
    override def hashPurpose: HashPurpose = HashPurposeTest.testHashPurpose
    override def deserializedFrom: Option[ByteString] = None
    override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
      ByteString.EMPTY
  }
}

class DefaultMessageDispatcherTest extends AsyncWordSpec with BaseTest with MessageDispatcherTest {

  "DefaultMessageDispatcher" should {
    behave like messageDispatcher(MessageDispatcher.DefaultFactory.create)
  }
}
