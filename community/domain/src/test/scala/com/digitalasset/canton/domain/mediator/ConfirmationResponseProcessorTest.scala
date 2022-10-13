// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.domain.mediator.store.{
  InMemoryFinalizedResponseStore,
  InMemoryMediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.Verdict.{Approve, ParticipantReject}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.{DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil.{sequentialTraverse, sequentialTraverse_}
import com.digitalasset.canton.util.ShowUtil.*
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.{eq as eqMatch}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn
import scala.concurrent.Future

@nowarn("msg=match may not be exhaustive")
class ConfirmationResponseProcessorTest extends AsyncWordSpec with BaseTest {
  lazy val domainId: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::test"))

  private val factory = new ExampleTransactionFactory()(domainId = domainId)
  val mediatorId: MediatorId = factory.mediatorId
  private val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
  private val participant: ParticipantId = ExampleTransactionFactory.submitterParticipant

  private val notSignificantCounter: SequencerCounter = SequencerCounter(0)

  private val initialDomainParameters = TestDomainParameters.defaultDynamic

  val participantResponseTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMillis(100L)

  "ConfirmationResponseProcessor" should {
    val submitter = ExampleTransactionFactory.submitter
    val signatory = ExampleTransactionFactory.signatory
    val topology = TestingTopology(
      Set(domainId),
      Map(
        submitter -> Map(
          participant -> ParticipantAttributes(
            ParticipantPermission.Confirmation,
            TrustLevel.Ordinary,
          )
        ),
        signatory ->
          Map(
            participant -> ParticipantAttributes(
              ParticipantPermission.Confirmation,
              TrustLevel.Ordinary,
            )
          ),
      ),
      Set(mediatorId),
    )
    val identityFactory = TestingIdentityFactory(
      topology,
      loggerFactory,
      dynamicDomainParameters =
        initialDomainParameters.tryUpdate(participantResponseTimeout = participantResponseTimeout),
    )

    val domainSyncCryptoApi: DomainSyncCryptoClient =
      identityFactory.forOwnerAndDomain(mediatorId, domainId)

    val requestIdTs = CantonTimestamp.Epoch
    val requestId = RequestId(requestIdTs)
    val decisionTime = requestIdTs.plusSeconds(120)

    class Fixture(syncCryptoApi: DomainSyncCryptoClient = domainSyncCryptoApi) {
      val verdictSender: TestVerdictSender = new TestVerdictSender
      val timeTracker: DomainTimeTracker = mock[DomainTimeTracker]
      val mediatorState =
        new MediatorState(
          new InMemoryFinalizedResponseStore(loggerFactory),
          new InMemoryMediatorDeduplicationStore(loggerFactory),
          DomainTestMetrics.mediator,
          timeouts,
          loggerFactory,
        )
      val processor = new ConfirmationResponseProcessor(
        domainId,
        mediatorId,
        verdictSender,
        syncCryptoApi,
        timeTracker,
        mediatorState,
        testedProtocolVersion,
        loggerFactory,
      )
    }

    object Fixture {
      def apply() = new Fixture()
    }

    // Create a topology with several participants so that we can have several root hash messages or Malformed messages
    lazy val participant1 = participant
    lazy val participant2 = ExampleTransactionFactory.signatoryParticipant
    lazy val participant3 = ParticipantId("participant3")
    lazy val observer = ExampleTransactionFactory.observer

    val view = factory.MultipleRootsAndViewNestings.view0

    lazy val topology2 = TestingTopology(
      Set(domainId),
      Map(
        submitter -> Map(
          participant1 -> ParticipantAttributes(
            ParticipantPermission.Confirmation,
            TrustLevel.Ordinary,
          )
        ),
        signatory -> Map(
          participant2 -> ParticipantAttributes(
            ParticipantPermission.Confirmation,
            TrustLevel.Ordinary,
          )
        ),
        observer -> Map(
          participant3 -> ParticipantAttributes(
            ParticipantPermission.Confirmation,
            TrustLevel.Ordinary,
          )
        ),
      ),
      Set(mediatorId),
    )
    lazy val identityFactory2 =
      TestingIdentityFactory(topology2, loggerFactory, initialDomainParameters)
    lazy val domainSyncCryptoApi2: DomainSyncCryptoClient =
      identityFactory2.forOwnerAndDomain(SequencerId(domainId), domainId)

    def signedResponse(
        confirmers: Set[LfPartyId],
        view: TransactionView,
        verdict: LocalVerdict,
        requestId: RequestId,
    ): Future[SignedProtocolMessage[MediatorResponse]] = {
      val response: MediatorResponse = MediatorResponse.tryCreate(
        requestId,
        participant,
        Some(view.viewHash),
        verdict,
        Some(fullInformeeTree.transactionId.toRootHash),
        confirmers,
        factory.domainId,
        testedProtocolVersion,
      )
      val participantCrypto = identityFactory.forOwner(participant)
      SignedProtocolMessage.tryCreate(
        response,
        participantCrypto.tryForDomain(domainId).currentSnapshotApproximation,
        participantCrypto.pureCrypto,
        testedProtocolVersion,
      )
    }

    // TODO(i10210): We probably do not want this appearing in the context of the logged error
    lazy val checkTestedProtocolVersion: Map[String, String] => Assertion =
      _ should contain(
        "representativeProtocolVersion" -> Verdict
          .protocolVersionRepresentativeFor(
            testedProtocolVersion
          )
          .toString
      )
    lazy val shouldBeViewThresholdBelowMinimumAlarm: LogEntry => Assertion =
      _.shouldBeCantonError(
        MediatorError.MalformedMessage,
        _ should fullyMatch regex raw"Rejected transaction as a view has threshold below the confirmation policy's minimum threshold. viewHash=ViewHash\(SHA-256:[0-9a-f]*...\), threshold=0",
        checkTestedProtocolVersion,
      )

    "timestamp of mediator request is propagated" in {
      val sut = Fixture()
      val testMediatorRequest = new InformeeMessage(fullInformeeTree)(testedProtocolVersion) {
        override def informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] = {
          super.informeesAndThresholdByView map { case (key, (informee, _)) =>
            (key, (informee, NonNegativeInt.zero))
          }
        }

        override def rootHash: Option[RootHash] = None // don't require root hash messages
      }
      val requestTimestamp = CantonTimestamp.Epoch.plusSeconds(120)
      for {
        _ <- loggerFactory.assertLogs(
          sut.processor.processRequest(
            RequestId(requestTimestamp),
            SequencerCounter(0),
            requestTimestamp.plusSeconds(60),
            requestTimestamp.plusSeconds(120),
            testMediatorRequest,
            List.empty,
          ),
          shouldBeViewThresholdBelowMinimumAlarm,
        )

      } yield {
        val sentResult = sut.verdictSender.sentResults.loneElement
        sentResult.requestId.unwrap shouldBe requestTimestamp
      }
    }

    "request timestamp is propagated to mediator result when response aggregation is performed" should {
      // Send mediator request
      val informeeMessage = new InformeeMessage(fullInformeeTree)(testedProtocolVersion) {
        override def informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] = {
          val top = super.informeesAndThresholdByView
          top map { case (key, (informee, _)) =>
            if (informee == Set(submitter))
              (key, (informee, NonNegativeInt.one))
            else (key, (informee, NonNegativeInt.zero))
          }
        }

        override def rootHash: Option[RootHash] = None // don't require root hash messages
      }
      val requestTimestamp = CantonTimestamp.Epoch.plusSeconds(12345)
      val reqId = RequestId(requestTimestamp)
      val mockSnapshot = mock[DomainSnapshotSyncCryptoApi]
      val mockSignature = SymbolicCrypto.emptySignature

      val mockTopologySnapshot = mock[TopologySnapshot]
      when(mockTopologySnapshot.findDynamicDomainParametersOrDefault(anyBoolean)(any[TraceContext]))
        .thenReturn(Future.successful(initialDomainParameters))

      when(mockSnapshot.verifySignature(any[Hash], any[KeyOwner], any[Signature]))
        .thenReturn(EitherT.rightT(()))
      when(mockSnapshot.sign(any[Hash])(anyTraceContext))
        .thenReturn(EitherT.rightT[Future, SyncCryptoError](mockSignature))
      when(mockSnapshot.ipsSnapshot).thenReturn(mockTopologySnapshot)

      val mockedSnapshotCrypto = new DomainSyncCryptoClient(
        domainSyncCryptoApi.owner,
        domainSyncCryptoApi.domainId,
        domainSyncCryptoApi.ips,
        domainSyncCryptoApi.crypto,
        CachingConfigs.testing,
        timeouts,
        FutureSupervisor.Noop,
        loggerFactory,
      ) {
        override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
            traceContext: TraceContext
        ): Future[DomainSnapshotSyncCryptoApi] =
          if (timestamp == requestTimestamp) Future.successful(mockSnapshot)
          else super.snapshot(timestamp)
      }

      val responseF =
        signedResponse(
          Set(submitter),
          view,
          LocalApprove()(testedProtocolVersion),
          reqId,
        )

      def handleEvents(sut: ConfirmationResponseProcessor): Future[Unit] =
        for {
          response <- responseF
          _ <- loggerFactory.assertLogs(
            sut.processRequest(
              reqId,
              notSignificantCounter,
              requestTimestamp.plusSeconds(60),
              requestTimestamp.plusSeconds(120),
              informeeMessage,
              List.empty,
            ),
            shouldBeViewThresholdBelowMinimumAlarm,
          )
          _ <- sut.processResponse(
            CantonTimestamp.Epoch,
            notSignificantCounter,
            requestTimestamp.plusSeconds(60),
            requestTimestamp.plusSeconds(120),
            response,
          )
        } yield ()

      "verifies the response signature with the timestamp from the request" in {
        val sut = new Fixture(mockedSnapshotCrypto)
        for {
          response <- responseF
          _ <- handleEvents(sut.processor)
          _ = verify(mockSnapshot, timeout(1000)).verifySignature(
            any[Hash],
            any[KeyOwner],
            eqMatch(response.signature),
          )
        } yield succeed
      }

      "mediator response contains timestamp from the request" in {
        val sut = new Fixture(mockedSnapshotCrypto)
        for {
          _ <- handleEvents(sut.processor)
        } yield {
          val sentResult = sut.verdictSender.sentResults.loneElement
          sentResult.requestId.unwrap shouldBe requestTimestamp
        }
      }
    }

    "inactive mediator ignores requests" in {
      val otherMediatorId = MediatorId(UniqueIdentifier.tryCreate("mediator", "other"))
      val topology3 = topology.copy(mediators = Set(otherMediatorId))
      val identityFactory3 = TestingIdentityFactory(
        topology3,
        loggerFactory,
        dynamicDomainParameters = initialDomainParameters,
      )
      val domainSyncCryptoApi3 = identityFactory3.forOwnerAndDomain(mediatorId, domainId)
      val sut = new Fixture(domainSyncCryptoApi3)

      val mediatorRequest = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        mediatorRequest.rootHash.value,
        domainId,
        testedProtocolVersion,
        mediatorRequest.viewType,
        SerializedRootHashMessagePayload.empty,
      )

      val sc = SequencerCounter(100)
      val ts = CantonTimestamp.ofEpochSecond(sc.v)
      val requestId = RequestId(ts)
      for {
        _ <- sut.processor.processRequest(
          RequestId(ts),
          notSignificantCounter,
          ts.plusSeconds(60),
          ts.plusSeconds(120),
          mediatorRequest,
          List(
            OpenEnvelope(
              rootHashMessage,
              Recipients.cc(mediatorId, participant),
              testedProtocolVersion,
            )
          ),
        )
        _ = sut.verdictSender.sentResults shouldBe empty

        // If it nevertheless gets a response, it will complain about the request not being known
        response <- signedResponse(
          Set(submitter),
          view,
          LocalApprove()(testedProtocolVersion),
          requestId,
        )
        _ <- loggerFactory.assertLogs(
          sut.processor.processResponse(
            ts.immediateSuccessor,
            sc + 1L,
            ts.plusSeconds(60),
            ts.plusSeconds(120),
            response,
          ), {
            _.shouldBeCantonError(
              MediatorError.MalformedMessage,
              _ shouldBe show"Unknown request $requestId: received mediator response by $participant for view hash ${view.viewHash} for root hash ${mediatorRequest.rootHash.value}",
              checkTestedProtocolVersion,
            )
          },
        )
      } yield {
        succeed
      }
    }

    "accept root hash messages" in {
      val sut = new Fixture(domainSyncCryptoApi2)
      val correctRootHash = RootHash(TestHash.digest("root-hash"))
      // Create a custom informee message with several recipient participants
      val informeeMessage = new InformeeMessage(fullInformeeTree)(testedProtocolVersion) {
        override val informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] = {
          val submitterI = Informee.tryCreate(submitter, 1)
          val signatoryI = Informee.tryCreate(signatory, 1)
          val observerI = Informee.tryCreate(observer, 1)
          Map(
            view.viewHash -> (Set(
              submitterI,
              signatoryI,
              observerI,
            ) -> NonNegativeInt.one)
          )
        }

        override def rootHash: Option[RootHash] = correctRootHash.some
      }
      val allParticipants = NonEmpty(Seq, participant1, participant2, participant3)

      val correctViewType = informeeMessage.viewType
      val rootHashMessage =
        RootHashMessage(
          correctRootHash,
          domainId,
          testedProtocolVersion,
          correctViewType,
          SerializedRootHashMessagePayload.empty,
        )

      val tests = List[(String, Seq[Recipients])](
        "individual messages" -> allParticipants.map(p => Recipients.cc(mediatorId, p)),
        "just one message" -> Seq(
          Recipients.groups(allParticipants.map(p => NonEmpty.mk(Set, p, mediatorId)))
        ),
        "mixed" -> Seq(
          Recipients.groups(
            NonEmpty.mk(
              Seq,
              NonEmpty.mk(Set, participant1, mediatorId),
              NonEmpty.mk(Set, participant2, mediatorId),
            )
          ),
          Recipients.cc(participant3, mediatorId),
        ),
      )

      sequentialTraverse_(tests.zipWithIndex) { case ((_testName, recipients), i) =>
        withClueF("testname") {
          val rootHashMessages =
            recipients.map(r => OpenEnvelope(rootHashMessage, r, testedProtocolVersion))
          val ts = CantonTimestamp.ofEpochSecond(i.toLong)
          sut.processor.processRequest(
            RequestId(ts),
            notSignificantCounter,
            ts.plusSeconds(60),
            ts.plusSeconds(120),
            informeeMessage,
            rootHashMessages,
          )
        }
      }.map(_ => succeed)
    }

    "send rejections when receiving wrong root hash messages" in {
      val sut = Fixture()

      val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
      val rootHash = informeeMessage.rootHash.value
      val wrongRootHash =
        RootHash(
          domainSyncCryptoApi.pureCrypto.digest(HashPurposeTest.testHashPurpose, ByteString.EMPTY)
        )
      val correctViewType = informeeMessage.viewType
      val wrongViewType = TransferInViewType
      require(correctViewType != wrongViewType)
      val correctRootHashMessage =
        RootHashMessage(
          rootHash,
          domainId,
          testedProtocolVersion,
          correctViewType,
          SerializedRootHashMessagePayload.empty,
        )
      val wrongRootHashMessage = correctRootHashMessage.copy(rootHash = wrongRootHash)
      val wrongViewTypeRHM = correctRootHashMessage.copy(viewType = wrongViewType)
      val otherMember = DomainTopologyManagerId(domainId)

      def exampleForRequest(
          request: MediatorRequest,
          rootHashMessages: (RootHashMessage[SerializedRootHashMessagePayload], Recipients)*
      ): (MediatorRequest, List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]]) =
        (
          request,
          rootHashMessages.map { case (rootHashMessage, recipients) =>
            OpenEnvelope(rootHashMessage, recipients, testedProtocolVersion)
          }.toList,
        )
      def example(
          rootHashMessages: (RootHashMessage[SerializedRootHashMessagePayload], Recipients)*
      ) =
        exampleForRequest(informeeMessage, rootHashMessages: _*)

      val batchWithoutRootHashMessages = example()
      val batchWithWrongRootHashMessage =
        example(wrongRootHashMessage -> Recipients.cc(mediatorId, participant))
      val batchWithWrongViewType =
        example(wrongViewTypeRHM -> Recipients.cc(mediatorId, participant))
      val batchWithDifferentViewTypes =
        example(
          correctRootHashMessage -> Recipients.cc(mediatorId, participant),
          wrongViewTypeRHM -> Recipients.cc(mediatorId, otherMember),
        )
      val batchWithRootHashMessageWithTooManyRecipients =
        example(correctRootHashMessage -> Recipients.cc(mediatorId, participant, otherMember))
      val batchWithRootHashMessageWithTooFewRecipients =
        example(correctRootHashMessage -> Recipients.cc(mediatorId))
      val batchWithRepeatedRootHashMessage = example(
        correctRootHashMessage -> Recipients.cc(mediatorId, participant),
        correctRootHashMessage -> Recipients.cc(mediatorId, participant),
      )
      val batchWithDivergingRootHashMessages = example(
        correctRootHashMessage -> Recipients.cc(mediatorId, participant),
        wrongRootHashMessage -> Recipients.cc(mediatorId, participant),
      )
      val batchWithSuperfluousRootHashMessage = example(
        correctRootHashMessage -> Recipients.cc(mediatorId, participant),
        correctRootHashMessage -> Recipients.cc(mediatorId, otherMember),
      )
      val batchWithDifferentPayloads = example(
        correctRootHashMessage -> Recipients.cc(mediatorId, participant),
        correctRootHashMessage.copy(
          payload = SerializedRootHashMessagePayload(ByteString.copyFromUtf8("other paylroosoad"))
        ) -> Recipients
          .cc(mediatorId, otherMember),
      )
      val requestWithoutExpectedRootHashMessage = exampleForRequest(
        new InformeeMessage(fullInformeeTree)(testedProtocolVersion) {
          override def rootHash: Option[RootHash] = None
        },
        correctRootHashMessage -> Recipients.cc(mediatorId, participant),
      )

      // format: off
      val testCases
        : Seq[(((MediatorRequest, List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]]), String),
               List[(Set[Member], ViewType)])] = List(

        (batchWithoutRootHashMessages  -> show"Missing root hash message for informee participants: $participant") -> List.empty,

        (batchWithWrongRootHashMessage -> show"Wrong root hashes: $wrongRootHash") ->
          List(Set[Member](participant) -> correctViewType),

        (batchWithWrongViewType -> show"View types in root hash messages differ from expected view type $correctViewType: $wrongViewType") ->
          List(Set[Member](participant) -> wrongViewType),

        (batchWithDifferentViewTypes -> show"View types in root hash messages differ from expected view type $correctViewType: $wrongViewType") ->
          List(Set[Member](participant) -> correctViewType, Set[Member](otherMember) -> wrongViewType),

        (batchWithRootHashMessageWithTooManyRecipients ->
          show"Root hash messages with wrong recipients tree: RecipientsTree(recipient group = Seq($mediatorId, $participant, $otherMember), children = Seq())") ->
          List(Set[Member](participant, otherMember) -> correctViewType),

        (batchWithRootHashMessageWithTooFewRecipients -> show"Root hash messages with wrong recipients tree: RecipientsTree(recipient group = $mediatorId, children = Seq())") -> List.empty,

        (batchWithRepeatedRootHashMessage             -> show"Several root hash messages for members: $participant") ->
          List(Set[Member](participant) -> correctViewType),

        (batchWithDivergingRootHashMessages -> show"Several root hash messages for members: $participant") ->
          List(Set[Member](participant) -> correctViewType),

        (batchWithSuperfluousRootHashMessage -> show"Superfluous root hash message for members: $otherMember") ->
          List(Set[Member](participant, otherMember) -> correctViewType),

        (batchWithDifferentPayloads -> show"Different payloads in root hash messages: Sizes ") ->
          List(Set[Member](participant, otherMember) -> correctViewType),

        (requestWithoutExpectedRootHashMessage -> show"No root hash messages expected, but received for members: $participant") ->
          List(Set[Member](participant) -> correctViewType)
      )
      // format: on

      sequentialTraverse_(testCases.zipWithIndex) {
        case ((((req, rootHashMessages), msg), _resultRecipientsAndViewTypes), sc) =>
          val ts = CantonTimestamp.ofEpochSecond(sc.toLong)
          withClueF(s"at test case #$sc") {
            loggerFactory.assertLogs(
              // This will not send a result message because there are no root hash messages in the batch.
              sut.processor.processRequest(
                RequestId(ts),
                notSignificantCounter,
                ts.plusSeconds(60),
                ts.plusSeconds(120),
                req,
                rootHashMessages,
              ),
              _.shouldBeCantonError(
                MediatorError.InvalidMessage,
                _ should startWith(s"Rejected transaction due to invalid root hash messages: $msg"),
                checkTestedProtocolVersion,
              ),
            )
          }
      }.map { _ =>
        val expectedResultRecipientsAndViewTypes: List[(String, List[(Set[Member], ViewType)])] =
          testCases.map(test => (test._1._2, test._2)).filter(test => test._2.nonEmpty).toList
        val resultBatches = sut.verdictSender.sentResults.toList.mapFilter(_.batch)
        resultBatches.size shouldBe expectedResultRecipientsAndViewTypes.size
        forAll(resultBatches.zip(expectedResultRecipientsAndViewTypes)) {
          case (resultBatch, expectedRecipientsAndViewTypes) =>
            val results = resultBatch.envelopes.map { envelope =>
              envelope.recipients -> ProtocolMessage
                .toKind[SignedProtocolMessage[MalformedMediatorRequestResult]](envelope)
                .map(_.message.viewType)
            }
            val ungroupedResults = results.flatMap { case (Recipients(trees), vt) =>
              trees.map(_ -> vt).toList
            }.toSet

            val expectedResults = expectedRecipientsAndViewTypes._2.toSet
            val expected = expectedResults.flatMap { case (recipients, viewType) =>
              recipients.map { member: Member =>
                RecipientsTree.leaf(NonEmpty(Set, member)) -> Some(viewType)
              }
            }
            withClue(s"Test case: ${expectedRecipientsAndViewTypes._1}") {
              ungroupedResults.size shouldBe expected.size
              ungroupedResults shouldBe expected
            }
        }
      }
    }

    "reject when declared mediator is wrong" in {
      val sut = Fixture()

      val otherMediatorId = MediatorId(UniqueIdentifier.tryCreate("mediator", "other"))
      val factoryOtherMediatorId =
        new ExampleTransactionFactory()(domainId = domainId, mediatorId = otherMediatorId)
      val fullInformeeTreeOther =
        factoryOtherMediatorId.MultipleRootsAndViewNestings.fullInformeeTree
      val mediatorRequest = InformeeMessage(fullInformeeTreeOther)(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        mediatorRequest.rootHash.value,
        domainId,
        testedProtocolVersion,
        mediatorRequest.viewType,
        SerializedRootHashMessagePayload.empty,
      )

      val sc = 10L
      val ts = CantonTimestamp.ofEpochSecond(sc)
      for {
        _ <- loggerFactory.assertLogs(
          sut.processor.processRequest(
            RequestId(ts),
            notSignificantCounter,
            ts.plusSeconds(60),
            ts.plusSeconds(120),
            mediatorRequest,
            List(
              OpenEnvelope(
                rootHashMessage,
                Recipients.cc(mediatorId, participant),
                testedProtocolVersion,
              )
            ),
          ),
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ shouldBe show"The declared mediator in the MediatorRequest ($otherMediatorId) is not the mediator that received the request ($mediatorId).",
            checkTestedProtocolVersion,
          ),
        )
      } yield succeed
    }

    "correct series of mediator events" in {
      val sut = Fixture()
      val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        domainId,
        testedProtocolVersion,
        ViewType.TransactionViewType,
        SerializedRootHashMessagePayload.empty,
      )

      for {
        _ <- sut.processor.processRequest(
          requestId,
          notSignificantCounter,
          requestIdTs.plusSeconds(60),
          decisionTime,
          informeeMessage,
          List(
            OpenEnvelope(
              rootHashMessage,
              Recipients.cc(mediatorId, participant),
              testedProtocolVersion,
            )
          ),
        )
        // should record the request
        requestState <- sut.mediatorState.fetch(requestId).value.map(_.value)
        _ = {
          val responseAggregation =
            ResponseAggregation(requestId, informeeMessage, testedProtocolVersion)(loggerFactory)
          assert(requestState === responseAggregation)
        }
        // receiving the confirmation response
        ts1 = CantonTimestamp.Epoch.plusMillis(1L)
        approvals: Seq[SignedProtocolMessage[MediatorResponse]] <- sequentialTraverse(
          List(
            view,
            factory.MultipleRootsAndViewNestings.view1,
            factory.MultipleRootsAndViewNestings.view11,
            factory.MultipleRootsAndViewNestings.view110,
          )
        )(view => {
          signedResponse(Set(submitter), view, LocalApprove()(testedProtocolVersion), requestId)
        })
        _ <- sequentialTraverse_(approvals)(
          sut.processor.processResponse(
            ts1,
            notSignificantCounter,
            ts1.plusSeconds(60),
            ts1.plusSeconds(120),
            _,
          )
        )
        // records the request
        updatedState <- sut.mediatorState.fetch(requestId).value
        _ = {
          updatedState should matchPattern {
            case Some(
                  ResponseAggregation(`requestId`, `informeeMessage`, `ts1`, Right(_states))
                ) =>
          }
          val ResponseAggregation(`requestId`, `informeeMessage`, `ts1`, Right(states)) =
            updatedState.value
          assert(
            states === Map(
              ViewHash.fromRootHash(view.rootHash) ->
                ResponseAggregation.ViewState(Set.empty, 0, List()),
              ViewHash.fromRootHash(factory.MultipleRootsAndViewNestings.view1.rootHash) ->
                ResponseAggregation.ViewState(Set(ConfirmingParty(signatory, 1)), 1, List()),
              ViewHash.fromRootHash(factory.MultipleRootsAndViewNestings.view10.rootHash) ->
                ResponseAggregation.ViewState(Set(ConfirmingParty(signatory, 1)), 1, List()),
              ViewHash.fromRootHash(factory.MultipleRootsAndViewNestings.view11.rootHash) ->
                ResponseAggregation.ViewState(Set(ConfirmingParty(signatory, 1)), 1, List()),
              ViewHash.fromRootHash(factory.MultipleRootsAndViewNestings.view110.rootHash) ->
                ResponseAggregation.ViewState(Set.empty, 0, List()),
            )
          )
        }
        // receiving the final confirmation response
        ts2 = CantonTimestamp.Epoch.plusMillis(2L)
        approvals <- sequentialTraverse(
          List(
            factory.MultipleRootsAndViewNestings.view1,
            factory.MultipleRootsAndViewNestings.view10,
            factory.MultipleRootsAndViewNestings.view11,
          )
        )(view =>
          signedResponse(Set(signatory), view, LocalApprove()(testedProtocolVersion), requestId)
        )
        _ <- sequentialTraverse_(approvals)(
          sut.processor.processResponse(
            ts2,
            notSignificantCounter,
            ts2.plusSeconds(60),
            ts2.plusSeconds(120),
            _,
          )
        )
        // records the request
        finalState <- sut.mediatorState.fetch(requestId).value
        _ = {
          inside(finalState) {
            case Some(ResponseAggregation(`requestId`, `informeeMessage`, `ts2`, state)) =>
              assert(state === Left(Approve(testedProtocolVersion)))
          }
        }
      } yield succeed
    }

    "receiving Malformed responses" in {
      // receiving an informee message
      val sut = new Fixture(domainSyncCryptoApi2)

      // Create a custom informee message with many quorums such that the first Malformed rejection doesn't finalize the request
      val informeeMessage = new InformeeMessage(fullInformeeTree)(testedProtocolVersion) {
        override val informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] = {
          val submitterI = Informee.tryCreate(submitter, 1)
          val signatoryI = Informee.tryCreate(signatory, 1)
          val observerI = Informee.tryCreate(observer, 1)
          Map(
            view.viewHash -> (Set(
              submitterI,
              signatoryI,
            ) -> NonNegativeInt.one),
            factory.MultipleRootsAndViewNestings.view1.viewHash -> (Set(
              submitterI,
              signatoryI,
              observerI,
            ) -> NonNegativeInt.one),
            factory.MultipleRootsAndViewNestings.view11.viewHash -> (Set(
              observerI,
              signatoryI,
            ) -> NonNegativeInt.one),
            factory.MultipleRootsAndViewNestings.view10.viewHash -> (Set(
              submitterI,
              signatoryI,
              observerI,
            ) -> NonNegativeInt.one),
          )
        }

        override def rootHash: Option[RootHash] = None // don't require root hash messages
      }
      val requestIdTs = CantonTimestamp.Epoch
      val requestId = RequestId(requestIdTs)

      val malformedMsg = "this is a test malformed response"
      def isMalformedWarn(participant: ParticipantId)(logEntry: LogEntry): Assertion = {
        logEntry.shouldBeCantonError(
          LocalReject.MalformedRejects.Payloads,
          _ shouldBe s"Rejected transaction due to malformed payload within views $malformedMsg",
          _ should contain("reportedBy" -> s"$participant"),
        )
      }

      def malformedResponse(
          participant: ParticipantId,
          viewHashO: Option[ViewHash] = None,
      ): Future[SignedProtocolMessage[MediatorResponse]] = {
        val response = MediatorResponse.tryCreate(
          requestId,
          participant,
          viewHashO,
          LocalReject.MalformedRejects.Payloads.Reject(malformedMsg)(testedProtocolVersion),
          Some(fullInformeeTree.transactionId.toRootHash),
          Set.empty,
          factory.domainId,
          testedProtocolVersion,
        )
        val participantCrypto = identityFactory2.forOwner(participant)
        SignedProtocolMessage.tryCreate(
          response,
          participantCrypto.tryForDomain(domainId).currentSnapshotApproximation,
          participantCrypto.pureCrypto,
          testedProtocolVersion,
        )
      }

      for {
        _ <- sut.processor.processRequest(
          requestId,
          notSignificantCounter,
          requestIdTs.plusSeconds(60),
          requestIdTs.plusSeconds(120),
          informeeMessage,
          List.empty,
        )

        // receiving a confirmation response
        ts1 = CantonTimestamp.Epoch.plusMillis(1L)
        malformed <- sequentialTraverse(
          List(
            malformedResponse(participant1),
            malformedResponse(
              participant3,
              Some(factory.MultipleRootsAndViewNestings.view1.viewHash),
            ),
            malformedResponse(
              participant3,
              Some(factory.MultipleRootsAndViewNestings.view11.viewHash),
            ),
            malformedResponse(participant2), // This should finalize the request
            malformedResponse(
              participant3,
              Some(factory.MultipleRootsAndViewNestings.view10.viewHash),
            ),
          )
        )(Predef.identity)

        // records the request
        _ <- loggerFactory.assertLogs(
          sequentialTraverse_(malformed)(
            sut.processor.processResponse(
              ts1,
              notSignificantCounter,
              ts1.plusSeconds(60),
              ts1.plusSeconds(120),
              _,
            )
          ),
          isMalformedWarn(participant1),
          isMalformedWarn(participant3),
          isMalformedWarn(participant3),
          isMalformedWarn(participant2),
        )

        finalState <- sut.mediatorState.fetch(requestId).value
        _ = inside(finalState) {
          case Some(
                ResponseAggregation(
                  _requestId,
                  _request,
                  _version,
                  Left(ParticipantReject(reasons)),
                )
              ) =>
            // TODO(#5337) These are only the rejections for the first view because this view happens to be finalized first.
            reasons.length shouldEqual 2
            reasons.foreach { case (party, reject) =>
              reject shouldBe LocalReject.MalformedRejects.Payloads.Reject(malformedMsg)(
                testedProtocolVersion
              )
              party should (contain(submitter) or contain(signatory))
            }
        }
      } yield succeed
    }

    "receiving late response" in {
      val sut = Fixture()
      val requestTs = CantonTimestamp.Epoch.plusMillis(1)
      val requestId = RequestId(requestTs)
      // response is just too late
      val participantResponseDeadline = requestIdTs.plus(participantResponseTimeout.unwrap)
      val responseTs = participantResponseDeadline.addMicros(1)

      val informeeMessage = InformeeMessage(fullInformeeTree)(testedProtocolVersion)
      val rootHashMessage = RootHashMessage(
        fullInformeeTree.transactionId.toRootHash,
        domainId,
        testedProtocolVersion,
        ViewType.TransactionViewType,
        SerializedRootHashMessagePayload.empty,
      )

      for {
        _ <- sut.processor.processRequest(
          requestId,
          notSignificantCounter,
          requestIdTs.plus(participantResponseTimeout.unwrap),
          requestIdTs.plusSeconds(120),
          informeeMessage,
          List(
            OpenEnvelope(
              rootHashMessage,
              Recipients.cc(mediatorId, participant),
              testedProtocolVersion,
            )
          ),
        )
        response <- signedResponse(
          Set(submitter),
          view,
          LocalApprove()(testedProtocolVersion),
          requestId,
        )
        _ <- loggerFactory.assertLogs(
          sut.processor
            .processResponse(
              responseTs,
              notSignificantCounter + 1,
              participantResponseDeadline,
              requestIdTs.plusSeconds(120),
              response,
            ),
          _.warningMessage shouldBe s"Response $responseTs is too late as request RequestId($requestTs) has already exceeded the participant response deadline [$participantResponseDeadline]",
        )
      } yield succeed
    }

    "timeout request that is not pending should not fail" in {
      // could happen if a timeout is scheduled but the request is previously finalized
      val sut = Fixture()
      val requestTs = CantonTimestamp.Epoch
      val requestId = RequestId(requestTs)
      val timeoutTs = requestTs.plusSeconds(20)

      // this request is not added to the pending state
      for {
        _ <- sut.processor.handleTimeout(requestId, timeoutTs, decisionTime)
      } yield succeed
    }
  }
}
