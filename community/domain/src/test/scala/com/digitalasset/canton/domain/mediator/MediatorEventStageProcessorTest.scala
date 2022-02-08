// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.{InMemoryFinalizedResponseStore, MediatorState}
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{
  DynamicDomainParameters,
  ExampleTransactionFactory,
  RequestId,
  TestDomainParameters,
}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.sequencing.{
  HandlerResult,
  OrdinaryProtocolEvent,
  RawProtocolEvent,
  SequencerTestUtils,
  UnsignedProtocolEventHandler,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  SequencerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.MonadUtil.sequentialTraverse_
import com.digitalasset.canton.{BaseTest, DomainId}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.Future

class MediatorEventStageProcessorTest extends AsyncWordSpec with BaseTest {
  private lazy val domainId = DefaultTestIdentities.domainId
  private lazy val mediatorId = DefaultTestIdentities.mediator
  private lazy val mediatorMetrics = DomainTestMetrics.mediator
  private lazy val participantResponseTimeout = NonNegativeFiniteDuration.ofSeconds(10)
  private lazy val factory = new ExampleTransactionFactory()(domainId = domainId)
  private lazy val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
  private lazy val alwaysReadyCheck = MediatorReadyCheck.alwaysReady(loggerFactory)
  private lazy val defaultDynamicDomainParameters: List[DynamicDomainParameters.WithValidity] =
    List(
      DynamicDomainParameters.WithValidity(
        CantonTimestamp.Epoch,
        None,
        TestDomainParameters.defaultDynamic.copy(participantResponseTimeout =
          participantResponseTimeout
        ),
      )
    )

  private class Env(
      dynamicDomainParameters: List[DynamicDomainParameters.WithValidity] =
        defaultDynamicDomainParameters
  ) {
    val identityClientEventHandler: UnsignedProtocolEventHandler = _ => HandlerResult.done
    val receivedEvents = mutable.Buffer[(RequestId, Seq[Traced[MediatorEvent]])]()

    val state = new MediatorState(
      new InMemoryFinalizedResponseStore(loggerFactory),
      mediatorMetrics,
      loggerFactory,
    )

    val domainSyncCryptoApi = new TestingIdentityFactory(
      TestingTopology(),
      loggerFactory,
      dynamicDomainParameters,
    ).forOwnerAndDomain(
      SequencerId(domainId),
      domainId,
    )

    val processor = new MediatorEventsProcessor(
      state,
      domainSyncCryptoApi,
      identityClientEventHandler,
      (requestId, events) => {
        receivedEvents.append((requestId, events))
        HandlerResult.done
      },
      alwaysReadyCheck,
      loggerFactory,
    )

    def deliver(timestamp: CantonTimestamp): Deliver[Nothing] =
      SequencerTestUtils.mockDeliver(0L, timestamp, domainId)

    def request(timestamp: CantonTimestamp): Deliver[DefaultOpenEnvelope] =
      Deliver.create[DefaultOpenEnvelope](
        0L,
        timestamp,
        domainId,
        None,
        Batch.of((InformeeMessage(fullInformeeTree), Recipients.cc(mediatorId))),
      )

    def handle(events: RawProtocolEvent*): FutureUnlessShutdown[Unit] =
      processor
        .handle(
          events.map(e =>
            OrdinarySequencedEvent(SignedContent(e, SymbolicCrypto.emptySignature, None))(
              traceContext
            )
          )
        )
        .flatMap(_.unwrap)

    def receivedEventsFor(requestId: RequestId): Seq[MediatorEvent] =
      receivedEvents.filter(_._1 == requestId).flatMap(_._2).map(_.value).to(Seq)

    def receivedEventsAt(ts: CantonTimestamp): Seq[MediatorEvent] = receivedEvents
      .flatMap { case (_, tracedEvents) =>
        tracedEvents.map(_.value)
      }
      .filter(_.timestamp == ts)
      .to(Seq)
  }

  "raise alarms when receiving bad sequencer event batches" in {
    val env = new Env()
    val informeeMessage = mock[InformeeMessage]
    when(informeeMessage.domainId).thenReturn(domainId)
    when(informeeMessage.rootHash).thenReturn(None)
    val signedConfirmationResponse = SignedProtocolMessage(mock[MediatorResponse], mock[Signature])
    when(signedConfirmationResponse.message.domainId).thenReturn(domainId)
    val informeeMessageWithWrongDomainId = mock[InformeeMessage]
    when(informeeMessageWithWrongDomainId.domainId)
      .thenReturn(DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong::domain")))
    val badBatches = List(
      (
        Batch.of[ProtocolMessage](
          informeeMessage -> RecipientsTest.testInstance,
          informeeMessage -> RecipientsTest.testInstance,
        ),
        List("Received more than one mediator request."),
      ),
      (
        Batch.of[ProtocolMessage](
          informeeMessage -> RecipientsTest.testInstance,
          signedConfirmationResponse -> RecipientsTest.testInstance,
        ),
        List("Received both mediator requests and mediator responses."),
      ),
      (
        Batch.of[ProtocolMessage](informeeMessageWithWrongDomainId -> RecipientsTest.testInstance),
        List("Received messages with wrong domain ids: List(wrong::domain)"),
      ),
    )

    sequentialTraverse_(badBatches) { case (batch, expectedMessages) =>
      loggerFactory.assertLogs(
        env.processor.handle(
          toTracedSignedEvents(Deliver.create(1L, CantonTimestamp.Epoch, domainId, None, batch))
        ),
        expectedMessages map { error => logEntry: LogEntry =>
          logEntry.errorMessage should include(error)
        }: _*
      )
    }.onShutdown(fail()).map(_ => succeed)
  }

  "timeouts" should {
    "be raised if a pending event timeouts" in {
      val pendingRequestTs = CantonTimestamp.Epoch.plusMillis(1)
      val pendingRequestId = RequestId(pendingRequestTs)
      val pendingRequest = responseAggregation(pendingRequestId)
      val env = new Env

      for {
        _ <- env.state.add(pendingRequest)
        deliverTs = pendingRequestTs.add(participantResponseTimeout.unwrap).addMicros(1)
        _ <- env.handle(env.deliver(deliverTs)).onShutdown(fail())
      } yield {
        env.receivedEventsFor(pendingRequestId).loneElement should matchPattern {
          case MediatorEvent.Timeout(_, `deliverTs`, `pendingRequestId`) =>
        }
      }
    }

    "be raised if a pending event timeouts, taking dynamic domain parameters into account" in {

      val domainParameters = List(
        DynamicDomainParameters.WithValidity(
          CantonTimestamp.Epoch,
          Some(CantonTimestamp.ofEpochSecond(5)),
          TestDomainParameters.defaultDynamic
            .copy(participantResponseTimeout = NonNegativeFiniteDuration.ofSeconds(4)),
        ),
        DynamicDomainParameters.WithValidity(
          CantonTimestamp.ofEpochSecond(5),
          None,
          TestDomainParameters.defaultDynamic
            .copy(participantResponseTimeout = NonNegativeFiniteDuration.ofSeconds(6)),
        ),
      )

      def getRequest(requestTs: CantonTimestamp) = {
        val pendingRequestId = RequestId(requestTs)
        responseAggregation(pendingRequestId)
      }

      val pendingRequest1Ts = CantonTimestamp.Epoch.plusSeconds(2)
      val pendingRequest1Id = RequestId(pendingRequest1Ts)
      val pendingRequest1 = getRequest(pendingRequest1Ts) // times out at (2 + 4) = 6

      val pendingRequest2Ts = CantonTimestamp.Epoch.plusSeconds(6)
      val pendingRequest2Id = RequestId(pendingRequest2Ts)

      /*
        The following times out at (6 + 6) = 12
        If dynamic domain parameters are not taken into account, it would be
        incorrectly marked as timed out at 11
       */
      val pendingRequest2 = getRequest(pendingRequest2Ts)

      val deliver1Ts = CantonTimestamp.Epoch.plusSeconds(11)
      val deliver2Ts = CantonTimestamp.Epoch.plusSeconds(12).addMicros(1)

      def test(
          deliverTs: CantonTimestamp,
          expectedEvents: Set[MediatorEvent],
      ): Future[Assertion] = {
        val env = new Env(domainParameters)

        for {
          _ <- env.state.add(pendingRequest1)
          _ <- env.state.add(pendingRequest2)
          _ <- env.handle(env.deliver(deliverTs)).onShutdown(fail())
        } yield env.receivedEventsAt(deliverTs).toSet shouldBe expectedEvents
      }

      for {
        assertion1 <- test(deliver1Ts, Set(MediatorEvent.Timeout(0, deliver1Ts, pendingRequest1Id)))
        assertion2 <- test(
          deliver2Ts,
          Set(
            MediatorEvent.Timeout(0, deliver2Ts, pendingRequest1Id),
            MediatorEvent.Timeout(0, deliver2Ts, pendingRequest2Id),
          ),
        )
      } yield (assertion1, assertion2) shouldBe (succeed, succeed)
    }

    "be raised for a request that is potentially created during the batch of events" in {
      val env = new Env
      val firstRequestTs = CantonTimestamp.Epoch.plusMillis(1)
      val requestId = RequestId(firstRequestTs)
      val timesOutAt = firstRequestTs.add(participantResponseTimeout.unwrap).addMicros(1)

      for {
        _ <- env.handle(env.request(firstRequestTs), env.deliver(timesOutAt)).onShutdown(fail())
      } yield {
        env.receivedEventsFor(requestId) should matchPattern {
          case Seq(
                MediatorEvent.Request(_, `firstRequestTs`, InformeeMessage(_), _),
                MediatorEvent.Timeout(_, `timesOutAt`, `requestId`),
              ) =>
        }
      }
    }
  }

  private def toTracedSignedEvents(
      delivers: Deliver[DefaultOpenEnvelope]*
  ): Seq[OrdinaryProtocolEvent] =
    delivers.map { deliver =>
      OrdinarySequencedEvent(SignedContent(deliver, SymbolicCrypto.emptySignature, None))(
        traceContext
      )
    }

  private def responseAggregation(requestId: RequestId): ResponseAggregation =
    ResponseAggregation(
      requestId,
      InformeeMessage(fullInformeeTree),
    )(loggerFactory)
}
