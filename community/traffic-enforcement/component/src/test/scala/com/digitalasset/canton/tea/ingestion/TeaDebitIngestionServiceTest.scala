// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.offset_checkpoint.{OffsetCheckpoint, SynchronizerTime}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.services.completions.CompletionServiceClient
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.tea.projection.{
  AccountId,
  DeltaEvent,
  EventSource,
  EventType,
  OffsetDeltaEvent,
  ProjectionEvent,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class TeaDebitIngestionServiceTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private implicit val actorSystem: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: Materializer = Materializer(actorSystem)

  override def afterAll(): Unit = {
    materializer.shutdown()
    Await.result(actorSystem.terminate(), 30.seconds)
    super.afterAll()
  }

  private val alice = AccountId.tryCreate("alice")
  private val bob = AccountId.tryCreate("bob")

  private val defaultRecordTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(1000)

  /** Builds the service under test on top of freshly created mocks of the two services. The mocks
    * are returned so that individual tests can refine their stubbing or verify the interactions.
    */
  private def serviceWith(
      completions: Seq[CompletionStreamResponse] = Seq.empty,
      ledgerEnd: Long = 0L,
      config: ProjectionConfig = ProjectionConfig(),
  ): (TeaDebitIngestionService, CompletionServiceClient, StateServiceClient) = {
    val completionServiceClient = mock[CompletionServiceClient]
    val stateServiceClient = mock[StateServiceClient]

    when(stateServiceClient.getLedgerEndOffset(any[Option[String]])(any[TraceContext]))
      .thenReturn(Future.successful(ledgerEnd))
    when(
      completionServiceClient
        .getCompletionsSource(any[Long], any[Seq[String]], any[Option[String]])(any[TraceContext])
    ).thenReturn(Source(completions.toList))

    val service =
      new TeaDebitIngestionService(
        completionServiceClient,
        stateServiceClient,
        config,
        loggerFactory,
      )
    (service, completionServiceClient, stateServiceClient)
  }

  private def completionResponse(
      actAs: Seq[String],
      offset: Long,
      paidTrafficCost: Long,
      recordTime: Option[CantonTimestamp] = Some(defaultRecordTime),
  ): CompletionStreamResponse =
    CompletionStreamResponse(
      CompletionStreamResponse.CompletionResponse.Completion(
        Completion.defaultInstance.copy(
          actAs = actAs,
          offset = offset,
          paidTrafficCost = paidTrafficCost,
          synchronizerTime = recordTime.map(rt =>
            SynchronizerTime(
              synchronizerId = "synchronizer::tea",
              recordTime = Some(rt.toProtoTimestamp),
            )
          ),
        )
      )
    )

  private val checkpointResponse: CompletionStreamResponse =
    CompletionStreamResponse(
      CompletionStreamResponse.CompletionResponse.OffsetCheckpoint(
        OffsetCheckpoint.defaultInstance.copy(offset = 99L)
      )
    )

  /** Drains the (finite) debit source and returns the decoded projection events. */
  private def runEvents(source: Source[Traced[ProjectionEvent], NotUsed]): Seq[ProjectionEvent] =
    source.runWith(Sink.seq).futureValue.map(_.value)

  "TeaDebitIngestionService" should {

    "use the configured initial offset" in {
      val (service, completionServiceClient, stateServiceClient) = serviceWith(
        ledgerEnd = 42L,
        config = ProjectionConfig(initialCompletionOffsetBeginExclusive = Some(31380)),
      )

      runEvents(service.grpcSource(beginOffset = None))

      verify(stateServiceClient, never).getLedgerEndOffset(any[Option[String]])(any[TraceContext])
      verify(completionServiceClient).getCompletionsSource(
        eqTo(31380L),
        eqTo(Seq.empty[String]),
        any[Option[String]],
      )(any[TraceContext])
    }

    "resolve the begin offset from the ledger end when none is provided" in {
      val (service, completionServiceClient, stateServiceClient) = serviceWith(ledgerEnd = 42L)

      runEvents(service.grpcSource(beginOffset = None))

      verify(stateServiceClient).getLedgerEndOffset(any[Option[String]])(any[TraceContext])
      verify(completionServiceClient).getCompletionsSource(
        eqTo(42L),
        eqTo(Seq.empty[String]),
        any[Option[String]],
      )(any[TraceContext])
    }

    "use the provided begin offset without querying the ledger end" in {
      val (service, completionServiceClient, stateServiceClient) = serviceWith()

      runEvents(service.grpcSource(beginOffset = Some(7L)))

      verify(stateServiceClient, never).getLedgerEndOffset(any[Option[String]])(any[TraceContext])
      verify(completionServiceClient).getCompletionsSource(
        eqTo(7L),
        eqTo(Seq.empty[String]),
        any[Option[String]],
      )(any[TraceContext])
    }

    "emit a negative (debit) delta event for a completion with a single act-as party" in {
      val (service, _, _) = serviceWith(
        completions = Seq(
          completionResponse(actAs = Seq("alice"), offset = 5L, paidTrafficCost = 3L)
        )
      )

      runEvents(service.grpcSource(beginOffset = Some(0L))) shouldBe Seq(
        ProjectionEvent(
          alice,
          OffsetDeltaEvent(
            DeltaEvent(-3L, defaultRecordTime, EventType.Usage, EventSource.LedgerAPI),
            offset = 5L,
          ),
        )
      )
    }

    "preserve order and drop checkpoints and multi-party completions" in {
      val (service, _, _) = serviceWith(
        completions = Seq(
          completionResponse(actAs = Seq("alice"), offset = 1L, paidTrafficCost = 2L),
          checkpointResponse,
          completionResponse(actAs = Seq("bob", "carol"), offset = 2L, paidTrafficCost = 7L),
          completionResponse(actAs = Seq("bob"), offset = 3L, paidTrafficCost = 4L),
        )
      )

      runEvents(service.grpcSource(beginOffset = Some(0L))) shouldBe Seq(
        ProjectionEvent(
          alice,
          OffsetDeltaEvent(
            DeltaEvent(-2L, defaultRecordTime, EventType.Usage, EventSource.LedgerAPI),
            1L,
          ),
        ),
        ProjectionEvent(
          bob,
          OffsetDeltaEvent(
            DeltaEvent(-4L, defaultRecordTime, EventType.Usage, EventSource.LedgerAPI),
            3L,
          ),
        ),
      )
    }

    "drop and warn about a completion without an act-as party" in {
      val (service, _, _) = serviceWith(
        completions = Seq(completionResponse(actAs = Seq.empty, offset = 1L, paidTrafficCost = 3L))
      )

      val events = loggerFactory.assertLogs(
        runEvents(service.grpcSource(beginOffset = Some(0L))),
        _.errorMessage should include("No actAs for completion"),
      )
      events shouldBe empty
    }

    "drop and warn about a completion with an invalid act-as party" in {
      // AccountId is limited to 255 chars
      val invalidActAs = Iterator.continually('a').take(256).mkString
      val (service, _, _) = serviceWith(
        completions =
          Seq(completionResponse(actAs = Seq(invalidActAs), offset = 1L, paidTrafficCost = 3L))
      )

      val events = loggerFactory.assertLogs(
        runEvents(service.grpcSource(beginOffset = Some(0L))),
        _.warningMessage should include("is not a valid AccountId"),
      )
      events shouldBe empty
    }

    "drop responses that are not completions" in {
      val (service, _, _) = serviceWith(completions = Seq(checkpointResponse))

      runEvents(service.grpcSource(beginOffset = Some(0L))) shouldBe empty
    }

    "fail when the completion carries no record time" in {
      val (service, _, _) = serviceWith(
        completions = Seq(
          completionResponse(
            actAs = Seq("alice"),
            offset = 5L,
            paidTrafficCost = 3L,
            recordTime = None,
          )
        )
      )

      val ex = the[IllegalStateException] thrownBy timeouts.default.await_("no timestamp")(
        service.grpcSource(beginOffset = Some(0L)).runWith(Sink.seq)
      )
      ex.getMessage should include("Empty synchronizer time on completion")
    }
  }
}
