// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.offset_checkpoint.{OffsetCheckpoint, SynchronizerTime}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.services.completions.CompletionServiceClient
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.tea.projection.{ProjectionEvent, TestCompletions}
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.rpc.status.Status
import io.grpc.Status.Code
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class TeaDebitIngestionServiceTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private implicit val actorSystem: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: Materializer = Materializer(actorSystem)

  override def afterAll(): Unit = {
    materializer.shutdown()
    Await.result(actorSystem.terminate(), 30.seconds)
    super.afterAll()
  }

  private val defaultRecordTime: CantonTimestamp = CantonTimestamp.ofEpochSecond(1000)

  /** Builds the service under test on top of a freshly created mock of the completion service. The
    * mock is returned so that individual tests can refine its stubbing or verify the interactions.
    */
  private def serviceWith(
      completions: Seq[CompletionStreamResponse] = Seq.empty,
      ledgerEndAtStart: Long = 0L,
      config: ProjectionConfig = ProjectionConfig(),
  ): (TeaDebitIngestionService, CompletionServiceClient) = {
    val completionServiceClient = mock[CompletionServiceClient]

    when(
      completionServiceClient
        .getCompletionsSource(any[Long], any[Seq[String]], any[Option[String]])(any[TraceContext])
    ).thenReturn(Source(completions.toList))

    val service =
      new TeaDebitIngestionService(
        completionServiceClient,
        ledgerEndAtStart,
        config,
        loggerFactory,
      )
    (service, completionServiceClient)
  }

  private def verifyCompletionsRequestedFrom(
      completionServiceClient: CompletionServiceClient,
      offset: Long,
      count: Int = 1,
  ): Unit =
    verify(completionServiceClient, times(count)).getCompletionsSource(
      eqTo(offset),
      eqTo(Seq.empty[String]),
      any[Option[String]],
    )(any[TraceContext])

  private def completionResponse(
      actAs: Seq[String],
      offset: Long,
      paidTrafficCost: Long,
      recordTime: Option[CantonTimestamp] = Some(defaultRecordTime),
      status: Option[Status] = Some(Status(Code.OK.value)),
  ): CompletionStreamResponse = TraceContext.withNewTraceContext("test-tc") { tc =>
    CompletionStreamResponse(
      CompletionStreamResponse.CompletionResponse.Completion(
        Completion.defaultInstance.copy(
          userId = "user",
          commandId = s"command-$offset",
          submissionId = s"submission-$offset",
          status = status,
          actAs = actAs,
          offset = offset,
          paidTrafficCost = paidTrafficCost,
          synchronizerTime = recordTime.map(rt =>
            SynchronizerTime(
              synchronizerId = "synchronizer::tea",
              recordTime = Some(rt.toProtoTimestamp),
            )
          ),
          traceContext = SerializableTraceContext(tc).toDamlProto,
        )
      )
    )
  }

  private def checkpointResponse(
      offset: Long,
      recordTime: Option[CantonTimestamp] = Some(defaultRecordTime),
  ): CompletionStreamResponse =
    CompletionStreamResponse(
      CompletionStreamResponse.CompletionResponse.OffsetCheckpoint(
        OffsetCheckpoint(
          offset = offset,
          synchronizerTimes = recordTime.toList.map(rt =>
            SynchronizerTime(
              synchronizerId = "synchronizer::tea",
              recordTime = Some(rt.toProtoTimestamp),
            )
          ),
        )
      )
    )

  /** Drains the (finite) debit source and returns the decoded projection events. */
  private def runEvents(source: Source[Traced[ProjectionEvent], NotUsed]): Seq[ProjectionEvent] =
    source.runWith(Sink.seq).futureValue.map(_.value)

  "TeaDebitIngestionService" should {

    "use the configured initial offset" in {
      val (service, completionServiceClient) = serviceWith(
        ledgerEndAtStart = 42L,
        config = ProjectionConfig(initialCompletionOffsetBeginExclusive = Some(31380)),
      )

      runEvents(service.grpcSource(beginOffset = None))

      verifyCompletionsRequestedFrom(completionServiceClient, 31380L)
    }

    "start from the ledger end at start when no offset is provided" in {
      val (service, completionServiceClient) = serviceWith(ledgerEndAtStart = 42L)

      runEvents(service.grpcSource(beginOffset = None))

      verifyCompletionsRequestedFrom(completionServiceClient, 42L)
    }

    "use the provided begin offset" in {
      val (service, completionServiceClient) = serviceWith(ledgerEndAtStart = 42L)

      runEvents(service.grpcSource(beginOffset = Some(7L)))

      verifyCompletionsRequestedFrom(completionServiceClient, 7L)
    }

    "not skip completions recorded while reconnecting without a persisted offset" in {
      val (service, completionServiceClient) = serviceWith(ledgerEndAtStart = 42L)
      // The first connection fails, e.g. because the Ledger API server is not up yet. A completion
      // is recorded at offset 43 before the retry connects.
      when(
        completionServiceClient
          .getCompletionsSource(any[Long], any[Seq[String]], any[Option[String]])(any[TraceContext])
      ).thenReturn(
        Source.failed(
          io.grpc.Status.UNAVAILABLE.withDescription("Could not find server").asRuntimeException()
        ),
        Source.single(completionResponse(actAs = Seq("alice"), offset = 43L, paidTrafficCost = 3L)),
      )

      service.grpcSource(beginOffset = None).runWith(Sink.seq).failed.futureValue
      // The projection retries with no offset, as nothing was emitted
      runEvents(service.grpcSource(beginOffset = None)).map(_.offset) shouldBe Seq(43L)

      verifyCompletionsRequestedFrom(completionServiceClient, 42L, count = 2)
    }

    "preserve order, keep multi-party and zero-cost completions, and drop checkpoints" in {
      val (service, _) = serviceWith(
        completions = Seq(
          checkpointResponse(offset = 1L),
          completionResponse(actAs = Seq("alice"), offset = 2L, paidTrafficCost = 2L),
          checkpointResponse(offset = 3L),
          completionResponse(actAs = Seq("bob", "carol"), offset = 4L, paidTrafficCost = 7L),
          completionResponse(
            actAs = Seq("bob"),
            offset = 5L,
            paidTrafficCost = 4L,
            status = Some(Status(Code.ABORTED.value)),
          ),
          completionResponse(actAs = Seq("carol"), offset = 6L, paidTrafficCost = 0L),
        )
      )

      runEvents(service.grpcSource(beginOffset = Some(0L))) shouldBe Seq(
        TestCompletions.checkpoint(1L, defaultRecordTime),
        TestCompletions.event(2L, defaultRecordTime, Set("alice"), 2L),
        TestCompletions.checkpoint(3L, defaultRecordTime),
        TestCompletions.event(4L, defaultRecordTime, Set("bob", "carol"), 7L),
        TestCompletions.event(5L, defaultRecordTime, Set("bob"), 4L, accepted = false),
        TestCompletions.event(6L, defaultRecordTime, Set("carol"), 0L),
      )
    }

    "emit a checkpoint for a completion whose party doesn't parse" in {
      // Party ids are limited to 255 chars
      val invalidActAs = Iterator.continually('a').take(256).mkString
      val (service, _) = serviceWith(
        completions =
          Seq(completionResponse(actAs = Seq(invalidActAs), offset = 1L, paidTrafficCost = 3L))
      )

      val events = loggerFactory.assertLogs(
        runEvents(service.grpcSource(beginOffset = Some(0L))),
        _.errorMessage should include("Invalid identifiers on completion"),
      )
      events shouldBe Seq(TestCompletions.checkpoint(1L, defaultRecordTime))
    }

    "fail when the completion carries no record time" in {
      val (service, _) = serviceWith(
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
