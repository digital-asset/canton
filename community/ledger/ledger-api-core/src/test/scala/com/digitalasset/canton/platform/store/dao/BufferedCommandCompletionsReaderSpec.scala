// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.OffsetGen.offset
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{Party, UserId}
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BufferedCommandCompletionsReaderSpec
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with PekkoBeforeAndAfterAll
    with HasExecutionContext
    with ScalaFutures {

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val metrics = LedgerApiServerMetrics.ForTesting
  private val someSynchronizerId = SynchronizerId.tryFromString("some::synchronizer id")

  private val partyAlice: Party = Ref.Party.assertFromString("alice")
  private val partyBob: Party = Ref.Party.assertFromString("bob")
  private val partyCharlie: Party = Ref.Party.assertFromString("charlie")

  private val offset1 = offset(1L)
  private val offset2 = offset(2L)
  private val offset3 = offset(3L)

  private val bufferedUpdates = Seq(
    acceptedUpdate(offset1, commandId = "accepted-alice", actAs = Seq(partyAlice.toString)),
    acceptedUpdate(
      offset2,
      commandId = "accepted-alice-bob",
      actAs = Seq(partyAlice.toString, partyBob.toString),
    ),
    rejectedUpdate(offset3, commandId = "rejected-bob", actAs = Seq(partyBob.toString)),
  )

  "BufferedCommandCompletionsReader" should {
    "return all buffered completions for wildcard party reads" in {
      val responses = bufferedReader(bufferedUpdates)
        .getCommandCompletions(offset1, offset3, userId = None, parties = Set.empty[Party])
        .runWith(Sink.seq)

      responses.futureValue.map(render) shouldBe Seq(
        offset1 -> ("accepted-alice", Seq("alice")),
        offset2 -> ("accepted-alice-bob", Seq("alice", "bob")),
        offset3 -> ("rejected-bob", Seq("bob")),
      )
    }

    "filter buffered completions for explicit party reads" in {
      val responses = bufferedReader(bufferedUpdates)
        .getCommandCompletions(offset1, offset3, userId = None, parties = Set(partyAlice))
        .runWith(Sink.seq)

      responses.futureValue.map(render) shouldBe Seq(
        offset1 -> ("accepted-alice", Seq("alice")),
        offset2 -> ("accepted-alice-bob", Seq("alice")),
      )
    }

    "drop buffered completions for non-matching explicit party reads" in {
      bufferedReader(bufferedUpdates)
        .getCommandCompletions(offset1, offset3, userId = None, parties = Set(partyCharlie))
        .runWith(Sink.seq)
        .futureValue shouldBe Seq.empty
    }
  }

  private def bufferedReader(
      updates: Seq[TransactionLogUpdate]
  ): BufferedCommandCompletionsReader = {
    val buffer = new InMemoryFanoutBuffer(
      maxBufferSize = updates.size,
      metrics = metrics,
      maxBufferedChunkSize = updates.size,
      loggerFactory = loggerFactory,
    )
    updates.foreach(buffer.push)

    BufferedCommandCompletionsReader(
      delegate = new LedgerDaoCommandCompletionsReader {
        override def getCommandCompletions(
            startInclusive: Offset,
            endInclusive: Offset,
            userId: Option[UserId],
            parties: Set[Party],
        )(implicit
            loggingContext: LoggingContextWithTrace
        ): Source[(Offset, CompletionStreamResponse), NotUsed] =
          fail(
            s"Unexpected persistence fetch startInclusive=$startInclusive endInclusive=$endInclusive userId=$userId parties=$parties"
          )
      },
      inMemoryFanoutBuffer = buffer,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )
  }

  private def render(
      response: (Offset, CompletionStreamResponse)
  ): (Offset, (String, Seq[String])) = {
    val (offset, streamResponse) = response
    val completion = streamResponse.completionResponse.completion
      .getOrElse(fail("expected completion response"))
    offset -> (completion.commandId, completion.actAs)
  }

  private def acceptedUpdate(
      offset: Offset,
      commandId: String,
      actAs: Seq[String],
  ): TransactionLogUpdate.TransactionAccepted =
    TransactionLogUpdate.TransactionAccepted(
      updateId = TestUpdateId(s"update-$commandId").toHexString,
      commandId = commandId,
      workflowId = s"workflow-$commandId",
      effectiveAt = Timestamp.Epoch,
      offset = offset,
      events = Vector.empty,
      completionStreamResponseO = Some(completionResponse(commandId, actAs)),
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
      externalTransactionHash = None,
    )(TraceContext.empty)

  private def rejectedUpdate(
      offset: Offset,
      commandId: String,
      actAs: Seq[String],
  ): TransactionLogUpdate.TransactionRejected =
    TransactionLogUpdate.TransactionRejected(
      offset = offset,
      completionStreamResponse = completionResponse(commandId, actAs),
    )(TraceContext.empty)

  private def completionResponse(
      commandId: String,
      actAs: Seq[String],
  ): CompletionStreamResponse =
    CompletionStreamResponse.defaultInstance.withCompletion(
      Completion.defaultInstance.copy(
        commandId = commandId,
        updateId = TestUpdateId(s"completion-$commandId").toHexString,
        userId = "buffer-user",
        actAs = actAs,
      )
    )
}
