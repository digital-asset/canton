// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.ArgumentCaptor
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class BufferedCommandCompletionsReaderSpec
    extends AnyWordSpec
    with BaseTest
    with PekkoBeforeAndAfterAll
    with HasExecutionContext {

  private implicit val lc: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  private val metrics = LedgerApiServerMetrics.ForTesting
  private val someSynchronizerId = SynchronizerId.tryFromString("some::synchronizer id")

  private val user1: Ref.UserId = Ref.UserId.assertFromString("user1")
  private val user2: Ref.UserId = Ref.UserId.assertFromString("user2")
  private val party1 = Ref.Party.assertFromString("party1")
  private val party2 = Ref.Party.assertFromString("party2")
  private val party3 = Ref.Party.assertFromString("party3")

  private def offset(idx: Long): Offset = Offset.tryFromLong(1000000000L + idx)

  private def completionResponse(
      userId: String,
      actAs: Seq[String],
      transactionHash: Option[ByteString] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.defaultInstance.withCompletion(
      Completion.defaultInstance.copy(
        userId = userId,
        actAs = actAs,
        commandId = s"cmd-$userId-${actAs.mkString("-")}",
        transactionHash = transactionHash,
      )
    )

  private def txAccepted(
      idx: Long,
      userId: String,
      actAs: Seq[String],
      hash: Option[Hash] = None,
  ): TransactionLogUpdate.TransactionAccepted =
    TransactionLogUpdate.TransactionAccepted(
      updateId = s"tx-$idx",
      commandId = s"cmd-$userId-${actAs.mkString("-")}",
      workflowId = "",
      effectiveAt = Timestamp.Epoch,
      offset = offset(idx),
      events = Vector.empty,
      completionStreamResponseO = Some(completionResponse(userId, actAs, hash.map(_.unwrap))),
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
      transactionHash = hash,
    )(TraceContext.empty)

  private def txRejected(
      idx: Long,
      userId: String,
      actAs: Seq[String],
      hash: Option[Hash] = None,
  ): TransactionLogUpdate.TransactionRejected =
    TransactionLogUpdate.TransactionRejected(
      offset = offset(idx),
      completionStreamResponse = completionResponse(userId, actAs, hash.map(_.unwrap)),
    )(TraceContext.empty)

  /** The completion carried by a buffered accepted transaction, as the reader surfaces it. */
  private def completionOf(accepted: TransactionLogUpdate.TransactionAccepted): Completion =
    accepted.completionStreamResponseO.value.getCompletion

  private def completionOf(rejected: TransactionLogUpdate.TransactionRejected): Completion =
    rejected.completionStreamResponse.getCompletion

  /** A delegate that should never be called (all data comes from the buffer). */
  private val neverCalledDelegate: LedgerDaoCommandCompletionsReader =
    new LedgerDaoCommandCompletionsReader {
      override def getCommandCompletions(
          startInclusive: Offset,
          endInclusive: Offset,
          userId: Option[Ref.UserId],
          parties: Set[Ref.Party],
      )(implicit
          loggingContext: LoggingContextWithTrace
      ): Source[(Offset, CompletionStreamResponse), NotUsed] =
        Source.empty

      override def getCompletionByHash(
          hash: Array[Byte],
          maxRejectedCompletions: Int,
          parties: Set[Ref.Party],
          rejectedBeforeOffset: Option[Offset],
          includeAccepted: Boolean,
      )(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[BufferedCommandCompletionsReader.CompletionsByHash] =
        Future.successful(BufferedCommandCompletionsReader.CompletionsByHash(None, Vector.empty))
    }

  private def mkHash(input: String): Hash =
    Hash.digest(
      HashPurpose.PreparedSubmission,
      ByteString.copyFromUtf8(input),
      HashAlgorithm.Sha256,
    )

  private def buildReaderWith(
      entries: Seq[TransactionLogUpdate],
      delegate: LedgerDaoCommandCompletionsReader,
  ): BufferedCommandCompletionsReader = {
    val buffer = new InMemoryFanoutBuffer(
      maxBufferSize = math.max(entries.size + 1, 1),
      metrics = metrics,
      maxBufferedChunkSize = math.max(entries.size + 1, 1),
      loggerFactory = loggerFactory,
    )
    entries.foreach(buffer.push)
    BufferedCommandCompletionsReader(
      dbReader = delegate,
      inMemoryFanoutBuffer = buffer,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )
  }

  private def buildReader(
      entries: Seq[TransactionLogUpdate]
  ): BufferedCommandCompletionsReader =
    buildReaderWith(entries, neverCalledDelegate)

  private def runStream(
      reader: BufferedCommandCompletionsReader,
      startIdx: Long,
      endIdx: Long,
      userId: Option[Ref.UserId],
      parties: Set[Ref.Party],
  ): Seq[CompletionStreamResponse] =
    reader
      .getCommandCompletions(
        startInclusive = offset(startIdx),
        endInclusive = offset(endIdx),
        userId = userId,
        parties = parties,
      )
      .runWith(Sink.seq)
      .futureValue
      .map(_._2)

  "BufferedCommandCompletionsReader filter" should {

    "return completions from all users when userId is None" in {
      val reader = buildReader(
        Seq(
          txAccepted(1, user1, Seq(party1.toString)),
          txAccepted(2, user2, Seq(party1.toString)),
        )
      )

      val results = runStream(reader, 1, 2, userId = None, parties = Set(party1))
      results should have size 2
      results.map(_.getCompletion.userId) should contain theSameElementsAs Seq(user1, user2)
    }

    "return all completions regardless of actAs when parties is empty" in {
      val reader = buildReader(
        Seq(
          txAccepted(1, user1, Seq(party1.toString)),
          txAccepted(2, user1, Seq(party2.toString)),
          txAccepted(3, user1, Seq(party3.toString)),
        )
      )

      val results = runStream(reader, 1, 3, userId = Some(user1), parties = Set.empty)
      results should have size 3
      // When parties is empty, actAs should not be filtered
      results.flatMap(_.getCompletion.actAs).toSet shouldBe Set(
        party1.toString,
        party2.toString,
        party3.toString,
      )
    }

    "return everything when both userId is None and parties is empty" in {
      val reader = buildReader(
        Seq(
          txAccepted(1, user1, Seq(party1.toString)),
          txAccepted(2, user2, Seq(party2.toString)),
          txRejected(3, user1, Seq(party3.toString)),
        )
      )

      val results = runStream(reader, 1, 3, userId = None, parties = Set.empty)
      results should have size 3
    }

    "filter by userId when userId is Some" in {
      val reader = buildReader(
        Seq(
          txAccepted(1, user1, Seq(party1.toString)),
          txAccepted(2, user2, Seq(party1.toString)),
          txRejected(3, user1, Seq(party1.toString)),
        )
      )

      val results = runStream(reader, 1, 3, userId = Some(user1), parties = Set(party1))
      results should have size 2
      results.foreach(_.getCompletion.userId shouldBe user1)
    }

    "strip non-matching actAs parties from completions" in {
      val reader = buildReader(
        Seq(
          txAccepted(1, user1, Seq(party1.toString, party2.toString, party3.toString))
        )
      )

      val results = runStream(reader, 1, 1, userId = Some(user1), parties = Set(party1))
      results should have size 1
      results.head.getCompletion.actAs shouldBe Seq(party1.toString)
    }

    "reject completion when party filter removes all actAs" in {
      val reader = buildReader(
        Seq(
          txAccepted(1, user1, Seq(party1.toString))
        )
      )

      val results = runStream(reader, 1, 1, userId = Some(user1), parties = Set(party2))
      results shouldBe empty
    }

    "handle rejected transactions with the same filter logic" in {
      val reader = buildReader(
        Seq(
          txRejected(1, user1, Seq(party1.toString)),
          txRejected(2, user2, Seq(party1.toString)),
        )
      )

      val results = runStream(reader, 1, 2, userId = None, parties = Set(party1))
      results should have size 2

      val filteredByUser = runStream(reader, 1, 2, userId = Some(user1), parties = Set(party1))
      filteredByUser should have size 1
      filteredByUser.head.getCompletion.userId shouldBe user1
    }
  }

  "planCompletionsByHash" should {

    "use the accepted from the buffer without asking the DB for it" in {
      val accepted = txAccepted(1, user1, Seq(party1.toString))
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = Some(accepted),
        bufferedRejections = Vector.empty,
        maxRejectedCompletions = 0,
        parties = Set.empty,
      )

      plan.acceptedFromBuffer shouldBe Some(completionOf(accepted))
      plan.dbQuery shouldBe None
    }

    "request the accepted from the DB when it is absent in the buffer" in {
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = None,
        bufferedRejections = Vector.empty,
        maxRejectedCompletions = 0,
        parties = Set.empty,
      )

      plan.acceptedFromBuffer shouldBe None
      plan.dbQuery.map(_.includeAccepted) shouldBe Some(true)
      plan.dbQuery.map(_.maxRejectedCompletions) shouldBe Some(0)
    }

    "reduce the DB rejection limit by the number of buffered rejections" in {
      val newerRejection = txRejected(2, user1, Seq(party1.toString))
      val olderRejection = txRejected(1, user1, Seq(party1.toString))
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = Some(txAccepted(3, user1, Seq(party1.toString))),
        bufferedRejections = Vector(newerRejection, olderRejection),
        maxRejectedCompletions = 5,
        parties = Set.empty,
      )

      plan.rejectionsFromBuffer shouldBe Vector(
        completionOf(newerRejection),
        completionOf(olderRejection),
      )
      plan.dbQuery.map(_.maxRejectedCompletions) shouldBe Some(3)
      plan.dbQuery.map(_.includeAccepted) shouldBe Some(false)
    }

    "not call the DB when the buffer meets the rejection limit and accepted is buffered" in {
      val newerRejection = txRejected(2, user1, Seq(party1.toString))
      val olderRejection = txRejected(1, user1, Seq(party1.toString))
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = Some(txAccepted(3, user1, Seq(party1.toString))),
        bufferedRejections = Vector(newerRejection, olderRejection),
        maxRejectedCompletions = 2,
        parties = Set.empty,
      )

      plan.rejectionsFromBuffer shouldBe Vector(
        completionOf(newerRejection),
        completionOf(olderRejection),
      )
      plan.dbQuery shouldBe None
    }

    "request the accepted when buffered rejections meet the rejection limit" in {
      val newerRejection = txRejected(2, user1, Seq(party1.toString))
      val olderRejection = txRejected(1, user1, Seq(party1.toString))
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = None,
        bufferedRejections = Vector(newerRejection, olderRejection),
        maxRejectedCompletions = 2,
        parties = Set.empty,
      )

      plan.rejectionsFromBuffer shouldBe Vector(
        completionOf(newerRejection),
        completionOf(olderRejection),
      )
      plan.dbQuery shouldBe Some(
        BufferedCommandCompletionsReader.DbCompletionsQuery(
          maxRejectedCompletions = 0,
          rejectedBeforeOffset = Some(offset(1)),
          includeAccepted = true,
        )
      )
    }

    "use the last buffered rejection offset as the DB before-offset" in {
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = Some(txAccepted(4, user1, Seq(party1.toString))),
        bufferedRejections = Vector(
          txRejected(3, user1, Seq(party1.toString)),
          txRejected(2, user1, Seq(party1.toString)),
        ),
        maxRejectedCompletions = 5,
        parties = Set.empty,
      )

      plan.dbQuery.flatMap(_.rejectedBeforeOffset) shouldBe Some(offset(2))
    }

    "set no DB before-offset when the buffer has no rejections" in {
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = None,
        bufferedRejections = Vector.empty,
        maxRejectedCompletions = 5,
        parties = Set.empty,
      )

      plan.dbQuery.flatMap(_.rejectedBeforeOffset) shouldBe None
    }

    "treat a buffered accepted dropped by the filter as authoritative, not absent" in {
      // The buffer holds the accepted for this hash, but the filter redacts it away. The buffer is
      // still authoritative for the accepted, so we must not fall back to fetching it from the DB.
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = Some(txAccepted(1, user1, Seq(party2.toString))),
        bufferedRejections = Vector.empty,
        maxRejectedCompletions = 0,
        parties = Set(party1),
      )

      plan.acceptedFromBuffer shouldBe None
      plan.dbQuery shouldBe None
    }

    "count only filter-visible rejections toward the rejection limit" in {
      val visibleRejection = txRejected(2, user1, Seq(party1.toString))
      val droppedRejection = txRejected(1, user1, Seq(party2.toString))
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = None,
        bufferedRejections = Vector(visibleRejection, droppedRejection),
        maxRejectedCompletions = 5,
        parties = Set(party1),
      )

      plan.rejectionsFromBuffer shouldBe Vector(
        completionOf(visibleRejection).withActAs(Seq(party1.toString))
      )
      // Only one of the two buffered rejections was visible, so the DB may still return four more.
      plan.dbQuery.map(_.maxRejectedCompletions) shouldBe Some(4)
    }

    "take the DB before-offset from the last filter-visible rejection" in {
      // The older (last) buffered rejection is dropped by the filter, so the boundary offset for
      // the DB query must come from the last visible rejection, not the dropped one.
      val visibleRejection = txRejected(3, user1, Seq(party1.toString))
      val droppedRejection = txRejected(2, user1, Seq(party2.toString))
      val plan = BufferedCommandCompletionsReader.planCompletionsByHash(
        bufferedAccepted = None,
        bufferedRejections = Vector(visibleRejection, droppedRejection),
        maxRejectedCompletions = 5,
        parties = Set(party1),
      )

      plan.dbQuery.flatMap(_.rejectedBeforeOffset) shouldBe Some(offset(3))
    }
  }

  "applyFilter" should {

    "pass the completion through unchanged for empty parties (no filter)" in {
      val completion =
        completionResponse(user1, Seq(party1.toString, party2.toString)).getCompletion
      BufferedCommandCompletionsReader.applyFilter(
        completion,
        Set.empty,
      ) shouldBe Some(completion)
    }

    "redact actAs to the filter parties" in {
      val completion =
        completionResponse(user1, Seq(party1.toString, party2.toString)).getCompletion
      val filtered = BufferedCommandCompletionsReader.applyFilter(
        completion,
        Set(party1),
      )
      filtered.value.actAs shouldBe Seq(party1.toString)
    }

    "drop a completion whose actAs does not overlap the filter parties" in {
      val completion = completionResponse(user1, Seq(party2.toString)).getCompletion
      BufferedCommandCompletionsReader.applyFilter(
        completion,
        Set(party1),
      ) shouldBe None
    }

    "return the completion regardless of the submitting user" in {
      val completion = completionResponse(user2, Seq(party1.toString)).getCompletion
      val filtered = BufferedCommandCompletionsReader.applyFilter(
        completion,
        Set(party1),
      )
      filtered.value.userId shouldBe user2
      filtered.value.actAs shouldBe Seq(party1.toString)
    }
  }

  "mergeCompletionsByHash" should {

    "return the buffer-only contribution when there is no DB result" in {
      val acceptedCompletion =
        completionResponse(user1, Seq(party1.toString)).getCompletion
      val rejection = completionResponse(user2, Seq(party2.toString)).getCompletion
      val plan = BufferedCommandCompletionsReader.CompletionsByHashPlan(
        acceptedFromBuffer = Some(acceptedCompletion),
        rejectionsFromBuffer = Vector(rejection),
        dbQuery = None,
      )

      val merged = BufferedCommandCompletionsReader.mergeCompletionsByHash(plan, None)

      merged.accepted shouldBe Some(acceptedCompletion)
      merged.rejected shouldBe Vector(rejection)
    }

    "prefer the DB accepted over the buffer accepted" in {
      val bufferAccepted = completionResponse(user1, Seq(party1.toString)).getCompletion
      val dbAccepted = completionResponse(user2, Seq(party2.toString)).getCompletion
      val plan = BufferedCommandCompletionsReader.CompletionsByHashPlan(
        acceptedFromBuffer = Some(bufferAccepted),
        rejectionsFromBuffer = Vector.empty,
        dbQuery = None,
      )

      val merged = BufferedCommandCompletionsReader.mergeCompletionsByHash(
        plan,
        Some(BufferedCommandCompletionsReader.CompletionsByHash(Some(dbAccepted), Vector.empty)),
      )

      // the planner never asks the DB for an accepted it already has, but the merge still
      // documents that a DB accepted wins should both ever be present
      merged.accepted shouldBe Some(dbAccepted)
    }

    "fall back to the buffer accepted when the DB has none" in {
      val bufferAccepted = completionResponse(user1, Seq(party1.toString)).getCompletion
      val plan = BufferedCommandCompletionsReader.CompletionsByHashPlan(
        acceptedFromBuffer = Some(bufferAccepted),
        rejectionsFromBuffer = Vector.empty,
        dbQuery = None,
      )

      val merged = BufferedCommandCompletionsReader.mergeCompletionsByHash(
        plan,
        Some(BufferedCommandCompletionsReader.CompletionsByHash(None, Vector.empty)),
      )

      merged.accepted shouldBe Some(bufferAccepted)
    }

    "keep buffer rejections before DB rejections" in {
      val bufferRejection = completionResponse(user1, Seq(party1.toString)).getCompletion
      val dbRejection = completionResponse(user2, Seq(party2.toString)).getCompletion
      val plan = BufferedCommandCompletionsReader.CompletionsByHashPlan(
        acceptedFromBuffer = None,
        rejectionsFromBuffer = Vector(bufferRejection),
        dbQuery = None,
      )

      val merged = BufferedCommandCompletionsReader.mergeCompletionsByHash(
        plan,
        Some(BufferedCommandCompletionsReader.CompletionsByHash(None, Vector(dbRejection))),
      )

      merged.rejected shouldBe Vector(bufferRejection, dbRejection)
    }
  }

  "getCompletionByHash" should {

    val hash = mkHash("hash-1")
    val hashBytes = hash.unwrap.toByteArray

    def stubDelegate(
        result: BufferedCommandCompletionsReader.CompletionsByHash
    ): LedgerDaoCommandCompletionsReader = {
      val delegate = mock[LedgerDaoCommandCompletionsReader]
      when(
        delegate.getCompletionByHash(
          any[Array[Byte]],
          any[Int],
          any[Set[Ref.Party]],
          any[Option[Offset]],
          any[Boolean],
        )(
          any[LoggingContextWithTrace]
        )
      ).thenReturn(Future.successful(result))
      delegate
    }

    /** Assert the delegate was asked for the DB remainder with exactly these derived arguments. */
    def verifyDbCall(
        delegate: LedgerDaoCommandCompletionsReader,
        maxRejected: Int,
        rejectedBeforeOffset: Option[Offset],
        includeAccepted: Boolean,
    ): Unit = {
      val hashCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
      verify(delegate).getCompletionByHash(
        hashCaptor.capture(),
        eqTo(maxRejected),
        eqTo(Set.empty[Ref.Party]),
        eqTo(rejectedBeforeOffset),
        eqTo(includeAccepted),
      )(any[LoggingContextWithTrace])
      hashCaptor.getValue.toVector shouldBe hashBytes.toVector
    }

    /** Assert the delegate was never asked for anything (everything came from the buffer). */
    def verifyNoDbCall(delegate: LedgerDaoCommandCompletionsReader): Unit =
      verify(delegate, never).getCompletionByHash(
        any[Array[Byte]],
        any[Int],
        any[Set[Ref.Party]],
        any[Option[Offset]],
        any[Boolean],
      )(any[LoggingContextWithTrace])

    "serve entirely from the buffer without calling the DB" in {
      val delegate = stubDelegate(
        BufferedCommandCompletionsReader.CompletionsByHash(None, Vector.empty)
      )
      val accepted = txAccepted(1, user1, Seq(party1.toString), hash = Some(hash))
      val reader = buildReaderWith(Seq(accepted), delegate)

      val result =
        reader
          .getCompletionByHash(
            hashBytes,
            maxRejectedCompletions = 0,
            parties = Set.empty,
          )
          .futureValue

      result.accepted shouldBe Some(completionOf(accepted))
      result.accepted.value.transactionHash shouldBe Some(hash.unwrap)
      result.rejected shouldBe empty
      verifyNoDbCall(delegate)
    }

    "fetch the accepted from the DB when it is absent from the buffer" in {
      val dbAccepted = completionResponse(
        user2,
        Seq(party2.toString),
        Some(hash.unwrap),
      ).getCompletion
      val delegate = stubDelegate(
        BufferedCommandCompletionsReader.CompletionsByHash(Some(dbAccepted), Vector.empty)
      )
      val reader = buildReaderWith(Seq.empty, delegate)

      val result =
        reader
          .getCompletionByHash(
            hashBytes,
            maxRejectedCompletions = 0,
            parties = Set.empty,
          )
          .futureValue

      result.accepted shouldBe Some(dbAccepted)
      result.accepted.value.transactionHash shouldBe Some(hash.unwrap)
      verifyDbCall(delegate, maxRejected = 0, rejectedBeforeOffset = None, includeAccepted = true)
    }

    "fetch rejections from the DB when the buffer is empty" in {
      val dbRej1 = completionResponse("db", Seq("a")).getCompletion
      val dbRej2 = completionResponse("db", Seq("b")).getCompletion
      val delegate = stubDelegate(
        BufferedCommandCompletionsReader.CompletionsByHash(None, Vector(dbRej1, dbRej2))
      )
      val reader = buildReaderWith(Seq.empty, delegate)

      val result =
        reader
          .getCompletionByHash(
            hashBytes,
            maxRejectedCompletions = 5,
            parties = Set.empty,
          )
          .futureValue

      result.accepted shouldBe None
      result.rejected shouldBe Vector(dbRej1, dbRej2)
      verifyDbCall(delegate, maxRejected = 5, rejectedBeforeOffset = None, includeAccepted = true)
    }

    "combine buffered rejections with the DB remainder" in {
      val dbRej1 = completionResponse("db", Seq("a")).getCompletion
      val dbRej2 = completionResponse("db", Seq("b")).getCompletion
      val delegate = stubDelegate(
        BufferedCommandCompletionsReader.CompletionsByHash(None, Vector(dbRej1, dbRej2))
      )
      val accepted = txAccepted(3, user1, Seq(party1.toString), hash = Some(hash))
      val olderRejection = txRejected(1, user1, Seq(party1.toString), hash = Some(hash))
      val newerRejection = txRejected(2, user2, Seq(party2.toString), hash = Some(hash))
      val reader = buildReaderWith(
        Seq(olderRejection, newerRejection, accepted),
        delegate,
      )

      val result =
        reader
          .getCompletionByHash(
            hashBytes,
            maxRejectedCompletions = 5,
            parties = Set.empty,
          )
          .futureValue

      result.accepted shouldBe Some(completionOf(accepted))
      result.rejected shouldBe Vector(
        completionOf(newerRejection),
        completionOf(olderRejection),
        dbRej1,
        dbRej2,
      )
      verifyDbCall(
        delegate,
        maxRejected = 3,
        rejectedBeforeOffset = Some(offset(1)),
        includeAccepted = false,
      )
    }

    "fetch the accepted when buffered rejections meet the rejection limit" in {
      val dbAccepted = completionResponse(
        user2,
        Seq(party2.toString),
        Some(hash.unwrap),
      ).getCompletion
      val delegate = stubDelegate(
        BufferedCommandCompletionsReader.CompletionsByHash(Some(dbAccepted), Vector.empty)
      )
      val olderRejection = txRejected(1, user1, Seq(party1.toString), hash = Some(hash))
      val newerRejection = txRejected(2, user2, Seq(party2.toString), hash = Some(hash))
      val reader = buildReaderWith(Seq(olderRejection, newerRejection), delegate)

      val result =
        reader
          .getCompletionByHash(
            hashBytes,
            maxRejectedCompletions = 2,
            parties = Set.empty,
          )
          .futureValue

      result.accepted shouldBe Some(dbAccepted)
      result.rejected shouldBe Vector(
        completionOf(newerRejection),
        completionOf(olderRejection),
      )
      verifyDbCall(
        delegate,
        maxRejected = 0,
        rejectedBeforeOffset = Some(offset(1)),
        includeAccepted = true,
      )
    }

    "not call the DB when the buffer already meets the rejection limit" in {
      val delegate = stubDelegate(
        BufferedCommandCompletionsReader.CompletionsByHash(None, Vector.empty)
      )
      val accepted = txAccepted(3, user1, Seq(party1.toString), hash = Some(hash))
      val olderRejection = txRejected(1, user1, Seq(party1.toString), hash = Some(hash))
      val newerRejection = txRejected(2, user2, Seq(party2.toString), hash = Some(hash))
      val reader = buildReaderWith(
        Seq(olderRejection, newerRejection, accepted),
        delegate,
      )

      val result =
        reader
          .getCompletionByHash(
            hashBytes,
            maxRejectedCompletions = 2,
            parties = Set.empty,
          )
          .futureValue

      result.accepted shouldBe Some(completionOf(accepted))
      result.rejected shouldBe Vector(
        completionOf(newerRejection),
        completionOf(olderRejection),
      )
      verifyNoDbCall(delegate)
    }
  }
}
