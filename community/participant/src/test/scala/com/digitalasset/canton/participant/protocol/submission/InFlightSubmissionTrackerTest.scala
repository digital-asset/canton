// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Eval
import cats.syntax.option.*
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.DefaultParticipantStateValues
import com.digitalasset.canton.participant.store.memory.InMemoryInFlightSubmissionStore
import com.digitalasset.canton.platform.indexer.parallel.{PostPublishData, PublishSource}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DefaultDamlValues}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

class InFlightSubmissionTrackerTest extends AsyncWordSpec with BaseTest {

  private lazy val synchronizerId = SynchronizerId.tryFromString("synchronizer::id")
  private lazy val otherSynchronizerId = SynchronizerId.tryFromString("other::id")
  private lazy val completionInfo = DefaultParticipantStateValues.completionInfo(List.empty)
  private lazy val sequencedAt = CantonTimestamp.Epoch.plusSeconds(1)

  private class RecordingDeduplicator extends NoCommandDeduplicator {
    val processed: AtomicReference[Seq[PostPublishData]] = new AtomicReference(Seq.empty)

    override def processPublications(
        publications: Seq[PostPublishData]
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      processed.getAndUpdate(_ ++ publications).discard
      FutureUnlessShutdown.unit
    }
  }

  private def reassignmentSubmission(
      index: Int,
      submissionSynchronizerId: SynchronizerId,
  ): InFlightSubmission[UnsequencedSubmission] =
    InFlightSubmission(
      changeIdHash = ChangeIdHash(DefaultDamlValues.lfhash(index)),
      submissionId = DefaultDamlValues.submissionId(index).some,
      submissionSynchronizerId = submissionSynchronizerId,
      messageUuid = new UUID(0, index.toLong),
      rootHashO = None,
      sequencingInfo = UnsequencedSubmission(
        CantonTimestamp.Epoch.plusSeconds(10),
        ReassignmentSubmissionTrackingData.create(
          completionInfo,
          SubmissionTrackingData.TimeoutCause,
          submissionSynchronizerId.toPhysical,
        ),
      ),
      submissionTraceContext = TraceContext.empty,
    )

  private def publication(
      publishSource: PublishSource,
      isTransaction: Boolean,
  ): PostPublishData =
    PostPublishData(
      submissionSynchronizerId = synchronizerId,
      publishSource = publishSource,
      userId = completionInfo.userId,
      commandId = completionInfo.commandId,
      actAs = completionInfo.actAs.toSet,
      offset = Offset.tryFromLong(1L),
      publicationTime = CantonTimestamp.Epoch,
      submissionId = completionInfo.submissionId,
      accepted = true,
      isTransaction = isTransaction,
      traceContext = TraceContext.empty,
    )

  "processPublications" should {
    "delete the in-flight entries of reassignments on their synchronizer without deduplicating them" in {
      val store = new InMemoryInFlightSubmissionStore(loggerFactory)
      val deduplicator = new RecordingDeduplicator
      val tracker = new InFlightSubmissionTracker(Eval.now(store), deduplicator, loggerFactory)

      val sequenced = reassignmentSubmission(1, synchronizerId)
      val unsequenced = reassignmentSubmission(2, synchronizerId)
      val sequencedOnOtherSynchronizer = reassignmentSubmission(3, otherSynchronizerId)

      val transactionPublication =
        publication(PublishSource.Local(new UUID(0, 9)), isTransaction = true)
      val reassignmentPublications = Seq(
        publication(PublishSource.Sequencer(sequencedAt), isTransaction = false),
        publication(PublishSource.Local(unsequenced.messageUuid), isTransaction = false),
      )

      for {
        _ <- store.register(sequenced).valueOrFailShutdown("register sequenced")
        _ <- store.register(unsequenced).valueOrFailShutdown("register unsequenced")
        _ <- store
          .register(sequencedOnOtherSynchronizer)
          .valueOrFailShutdown("register on other synchronizer")
        _ <- store
          .observeSequencing(
            synchronizerId,
            Map(sequenced.messageId -> SequencedSubmission(sequencedAt)),
          )
          .failOnShutdown
        _ <- store
          .observeSequencing(
            otherSynchronizerId,
            Map(sequencedOnOtherSynchronizer.messageId -> SequencedSubmission(sequencedAt)),
          )
          .failOnShutdown
        _ <- tracker
          .processPublications(transactionPublication +: reassignmentPublications)
          .failOnShutdown
        sequencedLookup <- store.lookup(sequenced.changeIdHash).value.failOnShutdown
        unsequencedLookup <- store.lookup(unsequenced.changeIdHash).value.failOnShutdown
        otherSynchronizerLookup <- store
          .lookup(sequencedOnOtherSynchronizer.changeIdHash)
          .value
          .failOnShutdown
      } yield {
        deduplicator.processed.get() shouldBe Seq(transactionPublication)
        sequencedLookup shouldBe None
        unsequencedLookup shouldBe None
        // Sequenced at the same timestamp as `sequenced` on otherSynchronizerId, which has no
        // publication, so this entry stays in flight.
        otherSynchronizerLookup.map(_.changeIdHash) shouldBe Some(
          sequencedOnOtherSynchronizer.changeIdHash
        )
      }
    }
  }
}
