// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.store.AcsReplicationProgress
import com.digitalasset.canton.participant.store.memory.InMemoryPartyReplicationIndexingStore
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, RepairCounter}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

/** Component test for [[PartyReplicationFileImporter]] focusing on stream resiliency.
  *
  * Test Goal: Verify the stream resumption logic for crash recovery behavior.
  *
  * Asserts:
  *   - Progress is accurately saved to the state manager after each processed chunk.
  *   - Mid-stream failures (e.g., corrupt data or stream disconnects) are handled gracefully.
  *   - Re-initiating the import successfully restarts the stream from the beginning to overwrite
  *     contracts wiped by the node startup sequence, and completes the remaining work.
  */
class PartyReplicationFileImporterTest extends AsyncWordSpec with BaseTest with BeforeAndAfterAll {

  private implicit val actorSystem: ActorSystem = ActorSystem("PartyReplicationFileImporterTest")

  // A lightweight fake to track state updates without needing a real database
  class FakeAcsReplicationProgress extends AcsReplicationProgress {
    var state: Option[PartyReplicationStatus.AcsReplicationProgress] = None

    override def getAcsReplicationProgress(
        requestId: AddPartyRequestId
    )(implicit traceContext: TraceContext): Option[PartyReplicationStatus.AcsReplicationProgress] =
      state

    override def updateAcsReplicationProgress(
        requestId: AddPartyRequestId,
        progress: PartyReplicationStatus.AcsReplicationProgress,
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
      state = Some(progress)
      EitherT.pure[FutureUnlessShutdown, String](())
    }
  }

  "PartyReplicationFileImporter" should {

    "restart from the beginning of a failed stream correctly" in {
      val requestId = Hash.digest(
        HashPurpose.OnlinePartyReplicationId,
        ByteString.copyFromUtf8("dummy-request-id-for-testing"),
        HashAlgorithm.Sha256,
      )
      val psid =
        PhysicalSynchronizerId.tryFromString(s"da::dummy-psid::${testedProtocolVersion.toString}-0")
      val effectiveTime = EffectiveTime.MinValue
      val persistsContracts = mock[TargetParticipantAcsPersistence.PersistsContracts]
      val requestTracker = mock[RequestTracker]
      val indexingStore = new InMemoryPartyReplicationIndexingStore(
        pauseIndexingDuringOnPR = true,
        loggerFactory = loggerFactory,
      )(executionContext)

      val fakeProgress = new FakeAcsReplicationProgress()

      fakeProgress.state = Some(
        PartyReplicationStatus.EphemeralFileImporterProgress(
          NonNegativeLong.zero,
          RepairCounter.Genesis,
          fullyProcessedAcs = false,
          mock[PartyReplicationFileImporter],
        )
      )

      // Create 100 mock contracts (they are empty, but we will bypass validation)
      val allContracts =
        (1 to 100).map(_ => ActiveContract.tryCreate(LapiActiveContract.defaultInstance)).toList

      // Crash simulation: Create a source that deliberately crashes at contract 50
      val failingSource: Source[ActiveContract, NotUsed] = Source(allContracts).zipWithIndex
        .map { case (c, idx) =>
          if (idx == 50) throw new RuntimeException("Stream failed midway!")
          c
        }

      // Helper to create our importer with stubbed contract processing
      def createImporter(source: Source[ActiveContract, NotUsed]) = {
        // Because of SyncEphemeralStateFactory.cleanupPersistentState, the importer starts from zero on every invocation
        var runningTotal = 0L

        new PartyReplicationFileImporter(
          requestId,
          psid,
          effectiveTime,
          fakeProgress,
          persistsContracts,
          requestTracker,
          source,
          indexingStore,
          None,
          isClosing = () => false,
          loggerFactory,
        )(executionContext, Materializer(actorSystem)) {
          // Bypass real Daml-LF validation and pretend we successfully imported the chunk
          override def importContracts(
              contracts: NonEmpty[Seq[ActiveContract]]
          )(implicit
              traceContext: TraceContext
          ): EitherT[FutureUnlessShutdown, String, NonNegativeLong] = {
            // Accumulate the count just like the real stream processing would
            runningTotal += contracts.size.toLong

            EitherT.pure[FutureUnlessShutdown, String](
              NonNegativeLong.tryCreate(runningTotal)
            )
          }
        }
      }

      val crashingImporter = createImporter(failingSource)

      for {
        // Run the first import, expecting a Left (failure)
        crashResult <- crashingImporter
          .importAcsSnapshot()
          .value
          .failOnShutdown("Crashing import failed")

        _ = crashResult.isLeft shouldBe true
        _ = crashResult.left.value should include("Stream failed midway!")

        // Verify the checkpoint recorded exactly 50 contracts before failing
        savedStateAfterCrash = fakeProgress.state.value
        _ = savedStateAfterCrash.processedContractCount.unwrap shouldBe 50L
        _ = savedStateAfterCrash.fullyProcessedAcs shouldBe false

        // Resume the import with a fresh source of all 100 contracts
        freshSource = Source(allContracts)
        resumingImporter = createImporter(freshSource)

        // Run the second import, expecting a Right (success)
        resumeResult <- resumingImporter
          .importAcsSnapshot()
          .value
          .failOnShutdown("Resuming import failed")

      } yield {
        resumeResult.isRight shouldBe true

        // Verify the stream restarted from zero and processed all 100 contracts,
        // safely overwriting the first 50 contracts.
        val finalState = fakeProgress.state.value
        finalState.processedContractCount.unwrap shouldBe 100L
        finalState.fullyProcessedAcs shouldBe true
      }
    }
  }

  override protected def afterAll(): Unit = {
    actorSystem.terminate().discard
    super.afterAll()
  }
}
