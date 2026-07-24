// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.{Eval, Monad}
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  CheckpointFenceOr,
  NotCheckpointFence,
  ProcessingContext,
}
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  CheckpointType,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Source}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/** Used to reinitialize the ACS commitment checkpoint store, party - and participant digest
  * journals.
  *
  * Note: RunningDigestProcessor is running mutually exclusively with the Reinitializing thus it is
  * safe to assume there is no writing into the digest store when we run this process
  *
  * Sequential data flow upon calling the start():
  *   1. Using the synchronizerId, it gets the ledger End `lastOffset`:
  *      [[com.digitalasset.canton.data.Offset]] and corresponding `recordTime`:
  *      [[com.digitalasset.canton.data.CantonTimestamp]] creating the
  *      [[com.digitalasset.canton.participant.commitment.Timepoint]] for reinitialization.
  *   1. Get a `snapshot` for all digests in both party - and participant ACS Digest journals
  *   1. For each digest's key, It places a tombstone at the
  *      [[com.digitalasset.canton.participant.commitment.Timepoint]] calculated in the first step.
  *      Tombstone means an empty Digest [[scala.None]]. This marks the reinitialization process for
  *      each keys in the store.
  *   1. Based on the topology snapshot at
  *      [[com.digitalasset.canton.participant.commitment.Timepoint]], create the
  *      [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.ProcessingContext]]
  *      events for
  *      [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.AcsUpdate]]s
  *   1. Call the [[com.digitalasset.canton.participant.commitment.InMemoryDigestAccumulator]] to
  *      calculate the digests and (over)write the tombstones with this recalculated value. Note:
  *      this (over)write can be partial, because there are keys which represents
  *      parties/participants offboarded or deactivated contracts, hence we can still see tombstones
  *      at the reinitializing [[com.digitalasset.canton.participant.commitment.Timepoint]]s
  *   1. Append a new checkpoint at the reinitializing
  *      [[com.digitalasset.canton.participant.commitment.Timepoint]]
  *
  * TODO(#33422) - Disaster Recovery Notes: For DR, we either need stable order of ACS stream, to
  * continue where we left off or solve it in another way
  *
  * TODO(#33422) - Handle shutdown correctly
  *
  * TODO(#33422) - either add a flag to the existing `reinitialize_commitments` command or create a
  * new one (should be discussed)
  */
final class ReinitializingDigestProcessor(
    synchronizerId: SynchronizerId,
    thisParticipantId: ParticipantId,
    stringInterningEval: Eval[StringInterning],
    indexService: InternalIndexService,
    ledgerApiStore: LedgerApiStore,
    acsDigestStore: AcsDigestStore,
    acsCommitmentConfig: AcsCommitmentConfig,
    getTopologySnapshot: Traced[CantonTimestamp] => TopologySnapshot,
    hashOps: HashOps,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    mat: Materializer,
) extends NamedLogging
    with BaseDigestProcessor {

  private val thisLfParticipantId: LedgerParticipantId = thisParticipantId.toLf
  private val writeJournalTombstonesBatchSize =
    acsCommitmentConfig.reinitializingJournalTombstonesBatchSize.unwrap
  private val counterpartyBatchSize = acsCommitmentConfig.counterpartyBatchSize.unwrap
  private val tracingMode = acsCommitmentConfig.tracing

  override def start()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = for {
    // reinit - N: Offset, T(N): CantonTimeStamp = Ledger End
    reinitializingTimepoint <- ledgerEndTimepointFUS()

    // Delete potential updates in the future
    _ <- acsDigestStore.deleteAfter(reinitializingTimepoint.offset)

    _ <- writeTombstonesToJournals(
      tombstoneTimepoint = reinitializingTimepoint
    )

    _ <-
      PekkoUtil
        .runSupervised(
          reinitAcsUpdates(
            reinitializingTimepoint = reinitializingTimepoint,
            topologySnapshot = getTopologySnapshot(Traced(reinitializingTimepoint.recordTime)),
          ).via(inMemoryDigestAccumulator)
            .toMat(PekkoUtil.sinkIgnoreFUS)(Keep.right),
          errorLogMessagePrefix = "RecomputeAndAppendNewDigestsToJournal",
        )
  } yield ()

  private[commitment] def reinitAcsUpdates(
      reinitializingTimepoint: Timepoint,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): Source[ProcessingContext[CheckpointFenceOr[AcsUpdate]], NotUsed] = {
    val acsUpdates = indexService
      .counterParties(synchronizerId, reinitializingTimepoint.offset, party = None)
      .grouped(counterpartyBatchSize)
      .flatMap { counterparties =>
        val counterpartiesSet = counterparties.toSet

        indexService
          .acs(synchronizerId, reinitializingTimepoint.offset, counterpartiesSet, Set.empty)
          // we get all the active contracts by the offset
          .mapAsyncAndDrainUS(1) { activeContractOfCounterparty =>
            val stakeholdersOfContract = activeContractOfCounterparty.stakeholders

            for {
              // get the map of (party -> Set of participants where it is onboarded)
              partyToParticipant <- getOnboardedParticipantsOfParties(
                topologySnapshot,
                stakeholdersOfContract,
              )
            } yield {
              val stakeholdersToHostingParticipants = stakeholdersOfContract.view
                .filter(counterpartiesSet.contains)
                .map { sh =>
                  sh -> partyToParticipant
                    .getOrElse(sh, Set.empty)
                }
                .toMap

              val thisParticipantStakeholders = partyToParticipant.collect {
                case (party, hostingParticipants)
                    if hostingParticipants.contains(thisLfParticipantId) =>
                  party
              }

              val acsUpdate = AcsUpdate(
                stakeholdersToHostingParticipants,
                thisParticipantStakeholders.toSeq,
                activeContractOfCounterparty.contractId,
                activeContractOfCounterparty.reassignmentCounter,
                isActivation = true,
              )

              ProcessingContext(
                reinitializingTimepoint,
                NotCheckpointFence(topologySnapshot, acsUpdate),
              )
            }
          }

      }

    acsUpdates.concat(
      Source.single(
        ProcessingContext(reinitializingTimepoint, CheckpointFence(CheckpointType.Reinitialization))
      )
    )
  }

  private[commitment] def writeTombstonesToJournals(
      tombstoneTimepoint: Timepoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    Seq[AcsDigestStore.DigestJournal[?, ?]](acsDigestStore.party, acsDigestStore.participant)
      .parTraverse_(store =>
        writeTombstonesTo(store)(tombstoneTimepoint, writeJournalTombstonesBatchSize)
      )

  private def writeTombstonesTo[K, V](journal: AcsDigestStore.DigestJournal[K, V])(
      tombstoneTimepoint: Timepoint,
      pageSize: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    type LoopState = Either[journal.SnapshotPaginationToken, journal.AtInclusive]
    val initialState: LoopState = Right(tombstoneTimepoint.offset)

    Monad[FutureUnlessShutdown].tailRecM(initialState) { currentState =>
      for {
        (acsDigests, doneOrNextToken) <- journal.snapshot(currentState, pageSize)

        nextStateE = doneOrNextToken.swap match {
          case Right(_paginationTokenDone) => Either.unit
          case Left(token) => Left(Left(token))
        }

        tombstoneDigests = createTombstonesFrom(acsDigests, tombstoneTimepoint)

        _ <- journal.upsertDigestUpdates(tombstoneDigests)
      } yield nextStateE
    }
  }

  private def createTombstonesFrom[K, V](
      acsDigestUpdates: immutable.Iterable[AcsDigestUpdate[K, V]],
      tombstoneTimepoint: Timepoint,
  ) =
    acsDigestUpdates.map { acsDigestUpdate =>
      val newReplacesOffsetTime =
        // When we already have an update at the tombstone offset we need to point back properly to the last update
        if (tombstoneTimepoint.offset == acsDigestUpdate.digestUpdate.offset)
          acsDigestUpdate.replacesOffset
        // otherwise it is a past update so we are good to use the digest's offset
        else Some(acsDigestUpdate.digestUpdate.offset)
      AcsDigestStore.AcsDigestUpdate(
        digestUpdate = AcsDigest(
          key = acsDigestUpdate.digestUpdate.key,
          offset = tombstoneTimepoint.offset,
          timestamp = tombstoneTimepoint.recordTime,
          digestO = None,
          trace = None,
        ),
        replacesOffset = newReplacesOffsetTime,
      )
    }

  private[commitment] def ledgerEndTimepointFUS()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Timepoint] =
    for {
      // TODO(#33422) - Once the Github issue 27992 is solved, switch to new method
      ledgerEndO <- FutureUnlessShutdown.pure(ledgerApiStore.ledgerEnd)

      reinitTimepoint = ledgerEndO
        .flatMap { end =>
          end.synchronizerIndices
            .get(synchronizerId)
            .map(index => Timepoint(end.lastOffset)(index.recordTime))
        }
        .getOrElse(ErrorUtil.invalidState("There is no suitable last offset in the Ledger"))

    } yield reinitTimepoint

  override protected def digestAccumulator: SequentialDigestAccumulator =
    new SequentialDigestAccumulator(
      thisLfParticipantId,
      acsDigestStore,
      stringInterningEval.value,
      hashOps,
      tracingMode,
      loggerFactory,
    )

}
