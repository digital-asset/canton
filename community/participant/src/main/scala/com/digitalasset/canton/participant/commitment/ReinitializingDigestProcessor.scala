// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  CheckpointFence,
  CheckpointFenceOr,
  ContractChange,
  ContractChangeBatch,
  NotCheckpointFence,
  ProcessingContext,
}
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  CheckpointType,
  DigestJournal,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

trait ReinitializingDigestProcessor extends BaseDigestProcessor {
  def reinitializingTimepoint: Option[Timepoint]
}

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
  *      [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.ContractChangeBatch]]es
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
  */
class ReinitializingDigestProcessorImpl(
    thisParticipantId: ParticipantId,
    override val synchronizerId: SynchronizerId,
    acsCommitmentConfig: AcsCommitmentConfig,
    digestAccumulator: DigestAccumulator,
    protected override val acsDigestStore: AcsDigestStore,
    indexService: InternalIndexService,
    getTopologySnapshot: Traced[CantonTimestamp] => FutureUnlessShutdown[TopologySnapshot],
    ledgerApiStore: LedgerApiStore,
    enableAdditionalConsistencyChecks: Boolean,
    private[canton] override val metrics: CommitmentMetrics,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    mat: Materializer,
) extends ReinitializingDigestProcessor {

  private val thisLfParticipantId: LedgerParticipantId = thisParticipantId.toLf
  private val writeJournalTombstonesBatchSize =
    acsCommitmentConfig.reinitializingJournalTombstonesBatchSize.unwrap
  private val counterpartyBatchSize = acsCommitmentConfig.counterpartyBatchSize.unwrap
  private val reinitializingTimepointRef = new AtomicReference[Option[Timepoint]](None)

  override def reinitializingTimepoint: Option[Timepoint] = reinitializingTimepointRef.get()

  override protected def startPipelineInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(KillSwitch, Future[Unit])] =
    for {
      // reinit - N: Offset, T(N): CantonTimeStamp = Ledger End
      reinitializingTimepoint <- ledgerEndTimepointFUS()

      _ = reinitializingTimepointRef.set(Some(reinitializingTimepoint))

      // Delete potential updates in the future
      _ <- acsDigestStore.deleteAfter(reinitializingTimepoint.offset)

      _ <- writeTombstonesToJournals(
        tombstoneTimepoint = reinitializingTimepoint
      )
      topologySnapshot <- getTopologySnapshot(Traced(reinitializingTimepoint.recordTime))
    } yield {
      val (ks, doneF) = PekkoUtil
        .runSupervised(
          reinitializeContractChanges(
            reinitializingTimepoint = reinitializingTimepoint,
            topologySnapshot = topologySnapshot,
          ).via(digestAccumulator.flow())
            .mapAsyncAndDrainUS(1)(writeCheckpoint)
            .toMat(Sink.ignore)(Keep.both),
          errorLogMessagePrefix = "RecomputeAndAppendNewDigestsToJournal",
        )
      (ks, doneF.void)
    }

  private[commitment] def reinitializeContractChanges(
      reinitializingTimepoint: Timepoint,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): Source[ProcessingContext[CheckpointFenceOr[ContractChangeBatch]], KillSwitch] = {
    metrics.reinitializeParties.updateValue(0)
    metrics.reinitializeContractChanges.updateValue(0)
    val acsUpdates = indexService
      .counterParties(synchronizerId, reinitializingTimepoint.offset, party = None)
      .viaMat(KillSwitches.single)(Keep.right)
      .grouped(counterpartyBatchSize)
      .flatMap { counterparties =>
        val counterpartiesSet = counterparties.toSet
        metrics.reinitializeParties.updateValue(_ + counterpartiesSet.size)

        indexService
          .acs(synchronizerId, reinitializingTimepoint.offset, counterpartiesSet, Set.empty)
          .batch(
            max = acsCommitmentConfig.contractChangeClassificationBatchSize.unwrap.toLong,
            Vector(_),
          )(_ :+ _)
          // we get all the active contracts by the offset
          .mapAsyncAndDrainUS(1) { activeContractsOfCounterparties =>
            metrics.reinitializeContractChanges.updateValue(_ + 1)
            val stakeholdersOfContracts =
              activeContractsOfCounterparties.iterator.flatMap(_.stakeholders).toSet

            for {
              // get the map of (party -> Set of participants where it is onboarded)
              partyToParticipant <- getOnboardedParticipantsOfParties(
                topologySnapshot,
                stakeholdersOfContracts,
              )
            } yield {
              val contractChanges = activeContractsOfCounterparties.map {
                activeContractOfCounterparty =>
                  // emit the classification update for all stakeholders of the current stakeholder batch
                  // of the contract and their respective hosting participants.
                  val counterpartyStakeholders =
                    activeContractOfCounterparty.stakeholders.iterator
                      .filter(counterpartiesSet.contains)
                      .toSet

                  val locallyHostedStakeholders =
                    activeContractOfCounterparty.stakeholders.iterator.filter { sh =>
                      partyToParticipant.getOrElse(sh, Set.empty).contains(thisLfParticipantId)
                    }.toSeq

                  ContractChange(
                    counterpartyStakeholders,
                    locallyHostedStakeholders,
                    activeContractOfCounterparty.contractId,
                    activeContractOfCounterparty.reassignmentCounter,
                    isActivation = true,
                  )
              }

              val counterpartiesToParticipant = activeContractsOfCounterparties.iterator
                .flatMap(_.stakeholders)
                .distinct
                .filter(counterpartiesSet)
                .map(party => party -> partyToParticipant.getOrElse(party, Set.empty))
                .toMap

              ProcessingContext(
                reinitializingTimepoint,
                NotCheckpointFence(
                  topologySnapshot,
                  ContractChangeBatch.create(
                    counterpartiesToParticipant,
                    contractChanges,
                    enableAdditionalConsistencyChecks,
                  ),
                ),
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
    Seq[AcsDigestStore.DigestJournal[?]](acsDigestStore.party, acsDigestStore.participant)
      .parTraverse_(store =>
        writeTombstonesTo(store)(tombstoneTimepoint, writeJournalTombstonesBatchSize)
      )

  private def writeTombstonesTo[K](journal: AcsDigestStore.DigestJournal[K])(
      tombstoneTimepoint: Timepoint,
      pageSize: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    DigestJournal
      .processSnapshotInBatchesE(journal)(tombstoneTimepoint.offset, pageSize) { acsDigests =>
        val tombstones = createTombstonesFrom(acsDigests, tombstoneTimepoint)
        journal.upsertDigestUpdates(tombstones)
      }

  private def createTombstonesFrom[K](
      acsDigestUpdates: immutable.Iterable[AcsDigestUpdate[K]],
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
}
