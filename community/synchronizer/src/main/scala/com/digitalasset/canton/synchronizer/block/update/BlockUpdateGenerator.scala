// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{LsuSequencingTestMessage, ProtocolMessage}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationId,
  AggregationRule,
  AllMembersOfSynchronizer,
  Batch,
  MediatorGroupRecipient,
  MemberRecipientOrBroadcast,
  SequencersOfSynchronizer,
}
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.*
import com.digitalasset.canton.synchronizer.block.data.{BlockEphemeralState, BlockInfo}
import com.digitalasset.canton.synchronizer.block.{BlockEvents, LedgerBlockEvent, RawLedgerBlock}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  InvalidLedgerEvent,
  SequencingTimeNotAdmissible,
}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.time.{
  DisasterRecoverySequencingTimeUpperBound,
  LsuSequencingBounds,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.synchronizer.sequencer.{AnnouncedLsu, SubmissionOutcome}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.util.collection.IterableUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/** Exposes functions that take the deserialized contents of a block from a blockchain integration
  * and compute the new [[BlockUpdate]]s.
  *
  * These functions correspond to the following steps in the block processing stream pipeline:
  *   1. Extracting block events from a raw ledger block ([[extractBlockEvents]]).
  *   1. Chunking such block events into either event chunks terminated by a sequencer-addessed
  *      event or a block completion ([[chunkBlock]]).
  *   1. Validating and enriching chunks to yield block updates ([[processBlockChunk]]).
  *
  * In particular, these functions are responsible for the final timestamp assignment of a given
  * submission request. The timestamp assignment works as follows:
  *   1. an initial timestamp is assigned to the submission request by the sequencer that writes it
  *      to the ledger
  *   1. each sequencer that reads the block potentially adapts the previously assigned timestamp
  *      deterministically via `ensureStrictlyIncreasingTimestamp`
  *   1. this timestamp is used to compute the [[BlockUpdate]]s
  *
  * Reasoning:
  *   - Step 1 is done so that every sequencer sees the same timestamp for a given event.
  *   - Step 2 is needed because different sequencers may assign the same timestamps to different
  *     events or may not assign strictly increasing timestamps due to clock skews.
  *
  * Invariants: For step 2, we assume that every sequencer observes the same stream of events from
  * the underlying ledger (and especially that events are always read in the same order).
  */
trait BlockUpdateGenerator {
  import BlockUpdateGenerator.*

  type InternalState

  def internalStateFor(state: BlockEphemeralState): InternalState

  def extractBlockEvents(tracedBlock: Traced[RawLedgerBlock]): Traced[BlockEvents]

  def chunkBlock(block: BlockEvents)(implicit
      traceContext: TraceContext
  ): immutable.Iterable[BlockChunk]

  def processBlockChunk(state: InternalState, chunk: BlockChunk)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate)]

}

object BlockUpdateGenerator {

  sealed trait BlockChunk extends Product with Serializable
  final case class NextChunk(
      blockHeight: Long,
      chunkIndex: Int,
      events: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  ) extends BlockChunk
  final case class MaybeTopologyTickChunk(
      blockHeight: Long,
      baseBlockSequencingTime: CantonTimestamp,
      tickTopologyAtLeastAt: Option[CantonTimestamp],
  ) extends BlockChunk
  final case class EndOfBlock(blockHeight: Long) extends BlockChunk
}

class BlockUpdateGeneratorImpl(
    synchronizerSyncCryptoApi: SynchronizerCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    lsuSequencingBounds: Option[LsuSequencingBounds],
    drSequencingTimeUpperBound: Option[DisasterRecoverySequencingTimeUpperBound],
    getAnnouncedLsu: => Option[AnnouncedLsu],
    producePostOrderingTopologyTicks: Boolean,
    metrics: SequencerMetrics,
    batchingConfig: BatchingConfig,
    consistencyChecks: Boolean,
    memberValidator: SequencerMemberValidator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val closeContext: CloseContext, tracer: Tracer)
    extends BlockUpdateGenerator
    with NamedLogging
    with Spanning {
  import BlockUpdateGenerator.*
  import BlockUpdateGeneratorImpl.*

  private val epsilon = synchronizerSyncCryptoApi.staticSynchronizerParameters.topologyChangeDelay
  private val protocolVersion = synchronizerSyncCryptoApi.psid.protocolVersion
  private val reorderer =
    if (protocolVersion <= ProtocolVersion.v34) BlockReorderer.NoOp
    else new BlockReorderer.Impl(consistencyChecks, loggerFactory)

  private val blockChunkProcessor =
    new BlockChunkProcessor(
      synchronizerSyncCryptoApi,
      sequencerId,
      rateLimitManager,
      orderingTimeFixMode,
      lsuSequencingBounds,
      batchingConfig,
      loggerFactory,
      metrics,
      memberValidator = memberValidator,
    )

  override type InternalState = State

  override def internalStateFor(state: BlockEphemeralState): InternalState = State(
    lastBlockTs = state.latestBlock.lastTs,
    lastChunkTs = state.latestBlock.lastTs,
    latestSequencerEventTimestamp = state.latestBlock.latestSequencerEventTimestamp,
    latestPendingTopologyTransactionTimestamp =
      state.latestBlock.latestPendingTopologyTransactionTimestamp,
    inFlightAggregations = state.inFlightAggregations,
  )

  /** Return true if the event contains only [[LsuSequencingTestMessage]] and recipients are
    * mediator groups. Since the method open envelopes, which is resources consuming, should be
    * called only before upgrade time.
    */
  private def isAllowedBeforeUpgradeTime(event: LedgerBlockEvent) =
    event match {
      case Send(_, signedSubmissionRequest, _, _) =>
        val (openEnvelopes, errors) = Batch.openEnvelopes(signedSubmissionRequest.content.batch)(
          protocolVersion,
          synchronizerSyncCryptoApi.pureCrypto,
        )

        val lsuSequencingTestMessages = openEnvelopes.envelopes.mapFilter(
          ProtocolMessage.select[LsuSequencingTestMessage]
        )

        val isNotTimeProof = openEnvelopes.envelopes.nonEmpty
        val onlyLsuSequencingTestMessages = lsuSequencingTestMessages.sizeCompare(
          openEnvelopes.envelopes
        ) == 0

        val hasNonMediatorGroupRecipient =
          signedSubmissionRequest.content.batch.allRecipients.exists {
            case _: MediatorGroupRecipient => false
            case _ => true
          }

        // if there are only lsu sequencing test messages and if only recipients are mediator groups
        isNotTimeProof && onlyLsuSequencingTestMessages && errors.isEmpty && !hasNonMediatorGroupRecipient

      case _: Acknowledgment => false
    }

  override def extractBlockEvents(tracedBlock: Traced[RawLedgerBlock]): Traced[BlockEvents] =
    withSpan("BlockUpdateGenerator.extractBlockEvents") { blockTraceContext => _ =>
      val block = tracedBlock.value

      val ledgerBlockEvents = block.events.mapFilter { tracedEvent =>
        withSpan("BlockUpdateGenerator.extractBlockEvents") { implicit traceContext => _ =>
          // TODO(i29003): Defer decompression to addSnapshotsAndValidateSubmissions
          val maxBytesToDecompress = MaxBytesToDecompress.HardcodedDefault
          LedgerBlockEvent.fromRawBlockEvent(protocolVersion, maxBytesToDecompress)(
            tracedEvent.value
          ) match {
            case Left(error) =>
              InvalidLedgerEvent.Error(block.blockHeight, error).discard
              None

            case Right(event) =>
              val checksResult = for {
                _ <- checkLsuSequencingBounds(event, lsuSequencingBounds)
                _ <- checkDrSequencingTimeUpperBound(event, drSequencingTimeUpperBound)
              } yield ()

              checksResult.fold(
                err => {
                  err.log()
                  None
                },
                _ => Some(Traced(event)),
              )
          }
        }(tracedEvent.traceContext, tracer)
      }

      Traced(
        BlockEvents(
          block.blockHeight,
          CantonTimestamp.assertFromLong(block.baseSequencingTimeMicrosFromEpoch),
          // Reorder block events according to BlockReorderer priority before constructing BlockEvents.
          // (starting with pv35)
          reorderer.reordered(ledgerBlockEvents)(blockTraceContext),
          tickTopologyAtLeastAt =
            block.tickTopologyAtMicrosFromEpoch.map(CantonTimestamp.assertFromLong),
        )
      )(blockTraceContext)
    }(tracedBlock.traceContext, tracer)

  private def checkDrSequencingTimeUpperBound(
      event: LedgerBlockEvent,
      drSequencingTimeUpperBound: Option[DisasterRecoverySequencingTimeUpperBound],
  ): Either[CantonBaseError, Unit] =
    drSequencingTimeUpperBound match {
      case Some(drSequencingTimeUpperBound) =>
        Either.cond(
          event.timestamp < drSequencingTimeUpperBound.ts,
          (),
          SequencingTimeNotAdmissible.Error
            .afterOrAtUpperBound(event.timestamp, drSequencingTimeUpperBound.ts, event.toString),
        )

      case None => Right(())
    }

  /*
    For an event with sequencing time `ts`:
    - If `ts <= lsuSequencingBounds.lowerBoundSequencingTimeExclusive`, then fail.
    - If `lsuSequencingBounds.lowerBoundSequencingTimeExclusive < ts <= lsuSequencingBounds.upgradeTime`,
      then the check should be successful only if `isAllowedBeforeUpgradeTime(event)` is true.
    - If `ts > upgradeTime`, then success.
   */
  private def checkLsuSequencingBounds(
      event: LedgerBlockEvent,
      lsuSequencingBounds: Option[LsuSequencingBounds],
  ): Either[CantonBaseError, Unit] =
    lsuSequencingBounds match {
      case Some(lsuSequencingBounds) =>
        if (event.timestamp <= lsuSequencingBounds.lowerBoundSequencingTimeExclusive) {
          SequencingTimeNotAdmissible.Error
            .beforeOrAtLowerBound(
              sequencingTime = event.timestamp,
              lowerBound = lsuSequencingBounds.lowerBoundSequencingTimeExclusive,
              event.toString,
            )
            .asLeft
        } else if (event.timestamp <= lsuSequencingBounds.upgradeTime) {
          Either.cond(
            isAllowedBeforeUpgradeTime(event),
            (),
            SequencingTimeNotAdmissible.Error
              .beforeOrAtLowerBound(
                sequencingTime = event.timestamp,
                lowerBound = lsuSequencingBounds.upgradeTime,
                event.toString,
              ),
          )
        } else ().asRight

      case None => ().asRight
    }

  override def chunkBlock(
      blockEvents: BlockEvents
  )(implicit traceContext: TraceContext): immutable.Iterable[BlockChunk] = {
    val blockHeight = blockEvents.height
    metrics.block.height.updateValue(blockHeight)

    val tick = MaybeTopologyTickChunk(
      blockHeight,
      blockEvents.baseBlockSequencingTime,
      blockEvents.tickTopologyAtLeastAt,
    )

    // We must start a new chunk whenever the chunk processing advances lastSequencerEventTimestamp,
    //  otherwise the logic for retrieving a topology snapshot or traffic state could deadlock.
    val dataChunks = IterableUtil
      .splitAfter(blockEvents.events)(event => isAddressingSequencers(event.value))
      .zipWithIndex
      .map { case (events, index) =>
        NextChunk(blockHeight, index, events)
      }

    val chunks = dataChunks ++ Seq(tick) ++ Seq(EndOfBlock(blockHeight))

    logger.debug(
      s"Chunked block $blockHeight into ${dataChunks.size} data chunks and 1 topology tick chunk"
    )

    chunks
  }

  private def isAddressingSequencers(event: LedgerBlockEvent): Boolean =
    event match {
      case Send(_, signedOrderingRequest, _, _) =>
        val allRecipients =
          signedOrderingRequest.content.batch.allRecipients
        allRecipients.contains(AllMembersOfSynchronizer) ||
        allRecipients.contains(SequencersOfSynchronizer)
      case _ => false
    }

  override final def processBlockChunk(state: InternalState, chunk: BlockChunk)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate)] =
    chunk match {
      case EndOfBlock(height) =>
        val newState = state.copy(lastBlockTs = state.lastChunkTs)
        val update = CompleteBlockUpdate(
          BlockInfo(
            height,
            state.lastChunkTs,
            state.latestSequencerEventTimestamp,
            state.latestPendingTopologyTransactionTimestamp,
          )
        )
        logger.debug(s"Block $height completed with update $update")
        FutureUnlessShutdown.pure(newState -> update)
      case NextChunk(height, index, chunksEvents) =>
        blockChunkProcessor.processDataChunk(state, height, index, chunksEvents)
      case MaybeTopologyTickChunk(blockHeight, baseBlockSequencingTime, tickTopologyAtLeastAt) =>
        lazy val createTick = state.latestPendingTopologyTransactionTimestamp.exists { ts =>
          // If the latest topology transaction becomes effective between the end of the previous block and the end of
          // the current block, we will broadcast a tick at the end of the current block so all sequencer clients can
          // notice that enough time has passed for the topology state to be effective.
          //
          // We only keep track of the latest topology transaction instead of all currently non-effective transactions.
          // Reasoning: let's say we get 2 consecutive topology transactions T1 and T2, so either:
          // - T1 becomes effective in block B1, and T2 comes in a later block B2, in that case a tick is created in block B1.
          // - T1 becomes effective in block B1, and T2 comes in the same block after T1's effective time.
          //    We stop keeping track of T1's timestamp, but that's ok, because T2 acts as a tick for T1.
          // - T2 comes before T1 becomes effective. In that case, we stop keeping track of T1's timestamp, in favor of T2's.
          //    If T2 becomes effective on the same block as T1, then it makes no difference.
          //    If T2 becomes effective on the next block from where T1 becomes effective, then it means T1's tick will get
          //    delayed by one block and that, in general, fewer ticks are potentially created, which is a desirable outcome.
          val latestTopologyTransactionEffectiveTime = ts + epsilon
          val blockEnd = state.lastChunkTs.immediateSuccessor.max(baseBlockSequencingTime)
          state.lastBlockTs < latestTopologyTransactionEffectiveTime && latestTopologyTransactionEffectiveTime < blockEnd
        }

        getAnnouncedLsu.map(_.successor) match {
          case Some(upgrade)
              if upgrade.upgradeTime <= baseBlockSequencingTime && upgrade.upgradeTime > state.lastBlockTs =>
            logger.info(
              s"Emitting an LSU tick for the upgrade $upgrade at block $blockHeight with base sequencing time $baseBlockSequencingTime"
            )
            blockChunkProcessor.emitTick(
              state.copy(
                // There shouldn't be topology changes activated after the LSU upgrade time
                latestPendingTopologyTransactionTimestamp = None
              ),
              blockHeight,
              upgrade.upgradeTime,
              Left(AllMembersOfSynchronizer),
            )
          case _ =>
            // Starting with protocol version 35, topology ticks can be deterministically injected post-ordering
            // by sequencers, making time proofs unnecessary for observing topology transactions becoming effective.
            if (
              protocolVersion >= ProtocolVersion.v35 && producePostOrderingTopologyTicks && createTick
            ) {
              blockChunkProcessor.emitTick(
                state.copy(
                  // important to do this update from here instead of from inside emitTick,
                  // because if a tick is created using other currently supported methods such as time proof requests,
                  // we would lose track of this timestamp prematurely.
                  latestPendingTopologyTransactionTimestamp = None
                ),
                blockHeight,
                // DABFT assigns monotonically increasing timestamps to all ordered requests, including acks,
                //  but the sequencer does not (because acks are not events), so if an epoch ends with an ack and
                //  DABFT expects a tick at a certain timestamp to be able to query a topology snapshot and
                //  establish the ordering topology fot the next epoch, we must make sure that sequencing time advances
                //  at least until that timestamp, else the topology snapshot query could get stuck and the system
                //  could deadlock.
                tickAtLeastAt = state.lastChunkTs
                  .max(baseBlockSequencingTime)
                  .max(tickTopologyAtLeastAt.getOrElse(CantonTimestamp.MinValue)),
                groupRecipient = Left(AllMembersOfSynchronizer),
              )
            } else {
              tickTopologyAtLeastAt match {
                // The pre-protocol version 35 topology ticks is also still supported and
                // only the BFT sequencer can request to inject these topology ticks
                case Some(tickTopologyAtLeastAt) =>
                  blockChunkProcessor.emitTick(
                    state,
                    blockHeight,
                    tickTopologyAtLeastAt,
                    Right(SequencersOfSynchronizer),
                  )
                case None =>
                  FutureUnlessShutdown.pure((state, ChunkUpdate.noop))
              }
            }
        }
    }

}

object BlockUpdateGeneratorImpl {

  /** Internal state
    *
    * @param latestPendingTopologyTransactionTimestamp
    *   is used to determine whether a topology tick should be emitted at the end of the block, so
    *   it is updated whenever we see a topology transaction. We only use it to decide if we should
    *   emit a tick at the end of a block. It may be incorrect if a topology tx was rejected, but
    *   that doesn't matter much from the perspective of "ticking" the topology.
    */
  private[block] final case class State(
      lastBlockTs: CantonTimestamp,
      lastChunkTs: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      latestPendingTopologyTransactionTimestamp: Option[CantonTimestamp],
      inFlightAggregations: InFlightAggregations,
  )

  /** Positive outcome of the pre-validation step
    *
    * Case class used to carry over data from the parallel validation into the sequential validation
    * step
    *
    * @param recipients
    *   the resolved recipients for this request
    * @param aggregationInfo
    *   the computed aggregation-id and the validated aggregation rule
    */
  private[update] final case class PrevalidationOutcome(
      recipients: Set[MemberRecipientOrBroadcast],
      aggregationInfo: Option[(AggregationId, AggregationRule)],
  )

  /** Result of the parallel validation
    *
    * This step contains all the validation results which can be performed independently and
    * therefore can run in parallel
    */
  private[update] final case class SequencedPreValidatedSubmissionResult(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SignedSubmissionRequest,
      orderingSequencerId: SequencerId,
      consumeTraffic: SubmissionRequestValidator.TrafficConsumption,
      errorOrPrevalidationOutcome: Either[SubmissionOutcome, PrevalidationOutcome],
      sequencingSnapshot: TopologySnapshot,
  )(val traceContext: TraceContext)

}
