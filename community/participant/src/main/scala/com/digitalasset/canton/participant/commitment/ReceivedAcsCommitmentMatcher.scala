// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchedCommitmentMatchPeriod,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.AcsCommitmentProtocolMessage
import com.digitalasset.canton.tracing.{TraceContext, Traced, TracedMany}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{
  GarbageCollectedShardedSequentialProcessingQueue,
  ShardedSequentialProcessingQueue,
}
import com.digitalasset.canton.{LedgerParticipantId, checked}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/** Processes received ACS commitments in
  * [[com.digitalasset.canton.ledger.participant.state.InternalIndexService.AcsUpdateContainer]]:
  *   - Checks them against the outstanding and mismatched periods in the `store`
  *   - Updates the store with the matching outcome
  *   - Maintains a watermark for the last offset for which all envelopes have been processed
  */
class ReceivedAcsCommitmentMatcher(
    store: AcsCommitmentPeriodStore,
    stringInterningEval: Eval[StringInterning],
    override protected val loggerFactory: NamedLoggerFactory,
    parallelProcessingLimit: PositiveInt,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import ReceivedAcsCommitmentMatcher.*

  private val queue: ShardedSequentialProcessingQueue[LedgerParticipantId] =
    new GarbageCollectedShardedSequentialProcessingQueue[LedgerParticipantId]

  def flow(implicit traceContext: TraceContext): Flow[
    InternalIndexService.AcsUpdateContainer,
    Unit,
    NotUsed,
  ] =
    Flow[InternalIndexService.AcsUpdateContainer]
      .mapConcat(parse)
      .mapAsyncAndDrainUS(parallelism = parallelProcessingLimit.value)(dispatchToQueue)
      .mapConcat(_.toList)
      .conflateWithSeed(TracedMany.fromTraced)((acc, next) =>
        acc.accumulateTraced(offset => next.map(_.max(offset)))
      )
      .mapAsyncAndDrainUS(parallelism = 1)(persistWatermark)

  private def parse(
      input: InternalIndexService.AcsUpdateContainer
  ): Seq[AcsCommitmentMessageContainer] = {
    implicit val traceContext: TraceContext = input.traceContext
    input.acsUpdate match {
      case InternalIndexService.AcsUpdate.AcsCommitment(payload) =>
        ReceivedAcsCommitments.fromTrustedByteString(payload) match {
          case Right(commitments) =>
            val lastIndex = commitments.messages.size - 1
            commitments.messages.view.zipWithIndex.map { case (envelope, index) =>
              AcsCommitmentMessageContainer(envelope, input.offset, index == lastIndex)
            }.toSeq
          case Left(err) =>
            logger.warn(
              s"Failed to parse received ACS commitment at offset ${input.offset}. Discarding the update. $err"
            )
            Seq.empty
        }

      case _: InternalIndexService.AcsUpdate.AcsChangeUpdate => Seq.empty
      case _: InternalIndexService.AcsUpdate.EffectiveTopologyUpdate => Seq.empty
    }
  }

  private def dispatchToQueue(
      container: AcsCommitmentMessageContainer
  ): FutureUnlessShutdown[Option[Traced[Offset]]] = {
    implicit val traceContext: TraceContext = container.traceContext
    val offset = container.offset
    queue.executeUS(container.envelope.acsCommitment.sender)(
      processMessage(offset, container.envelope, container.lastEnvelopeInBatch),
      s"Process envelope at offset $offset",
    )
  }

  private def processMessage(
      offset: Offset,
      envelope: AcsCommitmentProtocolMessage,
      lastEnvelopeForOffset: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[Traced[Offset]]] = {
    val senderExternalized = envelope.acsCommitment.sender
    val sender = checked(
      // The signature check in ReceivedAcsCommitmentValidatorImpl.checkCommitmentSignature ensures
      // that the indexer has seen the counterparticipant ID and internalized it. Therefore it is
      // safe to internalize the sender. In detail:
      //  - The signature check only succeeds if the sender has an active `SynchronizerTrustCertificate`.
      //  - When the `SynchronizerTrustCertificate` becomes active, the indexer is notified about the admin party
      //    of the participant.
      //  - When the indexer is notified about a party, it adds the hosting participants to the string interning table.
      stringInterning.participantId.internalize(senderExternalized)
    )
    val period = envelope.acsCommitment.period
    val digest = envelope.acsCommitment.digest

    def partition[Digest, Off](
        overlapping: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]]
    ): (Seq[CommitmentMatchPeriod[Digest, Off]], Seq[CommitmentMatchPeriod[Digest, Off]]) = {
      val outside = Seq.newBuilder[CommitmentMatchPeriod[Digest, Off]]
      val inside = Seq.newBuilder[CommitmentMatchPeriod[Digest, Off]]

      overlapping.foreach { overlap =>
        val (out, in) = overlap.partition(period.fromExclusive, period.toInclusive)
        outside.addAll(out)
        inside.addOne(in)
      }

      (outside.result(), inside.result())
    }

    def toMatched[Digest, Off](
        intervals: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]]
    ): immutable.Iterable[MatchedCommitmentMatchPeriod] =
      intervals.map(_.copy(hashedDigest = (), offset = offset))

    for {
      outstanding <- store.lookupOutstanding(Seq(sender -> period))
      matchesMismatched <- store.lookupMismatchedByHash(Seq((sender, digest, period)))
      _ <- {
        val (outstandingOutside, outstandingInside) = partition(outstanding)
        val (mismatchOutside, mismatchToMatch) = partition(matchesMismatched)
        val (outstandingToMatch, outstandingToMismatch) =
          outstandingInside.partition(_.hashedDigest == digest)
        val outstandingMatchesToInsert = toMatched(outstandingToMatch)
        val mismatchedMatchesToInsert = toMatched(mismatchToMatch)
        val outstandingMismatchesToInsert = outstandingToMismatch.map { interval =>
          interval.copy(offset = offset, hashedDigest = Some(interval.hashedDigest))
        }

        store.persistMatchingOutcome(
          deleteOutstanding = outstanding,
          deleteMismatched = matchesMismatched,
          insertOutstanding = outstandingOutside,
          insertMismatchedOrUnexpected = mismatchOutside ++ outstandingMismatchesToInsert,
          insertMatched = outstandingMatchesToInsert ++ mismatchedMatchesToInsert,
        )
      }
      // TODO(#34324) Check whether the commitment was unexpected
    } yield Option.when(lastEnvelopeForOffset)(Traced(offset))
  }

  private def persistWatermark(offset: TracedMany[Offset]): FutureUnlessShutdown[Unit] = {
    implicit val batchTraceContext: TraceContext =
      TraceContext.ofBatch("persist-watermark-matching")(offset.traceContexts)(logger)
    store.increaseMatcherWatermark(offset.value)
  }

  private def stringInterning: StringInterning = stringInterningEval.value

}

object ReceivedAcsCommitmentMatcher {

  private final case class AcsCommitmentMessageContainer(
      envelope: AcsCommitmentProtocolMessage,
      offset: Offset,
      lastEnvelopeInBatch: Boolean,
  )(implicit val traceContext: TraceContext)

}
