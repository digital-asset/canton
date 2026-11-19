// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SequencerDeliverError,
  SignedContent,
}
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

import BlockSequencerFactory.OrderingTimeFixMode

/** The interface to the ordering functionality for a block sequencer.
  *
  * It is not BFT-specific, but an implementation may be Byzantine Fault Tolerant with respect to
  * the ordering functionality.
  *
  * BFT reads and writes require, however, client cooperation: if `f` is the maximum number of
  * tolerated faulty sequencer nodes, then a BFT read requires reading `f+1` streams and BFT writes
  * require either writing to `f+1` sequencers or writing to a single sequencer and be prepared to
  * retry on another sequencer if a BFT read for the corresponding event fails.
  *
  * An implementation must not rely on its output being final, as reordering and timestamp changes
  * are possible post-ordering.
  */
trait BlockOrderer extends AutoCloseable {

  /** The height of the first block to be returned by `subscribe`. */
  def firstBlockHeight: Long

  /** Determines if block timestamps emitted by `subscribe` are strictly monotonically increasing.
    */
  def orderingTimeFixMode: OrderingTimeFixMode

  /** Delivers a stream of blocks.
    *
    * Honest and correct implementations must ensure the following:
    *
    *   - If `firstBlockHeight` refers to a block whose sequencing number the sequencer node has not
    *     yet observed, returns a source that will eventually serve that block when it gets created.
    *   - This method can be called only once per instance, so implementations do not have to try to
    *     create separate sources on every call to this method: it is acceptable for the
    *     implementation to have one internal source and just return it.
    *   - The call must succeed if an earlier (across instances and restarts) call to `subscribe`
    *     delivered a block with height `firstBlockHeight` unless the block has been pruned in
    *     between, in which case it fails.
    *   - Block heights must be consecutive and start at `firstBlockHeight`.
    *   - For a given block height, all sequencer nodes of a synchronizer must emit the same block
    *     for that height.
    *   - Every submission that has been sent to an honest node through `send` will be included in
    *     the output stream on a best effort basis. That means, the output stream normally contains
    *     every submission, but submissions may sometimes be dropped due to high load, crashes,
    *     etc...
    *   - A submission may occur more than once in the output stream, as malicious sequencer nodes
    *     may replay requests.
    *   - An event in the output can not necessarily be parsed to
    *     [[com.digitalasset.canton.synchronizer.block.LedgerBlockEvent]]. If they can be parsed,
    *     the embedded signatures are not necessarily valid.
    *   - The events in the block may still be reordered, re-timestamped and extended by the
    *     post-processing pipeline of the synchronizer. Therefore, the BlockOrderer should not rely
    *     on the order of the events being preserved.
    */
  def subscribe()(implicit
      traceContext: TraceContext
  ): Source[Traced[RawLedgerBlock], KillSwitch]

  /** Orders a sender-signed submission. If the sequencer node is honest, this normally results in a
    * [[block.RawLedgerBlock.RawBlockEvent.Send]]. In exceptional cases (crashes, high load, ...), a
    * sequencer may drop submissions.
    */
  def send(signedSubmissionRequest: SignedSubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDeliverError, Unit]

  /** Orders an event reception acknowledgement. If the sequencer node is honest, this normally
    * results in a [[block.RawLedgerBlock.RawBlockEvent.Acknowledgment]]. In exceptional cases
    * (crashes, high load, ...), a sequencer may drop acknowledgements.
    */
  def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerDeliverError, Unit]

  def health(implicit
      traceContext: TraceContext
  ): Future[BlockOrdererHealthStatus]

  def adminServices: Seq[ServerServiceDefinition] = Seq.empty

  /** Provides information for the [[SequencerSnapshot.additional]] field, which can be used for
    * onboarding.
    */
  def sequencerSnapshotAdditionalInfo(
      timestamp: CantonTimestamp
  ): EitherT[Future, SequencerError, Option[v30.BftSequencerSnapshotAdditionalInfo]]

  /** Return a "current" sequencing time such that, when a `send` operation is subsequently called,
    * if sequenced, the sequencing time of the resulting event is guaranteed to be later than the
    * sequencing time previously returned by the `sequencingTime` call.
    */
  def sequencingTime(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]
}

object BlockOrderer {
  val DefaultInitialBlockHeight: Long = -1L
}

/** A block that a [[BlockOrderer]] delivers to the sequencer node.
  *
  * NOTE that the timestamps in the block or in the events of a block maybe be rewritten by the
  * block post-processing stage (OrderingTimeFixMode) and as such, are not guaranteed to be
  * preserved. This is necessary because some drivers may assign invalid timestamps to events.
  *
  * In addition, during block post-processing, events may be reordered and injected with additional
  * events such as time ticks. Therefore, the order of events in a block is not guaranteed to be
  * preserved either.
  *
  * @param blockHeight
  *   The height of the block. Block heights must be consecutive.
  * @param baseSequencingTimeMicrosFromEpoch
  *   - if the block is not empty, it will be the sequencing timestamp of the first event in the
  *     block
  *   - if the block is empty, it will be the sequencing timestamp of the block itself. It is
  *     guaranteed that future blocks won't move this time back. Also guaranteed that this time will
  *     move forward.
  * @param events
  *   The events in the given block, which may be reordered in the post-processing stage.
  * @param tickTopologyAtMicrosFromEpoch
  *   Set by the block orderer whenever it assesses that it may need to retrieve an up-to-date
  *   topology; it is set to the sequencing instant being used to query the topology snapshot. A
  *   non-byzantine block orderer will set it to a sequencing time later than or equal to all
  *   already ordered requests and earlier than 1 time tick before all future ordered requests. It
  *   will also set it as sporadically as possible to reduce sequencer storage consumption. The
  *   extra time tick reserved between blocks is for the sequencer to use, typically to inject an
  *   event that will update the topology processor's latest known time, so that the block orderer
  *   can successfully retrieve an up-to-date topology at [[tickTopologyAtMicrosFromEpoch]]. The
  *   block orderer needs to communicate this timestamp to the sequencer because the sequencer
  *   cannot possibly guess it, as some requests in ordered blocks may fail to be validated and be
  *   dropped by the sequencer after the blocks are ordered.
  */
final case class RawLedgerBlock(
    blockHeight: Long,
    baseSequencingTimeMicrosFromEpoch: Long,
    events: Seq[Traced[RawLedgerBlock.RawBlockEvent]],
    tickTopologyAtMicrosFromEpoch: Option[Long] = None,
)

object RawLedgerBlock {
  sealed trait RawBlockEvent extends Product with Serializable {
    val microsecondsSinceEpoch: Long
  }

  object RawBlockEvent {

    /** Individual send event
      *
      * @param microsecondsSinceEpoch
      *   note that this timestamp may be rewritten by the block post-processing stage.
      */
    final case class Send(
        request: ByteString,
        microsecondsSinceEpoch: Long,
        orderingSequencerId: String,
    ) extends RawBlockEvent

    final case class Acknowledgment(acknowledgement: ByteString, microsecondsSinceEpoch: Long)
        extends RawBlockEvent
  }
}

final case class BlockOrdererHealthStatus(
    isActive: Boolean,
    description: Option[String],
)
