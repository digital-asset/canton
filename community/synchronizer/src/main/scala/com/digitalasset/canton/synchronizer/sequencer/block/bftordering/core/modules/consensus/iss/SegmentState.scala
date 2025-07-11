// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.SegmentState.{
  RetransmissionResult,
  computeLeaderOfView,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.PbftMessageValidatorImpl
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FairBoundedQueue
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FairBoundedQueue.{
  DeduplicationStrategy,
  EnqueueResult,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class SegmentState(
    val segment: Segment,
    val epoch: Epoch,
    clock: Clock,
    completedBlocks: Seq[Block],
    abort: String => Nothing,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
    getViewChangeWindowSize: OrderingTopology => NonNegativeInt = orderingTopology =>
      NonNegativeInt.tryCreate(
        orderingTopology.size * SegmentState.ViewNumbersWindowTopologySizeFactor
      ),
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends NamedLogging {

  private val membership = epoch.currentMembership
  private val eligibleLeaders = membership.leaders
  private val originalLeaderIndex = eligibleLeaders.indexOf(segment.originalLeader)
  private val epochNumber = epoch.info.number
  private val viewChangeBlockMetadata = BlockMetadata(epochNumber, segment.slotNumbers.head1)
  private val pbftMessageValidator = new PbftMessageValidatorImpl(segment, epoch, metrics)(abort)

  // Only one view is active at a time, starting at view=0, inViewChange=false
  // - Upon view change start, due to timeout or >= f+1 votes, increment currentView and inViewChange=true
  // - Upon view change completion, inViewChange=false (and view stays the same)

  private var currentLeader: BftNodeId = segment.originalLeader
  private var currentViewNumber: ViewNumber = ViewNumber.First
  private var inViewChange: Boolean = false
  private var strongQuorumReachedForCurrentView: Boolean = false

  private val futureViewMessagesQueue =
    new FairBoundedQueue[SignedMessage[PbftNormalCaseMessage]](
      config.consensusQueueMaxSize,
      config.consensusQueuePerNodeQuota,
      // Drop newest to preserve continuity of messages
      DropStrategy.DropNewest,
      // We need to deduplicate to protect against nodes spamming with others' messages
      DeduplicationStrategy.PerNode(
        Some(metrics.consensus.postponedViewMessagesQueueDuplicatesMeter)
      ),
      maxSizeGauge = Some(metrics.consensus.postponedViewMessagesQueueMaxSize),
      metrics = Some(metrics),
      sizeGauge = Some(metrics.consensus.postponedViewMessagesQueueSize),
      dropMeter = Some(metrics.consensus.postponedViewMessagesQueueDropMeter),
      orderingStageLatencyLabel = Some(
        metrics.performance.orderingStageLatency.labels.stage.values.consensus.PostponedViewMessagesQueueLatency
      ),
    )
  private val viewChangeState = new mutable.HashMap[ViewNumber, PbftViewChangeState]
  private var discardedViewMessagesCount = 0
  private var discardedRetransmittedCommitCertsCount = 0
  private var retransmittedMessagesCount = 0
  private var retransmittedCommitCertificatesCount = 0

  private val segmentBlocks: NonEmpty[Seq[SegmentBlockState]] =
    segment.slotNumbers.map { blockNumber =>
      new SegmentBlockState(
        viewNumber =>
          new PbftBlockState(
            epoch.currentMembership,
            clock,
            pbftMessageValidator,
            currentLeader,
            viewNumber,
            abort,
            metrics,
            loggerFactory,
          ),
        completedBlocks.find(_.blockNumber == blockNumber),
        abort,
      )
    }

  @VisibleForTesting
  private[iss] def getViewChangeState =
    viewChangeState.toMap

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def processEvent(
      event: PbftEvent
  )(implicit traceContext: TraceContext): Seq[ProcessResult] =
    event match {
      case PbftSignedNetworkMessage(signedMessage) =>
        signedMessage.message match {
          case _: PbftNormalCaseMessage =>
            processNormalCaseMessage(
              signedMessage.asInstanceOf[SignedMessage[PbftNormalCaseMessage]]
            )
          case _: PbftViewChangeMessage =>
            processViewChangeNetworkMessage(
              signedMessage.asInstanceOf[SignedMessage[PbftViewChangeMessage]]
            )
        }
      case viewChangeEvent: PbftViewChangeEvent =>
        processViewChangeEvent(abort)(viewChangeEvent)
      case messagesStored: PbftMessagesStored =>
        processMessagesStored(messagesStored)
      case timeout: PbftTimeout =>
        processTimeout(timeout)
      case msg: RetransmittedCommitCertificate =>
        processCommitCertificate(msg)
    }

  private def processMessagesStored(pbftMessagesStored: PbftMessagesStored)(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] =
    pbftMessagesStored match {
      case _: PrePrepareStored | _: PreparesStored =>
        val blockIndex = segment.relativeBlockIndex(pbftMessagesStored.blockMetadata.blockNumber)
        val block = segmentBlocks(blockIndex)
        block.processMessagesStored(pbftMessagesStored)
      case _: NewViewStored =>
        segmentBlocks.forgetNE.flatMap { block =>
          block.processMessagesStored(pbftMessagesStored)
        }
    }

  def confirmCompleteBlockStored(blockNumber: BlockNumber): Unit =
    segmentBlocks(segment.relativeBlockIndex(blockNumber)).confirmCompleteBlockStored()

  def isBlockComplete(blockNumber: BlockNumber): Boolean =
    segmentBlocks(segment.relativeBlockIndex(blockNumber)).isComplete

  def isSegmentComplete: Boolean = blockCompletionState.forall(identity)

  def blockCommitMessages(blockNumber: BlockNumber): Seq[SignedMessage[Commit]] =
    segmentBlocks(segment.relativeBlockIndex(blockNumber)).blockCommitMessages

  def currentView: ViewNumber = currentViewNumber

  def prepareVotes: Map[BftNodeId, Long] = sumOverInProgressBlocks(_.prepareVoters)

  def commitVotes: Map[BftNodeId, Long] = sumOverInProgressBlocks(_.commitVoters)

  def discardedMessageCount: Int =
    discardedViewMessagesCount
      + discardedRetransmittedCommitCertsCount
      + segmentBlocks.forgetNE.map(_.discardedMessages).sum
      + viewChangeState.values.map(_.discardedMessages).sum

  private[iss] def retransmittedMessages = retransmittedMessagesCount
  private[iss] def retransmittedCommitCertificates = retransmittedCommitCertificatesCount

  def leader: BftNodeId = currentLeader

  def status: ConsensusStatus.SegmentStatus =
    if (isSegmentComplete) ConsensusStatus.SegmentStatus.Complete
    else if (inViewChange)
      ConsensusStatus.SegmentStatus.InViewChange(
        currentViewNumber,
        viewChangeMessagesPresent = viewChangeState
          .get(currentViewNumber)
          .map(_.viewChangeMessageReceivedStatus)
          .getOrElse(Seq.empty),
        segment.slotNumbers.map(isBlockComplete),
      )
    else
      ConsensusStatus.SegmentStatus.InProgress(
        currentViewNumber,
        segmentBlocks.map(_.status(currentViewNumber)).forgetNE,
      )

  def messagesToRetransmit(
      from: BftNodeId,
      remoteStatus: ConsensusStatus.SegmentStatus.Incomplete,
  )(implicit
      traceContext: TraceContext
  ): RetransmissionResult = {
    val result = if (remoteStatus.viewNumber > currentViewNumber) {
      logger.debug(
        s"Node $from is in view ${remoteStatus.viewNumber}, which is higher than our current view $currentViewNumber, so we can't help with retransmissions"
      )
      RetransmissionResult.Empty
    } else if (inViewChange) {
      // if we are in a view change, we help others make progress to complete the view change
      val vcState = viewChangeState(currentViewNumber)
      val msgsToRetransmit = remoteStatus match {
        case status if status.viewNumber < currentViewNumber =>
          // if remote node is in an earlier view change, retransmit all view change messages we have
          vcState.viewChangeMessagesToRetransmit(Seq.empty)
        case ConsensusStatus.SegmentStatus.InViewChange(_, remoteVcMsgs, _) =>
          // if remote node is in the same view change, retransmit view change messages we have that they don't
          vcState.viewChangeMessagesToRetransmit(remoteVcMsgs)
        case _ =>
          // if they've completed the view change, we don't need to do anything (they are ahead of us)
          Seq.empty
      }
      // we do not retransmit commit certs in this case, since most relevant commit certs shall eventually be included
      // in the new-view message when the view change completes
      RetransmissionResult(msgsToRetransmit)
    } else {
      val localBlockStates = segmentBlocks

      remoteStatus match {
        // remote node is making progress on the same view, so we send them what we can to help complete blocks
        case ConsensusStatus.SegmentStatus.InProgress(viewNumber, remoteBlocksStatuses)
            if viewNumber == currentViewNumber =>
          localBlockStates
            .zip(remoteBlocksStatuses)
            .collect { case (localBlockState, inProgress: ConsensusStatus.BlockStatus.InProgress) =>
              (localBlockState, inProgress) // only look at blocks they haven't completed yet
            }
            .foldLeft(RetransmissionResult.Empty) {
              case (RetransmissionResult(msgs, ccs), (localBlockState, remoteBlockStatus)) =>
                localBlockState.consensusCertificate match {
                  case Some(cc: CommitCertificate) =>
                    // TODO(#24442): just send a few commits in cases that's enough for remote node to complete quorum
                    RetransmissionResult(msgs, cc +: ccs)
                  case _ =>
                    val newMsgs = msgs ++ localBlockState.messagesToRetransmit(
                      currentViewNumber,
                      remoteBlockStatus,
                    )
                    RetransmissionResult(newMsgs, ccs)
                }
            }

        // remote node is either is a previous view, or in the same view but in an unfinished view change that we've completed.
        // so we give them the new-view message and all messages we have for blocks they haven't completed yet
        case _ =>
          val newView = viewChangeState(currentViewNumber).newViewMessage.toList
          val remoteBlockStatusNoPreparesOrCommits = {
            val allMissing = Seq.fill(membership.sortedNodes.size)(false)
            ConsensusStatus.BlockStatus.InProgress(
              prePrepared = true,
              preparesPresent = allMissing,
              commitsPresent = allMissing,
            )
          }
          localBlockStates
            .zip(remoteStatus.areBlocksComplete)
            .collect {
              case (localBlockState, isRemoteComplete) if !isRemoteComplete => localBlockState
            }
            .foldLeft(RetransmissionResult(newView)) {
              case (RetransmissionResult(msgs, ccs), localBlockState) =>
                localBlockState.consensusCertificate match {
                  case Some(cc: CommitCertificate) =>
                    // TODO(#24442): rethink commit certs here, considering that some certs will be in the new-view message.
                    // we could either: exclude sending commit certs that are already in the new-view,
                    // not take that into account and just send commit certs regardless (which means we may send the same cert twice),
                    // or not send any certs at all considering that the new-view message will likely contain most if not all of them
                    RetransmissionResult(msgs, cc +: ccs)
                  case _ =>
                    val newMsgs =
                      localBlockState.messagesToRetransmit(
                        currentViewNumber,
                        remoteBlockStatusNoPreparesOrCommits,
                      )
                    RetransmissionResult(msgs ++ newMsgs, ccs)
                }
            }
      }
    }
    retransmittedMessagesCount += result.messages.size
    retransmittedCommitCertificatesCount += result.commitCerts.size
    result
  }

  private def sumOverInProgressBlocks(
      getVoters: SegmentBlockState => Iterable[BftNodeId]
  ): Map[BftNodeId, Long] =
    segmentBlocks.forgetNE
      .flatMap(getVoters)
      .groupBy(identity)
      .view
      .mapValues(_.size.toLong)
      .toMap

  // Normal Case: PrePrepare, Prepare, Commit
  private def processNormalCaseMessage(
      msg: SignedMessage[PbftNormalCaseMessage]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    var result = Seq.empty[ProcessResult]
    if (msg.message.viewNumber < currentViewNumber) {
      logger.info(
        s"Segment received PbftNormalCaseMessage with stale view ${msg.message.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
      discardedViewMessagesCount += 1
    } else if (msg.message.viewNumber > currentViewNumber || inViewChange) {
      logger.info(
        s"Segment received early PbftNormalCaseMessage; peer = ${msg.from}, " +
          s"message view = ${msg.message.viewNumber}, " +
          s"current view = $currentViewNumber, inViewChange = $inViewChange"
      )
      futureViewMessagesQueue.enqueue(msg.from, msg) match {
        case EnqueueResult.PerNodeQuotaExceeded(nodeId) =>
          logger.info(s"Node `$nodeId` exceeded its future view message queue quota")
        case EnqueueResult.TotalCapacityExceeded =>
          logger.info("Future view message queue total capacity has been exceeded")
        case EnqueueResult.Duplicate(nodeId) =>
          logger.info(s"Duplicate future view message for node `$nodeId` has been dropped")
        case EnqueueResult.Success =>
          logger.trace("Successfully postponed PbftNormalCaseMessage")
      }
    } else
      result = processPbftNormalCaseMessage(msg, msg.message.blockMetadata.blockNumber)
    result
  }

  /** process some kind of message, which will either be a network message or an internal event
    * @param process
    *   process the message and indicate if we should attempt to advance the view change process
    */
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  private def processViewChangeMessage[Message](
      message: Message,
      viewNumber: ViewNumber,
      process: PbftViewChangeState => Message => Boolean,
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    var result = Seq.empty[ProcessResult]
    if (viewNumber < currentViewNumber) {
      logger.info(
        s"Segment received PbftViewChangeMessage with stale view $viewNumber; " +
          s"current view = $currentViewNumber"
      )
      discardedViewMessagesCount += 1
    } else if (viewNumber == currentViewNumber && !inViewChange) {
      logger.info(
        s"Segment received PbftViewChangeMessage with matching view $viewNumber, " +
          s"but View Change is already complete, current view = $currentViewNumber"
      )
      discardedViewMessagesCount += 1
    } else {
      val viewChangeWindowSize = getViewChangeWindowSize(membership.orderingTopology)
      message match {
        case SignedMessage(_: ViewChange, _)
            if viewNumber > currentViewNumber + viewChangeWindowSize.value =>
          // We reject view change messages that are too far ahead to avoid OOMs
          //  due to filling the view change state with too many entries, as
          //  this could be leveraged by malicious nodes to perform DoS attacks.
          //
          // This strategy will not get the network into an unrecoverable state, even
          //  in the presence of f malicious nodes and some additional unresponsive nodes,
          //  because it is not possible to transition the responsive portion of the network into arbitrary high
          //  view numbers without the unavailable portion of the network being able to do so as well (as soon
          //  as it becomes available again):
          //
          //  - Entering a (potentially nested) view change is the only way a view number can be increased
          //  - Entering a (potentially nested) view change is guaranteed to be honest because at least one
          //    correct node is part of the necessary ViewChange message quorum
          //  - Correct view change behavior forbids starting a nested view change with less than
          //    a strong quorum
          //  - Correct view change behavior forbids skipping view numbers
          //  - A NewView message embeds a strong quorum and is always sufficient to increase the view
          //    number of nodes who observe it
          logger.info(
            s"Segment received ViewChange with view $viewNumber, " +
              s"but it is too far in the future (current view = $currentViewNumber, " +
              s"view numbers window size = $viewChangeWindowSize," +
              s"current ordering topology size = ${membership.orderingTopology.size})"
          )
          discardedViewMessagesCount += 1

        case _ =>
          val vcState = viewChangeState.getOrElseUpdate(
            viewNumber,
            new PbftViewChangeState(
              membership,
              computeLeader(viewNumber),
              viewNumber,
              segment.slotNumbers,
              metrics,
              loggerFactory,
            ),
          )
          if (process(vcState)(message) && vcState.shouldAdvanceViewChange) {
            result = advanceViewChange(viewNumber)
          }
      }
    }
    result
  }

  // View Change Case: ViewChange, NewView
  // Note: Similarly to the future message queue, we may want to limit how many concurrent viewChangeState
  //       entries exist in the map at any given point in time
  private def processViewChangeNetworkMessage(
      msg: SignedMessage[PbftViewChangeMessage]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] =
    processViewChangeMessage(msg, msg.message.viewNumber, _.processMessage)

  private def processViewChangeEvent(abort: String => Unit)(event: PbftViewChangeEvent)(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] =
    processViewChangeMessage(event, event.viewNumber, _.processEvent(abort))

  private def processCommitCertificate(msg: RetransmittedCommitCertificate)(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] = {
    val RetransmittedCommitCertificate(from, cc) = msg
    val blockNumber = cc.blockMetadata.blockNumber

    if (isBlockComplete(blockNumber)) {
      discardedRetransmittedCommitCertsCount += 1
      logger.debug(
        s"Discarded retransmitted commit cert for block $blockNumber from $from because block is already complete"
      )
      Seq.empty[ProcessResult]
    } else
      // the certificate has been validated previously, so we can just accept it now
      segmentBlocks(segment.relativeBlockIndex(blockNumber)).completeBlock(cc)
  }

  private def processTimeout(
      timeout: PbftTimeout
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    var result = Seq.empty[ProcessResult]
    if (timeout.viewNumber < currentViewNumber)
      logger.info(
        s"Segment received PbftTimeout with stale view ${timeout.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
    else if (timeout.viewNumber > currentViewNumber)
      abort(
        s"Segment should not receive timeout from future view ${timeout.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
    else {
      val nextViewNumber = ViewNumber(timeout.viewNumber + 1)
      viewChangeState
        .getOrElseUpdate(
          nextViewNumber,
          new PbftViewChangeState(
            membership,
            computeLeader(nextViewNumber),
            nextViewNumber,
            segment.slotNumbers,
            metrics,
            loggerFactory,
          ),
        )
        .discard
      result = advanceViewChange(nextViewNumber)
    }
    result
  }

  private def blockCompletionState: Seq[Boolean] =
    (0 until segment.slotNumbers.size).map(idx => isBlockComplete(segment.slotNumbers(idx)))

  private def computeLeader(viewNumber: ViewNumber): BftNodeId =
    computeLeaderOfView(viewNumber, originalLeaderIndex, eligibleLeaders)

  private[iss] def isViewChangeInProgress: Boolean = inViewChange

  @VisibleForTesting
  private[iss] def futureQueueSize: Int = futureViewMessagesQueue.size

  private final class ViewChangeAction[ConditionResult](
      condition: (ViewNumber, PbftViewChangeState) => Option[ConditionResult],
      action: (
          ViewNumber,
          ConditionResult,
          PbftViewChangeState,
      ) => TraceContext => Seq[ProcessResult],
  ) {
    private var highestViewNumber = ViewNumber.First
    def run(viewNumber: ViewNumber, state: PbftViewChangeState)(implicit
        traceContext: TraceContext
    ): Seq[ProcessResult] =
      if (viewNumber > highestViewNumber) {
        condition(viewNumber, state) match {
          case Some(value) =>
            highestViewNumber = viewNumber
            action(viewNumber, value, state)(traceContext)
          case None => Seq.empty
        }
      } else {
        Seq.empty
      }
  }

  private def viewChangeAction(
      condition: (ViewNumber, PbftViewChangeState) => Boolean
  )(
      action: (ViewNumber, PbftViewChangeState) => TraceContext => Seq[ProcessResult]
  ): ViewChangeAction[Unit] = new ViewChangeAction[Unit](
    (viewNumber, state) => Option.when(condition(viewNumber, state))(()),
    (viewNumber, _, state) => action(viewNumber, state),
  )

  private def viewChangeActionOpt[ConditionResult](
      condition: (ViewNumber, PbftViewChangeState) => Option[ConditionResult]
  )(
      action: (
          ViewNumber,
          ConditionResult,
          PbftViewChangeState,
      ) => TraceContext => Seq[ProcessResult]
  ): ViewChangeAction[ConditionResult] = new ViewChangeAction(condition, action)

  private val startViewChangeAction = viewChangeAction { case (viewNumber, _) =>
    val hasStartedThisViewChange = currentViewNumber >= viewNumber
    !hasStartedThisViewChange
  } { case (viewNumber, viewState) =>
    _ =>
      currentViewNumber = viewNumber
      currentLeader = computeLeader(viewNumber)
      inViewChange = true
      strongQuorumReachedForCurrentView = false
      // if we got the new-view message before anything else (common during rehydration),
      // then no need to create a view-change message
      if (viewState.newViewMessage.isDefined) Seq.empty
      else {
        viewState.viewChangeFromSelf match {
          // if we rehydrated a view-change message from self, we don't need to create or store it again
          case Some(_rehydratedViewChangeMessage) =>
            viewState.markViewChangeFromSelfAsComingFromRehydration()
            Seq.empty
          case None =>
            val viewChangeMessage = createViewChangeMessage(viewNumber)
            Seq(SignPbftMessage(viewChangeMessage))
        }
      }
  }

  private val sendViewChangeAction = viewChangeActionOpt { case (_, viewState) =>
    Option.when(viewState.newViewMessage.isEmpty)(()).flatMap(_ => viewState.viewChangeFromSelf)
  } { case (_, vc, viewState) =>
    _ =>
      Seq(
        SendPbftMessage(
          vc,
          if (viewState.isViewChangeFromSelfRehydration) {
            None
          } else {
            Some(StoreViewChangeMessage(vc))
          },
        )
      )
  }

  private val startNestedViewChangeTimerAction = viewChangeAction { case (_, viewState) =>
    !strongQuorumReachedForCurrentView && viewState.reachedStrongQuorum
  } { case (viewNumber, _) =>
    _ =>
      strongQuorumReachedForCurrentView = true
      Seq(
        ViewChangeStartNestedTimer(viewChangeBlockMetadata, viewNumber)
      )
  }

  private val signBottomPrePreparesAction = viewChangeAction { case (viewNumber, viewState) =>
    val thisViewChangeIsInProgress = currentViewNumber == viewNumber && inViewChange
    viewState.shouldCreateNewView && !viewState.haveSignedPrePrepares && thisViewChangeIsInProgress
  } { case (viewNumber, viewState) =>
    _ =>
      val prePrepares = viewState.constructPrePreparesForNewView(viewChangeBlockMetadata)
      Seq(
        SignPrePreparesForNewView(
          viewChangeBlockMetadata,
          viewNumber,
          prePrepares,
        )
      )
  }

  private val createNewViewAction = viewChangeActionOpt { case (viewNumber, viewState) =>
    val thisViewChangeIsInProgress = currentViewNumber == viewNumber && inViewChange

    Option
      .when(thisViewChangeIsInProgress)(())
      .flatMap(_ => viewState.getSignedPrePreparesForSegment)
  } { case (_, prePrepares, viewState) =>
    _ =>
      val newViewMessage = viewState
        .createNewViewMessage(
          viewChangeBlockMetadata,
          prePrepares,
          abort,
        )
      Seq(SignPbftMessage(newViewMessage))
  }

  private val sendNewViewAction = viewChangeActionOpt { case (_, viewState) =>
    Option.when(viewState.shouldSendNewView)(viewState.newViewMessage).flatten
  } { case (_, newViewMessage, _) =>
    _ => Seq(SendPbftMessage(newViewMessage, Some(StoreViewChangeMessage(newViewMessage))))
  }

  private val completeViewChangeAction = viewChangeActionOpt { case (viewNumber, viewState) =>
    val thisViewChangeIsInProgress = currentViewNumber == viewNumber && inViewChange
    Option.when(thisViewChangeIsInProgress)(()).flatMap(_ => viewState.newViewMessage)
  } { case (_, newView, _) =>
    implicit traceContext => completeViewChange(newView)
  }

  private def advanceViewChange(
      viewNumber: ViewNumber
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    val viewState = viewChangeState(viewNumber)
    // Note that each result (startViewChange, sendViewChange, startNestedViewChangeTimer, signBottomPrePrepares, createNewView, sendNewView, completeViewChange)
    // should occur at most once per view number
    Seq(
      startViewChangeAction,
      sendViewChangeAction,
      startNestedViewChangeTimerAction,
      signBottomPrePreparesAction,
      createNewViewAction,
      sendNewViewAction,
      completeViewChangeAction,
    ).flatMap { action =>
      action.run(viewNumber, viewState)
    }
  }

  private def createViewChangeMessage(
      newViewNumber: ViewNumber
  ): ViewChange = {
    val consensusCerts =
      segmentBlocks.map(_.consensusCertificate).collect { case Some(cert) => cert }
    ViewChange.create(
      viewChangeBlockMetadata,
      newViewNumber,
      consensusCerts,
      from = membership.myId,
    )
  }

  private def completeViewChange(
      newView: SignedMessage[NewView]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    val blockToCommitCert: Map[BlockNumber, CommitCertificate] =
      newView.message.computedCertificatePerBlock.collect {
        case (blockNumber, cc: CommitCertificate) =>
          (blockNumber, cc)
      }
    val blockToPrePrepare =
      newView.message.prePrepares.groupBy(_.message.blockMetadata.blockNumber).collect {
        case (blockNumber, Seq(prePrepare)) =>
          (blockNumber, prePrepare)
        case _ =>
          abort(
            "There should be exactly one PrePrepare for each slot upon completing a view change"
          )
      }

    segmentBlocks.foreach(_.advanceView(newView.message.viewNumber))
    // Create the new set of blocks for the currentView
    val completedBlockResults = blockToCommitCert.flatMap { case (blockNumber, commitCert) =>
      val blockIndex = segment.relativeBlockIndex(blockNumber)
      segmentBlocks(blockIndex).completeBlock(commitCert)
    }

    // End the active view change
    inViewChange = false

    // Process previously queued messages that are now relevant for the current view.
    // It is important that this step happens before processing the pre-prepares for the new-view so that
    // during rehydration we can first process previously stored prepares and thus avoid that new conflicting prepares
    // are created as a result of rehydrating the new-view message's pre-prepares.
    val queuedMessages =
      futureViewMessagesQueue.dequeueAll(_.message.viewNumber == currentViewNumber)
    val futureMessageQueueResults =
      for {
        pbftMessage <- queuedMessages
        processResult <- processPbftNormalCaseMessage(
          pbftMessage,
          pbftMessage.message.blockMetadata.blockNumber,
        )
      } yield processResult

    // Call process (and advance) to bootstrap each block with the PrePrepare included in the NewView message
    val postViewChangeResults =
      for {
        (blockNumber, prePrepare) <- blockToPrePrepare.toList
          .sortBy(_._1)
          // filtering out completed blocks from commit certificates since there is no more need to process these pre-prepares
          .filterNot(kv => blockToCommitCert.contains(kv._1))
        processResult <- processPbftNormalCaseMessage(prePrepare, blockNumber)
      } yield processResult

    // Return the full set of ProcessResults, starting with ViewChangeCompleted
    val completeViewChangeResult = Seq(
      ViewChangeCompleted(
        viewChangeBlockMetadata,
        currentViewNumber,
        Option.when(newView.from != membership.myId)(StoreViewChangeMessage(newView)),
      )
    )

    completeViewChangeResult ++ completedBlockResults ++ postViewChangeResults ++ futureMessageQueueResults
  }

  private def processPbftNormalCaseMessage(
      pbftNormalCaseMessage: SignedMessage[PbftNormalCaseMessage],
      blockNumber: BlockNumber,
  )(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] = {
    val blockIndex = segment.relativeBlockIndex(blockNumber)
    segmentBlocks(blockIndex).processMessage(pbftNormalCaseMessage)
  }
}

object SegmentState {

  private final val ViewNumbersWindowTopologySizeFactor = 2

  final case class RetransmissionResult(
      messages: Seq[SignedMessage[PbftNetworkMessage]],
      commitCerts: Seq[CommitCertificate] = Seq.empty,
  )
  object RetransmissionResult {
    val Empty: RetransmissionResult = RetransmissionResult(Seq.empty, Seq.empty)
  }

  def computeLeaderOfView(
      viewNumber: ViewNumber,
      originalLeaderIndex: Int,
      eligibleLeaders: Seq[BftNodeId],
  ): BftNodeId = {
    val newIndex = (originalLeaderIndex + viewNumber) % eligibleLeaders.size
    eligibleLeaders(newIndex.toInt)
  }
}
