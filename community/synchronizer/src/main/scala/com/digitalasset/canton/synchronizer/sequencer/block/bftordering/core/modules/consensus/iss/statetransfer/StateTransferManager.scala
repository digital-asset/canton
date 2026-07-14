// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.TimeoutManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
  WorkflowId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Instant
import scala.util.{Failure, Success}

/** Manages a single state transfer instance in a client role and multiple state transfer instances
  * in a server role.
  *
  * It is meant to be used by Consensus behaviors only and is not thread-safe.
  *
  * Design document: https://docs.google.com/document/d/1oB1KtnpM7OiNDWQoRUL0NuoEFJYUjg58ECYIjSi4sIM
  */
class StateTransferManager[E <: Env[E]](
    thisNode: BftNodeId,
    dependencies: ConsensusModuleDependencies[E],
    epochStore: EpochStore[E],
    workflowId: WorkflowId,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(
    private val maybeCustomTimeoutManager: Option[
      TimeoutManager[E, Consensus.Message[E], String]
    ] = None
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends NamedLogging {

  private val stateTransferStartEpoch = new SingleUseCell[EpochNumber]

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var waitingForEpochTransfer: Option[Instant] = None

  // `StateTransferBehavior` ensures that we only transfer one epoch at a time, but tracking the blocks to verify
  //  per epoch makes the completion detection robust against stale or duplicate messages and reduces coupling
  //  to invariants offered by `StateTransferBehavior`.
  //  Supporting true concurrent multi-epoch transfers would also require per-epoch timeouts and latency tracking.
  private val epochToBlockNumbersToVerify =
    scala.collection.mutable.Map.empty[EpochNumber, scala.collection.mutable.Set[Long]]

  private val validator = new StateTransferMessageValidator[E](metrics, loggerFactory)

  private val messageSender = new StateTransferMessageSender[E](
    thisNode,
    dependencies,
    epochStore,
    workflowId,
    loggerFactory,
  )

  private val timeoutManager = maybeCustomTimeoutManager.getOrElse(
    new TimeoutManager[E, Consensus.Message[E], String](
      loggerFactory,
      config.epochStateTransferRetryTimeout,
      timeoutId = "state transfer",
      timeoutMetric = None,
    )
  )

  def inStateTransfer: Boolean = stateTransferStartEpoch.isDefined

  def startCatchUp(
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      latestCompletedEpoch: EpochStore.Epoch,
      startEpoch: EpochNumber,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inStateTransfer) {
      logger.info("State transfer already in progress")
    } else {
      val latestCompletedEpochNumber = latestCompletedEpoch.info.number
      logger.info(
        s"Starting catch-up from epoch $startEpoch, latest completed epoch is $latestCompletedEpochNumber"
      )
      initStateTransfer(startEpoch)(abort)

      initiateSendBlockTransferRequest(
        startEpoch,
        membership,
        cryptoProvider,
        abort,
        nodeThatTimedOut = None,
      )
    }

  def stateTransferNewEpoch(
      newEpochNumber: EpochNumber,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      nodeThatTimedOut: Option[BftNodeId],
  )(
      abort: String => Nothing
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    if (inStateTransfer) {
      logger.info(s"State transfer requesting new epoch $newEpochNumber")
    } else {
      logger.info(s"Starting onboarding state transfer from epoch $newEpochNumber")
      initStateTransfer(newEpochNumber)(abort)
    }
    initiateSendBlockTransferRequest(
      newEpochNumber,
      membership,
      cryptoProvider,
      abort,
      nodeThatTimedOut,
    )
  }

  private def initiateSendBlockTransferRequest(
      newEpochNumber: EpochNumber,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
      abort: String => Nothing,
      nodeThatTimedOut: Option[BftNodeId],
  )(implicit context: E#ActorContextT[Consensus.Message[E]]): Unit = context.withNewTraceContext {
    implicit traceContext =>
      waitingForEpochTransfer = Some(Instant.now)
      val blockTransferRequest =
        StateTransferMessage.BlockTransferRequest.create(newEpochNumber, membership.myId)
      messageSender.signMessage(cryptoProvider, blockTransferRequest) { signedMessage =>
        sendBlockTransferRequest(signedMessage, membership, nodeThatTimedOut)(abort)
      }
  }

  private def initStateTransfer(startEpoch: EpochNumber)(abort: String => Nothing): Unit =
    stateTransferStartEpoch
      .putIfAbsent(startEpoch)
      .foreach(_ =>
        abort(
          "Internal inconsistency: state transfer manager in a client role should not be reused"
        )
      )

  /** Handles a state transfer message; if it's a response, it's handled in the context of the epoch
    * being currently state transferred by [[StateTransferBehavior]], which transfers epochs in
    * number sequence one at a time. Non-compliant attempts to send us responses for blocks that are
    * not in the current epoch will be caught by the validator.
    *
    * @return
    *   a [[StateTransferMessageResult]] indicating the result of handling the message
    */
  def handleStateTransferMessage(
      message: Consensus.StateTransferMessage,
      topologyInfo: OrderingTopologyInfo[E],
      latestCompletedEpoch: EpochStore.Epoch,
      currentEpochInfo: EpochInfo,
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    message match {
      case StateTransferMessage.UnverifiedStateTransferMessage(unverifiedMessage) =>
        validator.validateUnverifiedStateTransferNetworkMessage(
          unverifiedMessage,
          latestCompletedEpoch.info.number,
          topologyInfo,
        )

      case StateTransferMessage.VerifiedStateTransferMessage(message) =>
        handleStateTransferNetworkMessage(
          message,
          topologyInfo,
          latestCompletedEpoch,
          currentEpochInfo,
        )

      case StateTransferMessage.RetryBlockTransferRequest(request, nodeThatTimedOutO) =>
        logger.info(s"Retrying block transfer request for epoch ${request.message.epoch}")
        sendBlockTransferRequest(request, topologyInfo.currentMembership, nodeThatTimedOutO)(
          abort
        )
        StateTransferMessageResult.Continue

      case StateTransferMessage.BlockVerified(
            commitCert,
            currentEpochInfo,
            from,
          ) =>
        val currentEpochNumber = currentEpochInfo.number
        // Ensure we track the blocks to verify for the current epoch
        val blockNumbersToVerify = epochToBlockNumbersToVerify.getOrElseUpdate(
          currentEpochNumber,
          scala.collection.mutable.Set.from(
            currentEpochInfo.startBlockNumber to currentEpochInfo.lastBlockNumber
          ),
        )
        val prePrepare = commitCert.prePrepare.message
        val blockMetadata = prePrepare.blockMetadata
        val blockNumber = blockMetadata.blockNumber
        blockNumbersToVerify -= blockNumber
        if (blockNumbersToVerify.isEmpty) {
          // Epoch blocks transfer complete, cancel the timeout and clean up the state
          cancelTimeoutForEpoch(currentEpochNumber)
          epochToBlockNumbersToVerify.remove(currentEpochNumber).discard
        }
        storeBlock(commitCert, currentEpochInfo, from)

      case StateTransferMessage.BlockStored(commitCert, currentEpochInfo, from) =>
        if (inStateTransfer) {
          handleStoredBlock(commitCert, currentEpochInfo)
        } else {
          logger.info(
            s"Stored block ${commitCert.prePrepare.message.blockMetadata} from '$from' while not in state transfer"
          )
        }
        StateTransferMessageResult.Continue
    }

  def cancelTimeoutForEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"State transfer cancelling a timeout for epoch $epochNumber")
    timeoutManager.cancelTimeout()
  }

  def emitEpochTransferLatency(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Epoch $epochNumber has been fully transferred")
    import metrics.performance.orderingStageLatency.*
    val now = Instant.now()
    emitOrderingStageLatency(
      labels.stage.values.consensus.stateTransfer.TotalEpochTransferLatency,
      // Always emit batch wait latency for dashboard clarity, even if 0
      startInstant = waitingForEpochTransfer.orElse(Some(now)),
      endInstant = now,
      cleanup = () => waitingForEpochTransfer = None,
    )
  }

  private def handleStateTransferNetworkMessage(
      message: Consensus.StateTransferMessage.StateTransferNetworkMessage,
      orderingTopologyInfo: OrderingTopologyInfo[E],
      latestCompletedEpoch: EpochStore.Epoch,
      currentEpochInfo: EpochInfo,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult =
    message match {
      case request @ StateTransferMessage.BlockTransferRequest(epoch, from) =>
        logger.info(s"'$from' is requesting block transfer for epoch $epoch")
        messageSender.sendBlockTransferResponses(
          orderingTopologyInfo.currentCryptoProvider,
          to = from,
          request.epoch,
          latestCompletedEpoch,
        )
        StateTransferMessageResult.Continue

      case response: StateTransferMessage.BlockTransferResponse =>
        // TODO(#25082) consider authorizing/handling a response only if it comes `from` the requested node
        if (inStateTransfer) {
          handleBlockTransferResponse(
            response,
            orderingTopologyInfo,
            currentEpochInfo,
          )
        } else {
          val blockMetadata = response.commitCertificate.map(_.prePrepare.message.blockMetadata)
          logger.info(
            s"Received block transfer response for block $blockMetadata " +
              s"from ${response.from} while not in state transfer; ignoring unneeded response"
          )
          StateTransferMessageResult.Continue
        }
    }

  private def sendBlockTransferRequest(
      request: SignedMessage[StateTransferMessage.BlockTransferRequest],
      membership: Membership,
      nodeThatTimedOut: Option[BftNodeId],
  )(abort: String => Nothing)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (inStateTransfer) {
      val possibleRecipients = membership.otherNodes.toSeq
      if (possibleRecipients.isEmpty)
        abort(
          "Internal inconsistency: there should be at least one serving node to send a block transfer request to"
        )
      logger.debug(
        s"Sending a block transfer request for epoch ${request.message.epoch} to a random " +
          s"authenticated peer among $possibleRecipients (retry interval = ${config.epochStateTransferRetryTimeout})"
      )
      messageSender.sendBlockTransferRequest(
        request,
        possibleRecipients,
        nodeThatTimedOut,
        Some(chosenRecipientO =>
          timeoutManager
            .scheduleTimeout(
              StateTransferMessage.RetryBlockTransferRequest(request, chosenRecipientO)
            )
        ),
      )
    } else {
      logger.info("Not sending a block transfer request when not in state transfer (likely a race)")
    }

  private def handleBlockTransferResponse(
      response: StateTransferMessage.BlockTransferResponse,
      orderingTopologyInfo: OrderingTopologyInfo[E],
      currentEpochInfo: EpochInfo,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult = {
    val from = response.from
    response.commitCertificate match {
      case None =>
        StateTransferMessageResult.NothingToStateTransfer(from)
      case Some(commitCert) =>
        validator.verifyCommitCertificateSignatures(
          commitCert,
          from,
          orderingTopologyInfo,
          currentEpochInfo,
        )
        StateTransferMessageResult.Continue
    }
  }

  private def storeBlock(
      commitCert: CommitCertificate,
      currentEpochInfo: EpochInfo,
      from: BftNodeId,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult = {
    context.pipeToSelf(
      epochStore.addOrderedBlockAtomically(commitCert.prePrepare, commitCert.commits.map(Traced(_)))
    ) {
      case Success(_) =>
        Some(StateTransferMessage.BlockStored(commitCert, currentEpochInfo, from))
      case Failure(exception) =>
        Some(Consensus.ConsensusMessage.AsyncException(exception))
    }
    StateTransferMessageResult.Continue
  }

  private def handleStoredBlock(
      commitCert: CommitCertificate,
      currentEpochInfo: EpochInfo,
  )(implicit traceContext: TraceContext): Unit = {
    val prePrepare = commitCert.prePrepare.message
    val blockMetadata = prePrepare.blockMetadata
    val isBlockLastInEpoch = blockMetadata.blockNumber == currentEpochInfo.lastBlockNumber

    // Blocks within an epoch can be received and stored out of order, but that's fine because the Output module
    //  orders them (has a Peano queue).
    logger.debug(s"State transfer sending block $blockMetadata to Output")
    messageSender.sendBlockToOutput(prePrepare, isBlockLastInEpoch)
  }
}

sealed trait StateTransferMessageResult extends Product with Serializable
object StateTransferMessageResult {
  // Signals that state transfer is in progress
  case object Continue extends StateTransferMessageResult
  final case class NothingToStateTransfer(from: BftNodeId) extends StateTransferMessageResult
}
