// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
  WorkflowId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
  OrderingMode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.{Failure, Success}

/** Belongs to [[StateTransferManager]] and sends state transfer-related messages. */
final class StateTransferMessageSender[E <: Env[E]](
    thisNode: BftNodeId,
    consensusDependencies: ConsensusModuleDependencies[E],
    epochStore: EpochStore[E],
    workflowId: WorkflowId,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    config: BftBlockOrdererConfig,
    metricsContext: MetricsContext,
) extends NamedLogging {

  import StateTransferMessageSender.*

  def sendBlockTransferRequest(
      blockTransferRequest: SignedMessage[StateTransferMessage.BlockTransferRequest],
      possibleRecipients: Seq[BftNodeId],
      nodesThatTimedOut: Seq[BftNodeId],
      onRecipientDecision: Option[Seq[BftNodeId] => Unit],
  )(implicit traceContext: TraceContext): Unit =
    // Ask a single node for an entire epoch of blocks to compromise between noise for different nodes
    //  and load balancing. Note that we're shuffling (instead of round-robin), so the same node might be chosen
    //  multiple times in a row, resulting in uneven load balancing for certain periods. On the other hand, shuffling
    //  is more straightforward code-wise. A potentially irrelevant implication is that the order in which nodes
    //  are chosen is less predictable (e.g., by malicious nodes), and, at the same time, harder to reason about.
    // TODO(#24940) consider not rotating when everything runs smoothly
    consensusDependencies.p2pNetworkOut.asyncSend(
      P2PNetworkOut.SendToRandomAuthenticated(
        wrapSignedMessage(blockTransferRequest),
        possibleRecipients,
        // Already considering all nodes that were in the relevant topology
        secondChoiceRecipientsPool = None,
        Some(workflowId),
        nodesThatTimedOut,
        onRecipientDecision,
      )
    )

  def sendBlockTransferResponses(
      activeCryptoProvider: CryptoProvider[E],
      to: BftNodeId,
      forEpoch: EpochNumber,
      latestCompletedEpoch: EpochStore.Epoch,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit = {
    val latestCompletedEpochNumber = latestCompletedEpoch.info.number
    if (latestCompletedEpochNumber < forEpoch) {
      logger.info(
        s"Nothing to state transfer to '$to' for epoch $forEpoch, " +
          s"latest locally completed epoch is $latestCompletedEpochNumber"
      )
      sendBlockTransferResponsesForCommitCertificates(
        activeCryptoProvider,
        to,
        commitCerts = Seq.empty,
      )
    } else {
      logger.debug(s"Loading blocks from epoch $forEpoch")
      context.pipeToSelf(
        // We load only one epoch at a time to avoid OOM errors.
        epochStore.loadCompleteBlocks(forEpoch, forEpoch)
      ) {
        case Success(blocks) =>
          val commitCerts = blocks.map(_.commitCertificate)
          val startBlockNumber = blocks.map(_.blockNumber).minOption
          logger.info(
            s"Sending block transfer responses from epoch $forEpoch (start block number = $startBlockNumber) to '$to'"
          )
          sendBlockTransferResponsesForCommitCertificates(
            activeCryptoProvider,
            to,
            commitCerts,
          )
          None // do not send anything back
        case Failure(exception) => Some(Consensus.ConsensusMessage.AsyncException(exception))
      }
    }
  }

  private def sendBlockTransferResponsesForCommitCertificates(
      activeCryptoProvider: CryptoProvider[E],
      to: BftNodeId,
      commitCerts: Seq[CommitCertificate],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (synchronizerProtocolVersion >= ProtocolVersion.v37) {
      // Protocol versions >= 37 can carry multiple commit certificates per response, reusing the retransmission
      //  response message. We group them using a configurable size limit, similarly to retransmission responses, to
      //  avoid exceeding the max gRPC message size.
      val groupsOfCommitCerts =
        config.consensusWindowSizeForStateTransferOfCommitCertificates match {
          case Some(windowSize) if commitCerts.nonEmpty =>
            commitCerts.grouped(windowSize.value).toSeq
          // If no window size is specified, send all commit certificates in a single message. Also send a single
          //  (empty) response when there is nothing to state transfer.
          case _ => Seq(commitCerts)
        }
      groupsOfCommitCerts.foreach { commitCertsGroup =>
        val response =
          StateTransferMessage.BlockTransferResponse31.create(commitCertsGroup, from = thisNode)
        sendResponse(response, activeCryptoProvider, to)
      }
    } else {
      // Protocol versions < 37 carry at most one commit certificate per response.
      if (commitCerts.isEmpty) {
        val response =
          StateTransferMessage.BlockTransferResponse30.create(
            commitCertificate = None,
            from = thisNode,
          )
        sendResponse(response, activeCryptoProvider, to)
      } else {
        commitCerts.foreach { commitCert =>
          val response =
            StateTransferMessage.BlockTransferResponse30.create(Some(commitCert), from = thisNode)
          sendResponse(response, activeCryptoProvider, to)
        }
      }
    }

  def sendBlockToOutput(prePrepare: PrePrepare, lastInEpoch: Boolean)(implicit
      traceContext: TraceContext
  ): Unit = {
    val blockMetadata = prePrepare.blockMetadata

    consensusDependencies.output.asyncSend(
      Output.BlockOrdered(
        OrderedBlockForOutput(
          OrderedBlock(
            blockMetadata,
            prePrepare.block.proofs,
            prePrepare.canonicalCommitSet,
          ),
          prePrepare.viewNumber,
          prePrepare.from,
          lastInEpoch,
          OrderingMode.StateTransfer,
        )
      )
    )
  }

  def signMessage[Message <: StateTransferMessage.StateTransferNetworkMessage](
      cryptoProvider: CryptoProvider[E],
      stateTransferMessage: Message,
  )(
      continuation: SignedMessage[Message] => Unit
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    context.pipeToSelf(
      cryptoProvider.signMessage(
        stateTransferMessage,
        AuthenticatedMessageType.BftSignedStateTransferMessage,
      )
    ) {
      case Failure(exception) =>
        logger.error(
          s"Can't sign state transfer message ${shortType(stateTransferMessage)}",
          exception,
        )
        None
      case Success(Left(errors)) =>
        logger.error(
          s"Can't sign state transfer message ${shortType(stateTransferMessage)}: $errors"
        )
        None
      case Success(Right(signedMessage)) =>
        continuation(signedMessage)
        None
    }

  private def sendResponse[Message <: StateTransferMessage.StateTransferNetworkMessage](
      response: Message,
      cryptoProvider: CryptoProvider[E],
      to: BftNodeId,
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    signMessage(cryptoProvider, response) { signedMessage =>
      consensusDependencies.p2pNetworkOut.asyncSend(
        P2PNetworkOut.send(wrapSignedMessage(signedMessage), to)
      )
    }
}

private object StateTransferMessageSender {

  private def wrapSignedMessage(
      signedMessage: SignedMessage[StateTransferMessage.StateTransferNetworkMessage]
  ): P2PNetworkOut.BftOrderingNetworkMessage =
    P2PNetworkOut.BftOrderingNetworkMessage.StateTransferMessage(signedMessage)
}
