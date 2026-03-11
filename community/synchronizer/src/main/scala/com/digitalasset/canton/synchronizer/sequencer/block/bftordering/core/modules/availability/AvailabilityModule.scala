// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.crypto.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch.BatchValidityDurationEpochs
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  DisseminationStatus,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  MessageAuthorizer,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
  OrderingRequestBatch,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalDissemination,
  RemoteProtocolMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.BftNodeShuffler
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Random, Success, Try}

import AvailabilityModuleMetrics.{emitDisseminationStateStats, emitInvalidMessage}

/** Trantor-inspired availability implementation.
  *
  * @param random
  *   the random source used to select what node to download batches from
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class AvailabilityModule[E <: Env[E]](
    initialMembership: Membership,
    initialEpochNumber: EpochNumber,
    initialCryptoProvider: CryptoProvider[E],
    availabilityStore: data.AvailabilityStore[E],
    clock: Clock,
    random: Random,
    metrics: BftOrderingMetrics,
    override val dependencies: AvailabilityModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    disseminationProtocolState: DisseminationProtocolState = new DisseminationProtocolState(),
    outputFetchProtocolState: MainOutputFetchProtocolState = new MainOutputFetchProtocolState(),
    checkTags: Boolean = true,
)(
    // Only passed in tests
    private var messageAuthorizer: MessageAuthorizer = initialMembership.orderingTopology,
    private val jitterConstructor: (BftBlockOrdererConfig, Random) => JitterStream =
      JitterStream.create,
)(implicit
    override val config: BftBlockOrdererConfig,
    synchronizerProtocolVersion: ProtocolVersion,
    mc: MetricsContext,
    tracer: Tracer,
) extends Availability[E]
    with HasDelayedInit[Availability.Message[E]] {

  import AvailabilityModule.*

  private val thisNode = initialMembership.myId
  private val nodeShuffler = new BftNodeShuffler(random)

  private var lastKnownEpochNumber = initialEpochNumber

  private var activeMembership = initialMembership
  private var activeCryptoProvider = initialCryptoProvider
  private var topologyChangedSinceLastProposalRequest = false

  private var waitingForBatchSince: Option[Instant] = None

  private val spanManager = new AvailabilityModuleSpanManager()

  disseminationProtocolState.lastProposalTime = Some(clock.now)

  @VisibleForTesting
  private[availability] def getActiveMembership = activeMembership
  @VisibleForTesting
  private[availability] def getActiveCryptoProvider = activeCryptoProvider
  @VisibleForTesting
  private[availability] def getMessageAuthorizer = messageAuthorizer

  override def receiveInternal(
      message: Availability.Message[E]
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case Availability.Start =>
        initiateMempoolPull(shortType(message))
        initCompleted(receiveInternal)

      case _ =>
        ifInitCompleted(message) {
          case Availability.Start =>

          case Availability.NoOp =>

          case message: Availability.LocalProtocolMessage[E] =>
            handleLocalProtocolMessage(message)

          case message: Availability.RemoteProtocolMessage =>
            handleRemoteProtocolMessage(message)

          case Availability.UnverifiedProtocolMessage(signedMessage) =>
            handleUnverifiedProtocolMessage(signedMessage)
        }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def handleUnverifiedProtocolMessage(
      signedMessage: SignedMessage[RemoteProtocolMessage]
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = signedMessage.message match {

    case message: Availability.RemoteOutputFetch.RemoteBatchDataFetched =>
      // We have a special case for RemoteBatchDataFetched where we don't check the signature. This is because we
      // might be behind and have not seen the new keys being used. By checking that the batch correspond to the batchId
      // we know we still got the correct data (a malicious node can't fake the batchId since it is an hash of the content).
      val from = signedMessage.from
      val keyId = FingerprintKeyId.toBftKeyId(signedMessage.signature.authorizingLongTermKey)

      if (!activeMembership.orderingTopology.contains(from)) {
        // if the node sending is not part of the topology, this is malicious behavior. Since RemoteBatchDataFetched is
        // a response. And if we don't know who is responding it is a warning.
        logger.warn(
          s"Received a message from '$from' signed with '$keyId' " +
            "but it cannot be verified in the currently known " +
            s"dissemination topology ${activeMembership.orderingTopology.nodesTopologyInfo}, dropping it"
        )
        emitInvalidMessage(metrics, from)
        return
      }

      handleRemoteBatchDataFetched(message)

    case _ =>
      // default case
      val from = signedMessage.from
      val keyId = FingerprintKeyId.toBftKeyId(signedMessage.signature.authorizingLongTermKey)
      if (messageAuthorizer.isAuthorized(from, keyId)) {
        logger.debug(s"Start to verify message from '$from'")
        pipeToSelf(
          activeCryptoProvider.verifySignedMessage(
            signedMessage,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )
        ) {
          case Failure(exception) =>
            abort(
              s"Can't verify signature for ${signedMessage.message} (signature ${signedMessage.signature})",
              exception,
            )
          case Success(Left(exception)) =>
            // Info because it can also happen at epoch boundaries
            logger.info(
              s"Skipping message since we can't verify signature for ${signedMessage.message} (signature ${signedMessage.signature}) reason=$exception"
            )
            emitInvalidMessage(metrics, from)
            Availability.NoOp
          case Success(Right(())) =>
            logger.debug(s"Verified message is from '$from''")
            signedMessage.message
        }
      } else {
        logger.info(
          s"Received a message from '$from' signed with '$keyId' " +
            "but it cannot be verified in the currently known " +
            s"dissemination topology ${activeMembership.orderingTopology.nodesTopologyInfo}, dropping it"
        )
      }
  }

  private def handleLocalProtocolMessage(
      message: Availability.LocalProtocolMessage[E]
  )(implicit context: E#ActorContextT[Availability.Message[E]], traceContext: TraceContext): Unit =
    message match {
      case message: Availability.LocalDissemination =>
        handleLocalDisseminationMessage(message)

      case message: Availability.Consensus[E] =>
        handleConsensusMessage(message)

      case message: Availability.LocalOutputFetch =>
        handleLocalOutputFetchMessage(message)
    }

  private def handleRemoteProtocolMessage(
      message: Availability.RemoteProtocolMessage
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case message: Availability.RemoteDissemination =>
        handleRemoteDisseminationMessage(message)

      case message: Availability.RemoteOutputFetch =>
        handleRemoteOutputFetchMessage(message)
    }

  private def handleLocalDisseminationMessage(
      disseminationMessage: Availability.LocalDissemination
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val actingOnMessageType = shortType(disseminationMessage)

    disseminationMessage match {
      case Availability.LocalDissemination.LocalBatchCreated(requests) =>
        emitBatchWaitLatency()
        val batch = OrderingRequestBatch.create(requests, lastKnownEpochNumber)
        val batchId = BatchId.from(batch)
        spanManager.trackSpansForBatch(
          batchId,
          spans = requests.map { t =>
            startSpan("BFTOrderer.Availability")(t.traceContext, tracer)._1
          },
        )

        logger.debug(
          s"$actingOnMessageType: received batch from local mempool containing messages ${requests
              .map(_.value.headerString)
              .mkString(", ")}; created batch ID $batchId with reference epoch $lastKnownEpochNumber, storing locally"
        )
        disseminationProtocolState.beingFirstSaved
          .put(batchId, InitialSaveInProgress(availabilityEnterInstant = Some(Instant.now)))
          .discard
        pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
          case Failure(exception) =>
            spanManager.endSpansWithError(batchId, "Failed to add batch")
            abort(s"Failed to add batch $batchId", exception)
          case Success(_) =>
            spanManager.addEventToBatchSpans(batchId, "Batch stored")
            Availability.LocalDissemination.LocalBatchesStored(Seq(Traced(batchId) -> batch))
        }

      case Availability.LocalDissemination.LocalBatchesStored(batches) =>
        logger.debug(
          s"$actingOnMessageType: persisted local batches ${batches.map(_._1)}, now signing"
        )
        signLocalBatchesAndContinue(batches)

      case Availability.LocalDissemination.RemoteBatchStored(
            batchId,
            epochNumber,
            from,
            addedToStore,
          ) =>
        outputFetchProtocolState.pendingRemoteBatchIdsToStore.remove(batchId).discard
        outputFetchProtocolState.localOutputMissingBatches.remove(batchId).discard
        updateOutputFetchStatus(batchId)
        logger.debug(
          s"$actingOnMessageType: local store persisted $batchId from $from (actually added = $addedToStore), signing"
        )
        if (addedToStore) {
          disseminationProtocolState.disseminationQuotas.addBatch(from, batchId, epochNumber)
        }
        signRemoteBatchAndContinue(batchId, epochNumber, from)

      case LocalDissemination.LocalBatchesStoredSigned(batches) =>
        disseminateLocalBatches(actingOnMessageType, batches)

      case LocalDissemination.RemoteBatchStoredSigned(batchId, from, signature) =>
        logger.debug(s"$actingOnMessageType: signed $batchId from $from, sending ACK")
        send(
          Availability.RemoteDissemination.RemoteBatchAcknowledged
            .create(
              batchId,
              from = thisNode,
              signature,
            ),
          to = from,
        )

      case LocalDissemination.RemoteBatchAcknowledgeVerified(batchId, from, signature) =>
        logger.debug(
          s"$actingOnMessageType: $from sent valid ACK for batch $batchId, " +
            "updating batches ready for ordering"
        )
        disseminationProtocolState.disseminationProgress.get(batchId).foreach { progress =>
          setProgress(
            actingOnMessageType,
            batchId,
            progress.addAck(AvailabilityAck(from, signature)),
          )
        }
        attemptSatisfyingProposalRequest(actingOnMessageType)
    }
  }

  private def signRemoteBatchAndContinue(
      batchId: BatchId,
      epochNumber: EpochNumber,
      from: BftNodeId,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      activeCryptoProvider.signHash(
        AvailabilityAck.hashFor(batchId, epochNumber, activeMembership.myId, metrics),
        "availability-sign-remote-batchId",
      )
    )(handleFailure(s"Failed to sign $batchId") { signature =>
      LocalDissemination.RemoteBatchStoredSigned(batchId, from, signature)
    })

  private def signLocalBatchesAndContinue(batches: Seq[(Traced[BatchId], OrderingRequestBatch)])(
      implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      context.sequenceFuture(
        batches.map { case (batchId, batch) =>
          activeCryptoProvider.signHash(
            AvailabilityAck
              .hashFor(batchId.value, batch.epochNumber, activeMembership.myId, metrics),
            "availability-sign-local-batchId",
          )
        },
        orderingStage = Some("availability-sign-local-batches"),
      )
    ) {
      case Failure(exception) =>
        batches.foreach(b => spanManager.endSpansWithError(b._1.value, "Failed to sign batch"))
        abort("Failed to sign local batches", exception)
      case Success(results) =>
        val (errors, signatures) = results.partitionMap(identity)
        if (errors.nonEmpty) {
          batches.foreach(b => spanManager.endSpansWithError(b._1.value, "Failed to sign batch"))
          abort(s"Failed to sign local batches: ${errors.map(_.toString)}")
        } else {
          Availability.LocalDissemination.LocalBatchesStoredSigned(
            batches.zip(signatures).map { case ((batchId, batch), signature) =>
              spanManager.addEventToBatchSpans(batchId.value, "Batch signed")
              Availability.LocalDissemination
                .LocalBatchStoredSigned(batchId, batch, Some(signature))
            }
          )
        }
    }

  private def disseminateLocalBatches(
      actingOnMessageType: => String,
      batches: Seq[Availability.LocalDissemination.LocalBatchStoredSigned],
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    logger.debug(
      s"$actingOnMessageType: local batches ${batches.map(_.batchId)} ready to disseminate"
    )

    batches.foreach {
      case Availability.LocalDissemination.LocalBatchStoredSigned(
            tracedBatchId,
            batch,
            maybeSignature,
          ) =>
        val batchId = tracedBatchId.value
        maybeSignature.foreach { signature =>
          // Batch first signed or re-signed
          val progress =
            disseminationProtocolState.disseminationProgress
              .get(batchId)
              .fold[DisseminationStatus](
                DisseminationStatus
                  .InProgress(
                    activeMembership,
                    tracedBatchId,
                    acks = Set(AvailabilityAck(thisNode, signature)),
                    epochNumber = batch.epochNumber,
                    stats = batch.stats,
                    availabilityEnterInstant = disseminationProtocolState.beingFirstSaved
                      .remove(batchId)
                      .flatMap(_.availabilityEnterInstant),
                  )
              )(
                // The local ack is missing if we check dissemination after re-signing, so we need to add it
                _.addAck(AvailabilityAck(thisNode, signature))
              )
              // When freshly (re-)signed, we check if the progress is already complete; this happens with F == 0,
              //  as the local node's AvailabilityAck already constitutes a weak quorum (F + 1) by itself
              .update()
          logger.debug(s"$actingOnMessageType: progress of stored and signed $batchId is $progress")
          setProgress(actingOnMessageType, batchId, progress)
        }
        // Disseminate [further] if needed
        disseminationProtocolState.disseminationProgress
          .get(batchId)
          .foreach { progress =>
            val nodesToBeSentBatch = progress.sendBatchTo
            locally {
              implicit val traceContext: TraceContext = tracedBatchId.traceContext
              multicast(
                message = Availability.RemoteDissemination.RemoteBatch
                  .create(batchId, batch, from = thisNode),
                nodes = nodesToBeSentBatch,
              )
            }
            setProgress(
              actingOnMessageType,
              batchId,
              progress.addSends(nodesToBeSentBatch),
            )
          }
        // Storing and signing a batch can make it ready for ordering if F == 0
        attemptSatisfyingProposalRequest(actingOnMessageType)
    }
  }

  private def traceCompletion(
      actingOnMessageType: => String,
      batchId: BatchId,
      completed: DisseminationStatus.Complete,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"$actingOnMessageType: $batchId has completed dissemination in ${completed.membership}"
    )
    spanManager.addEventToBatchSpans(batchId, "Batch completed dissemination")
    emitBatchDisseminationLatency(completed)
  }

  private def updateLastKnownEpochNumberAndForgetExpiredBatches(
      actingOnMessageType: => String,
      currentEpoch: EpochNumber,
  )(implicit
      traceContext: TraceContext,
      context: E#ActorContextT[Availability.Message[E]],
  ): Seq[BatchId] =
    if (currentEpoch < lastKnownEpochNumber) {
      abort(
        s"Trying to update lastKnownEpochNumber in Availability module to $currentEpoch which is lower than the current value $lastKnownEpochNumber"
      )
    } else if (lastKnownEpochNumber != currentEpoch) {
      lastKnownEpochNumber = currentEpoch
      val batchValidityDuration = OrderingRequestBatch.BatchValidityDurationEpochs
      val expiredEpoch = EpochNumber(lastKnownEpochNumber - batchValidityDuration)

      def isBatchExpired(batchEpochNumber: EpochNumber) = batchEpochNumber <= expiredEpoch

      val expiredBatchIds =
        disseminationProtocolState
          .disseminationStatusView(s => Option.when(isBatchExpired(s.epochNumber))(s))
          .map(_._1)

      if (expiredBatchIds.nonEmpty) {
        logger.warn(s"$actingOnMessageType: discarding expired batches: ${expiredBatchIds.toSeq}")
        disseminationProtocolState.disseminationProgress --= expiredBatchIds
      }

      disseminationProtocolState.disseminationQuotas.expireEpoch(initialEpochNumber, expiredEpoch)
      val evictionEpoch = EpochNumber(expiredEpoch - batchValidityDuration)
      disseminationProtocolState.disseminationQuotas.evictBatches(evictionEpoch)
    } else Seq.empty

  private def handleConsensusMessage(
      consensusMessage: Availability.Consensus[E]
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(consensusMessage)

    consensusMessage match {
      case Availability.Consensus.Ordered(batchIds) =>
        logger.debug(
          s"Consensus acknowledged ordered batches $batchIds without requesting a block proposal"
        )
        removeOrderedBatchesAndPullFromMempool(messageType, batchIds)
        spanManager.finishBlockSpan(batchIds)

      case Availability.Consensus.CreateProposal(
            forBlock,
            currentEpochNumber,
            currentMembership,
            currentCryptoProvider: CryptoProvider[E],
            ordered,
          ) =>
        val batchesToBeEvicted =
          updateLastKnownEpochNumberAndForgetExpiredBatches(messageType, currentEpochNumber)

        spanManager.finishBlockSpan(ordered)

        if (batchesToBeEvicted.nonEmpty)
          context.pipeToSelf(availabilityStore.gc(batchesToBeEvicted)) {
            case Failure(error) =>
              logger.error("Failed to remove batches", error)
              None
            case Success(_) =>
              logger.debug(s"Evicted ${batchesToBeEvicted.size} batches")
              None
          }

        handleProposalRequest(
          messageType,
          forBlock,
          currentMembership,
          currentCryptoProvider,
          ordered,
        )

      case Availability.Consensus.UpdateTopologyDuringStateTransfer(
            currentMembership,
            currentCryptoProvider: CryptoProvider[E],
          ) =>
        // During state transfer we only try to keep the topology up-to-date to increase the chance that
        //  the output module can fetch, but we don't trigger a review of the dissemination progress
        //  because we won't receive proposal requests until state transfer is complete, at which point
        //  we will update the topology again and review the dissemination progress.
        //  We also don't try to satisfy consensus requests because consensus is inactive during state transfer.
        updateActiveMembership(messageType, currentMembership, currentCryptoProvider)
    }
  }

  private def handleProposalRequest(
      actingOnMessageType: => String,
      forBlock: BlockNumber,
      currentMembership: Membership,
      currentCryptoProvider: CryptoProvider[E],
      orderedBatchIds: Seq[BatchId],
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val newNextToBeProvidedToConsensus =
      NextToBeProvidedToConsensus(forBlock, Some(config.maxBatchesPerBlockProposal))
    val currentOrExpectedProposalRequestBlockNumber =
      disseminationProtocolState.nextToBeProvidedToConsensus.forBlock

    disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal match {
      case Some(maxBatches) => // Current proposal request
        if (forBlock <= currentOrExpectedProposalRequestBlockNumber)
          abort(
            s"$actingOnMessageType: got proposal request from local consensus for block $forBlock " +
              s"but there is an existing one for the same or a higher block number $currentOrExpectedProposalRequestBlockNumber"
          )

      case None => // Expected proposal request
        if (forBlock < currentOrExpectedProposalRequestBlockNumber)
          abort(
            s"$actingOnMessageType: got proposal request from local consensus for block $forBlock " +
              s"but we are expecting one for at least block number $currentOrExpectedProposalRequestBlockNumber"
          )
    }

    logger.debug(
      s"$actingOnMessageType: recording proposal request $newNextToBeProvidedToConsensus from local consensus " +
        s"(existing one = ${disseminationProtocolState.nextToBeProvidedToConsensus}) and reviewing progress"
    )
    disseminationProtocolState.nextToBeProvidedToConsensus = newNextToBeProvidedToConsensus

    removeOrderedBatchesAndPullFromMempool(actingOnMessageType, orderedBatchIds)

    updateActiveMembership(actingOnMessageType, currentMembership, currentCryptoProvider)

    if (topologyChangedSinceLastProposalRequest) {
      updateAndAdvanceAllDisseminationProgressBasedOnActiveMembership(actingOnMessageType)
      emitDisseminationStateStats(metrics, disseminationProtocolState)
      topologyChangedSinceLastProposalRequest = false
    }

    // When we receive a proposal request, we notify consensus if there is nothing to propose yet
    attemptSatisfyingProposalRequest(actingOnMessageType, notifyConsensusIfNoReadyBatches = true)
  }

  private def updateActiveMembership(
      actingOnMessageType: => String,
      newMembership: Membership,
      newCryptoProvider: CryptoProvider[E],
  )(implicit traceContext: TraceContext): Unit = {
    val activeTopologyActivationTime = activeMembership.orderingTopology.activationTime.value
    val newTopologyActivationTime = newMembership.orderingTopology.activationTime.value
    if (activeTopologyActivationTime > newTopologyActivationTime) {
      logger.warn(
        s"$actingOnMessageType: tried to overwrite topology with activation time $activeTopologyActivationTime " +
          s"using outdated topology with activation time $newTopologyActivationTime, dropping"
      )
    } else {
      logger.debug(s"$actingOnMessageType: updating active ordering topology to $newMembership")
      if (newMembership.orderingTopology != activeMembership.orderingTopology)
        topologyChangedSinceLastProposalRequest = true
      activeMembership = newMembership
      activeCryptoProvider = newCryptoProvider
      messageAuthorizer = newMembership.orderingTopology
    }
  }

  private def removeOrderedBatchesAndPullFromMempool(
      actingOnMessageType: => String,
      orderedBatchIds: Seq[BatchId],
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"$actingOnMessageType: batches $orderedBatchIds have been acked from consensus as ordered"
    )
    orderedBatchIds.foreach { orderedBatchId =>
      val removed = disseminationProtocolState.disseminationProgress.remove(orderedBatchId)
      val now = Instant.now
      removed
        .flatMap(_.asComplete.map(_.readyForOrderingInstant))
        .fold {
          logger.info(
            s"$actingOnMessageType: batch $orderedBatchId has been acked from consensus as ordered " +
              s"but its dissemination progress is untracked; this can happen if the node has been restarted"
          )
        } { readyForOrderingInstant =>
          emitBatchAvailabilityTotalLatency(readyForOrderingInstant, end = now)
          logger.debug(
            s"$actingOnMessageType: batch $orderedBatchId is now ordered, removed from dissemination progress"
          )
        }
    }
    initiateMempoolPull(actingOnMessageType)
    emitDisseminationStateStats(metrics, disseminationProtocolState)
  }

  private def updateAndAdvanceAllDisseminationProgressBasedOnActiveMembership(
      actingOnMessageType: => String
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    updateAllDisseminationProgressBasedOnActiveMembership(actingOnMessageType)

    val batchesThatNeedSigning = mutable.ListBuffer[Traced[BatchId]]()
    val batchesThatNeedMoreDissemination =
      mutable.ListBuffer[(Traced[BatchId], DisseminationStatus.InProgress)]()

    // Continue all in-progress disseminations
    disseminationProtocolState.disseminationInProgressView
      .map(_._2)
      .foreach { disseminationProgress =>
        val tracedBatchId = disseminationProgress.tracedBatchId
        if (disseminationProgress.needsSigning) {
          batchesThatNeedSigning.addOne(tracedBatchId)
        } else {
          if (disseminationProgress.sendBatchTo.nonEmpty) {
            batchesThatNeedMoreDissemination
              .addOne(tracedBatchId -> disseminationProgress)
              .discard
          }
        }
      }

    if (batchesThatNeedSigning.sizeIs > 0)
      fetchBatchesAndThenSelfSend(batchesThatNeedSigning)(
        // Will trigger signing and then further dissemination
        Availability.LocalDissemination.LocalBatchesStored(_)
      )

    if (batchesThatNeedMoreDissemination.sizeIs > 0)
      fetchBatchesAndThenSelfSend(batchesThatNeedMoreDissemination.map(_._1)) { batches =>
        Availability.LocalDissemination.LocalBatchesStoredSigned(
          batches.zip(batchesThatNeedMoreDissemination.map(_._2)).map {
            case ((batchId, batch), _) =>
              // "signature = None" will trigger further dissemination without resigning
              Availability.LocalDissemination
                .LocalBatchStoredSigned(batchId, batch, signature = None)
          }
        )
      }
  }

  private def updateAllDisseminationProgressBasedOnActiveMembership[X <: DisseminationStatus](
      actingOnMessageType: => String
  )(implicit traceContext: TraceContext): Unit = {
    val reviewResult =
      disseminationProtocolState.disseminationProgress
        .map { case (_, disseminationStatus) =>
          disseminationStatus -> disseminationStatus.changeMembership(activeMembership)
        }
    reviewResult.foreach { case (previousStatus, newStatus) =>
      val batchId = newStatus.tracedBatchId.value
      val previousAcks = previousStatus.acks
      val updatedAcks = newStatus.acks
      if (previousAcks != updatedAcks) {
        logger.debug(
          s"$actingOnMessageType: updated dissemination acks for batch $batchId " +
            s"from $previousAcks to $updatedAcks " +
            s"due to the new topology ${activeMembership.orderingTopology}"
        )
      }
      setProgress(actingOnMessageType, batchId, newStatus)
    }
  }

  private def setProgress(
      actingOnMessageType: => String,
      batchId: BatchId,
      newStatus: DisseminationStatus,
  )(implicit traceContext: TraceContext): Unit = {
    val prevStatus = disseminationProtocolState.disseminationProgress.put(batchId, newStatus)
    (prevStatus, newStatus) match {
      case (Some(_: DisseminationStatus.InProgress), c: DisseminationStatus.Complete) =>
        traceCompletion(actingOnMessageType, batchId, c)
      case _ => ()
    }
  }

  private def fetchBatchesAndThenSelfSend(
      batchIds: Iterable[Traced[BatchId]]
  )(f: Seq[(Traced[BatchId], OrderingRequestBatch)] => LocalDissemination)(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(availabilityStore.fetchBatches(batchIds.map(_.value).toSeq)) {
      case Failure(error) =>
        abort("Failed to fetch batches", error)
      case Success(AvailabilityStore.MissingBatches(missingBatchIds)) =>
        abort(s"Some batches couldn't be fetched: $missingBatchIds")
      case Success(AvailabilityStore.AllBatches(batches)) =>
        val batchIdToTracedMap = batchIds.view.map(x => x.value -> x).toMap
        val batchesWithTraced = batches.map { case (batchId, batch) =>
          val tracedBatchId =
            batchIdToTracedMap.getOrElse(batchId, Traced(batchId)(TraceContext.empty))
          tracedBatchId -> batch
        }
        f(batchesWithTraced)
    }

  private def attemptSatisfyingProposalRequest(
      actingOnMessageType: => String,
      notifyConsensusIfNoReadyBatches: Boolean = false,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val currentForBlock = disseminationProtocolState.nextToBeProvidedToConsensus.forBlock
    val disseminationComplete = disseminationProtocolState.disseminationCompleteView
    val areThereReadyBatches = disseminationComplete.nonEmpty
    disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal.foreach {
      maxBatchesPerProposal => // Actual request from consensus present
        if (areThereReadyBatches) {
          val batchesToBeProposed = disseminationComplete.take(maxBatchesPerProposal.toInt)
          emitBatchesQueuedForBlockInclusionLatencies(batchesToBeProposed)
          locally {
            val tracedProofOfAvailabilities =
              batchesToBeProposed.view.map(_._2.tracedProofOfAvailability)
            val proposal =
              Consensus.LocalAvailability.ProposalCreated(
                currentForBlock,
                OrderingBlock(tracedProofOfAvailabilities.map(_.value).toSeq),
              )
            val (span, newContext) = startSpan(s"BFTOrderer.Availability.ProposeBlock")(
              context.traceContextOfBatch(tracedProofOfAvailabilities),
              tracer,
            )
            spanManager.trackSpanForBlock(
              span.setAttribute("poas", tracedProofOfAvailabilities.size.toLong),
              proposal.orderingBlock,
            )
            logger.debug(
              s"$actingOnMessageType: providing proposal with batch IDs " +
                s"${proposal.orderingBlock.proofs.map(_.batchId)} to local consensus"
            )(newContext)
            dependencies.consensus.asyncSend(proposal)(newContext, mc)
          }
          disseminationProtocolState.lastProposalTime = Some(clock.now)
          emitDisseminationStateStats(metrics, disseminationProtocolState)

          disseminationProtocolState.nextToBeProvidedToConsensus =
            disseminationProtocolState.nextToBeProvidedToConsensus.copy(
              forBlock = BlockNumber(
                currentForBlock + 1
              ), // We expect the next proposal request to be for a higher block number
              maxBatchesPerProposal = None,
            )
        } else if (notifyConsensusIfNoReadyBatches) {
          logger.debug(
            s"$actingOnMessageType: no proposals available yet to provide to local consensus, notifying it"
          )
          // We signal to consensus that we don't have proposals available immediately at the time it was requested.
          // However, a proposal will still be sent out when one is available.
          dependencies.consensus.asyncSend(Consensus.LocalAvailability.NoProposalAvailableYet)
        }
    }
  }

  private def handleRemoteDisseminationMessage(
      disseminationMessage: Availability.RemoteDissemination
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(disseminationMessage)

    disseminationMessage match {

      case Availability.RemoteDissemination.RemoteBatch(batchId, batch, from) =>
        logger.debug(s"$messageType: received request from $from to store batch $batchId")
        val validationStart = Instant.now
        (for {
          _ <- validateBatch(batchId, batch, from)
          _ <- validateDisseminationQuota(batchId, from)
        } yield batch).fold(
          error => logger.warn(error),
          batch => {
            emitBatchValidationLatency(validationStart)
            outputFetchProtocolState.pendingRemoteBatchIdsToStore.add(batchId).discard
            pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
              case Failure(exception) =>
                abort(s"Failed to add batch $batchId", exception)

              case Success(addedToStore) =>
                Availability.LocalDissemination
                  .RemoteBatchStored(batchId, batch.epochNumber, from, addedToStore)
            }
          },
        )

      case Availability.RemoteDissemination.RemoteBatchAcknowledged(batchId, from, signature) =>
        disseminationProtocolState.disseminationProgress
          .get(batchId)
          .fold {
            logger.debug(
              s"$messageType: got a remote ack for batch $batchId from $from " +
                "but the batch is unknown (potentially already proposed), ignoring"
            )
          } { disseminationProgress =>
            // Best-effort deduplication: if a node receives an AvailabilityAck from a peer
            // for a batchId that already exists, we can drop that Ack immediately, without
            // bothering to check the signature using the active crypto provider.
            // However, a duplicate ack may be received while the first Ack's signature is
            // being verified (which means it's not in `disseminationProtocolState` yet), so we
            // also need (and have) a duplicate check later in the post-verify message processing.
            if (disseminationProgress.ackOf(from).isDefined)
              logger.debug(
                s"$messageType: duplicate remote ack for batch $batchId from $from, ignoring"
              )
            else {
              val epochNumber = disseminationProgress.epochNumber
              pipeToSelf(
                activeCryptoProvider
                  .verifySignature(
                    AvailabilityAck.hashFor(batchId, epochNumber, from, metrics),
                    from,
                    signature,
                    "availability-signature-verify-ack",
                  )
              ) {
                case Failure(exception) =>
                  abort(s"Failed to verify $batchId from $from signature: $signature", exception)
                case Success(Left(exception)) =>
                  emitInvalidMessage(metrics, from)
                  logger.warn(
                    s"$messageType: $from sent invalid ACK for batch $batchId " +
                      s"(signature $signature doesn't match), ignoring",
                    exception,
                  )
                  Availability.NoOp
                case Success(Right(())) =>
                  LocalDissemination.RemoteBatchAcknowledgeVerified(batchId, from, signature)
              }
            }
          }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def handleLocalOutputFetchMessage(
      outputFetchMessage: Availability.LocalOutputFetch
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(outputFetchMessage)

    outputFetchMessage match {

      case Availability.LocalOutputFetch.FetchBlockData(blockForOutput) =>
        val batchIdsToFind = blockForOutput.orderedBlock.batchRefs.map(_.batchId)
        val request = new BatchesRequest(blockForOutput, mutable.SortedSet.from(batchIdsToFind))
        outputFetchProtocolState.pendingBatchesRequests.append(request)
        fetchBatchesForOutputRequest(request)

      case Availability.LocalOutputFetch.FetchedBlockDataFromStorage(request, result) =>
        result match {
          case AvailabilityStore.MissingBatches(missingBatchIds) =>
            request.missingBatches.filterInPlace(missingBatchIds.contains)
            if (request.missingBatches.isEmpty) {
              // this case can happen if:
              // * we stored a missing batch after the fetch request
              // * but the response of the stored occur before the fetch
              fetchBatchesForOutputRequest(request)
            } else {
              request.missingBatches.foreach { missingBatchId =>
                if (
                  !outputFetchProtocolState.pendingRemoteBatchIdsToStore.contains(missingBatchId)
                ) {
                  // we are missing batches, so for each batch we are missing we will request
                  // it from another node until we get all of them again.
                  outputFetchProtocolState
                    .findProofOfAvailabilityForMissingBatchId(missingBatchId)
                    .fold {
                      logger.warn(
                        s"we are missing proof of availability for $missingBatchId, so we don't know nodes to ask."
                      )
                    } { proofOfAvailability =>
                      fetchBatchDataFromNodes(
                        messageType,
                        proofOfAvailability,
                        request.blockForOutput.mode,
                      )
                    }
                } else {
                  logger.debug(
                    s"Missing batch $missingBatchId is actually in the process of being stored"
                  )
                }
              }
            }
          case AvailabilityStore.AllBatches(batches) =>
            // We received all the batches that the output module requested
            // so we can send them to output module
            request.missingBatches.clear()
            batches.foreach { case (batchId, _) =>
              disseminationProtocolState.disseminationQuotas.removeOrderedBatch(batchId)
            }
            dependencies.output.asyncSend(
              Output.BlockDataFetched(CompleteBlockData(request.blockForOutput, batches))
            )
        }
        outputFetchProtocolState.removeRequestsWithNoMissingBatches()

      case Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(batchId, maybeBatch) =>
        maybeBatch match {
          case Some(batch) =>
            logger.debug(
              s"$messageType: $batchId provided by local store"
            )
            outputFetchProtocolState.incomingBatchRequests
              .get(batchId)
              .toList
              .flatMap(_.toSeq)
              .foreach { nodeId =>
                logger.debug(
                  s"$messageType: node '$nodeId' had requested $batchId, sending it"
                )
                send(
                  Availability.RemoteOutputFetch.RemoteBatchDataFetched
                    .create(thisNode, batchId, batch),
                  nodeId,
                )
              }
          case None =>
            logger.debug(s"$messageType: $batchId not found in local store")
        }
        logger.debug(s"$messageType: removing $batchId from incoming batch requests")
        outputFetchProtocolState.incomingBatchRequests.remove(batchId).discard

      case Availability.LocalOutputFetch.FetchedBatchStored(batchId) =>
        outputFetchProtocolState.pendingRemoteBatchIdsToStore.remove(batchId).discard
        outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
          case Some(_) =>
            logger.debug(s"$messageType: $batchId was missing and is now persisted")
            outputFetchProtocolState.localOutputMissingBatches.remove(batchId).discard
            updateOutputFetchStatus(batchId)
          case None =>
            logger.info(s"$messageType: $batchId was not missing")
        }

      case Availability.LocalOutputFetch.FetchRemoteBatchDataTimeout(batchId) =>
        if (outputFetchProtocolState.pendingRemoteBatchIdsToStore.contains(batchId)) {
          logger.info(s"Won't retry fetching remote batch $batchId, because it is being stored")
          return
        }
        val status = outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
          case Some(value) => value
          case None =>
            logger.debug(
              s"$messageType: got timeout for batch $batchId that is not missing anymore, ignoring"
            )
            return
        }
        val (node, remainingNodes) =
          status.remainingNodesToTry.headOption match {
            case None =>
              val logMessage =
                s"$messageType: got fetch timeout for $batchId but no nodes to try left, " +
                  "restarting fetch from the beginning"
              if (
                status.numberOfAttempts % config.availabilityNumberOfAttemptsOfDownloadingOutputFetchBeforeWarning == 0
              ) {
                logger.warn(logMessage)
              } else {
                logger.info(logMessage)
              }
              // We tried all nodes and all timed out so we retry all again in the hope that we are just
              //  experiencing temporarily network outage.
              //  We have to keep retrying because the output module is blocked until we get these batches.
              //  If these batches cannot be retrieved, e.g. because the topology has changed too much and/or
              //  the nodes in the PoA are unreachable indefinitely, we'll need to resort (possibly manually)
              //  to state transfer incl. the batch payloads (when it is implemented).
              if (status.mode.isStateTransfer)
                extractNodes(None, useActiveTopology = true)
              else
                extractNodes(Some(status.originalProof.acks))

            case Some(node) =>
              logger.debug(
                s"$messageType: got fetch timeout for $batchId, trying fetch from $node"
              )
              (node, status.remainingNodesToTry.drop(1))
          }
        val missingBatchStatus =
          status.copy(
            remainingNodesToTry = remainingNodes,
            numberOfAttempts =
              status.numberOfAttempts + (if (status.remainingNodesToTry.isEmpty) 1 else 0),
          )
        outputFetchProtocolState.localOutputMissingBatches.update(
          batchId,
          missingBatchStatus,
        )
        startDownload(batchId, node, missingBatchStatus.calculateTimeout())

      // This message is only used for tests
      case Availability.LocalOutputFetch.FetchBatchDataFromNodes(proofOfAvailability, mode) =>
        fetchBatchDataFromNodes(messageType, proofOfAvailability, mode)
    }
  }

  private def handleRemoteOutputFetchMessage(
      outputFetchMessage: Availability.RemoteOutputFetch
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(outputFetchMessage)

    outputFetchMessage match {
      case Availability.RemoteOutputFetch.FetchRemoteBatchData(batchId, from) =>
        outputFetchProtocolState.incomingBatchRequests
          .updateWith(batchId) {
            case Some(value) =>
              logger.debug(
                s"$messageType: $from also requested batch $batchId (already fetching), noting"
              )
              Some(value.+(from))
            case None =>
              logger.debug(
                s"$messageType received from $from first request for batch $batchId, loading it"
              )
              // It's safe to run the fetch before setting the `incomingBatchRequests` entry
              //  because modules are single-threaded and the completion message will be
              //  processed afterward.
              pipeToSelf(availabilityStore.fetchBatches(Seq(batchId))) {
                case Failure(exception) =>
                  abort(s"failed to fetch batch $batchId", exception)
                case Success(result) =>
                  result match {
                    case AvailabilityStore.MissingBatches(_) =>
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(batchId, None)
                    case AvailabilityStore.AllBatches(Seq((_, result))) =>
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(
                        batchId,
                        Some(result),
                      )
                    case AvailabilityStore.AllBatches(batches) =>
                      abort(s"Wrong batches fetched. Requested only $batchId, got $batches")
                  }
              }
              Some(Set(from))
          }
          .discard

      case message: Availability.RemoteOutputFetch.RemoteBatchDataFetched =>
        handleRemoteBatchDataFetched(message)
    }
  }

  private def handleRemoteBatchDataFetched(
      message: Availability.RemoteOutputFetch.RemoteBatchDataFetched
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(message)
    val batchId = message.batchId

    outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
      case Some(_) =>
        val batch = message.batch
        val from = message.from
        validateBatch(batchId, batch, from).fold(
          error => logger.warn(error),
          _ => {
            logger.debug(s"$messageType: received $batchId, persisting it")
            outputFetchProtocolState.pendingRemoteBatchIdsToStore.add(batchId).discard
            pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
              case Failure(exception) =>
                abort(s"Failed to add batch $batchId", exception)
              case Success(_) =>
                Availability.LocalOutputFetch.FetchedBatchStored(batchId)
            }
          },
        )
      case None =>
        logger.debug(s"$messageType: received $batchId but nobody needs it, ignoring")
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def fetchBatchDataFromNodes(
      actingOnMessageType: => String,
      proofOfAvailability: ProofOfAvailability,
      mode: OrderedBlockForOutput.Mode,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    if (outputFetchProtocolState.localOutputMissingBatches.contains(proofOfAvailability.batchId)) {
      logger.debug(
        s"$actingOnMessageType: already trying to download ${proofOfAvailability.batchId}, ignoring"
      )
      return
    }
    if (proofOfAvailability.acks.isEmpty) {
      logger.error(s"$actingOnMessageType: proof of availability is missing, ignoring")
      return
    }
    val (node, remainingNodes) =
      if (mode.isStateTransfer)
        extractNodes(acks = None, useActiveTopology = true)
      else
        extractNodes(Some(proofOfAvailability.acks))
    logger.debug(
      s"$actingOnMessageType: fetch of ${proofOfAvailability.batchId} " +
        s"requested from local store, trying to fetch from $node"
    )
    val missingBatchStatus = MissingBatchStatus(
      proofOfAvailability.batchId,
      proofOfAvailability,
      remainingNodes,
      numberOfAttempts = 1,
      jitterStream = jitterConstructor(config, random),
      mode,
    )
    outputFetchProtocolState.localOutputMissingBatches.update(
      proofOfAvailability.batchId,
      missingBatchStatus,
    )
    startDownload(proofOfAvailability.batchId, node, missingBatchStatus.calculateTimeout())
  }

  private def updateOutputFetchStatus(
      newlyReceivedBatchId: BatchId
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    outputFetchProtocolState.pendingBatchesRequests.foreach { request =>
      request.missingBatches.remove(newlyReceivedBatchId).discard
      if (request.missingBatches.isEmpty) {
        // we got the last batch needed from an output-module request,
        // but we don't have the data for the batches in memory so we ask the availability store
        fetchBatchesForOutputRequest(request)
      }
    }
    outputFetchProtocolState.removeRequestsWithNoMissingBatches()
  }

  private def fetchBatchesForOutputRequest(
      request: BatchesRequest
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val batchIds = request.blockForOutput.orderedBlock.batchRefs.map(_.batchId)
    pipeToSelf(availabilityStore.fetchBatches(batchIds)) {
      case Failure(exception) =>
        abort(s"Failed to load batches $batchIds", exception)
      case Success(result) =>
        Availability.LocalOutputFetch.FetchedBlockDataFromStorage(request, result)
    }
  }

  private def startDownload(
      batchId: BatchId,
      node: BftNodeId,
      timeout: FiniteDuration,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    // We might consider doing parallel downloads in the future, as typically the network between nodes will have high
    //  bandwidth and should be able to support it. However, presently there is no evidence that this is a winning
    //  strategy in a majority of situations.
    context
      .delayedEvent(
        timeout,
        Availability.LocalOutputFetch.FetchRemoteBatchDataTimeout(batchId),
      )
      .discard
    send(
      Availability.RemoteOutputFetch.FetchRemoteBatchData.create(batchId, from = thisNode),
      node,
    )
  }

  private def extractNodes(
      acks: Option[Seq[AvailabilityAck]],
      useActiveTopology: Boolean = false,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): (BftNodeId, Seq[BftNodeId]) = {
    val nodes =
      if (useActiveTopology) activeMembership.otherNodes.toSeq
      else acks.getOrElse(abort("No availability acks provided for extracting nodes")).map(_.from)
    val shuffled = nodeShuffler.shuffle(nodes)
    val head = shuffled.headOption.getOrElse(abort("There should be at least one node to extract"))
    head -> shuffled.tail
  }

  private def initiateMempoolPull(
      actingOnMessageType: => String
  )(implicit traceContext: TraceContext): Unit = {
    recordStartWaitIfIdle()
    // we tell mempool we want enough batches to fill up a proposal in order to make up for the one we just created
    // times the multiplier in order to try to disseminate-ahead batches for a following proposal
    val atMost = config.maxBatchesPerBlockProposal * DisseminateAheadMultiplier -
      // if we have pending batches for ordering we subtract them in order for this buffer to not grow indefinitely
      disseminationProtocolState.disseminationProgress.size
    if (atMost > 0) {
      logger.debug(s"$actingOnMessageType: requesting at most $atMost batches from local mempool")
      dependencies.mempool.asyncSendNoTrace(Mempool.CreateLocalBatches(atMost.toShort))
    }
  }

  private def recordStartWaitIfIdle(): Unit = {
    import disseminationProtocolState.*
    if (
      nextToBeProvidedToConsensus.maxBatchesPerProposal.nonEmpty &&
      disseminationProtocolState.disseminationCompleteView.isEmpty &&
      disseminationProgress.isEmpty
    )
      waitingForBatchSince = Some(Instant.now)
  }

  private def handleFailure[ErrorType, Result](
      errorString: => String
  )(handle: Result => Availability.Message[E])(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Try[Either[ErrorType, Result]] => Availability.Message[E] = {
    case Failure(exception) =>
      abort(errorString, exception)
    case Success(Left(exception)) =>
      abort(errorString ++ s": $exception")
    case Success(Right(value)) =>
      handle(value)
  }

  private def send(message: RemoteProtocolMessage, to: BftNodeId)(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      activeCryptoProvider.signMessage(
        message,
        AuthenticatedMessageType.BftSignedAvailabilityMessage,
      )
    )(
      handleFailure(s"Can't sign message $message") { signedMessage =>
        dependencies.p2pNetworkOut.asyncSend(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(signedMessage),
            to,
          )
        )
        Availability.NoOp
      }
    )

  private def multicast(message: RemoteProtocolMessage, nodes: Set[BftNodeId])(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    if (nodes.nonEmpty)
      pipeToSelf(
        activeCryptoProvider.signMessage(
          message,
          AuthenticatedMessageType.BftSignedAvailabilityMessage,
        )
      )(
        handleFailure(s"Can't sign message $message") { signedMessage =>
          dependencies.p2pNetworkOut.asyncSend(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(signedMessage),
              nodes,
            )
          )
          Availability.NoOp
        }
      )

  private def validateBatch(
      batchId: BatchId,
      batch: OrderingRequestBatch,
      from: BftNodeId,
  ): Either[String, Unit] =
    for {
      _ <- Either.cond(
        BatchId.from(batch) == batchId,
        (), {
          emitInvalidMessage(metrics, from)
          s"BatchId doesn't match digest for remote batch from $from, skipping"
        },
      )

      _ <- Either.cond(
        batch.requests.sizeIs <= config.maxRequestsInBatch.toInt,
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains more requests (${batch.requests.size}) than allowed " +
            s"(${config.maxRequestsInBatch}), skipping"
        },
      )

      _ <- {
        Either.cond(
          batch.requests.map(_.value.payload).forall(_.size() <= config.maxRequestPayloadBytes),
          (), {
            emitInvalidMessage(metrics, from)
            s"Batch $batchId from '$from' contains one or more batches that exceed the maximum " +
              s"allowed request size bytes (${config.maxRequestPayloadBytes}), skipping"
          },
        )
      }

      _ <- Either.cond(
        !checkTags || batch.requests.map(_.value).forall(_.isTagValid),
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains requests with invalid tags, valid tags are: (${OrderingRequest.ValidTags
              .mkString(", ")}); skipping"
        },
      )

      _ <- Either.cond(
        batch.epochNumber > lastKnownEpochNumber - BatchValidityDurationEpochs,
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains an expired batch at epoch number ${batch.epochNumber} " +
            s"which is $BatchValidityDurationEpochs " +
            s"epochs or more older than last known epoch $lastKnownEpochNumber, skipping"
        },
      )

      _ <- Either.cond(
        batch.epochNumber < lastKnownEpochNumber + OrderingRequestBatch.BatchValidityDurationEpochs * 2,
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains a batch whose epoch number ${batch.epochNumber} is too far in the future " +
            s"compared to last known epoch $lastKnownEpochNumber, skipping"
        },
      )
    } yield ()

  private def validateDisseminationQuota(
      batchId: BatchId,
      from: BftNodeId,
  ): Either[String, Unit] = Either.cond(
    disseminationProtocolState.disseminationQuotas
      .canAcceptForNode(from, batchId, config.availabilityMaxNonOrderedBatchesPerNode.toInt),
    (), {
      emitInvalidMessage(metrics, from)
      s"Batch $batchId from '$from' cannot be taken because we have reached the limit of ${config.availabilityMaxNonOrderedBatchesPerNode} unordered and unexpired batches from " +
        s"this node that we can hold on to, skipping"
    },
  )

  private def emitBatchWaitLatency(): Unit = {
    import metrics.performance.orderingStageLatency.*
    val now = Instant.now()
    emitOrderingStageLatency(
      labels.stage.values.availability.BatchWait,
      // Always emit batch wait latency for dashboard clarity, even if 0
      startInstant = waitingForBatchSince.orElse(Some(now)),
      endInstant = now,
      cleanup = () => waitingForBatchSince = None,
    )
  }

  private def emitBatchAvailabilityTotalLatency(
      readyForOrderingInstant: Option[Instant],
      end: Instant,
  ): Unit = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.BatchAvailabilityTotal,
      readyForOrderingInstant,
      end,
    )
  }

  private def emitBatchDisseminationLatency(
      disseminationProgress: DisseminationStatus
  ): Unit = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.availability.BatchDissemination,
      disseminationProgress.availabilityEnterInstant,
    )
  }

  private def emitBatchesQueuedForBlockInclusionLatencies(
      batchesToBeProposed: Iterable[(BatchId, DisseminationStatus.Complete)]
  ): Unit = {
    val now = Instant.now
    import metrics.performance.orderingStageLatency.*
    batchesToBeProposed
      .map(_._2.readyForOrderingInstant)
      .foreach(
        emitOrderingStageLatency(
          labels.stage.values.availability.dissemination.BatchQueuedForBlockInclusion,
          _,
          now,
        )
      )
  }

  private def emitBatchValidationLatency(validationStart: Instant): Unit = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.availability.dissemination.BatchValidation,
      Some(validationStart),
    )
  }
}

object AvailabilityModule {

  private def parseAvailabilityNetworkMessage(
      from: BftNodeId,
      message: v30.AvailabilityMessage,
      originalMessage: ByteString,
  ): ParsingResult[Availability.RemoteProtocolMessage] =
    message.message match {
      case v30.AvailabilityMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
      case v30.AvailabilityMessage.Message.Ping(_) =>
        Left(ProtoDeserializationError.OtherError("Ping Received"))
      case v30.AvailabilityMessage.Message.StoreRequest(value) =>
        Availability.RemoteDissemination.RemoteBatch.fromProtoV30(from, value)(originalMessage)
      case v30.AvailabilityMessage.Message.StoreResponse(value) =>
        Availability.RemoteDissemination.RemoteBatchAcknowledged.fromProtoV30(from, value)(
          originalMessage
        )
      case v30.AvailabilityMessage.Message.BatchRequest(value) =>
        Availability.RemoteOutputFetch.FetchRemoteBatchData.fromProtoV30(from, value)(
          originalMessage
        )
      case v30.AvailabilityMessage.Message.BatchResponse(value) =>
        Availability.RemoteOutputFetch.RemoteBatchDataFetched.fromProtoV30(from, value)(
          originalMessage
        )
    }

  private[bftordering] def hasDisseminationQuorum(
      orderingTopology: OrderingTopology,
      votes: Int,
  ): Boolean =
    orderingTopology.hasWeakQuorum(votes)

  private[bftordering] def quorum(numberOfNodes: Int): Int =
    OrderingTopology.weakQuorumSize(numberOfNodes)

  def parseNetworkMessage(
      protoSignedMessage: v30.SignedMessage
  ): ParsingResult[Availability.UnverifiedProtocolMessage] =
    SignedMessage
      .fromProtoWithNodeId(v30.AvailabilityMessage)(from =>
        proto =>
          originalByteString => parseAvailabilityNetworkMessage(from, proto, originalByteString)
      )(protoSignedMessage)
      .map(Availability.UnverifiedProtocolMessage.apply)

  @VisibleForTesting
  private[availability] val DisseminateAheadMultiplier = 2
}
