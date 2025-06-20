// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.store.AcsInspection
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** The source participant processor exposes a party's active contracts on a specified synchronizer
  * and timestamp to a target participant as part of Online Party Replication.
  *
  * The interaction happens via the
  * [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]] API and
  * the source participant processor enforces the protocol guarantees made by a
  * [[PartyReplicationTargetParticipantProcessor]]. The following guarantees made by the source
  * participant processor are verifiable by the party replication protocol: The source participant
  *   - sends [[PartyReplicationSourceMessage.SourceParticipantIsReady]] when ready to send
  *     contracts,
  *   - only sends as many [[PartyReplicationSourceMessage.AcsChunk]]s as requested by the target
  *     participant to honor flow control,
  *   - sends [[PartyReplicationSourceMessage.AcsChunk]]s in strictly increasing and gap-free chunk
  *     id order,
  *   - sends [[PartyReplicationSourceMessage.EndOfACS]] iff the processor is closed by the next
  *     message,
  *   - and sends only deserializable payloads.
  *
  * @param synchronizerId
  *   The synchronizer id of the synchronizer to replicate active contracts within.
  * @param partyId
  *   The party id of the party to replicate active contracts for.
  * @param activeAfter
  *   The timestamp immediately after which the ACS snapshot is based, i.e. the time immediately
  *   after which the contract to be sent are active.
  * @param otherPartiesHostedByTargetParticipant
  *   The set of parties already hosted by the target participant (TP) other than the party being
  *   replicated. Used to skip over shared contracts already hosted on TP.
  * @param acsInspection
  *   Interface to inspect the ACS.
  * @param onProgress
  *   Callback to update progress wrt the number of active contracts sent.
  * @param onComplete
  *   Callback notification that the source participant has sent the entire ACS.
  * @param onError
  *   Callback notification that the source participant has errored.
  * @param protocolVersion
  *   The protocol version to use for now for the party replication protocol. Technically the online
  *   party replication protocol is a different protocol from the canton protocol.
  */
final class PartyReplicationSourceParticipantProcessor private (
    synchronizerId: SynchronizerId,
    partyId: PartyId,
    activeAfter: CantonTimestamp,
    // TODO(#23097): Revisit mechanism to consider "other parties" once we support support multiple concurrent OnPRs
    //  as the set of other parties would change dynamically.
    otherPartiesHostedByTargetParticipant: Set[LfPartyId],
    acsInspection: AcsInspection, // TODO(#24326): Stream the ACS via the Ledger Api instead.
    onProgress: NonNegativeInt => Unit,
    onComplete: NonNegativeInt => Unit,
    onError: String => Unit,
    protected val protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends SequencerChannelProtocolProcessor {
  private val chunkToSendUpToExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfContractsSent = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfContractsPerChunk = PositiveInt.two

  /** Once connected notify the target participant that the source participant is ready to be asked
    * to send contracts.
    */
  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // Once connected to target participant, send that source participant is ready.
    val status = PartyReplicationSourceMessage(
      PartyReplicationSourceMessage.SourceParticipantIsReady
    )(
      PartyReplicationSourceMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload("source participant ready", status.toByteString)
  }

  /** Handle instructions from the target participant
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    (for {
      instruction <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationInstruction
          .fromByteString(protocolVersion, payload)
          .leftMap(_.message)
      )
      previousChunkToSendUpToExclusive = chunkToSendUpToExclusive.get
      _ = logger.debug(
        s"Source participant has received instruction to send up to chunk ${instruction.maxCounter}"
      )
      // Check that the target participant is requesting higher chunk numbers.
      // The party replication protocol does not support retries as a TP is expected to establish a new connection
      // after disconnects if it needs to consume previously sent chunks.
      _ <- EitherTUtil.ifThenET(
        instruction.maxCounter < previousChunkToSendUpToExclusive
      ) {
        sendError(
          s"Target participant requested non-increasing chunk ${instruction.maxCounter} compared to previous chunk $previousChunkToSendUpToExclusive"
        )
      }
      newChunkToSendUpTo = instruction.maxCounter
      contracts <- readContracts(previousChunkToSendUpToExclusive, newChunkToSendUpTo)
      _ <- sendContracts(contracts)
      sendingUpToChunk = chunkToSendUpToExclusive.updateAndGet(
        _ + NonNegativeInt.tryCreate(contracts.size)
      )
      numContractsSent = numberOfContractsSent.updateAndGet(
        _ + NonNegativeInt.tryCreate(contracts.flatMap(_._1).size)
      )
      _ = onProgress(numContractsSent)

      // If there aren't enough contracts, send that we have reached the end of the ACS.
      _ <- EitherTUtil.ifThenET(sendingUpToChunk < newChunkToSendUpTo)(
        sendEndOfAcs(s"End of ACS after chunk $sendingUpToChunk").map(_ =>
          onComplete(numContractsSent)
        )
      )
    } yield ()).leftMap(_.tap { error =>
      logger.warn(s"Error while processing payload: $error")
      onError(error)
    })

  /** Reads contract chunks from the ACS in a brute-force fashion via AcsInspection until
    * TODO(#24326) reads the ACS via the Ledger API.
    */
  private def readContracts(
      newChunkToConsumerFrom: NonNegativeInt,
      newChunkToConsumeTo: NonNegativeInt,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Seq[
    (NonEmpty[Seq[ActiveContractOld]], NonNegativeInt)
  ]] = {
    val contracts = List.newBuilder[ActiveContractOld]
    synchronizeWithClosing(
      s"Read ACS from ${newChunkToConsumerFrom.unwrap} to $newChunkToConsumeTo"
    )(
      acsInspection
        .forEachVisibleActiveContract(
          synchronizerId,
          Set(partyId.toLf),
          Some(TimeOfChange(activeAfter.immediateSuccessor)),
        ) { case (contract, reassignmentCounter) =>
          val stakeholdersHostedByTargetParticipant =
            contract.metadata.stakeholders.intersect(otherPartiesHostedByTargetParticipant)
          if (stakeholdersHostedByTargetParticipant.isEmpty) {
            contracts += ActiveContractOld.create(synchronizerId, contract, reassignmentCounter)(
              protocolVersion
            )
          } else {
            // Skip contracts already hosted by the target participant.
            logger.debug(
              s"Skipping contract ${contract.contractId} as it is already hosted by ${stakeholdersHostedByTargetParticipant
                  .mkString(", ")} on the target participant between chunks $newChunkToConsumerFrom and $newChunkToConsumeTo}"
            )
          }
          Right(())
        }(traceContext, executionContext)
        .bimap(
          _.toString,
          _ =>
            contracts
              .result()
              .grouped(numberOfContractsPerChunk.unwrap)
              .toSeq
              .map(NonEmpty.from(_).getOrElse(throw new IllegalStateException("Grouping failed")))
              .zipWithIndex
              .slice(newChunkToConsumerFrom.unwrap, newChunkToConsumeTo.unwrap + 1)
              .map { case (chunk, index) => (chunk, NonNegativeInt.tryCreate(index)) },
        )
    )
  }

  private def sendContracts(
      indexedContractChunks: Seq[(NonEmpty[Seq[ActiveContractOld]], NonNegativeInt)]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(
      s"Source participant sending ${indexedContractChunks.size} chunks up to ${chunkToSendUpToExclusive.get().unwrap + indexedContractChunks.size}"
    )
    MonadUtil.sequentialTraverse_(indexedContractChunks) { case (chunkContracts, index) =>
      val acsChunk = PartyReplicationSourceMessage(
        PartyReplicationSourceMessage.AcsChunk(
          index,
          chunkContracts,
        )
      )(
        PartyReplicationSourceMessage.protocolVersionRepresentativeFor(protocolVersion)
      )
      sendPayload(s"ACS chunk $index", acsChunk.toByteString)
    }
  }

  private def sendEndOfAcs(endOfStreamMessage: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val endOfACS = PartyReplicationSourceMessage(
      PartyReplicationSourceMessage.EndOfACS
    )(
      PartyReplicationSourceMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    for {
      _ <- sendPayload(endOfStreamMessage, endOfACS.toByteString)
      _ <- sendCompleted(endOfStreamMessage)
    } yield ()
  }

  override def onDisconnected(status: Either[String, Unit])(implicit
      traceContext: TraceContext
  ): Unit = ()
}

object PartyReplicationSourceParticipantProcessor {
  def apply(
      synchronizerId: SynchronizerId,
      partyId: PartyId,
      activeAt: CantonTimestamp,
      partiesHostedByTargetParticipant: Set[LfPartyId],
      acsInspection: AcsInspection,
      onProgress: NonNegativeInt => Unit,
      onComplete: NonNegativeInt => Unit,
      onError: String => Unit,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PartyReplicationSourceParticipantProcessor =
    new PartyReplicationSourceParticipantProcessor(
      synchronizerId,
      partyId,
      activeAt,
      partiesHostedByTargetParticipant,
      acsInspection,
      onProgress,
      onComplete,
      onError,
      protocolVersion,
      timeouts,
      loggerFactory
        .append("synchronizerId", synchronizerId.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive),
    )
}
