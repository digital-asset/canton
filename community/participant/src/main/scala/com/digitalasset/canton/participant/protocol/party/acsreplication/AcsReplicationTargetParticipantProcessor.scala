// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.EphemeralSequencerChannelProgress
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicator.AcsReplicationRequestId
import com.digitalasset.canton.participant.config.OnlinePartyReplicationTargetConfig
import com.digitalasset.canton.participant.protocol.party.*
import com.digitalasset.canton.participant.protocol.party.AcsTransferContractHandler.AcsTransferCheckpoint
import com.digitalasset.canton.participant.protocol.party.TargetParticipantAcsPersistence.contractsToRequestEachTime
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationSourceParticipantMessage.GetAcsArguments
import com.digitalasset.canton.participant.store
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersionValidation
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** The target participant processor ingests a party's active contracts on a specific synchronizer
  * and timestamp from a source participant as part of Online Party Replication.
  *
  * The interaction happens via the
  * [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]] API and
  * the target participant processor enforces the protocol guarantees made by a
  * [[AcsReplicationSourceParticipantProcessor]]. The following guarantees made by the target
  * participant processor are verifiable at the party replication protocol: The target participant
  *   - sends a [[AcsReplicationTargetParticipantMessage.Initialize]] upon (re-)connecting,
  *   - requests contracts in a strictly increasing contract ordinal order,
  *   - and sends only deserializable payloads.
  *
  * @param psid
  *   The physical id of the synchronizer to replicate active contracts in.
  * @param partyId
  *   The party whose ACS is being replicated.
  * @param requestId
  *   The "add ACS" request id that this replication is associated with.
  * @param partyOnboardingAt
  *   The timestamp immediately on which the ACS snapshot is based.
  * @param excludedStakeholders
  *   Shared contract stakeholder parties excluded from the read ACS, for example as in the case of
  *   party replication, exclude parties already hosted by the target participant.
  * @param sourceParticipantId
  *   The source participant id needed for ACS digest signature verification.
  * @param agreedAt
  *   The time at which it was agreed to replicate the ACS via sequencer channel.
  * @param replicationProgressState
  *   Interface for processor to read and update ACS replication progress.
  * @param onError
  *   Callback notification that the target participant has encountered an error.
  * @param onDisconnect
  *   Callback notification that the target participant has disconnected.
  * @param acsTransferContractHandler
  *   Handler that validates and persists imported ACS contracts.
  * @param config
  *   Target participant configuration options.
  * @param testOnlyInterceptor
  *   Test interceptor only alters behavior in integration tests.
  */
// TODO(#35267) add the apply method back once PartyReplicator will supply TargetParticipantAcsPersistence
class AcsReplicationTargetParticipantProcessor(
    protected val psid: PhysicalSynchronizerId,
    partyId: PartyId,
    requestId: AcsReplicationRequestId,
    partyOnboardingAt: EffectiveTime,
    excludedStakeholders: Set[PartyId],
    sourceParticipantId: ParticipantId,
    agreedAt: CantonTimestamp,
    protected val replicationProgressState: store.AcsReplicationProgress,
    protected val onError: String => Unit,
    protected val onDisconnect: (String, TraceContext) => Unit,
    acsTransferContractHandler: AcsTransferContractHandler,
    config: OnlinePartyReplicationTargetConfig,
    protected val futureSupervisor: FutureSupervisor,
    protected val exitOnFatalFailures: Boolean,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    protected val testOnlyInterceptor: PartyReplicationTestInterceptor,
)(implicit override val executionContext: ExecutionContext)
    extends AcsReplicationProcessor {

  protected val processorStore: TargetParticipantStore = InMemoryProcessorStore.targetParticipant()

  override def replicatedContractsCount: NonNegativeLong = processorStore.processedContractsCount

  override protected def name: String = "acs-replication-target-processor"

  private val acsDigestHelper = new PartyReplicationAcsDigestHelper(
    GetAcsArguments(
      partyId,
      psid.logical,
      partyOnboardingAt.value,
      excludedStakeholders,
    ),
    sourceParticipantId,
    agreedAt,
    initialAcsHashO = replicationProgressState
      .getAcsReplicationProgress(requestId)(
        TraceContext.empty
      )
      .flatMap(_.acsHashO),
    psid,
  )

  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle connect to SP") {
    super.onConnected().map { _ =>
      // Upon connecting or reconnecting, clear the initial contract ordinal.
      processorStore.resetConnection()
      progressAcsReplication()
    }
  }

  /** Consume status updates and ACS batches from the source participant.
    *
    * Note: Assigning the internal contract ids to the contracts requires that all the contracts are
    * already persisted in the contract store.
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle payload from SP") {
    notifyCounterParticipantAndAcsReplicatorOnError(for {
      messageFromSP <- EitherT.fromEither[FutureUnlessShutdown](
        AcsReplicationSourceParticipantMessage
          .fromByteString(ProtocolVersionValidation(protocolVersion), protocolVersion, payload)
          .leftMap(deserializationError =>
            s"Failed to parse payload message from SP: ${deserializationError.message}"
          )
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        processorStore.initialContractOrdinalInclusiveO.isDefined,
        s"Received unexpected message from SP before initialized by TP: ${messageFromSP.dataOrStatus}",
      )
      replicationProgress <- EitherT.fromEither[FutureUnlessShutdown](
        replicationProgressState
          .getAcsReplicationProgress(requestId)
          .toRight(s"ACS replication $requestId not found in progress state")
      )
      replicatedContractCount = replicationProgress.processedContractCount
      _ <- messageFromSP.dataOrStatus match {
        case AcsReplicationSourceParticipantMessage.AcsBatch(contracts) =>
          val firstContractOrdinal = replicatedContractCount
          logger.debug(
            s"Received batch beginning at contract ordinal $firstContractOrdinal with contracts ${contracts.forgetNE
                .flatMap(_.contract.createdEvent.map(_.contractId))
                .mkString(", ")}"
          )
          for {
            _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
              !replicationProgress.fullyProcessedAcs,
              s"Received ACS batch from SP after EndOfACS at $firstContractOrdinal",
            )
            _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
              replicatedContractCount.unwrap + contracts.size <= processorStore.requestedContractsCount.unwrap,
              s"Received too many contracts from SP: processed ${replicatedContractCount.unwrap} + received ${contracts.size} > requested ${processorStore.requestedContractsCount.unwrap}",
            )
            validatedContracts <- EitherT.fromEither[FutureUnlessShutdown](
              ReceivedContractValidation.validateContracts(contracts, psid.logical)
            )
            deltaAcsDigest = acsDigestHelper.computeDeltaDigest(validatedContracts)
            acsDigestHashWithDelta = acsDigestHelper.computeUnionHash(deltaAcsDigest)
            checkpoint <- acsTransferContractHandler.handleValidatedContracts(
              validatedContracts,
              AcsTransferCheckpoint(
                replicationProgress.processedContractCount,
                replicationProgress.nextPersistenceCounter,
              ),
            )
            _ <- replicationProgressState.updateAcsReplicationProgress(
              requestId,
              EphemeralSequencerChannelProgress(
                checkpoint.processedContractCount,
                checkpoint.nextPersistenceCounter,
                Some(acsDigestHashWithDelta),
                fullyProcessedAcs = false,
                Some(this),
              ),
            )
            // Only commit delta to digest after status persistence has succeeded
            _ = acsDigestHelper.applyDelta(deltaAcsDigest)
            _ = processorStore.setProcessedContractsCount(checkpoint.processedContractCount)
          } yield ()
        case AcsReplicationSourceParticipantMessage.EndOfAcs(
              acsDigest,
              acsDigestByteString,
              signature,
            ) =>
          logger.info(
            s"Target participant has received end of data after ${replicatedContractCount.unwrap} contracts"
          )
          if (logger.underlying.isDebugEnabled()) {
            logger.debug(s"Received AcsDigest $acsDigest with signature $signature")
          }
          for {
            _ <- EitherT.fromEither[FutureUnlessShutdown](verifyAcsDigest(acsDigest))
            _ <- EitherTUtil.ifThenET(!config.disableAcsDigestSourceSignatureValidation)(
              verifySourceParticipantSignature(acsDigestByteString, signature)
            )
            _ <- replicationProgressState
              .updateAcsReplicationProgress(
                requestId,
                EphemeralSequencerChannelProgress(
                  replicationProgress.processedContractCount,
                  replicationProgress.nextPersistenceCounter,
                  replicationProgress.acsHashO,
                  fullyProcessedAcs = true,
                  Some(this),
                ),
              )
          } yield processorStore.setHasEndOfACSBeenReached()
      }
    } yield ()).map(_ => progressAcsReplication())
  }

  private def verifyAcsDigest(
      acsDigestReceived: AcsReplicationSourceParticipantMessage.AcsDigest
  ): Either[String, Unit] = {
    val expectedAcsDigest = acsDigestHelper.extractAcsDigest()
    Either.cond(
      expectedAcsDigest == acsDigestReceived,
      (),
      s"Received ACS digest from source participant $acsDigestReceived does not match expected ACS digest $expectedAcsDigest",
    )
  }

  private def verifySourceParticipantSignature(
      acsDigestByteString: ByteString,
      signature: Signature,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      cryptoClient <- EitherT.fromEither[FutureUnlessShutdown](
        getCryptoClient.toRight(s"Need crypto client for signature verification")
      )
      // Use signing key valid at the ACS snapshot asOf timestamp.
      snapshot <- EitherT.right[String](cryptoClient.snapshot(partyOnboardingAt.value))
      acsDigestHash = Hash
        .build(HashPurpose.AcsReplicationDigest, HashAlgorithm.Sha256)
        .addByteString(acsDigestByteString)
        .finish()

      _ <- snapshot
        .verifySignature(
          acsDigestHash,
          sourceParticipantId,
          signature,
          SigningKeyUsage.ProtocolOnly,
        )
        .leftMap(err => s"Problem with Source Participant ACS digest signature: $err")
    } yield ()

  override def progressAcsReplication()(implicit traceContext: TraceContext): Unit =
    // Skip progress check if more than one other task is already queued that performs this same progress check or
    // is going to schedule a progress check.
    if (executionQueue.isAtMostOneTaskScheduled) {
      executeAsync(s"Respond to source participant if needed") {

        val interceptorAction = Option
          .when(isChannelOpenForCommunication)(requestId)
          .flatMap(replicationProgressState.getAcsReplicationProgress)
          .map(testOnlyInterceptor.onTargetParticipantProgress)

        interceptorAction match {

          case Some(PartyReplicationTestInterceptor.Proceed) =>
            respondToSourceParticipant()

          case Some(PartyReplicationTestInterceptor.Fail(reason)) =>
            // Actively fail the asynchronous task and propagate the error
            EitherT.leftT[FutureUnlessShutdown, Unit](reason)

          case _ =>
            // Covers Wait, None, or closed channel
            EitherT.rightT[FutureUnlessShutdown, String](())
        }
      }
    }

  private def respondToSourceParticipant()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    replicationProgress <- EitherT.fromEither[FutureUnlessShutdown](
      replicationProgressState
        .getAcsReplicationProgress(requestId)
        .toRight(s"ACS replication $requestId not found in progress state")
    )
    replicatedContractCount = replicationProgress.processedContractCount
    _ <-
      if (replicationProgress.fullyProcessedAcs) {
        sendCompleted(
          "completing in response to source participant notification of end of data"
        )
      } else if (processorStore.initialContractOrdinalInclusiveO.isEmpty) {
        val initialContractOrdinalInclusive = replicatedContractCount
        logger.info(
          s"Connected. Requesting contracts from ${initialContractOrdinalInclusive.unwrap}"
        )
        val initializeSP = AcsReplicationTargetParticipantMessage(
          AcsReplicationTargetParticipantMessage.Initialize(initialContractOrdinalInclusive)
        )(
          AcsReplicationTargetParticipantMessage.protocolVersionRepresentativeFor(
            protocolVersion
          )
        )
        sendPayload("initialize source participant", initializeSP.toByteString).map { _ =>
          // Once the SP initialize message has been sent, set the initial contract ordinal
          // and reset the requested contracts count to the processed contracts count.
          processorStore.setInitialContractOrdinalInclusive(initialContractOrdinalInclusive)
          processorStore.setRequestedContractsCount(replicatedContractCount)
          progressAcsReplication()
        }
      } else if (replicatedContractCount == processorStore.requestedContractsCount) {
        logger.debug(
          s"Target participant has received all the contracts requested before ordinal ${replicatedContractCount.unwrap}. " +
            s"Requesting ${contractsToRequestEachTime.unwrap} more contracts from source participant"
        )
        requestNextSetOfContracts()
      } else {
        EitherT.rightT[FutureUnlessShutdown, String](())
      }
  } yield ()

  private def requestNextSetOfContracts()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val updatedContractOrdinalToRequestExclusive =
      processorStore.requestedContractsCount.map(_ + contractsToRequestEachTime.unwrap)
    val inclusiveContractOrdinal = updatedContractOrdinalToRequestExclusive.unwrap - 1
    val instructionMessage = AcsReplicationTargetParticipantMessage(
      AcsReplicationTargetParticipantMessage.SendAcsUpTo(
        NonNegativeLong.tryCreate(inclusiveContractOrdinal)
      )
    )(
      AcsReplicationTargetParticipantMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload(
      s"request next set of contracts up to ordinal $inclusiveContractOrdinal",
      instructionMessage.toByteString,
    ).map(_ => processorStore.setRequestedContractsCount(updatedContractOrdinalToRequestExclusive))
  }

  override protected def hasEndOfACSBeenReached: Boolean = processorStore.hasEndOfACSBeenReached
}
