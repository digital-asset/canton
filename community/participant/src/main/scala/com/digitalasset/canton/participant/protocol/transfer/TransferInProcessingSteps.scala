// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.lf.data.{Bytes, Ref}
import com.daml.lf.engine.Error as LfError
import com.daml.lf.interpretation.Error as LfInterpretationError
import com.daml.lf.transaction.Node.KeyWithMaintainers
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DecryptionError as _, EncryptionError as _, *}
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{
  ProcessingSteps,
  ProtocolProcessor,
  SingleDomainCausalTracker,
  TransferInUpdate,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{LfPartyId, LfWorkflowId, RequestCounter, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private[transfer] class TransferInProcessingSteps(
    domainId: DomainId,
    val participantId: ParticipantId,
    val engine: DAMLe,
    transferCoordination: TransferCoordination,
    seedGenerator: SeedGenerator,
    causalityTracking: Boolean,
    targetProtocolVersion: TargetProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransferProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      TransferInViewType,
      TransferInResult,
      PendingTransferIn,
    ]
    with NamedLogging {

  import TransferInProcessingSteps.*

  override def requestKind: String = "TransferIn"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submitterMetadata.submitter}, transferId ${param.transferId}"

  override type SubmissionResultArgs = PendingTransferSubmission

  override type PendingDataAndResponseArgs = TransferInProcessingSteps.PendingDataAndResponseArgs

  override type RequestType = ProcessingSteps.RequestType.TransferIn
  override val requestType = ProcessingSteps.RequestType.TransferIn

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions = {
    state.pendingTransferInSubmissions
  }

  override def submissionIdOfPendingRequest(pendingData: PendingTransferIn): RootHash =
    pendingData.rootHash

  override def prepareSubmission(
      param: SubmissionParam,
      mediatorId: MediatorId,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Submission] = {

    val SubmissionParam(
      submitterMetadata,
      transferId,
      workflowId,
      sourceProtocolVersion,
    ) = param
    val ipsSnapshot = recentSnapshot.ipsSnapshot
    val pureCrypto = recentSnapshot.pureCrypto
    val submitter = submitterMetadata.submitter

    def activeParticipantsOfParty(
        party: LfPartyId
    ): EitherT[Future, TransferProcessorError, Set[ParticipantId]] =
      EitherT(ipsSnapshot.activeParticipantsOf(party).map(_.keySet).map { participants =>
        Either.cond(
          participants.nonEmpty,
          participants,
          NoParticipantForReceivingParty(transferId, party),
        )
      })

    val result = for {
      transferData <- ephemeralState.transferLookup
        .lookup(transferId)
        .leftMap(err => NoTransferData(transferId, err))
      transferOutResult <- EitherT.fromEither[Future](
        transferData.transferOutResult.toRight(TransferOutIncomplete(transferId, participantId))
      )

      targetDomain = transferData.targetDomain
      _ = if (targetDomain != domainId)
        throw new IllegalStateException(
          s"Transfer-in $transferId: Transfer data for ${transferData.targetDomain} found on wrong domain $domainId"
        )

      stakeholders = transferData.transferOutRequest.stakeholders
      _ <- condUnitET[Future](
        stakeholders.contains(submitter),
        SubmittingPartyMustBeStakeholder(Some(transferId), submitter, stakeholders),
      )

      submitterRelationship <- EitherT(
        ipsSnapshot
          .hostedOn(submitter, participantId)
          .map(_.toRight(PartyNotHosted(transferId, submitter, participantId)))
      )

      _ <- condUnitET[Future](
        submitterRelationship.permission == ParticipantPermission.Submission,
        NoSubmissionPermission(Some(transferId), submitter, participantId),
      )
      transferInUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()
      fullTree = makeFullTransferInTree(
        pureCrypto,
        seed,
        submitterMetadata,
        workflowId,
        stakeholders,
        transferData.contract,
        transferData.creatingTransactionId,
        targetDomain,
        mediatorId,
        transferOutResult,
        transferInUuid,
        sourceProtocolVersion,
        targetProtocolVersion,
      )
      rootHash = fullTree.rootHash
      mediatorMessage = fullTree.mediatorMessage
      recipientsSet <- {
        stakeholders.toSeq
          .parTraverse(activeParticipantsOfParty)
          .map(_.foldLeft(Set.empty[Member])(_ ++ _))
      }
      recipients <- EitherT.fromEither[Future](
        Recipients
          .ofSet(recipientsSet)
          .toRight(NoStakeholders.logAndCreate(transferData.contract.contractId, logger))
      )

      viewMessage <- EncryptedViewMessageFactory
        .create(TransferInViewType)(fullTree, recentSnapshot, targetProtocolVersion.v)
        .leftMap[TransferProcessorError](EncryptionError)
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          domainId,
          targetProtocolVersion.v,
          ViewType.TransferInViewType,
          EmptyRootHashMessagePayload,
        )
      // Each member gets a message sent to itself and to the mediator
      val rootHashRecipients =
        Recipients.groups(
          checked(
            NonEmptyUtil.fromUnsafe(
              recipientsSet.toSeq.map(participant => NonEmpty(Set, mediatorId, participant))
            )
          )
        )
      val messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediatorId),
        viewMessage -> recipients,
        rootHashMessage -> rootHashRecipients,
      )
      TransferSubmission(Batch.of(targetProtocolVersion.v, messages: _*), rootHash)
    }

    result.mapK(FutureUnlessShutdown.outcomeK).widen[Submission]
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      submissionId: PendingSubmissionId,
  ): EitherT[Future, TransferProcessorError, SubmissionResultArgs] = {
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      Some(submissionParam.transferId),
      submissionParam.submitterLf,
      submissionId,
    )
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult =
    SubmissionResult(pendingSubmission.transferCompletion.future)

  override protected def decryptTree(snapshot: DomainSnapshotSyncCryptoApi)(
      envelope: OpenEnvelope[EncryptedViewMessage[TransferInViewType]]
  ): EitherT[Future, EncryptedViewMessageDecryptionError[TransferInViewType], WithRecipients[
    FullTransferInTree
  ]] =
    EncryptedViewMessage
      .decryptFor(
        snapshot,
        envelope.protocolMessage,
        participantId,
        targetProtocolVersion.v,
      ) { bytes =>
        FullTransferInTree
          .fromByteString(snapshot.pureCrypto)(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(WithRecipients(_, envelope.recipients))

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[FullTransferInTree]]],
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CheckActivenessAndWritePendingContracts] = {
    // TODO(M40): Send a rejection if malformedPayloads is non-empty
    val correctRootHashes = decryptedViews.map(_.unwrap)
    for {
      txInRequest <- EitherT.cond[Future](
        correctRootHashes.toList.sizeCompare(1) == 0,
        correctRootHashes.head1,
        ReceivedMultipleRequests(correctRootHashes.map(_.viewHash)): TransferProcessorError,
      )
      contractId = txInRequest.contract.contractId

      _ <- condUnitET[Future](
        txInRequest.domainId == domainId,
        UnexpectedDomain(
          txInRequest.transferOutResultEvent.transferId,
          targetDomain = txInRequest.domainId,
          receivedOn = domainId,
        ),
      ).leftWiden[TransferProcessorError]

      // TODO(M40): check recipients

      transferringParticipant = txInRequest.transferOutResultEvent.unwrap.informees
        .contains(participantId.adminParty.toLf)

      contractIdS = Set(contractId)
      contractCheck = ActivenessCheck(
        checkFresh = Set.empty,
        checkFree = contractIdS,
        checkActive = Set.empty,
        lock = contractIdS,
      )
      activenessSet = ActivenessSet(
        contracts = contractCheck,
        transferIds =
          if (transferringParticipant) Set(txInRequest.transferOutResultEvent.transferId)
          else Set.empty,
        // We check keys on only domains with unique contract key semantics and there cannot be transfers on such domains
        keys = ActivenessCheck.empty,
      )
    } yield CheckActivenessAndWritePendingContracts(
      activenessSet,
      Seq(WithTransactionId(txInRequest.contract, txInRequest.creatingTransactionId)),
      PendingDataAndResponseArgs(
        txInRequest,
        ts,
        rc,
        sc,
        snapshot,
        transferringParticipant,
      ),
    )
  }

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      causalityLookup: SingleDomainCausalTracker,
      activenessResultFuture: Future[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, StorePendingDataAndSendResponseAndCreateTimeout] = {

    val PendingDataAndResponseArgs(
      txInRequest,
      ts,
      rc,
      sc,
      targetCrypto,
      transferringParticipant,
    ) = pendingDataAndResponseArgs

    val transferId = txInRequest.transferOutResultEvent.transferId

    def checkStakeholders(
        transferInRequest: FullTransferInTree
    ): EitherT[Future, TransferProcessorError, Unit] = {
      val declaredContractStakeholders = transferInRequest.contract.metadata.stakeholders
      val declaredViewStakeholders = transferInRequest.stakeholders

      for {
        metadata <- engine
          .contractMetadata(
            transferInRequest.contract.contractInstance,
            declaredContractStakeholders,
          )
          .leftMap {
            case LfError.Interpretation(
                  e @ LfError.Interpretation.DamlException(
                    LfInterpretationError.FailedAuthorization(_, _)
                  ),
                  _,
                ) =>
              StakeholderMismatch(
                Some(transferId),
                declaredViewStakeholders = declaredViewStakeholders,
                declaredContractStakeholders = Some(declaredContractStakeholders),
                expectedStakeholders = Left(e.message),
              )
            case error => MetadataNotFound(error)
          }
        recomputedStakeholders = metadata.stakeholders
        _ <- condUnitET[Future](
          declaredViewStakeholders == recomputedStakeholders && declaredViewStakeholders == declaredContractStakeholders,
          StakeholderMismatch(
            Some(transferId),
            declaredViewStakeholders = declaredViewStakeholders,
            declaredContractStakeholders = Some(declaredContractStakeholders),
            expectedStakeholders = Right(recomputedStakeholders),
          ),
        ).leftWiden[TransferProcessorError]
      } yield ()
    }

    // The transferring participant must send on the causal state at the time of the transfer-out.
    // This state is sent to all participants hosting a party that the transferring participant confirms for.
    def checkCausalityState(
        confirmFor: Set[LfPartyId]
    ): EitherT[Future, TransferProcessorError, List[(CausalityMessage, Recipients)]] = {
      if (transferringParticipant && causalityTracking) {
        val clocksF = causalityLookup.awaitAndFetchTransferOut(transferId, confirmFor)
        for {
          clocks <- EitherT.liftF(clocksF)
          confirmForClocks <- {
            val (noInfoFor, clocksList) =
              confirmFor.toList.map(p => clocks.get(p).toRight(left = p)).separate

            val either = if (noInfoFor.isEmpty) {
              Right(clocksList)
            } else {
              logger.error(
                s"Transferring participant is missing causality information for ${noInfoFor}."
              )
              Left(CausalityInformationMissing(missingFor = noInfoFor.toSet))
            }
            EitherT.fromEither[Future](either)
          }

          recipients <- EitherT.liftF {
            confirmForClocks.parTraverse { clock =>
              val hostedBy =
                targetCrypto.ipsSnapshot.activeParticipantsOf(clock.partyId).map(_.keySet)
              hostedBy.map(ptps => (clock, ptps))
            }
          }

          causalityMessages = {
            recipients.flatMap { case (clock, hostedBy) =>
              val msg = CausalityMessage(domainId, targetProtocolVersion.v, transferId, clock)
              logger.debug(
                s"Sending causality message for $transferId with clock $clock to $hostedBy"
              )
              Recipients.ofSet(hostedBy).map(msg -> _).toList
            }
          }
        } yield causalityMessages
      } else EitherT.rightT[Future, TransferProcessorError](List.empty)
    }

    for {
      _ <- checkStakeholders(txInRequest)

      hostedStks <- EitherT.liftF[Future, TransferProcessorError, List[LfPartyId]](
        hostedStakeholders(
          txInRequest.contract.metadata.stakeholders.toList,
          targetCrypto.ipsSnapshot,
        )
      )

      transferDataO <- EitherT.right[TransferProcessorError](
        transferLookup.lookup(transferId).toOption.value
      )
      validationResultO <- validateTransferInRequest(
        ts,
        txInRequest,
        transferDataO,
        targetCrypto,
        transferringParticipant,
      )

      activenessResult <- EitherT.right[TransferProcessorError](activenessResultFuture)
      requestId = RequestId(ts)

      // construct pending data and response
      entry = PendingTransferIn(
        requestId,
        rc,
        sc,
        txInRequest.tree.rootHash,
        txInRequest.contract,
        txInRequest.submitterMetadata,
        txInRequest.workflowId,
        txInRequest.creatingTransactionId,
        transferringParticipant,
        transferId,
        hostedStks.toSet,
      )
      responsesAndCausalityMessages <- validationResultO match {
        case None => EitherT.rightT[Future, TransferProcessorError]((Seq.empty, Seq.empty))
        case Some(validationResult) =>
          val contractResult = activenessResult.contracts
          val localVerdict =
            if (activenessResult.isSuccessful) LocalApprove()(targetProtocolVersion.v)
            else if (contractResult.notFree.nonEmpty) {
              contractResult.notFree.toSeq match {
                case Seq((coid, state)) =>
                  if (state == ActiveContractStore.Archived)
                    LocalReject.TransferInRejects.ContractAlreadyArchived.Reject(show"coid=$coid")(
                      targetProtocolVersion.v
                    )
                  else
                    LocalReject.TransferInRejects.ContractAlreadyActive.Reject(show"coid=$coid")(
                      targetProtocolVersion.v
                    )
                case coids =>
                  throw new RuntimeException(
                    s"Activeness result for a transfer-in fails for multiple contract IDs $coids"
                  )
              }
            } else if (contractResult.alreadyLocked.nonEmpty)
              LocalReject.TransferInRejects.ContractIsLocked.Reject("")(targetProtocolVersion.v)
            else if (activenessResult.inactiveTransfers.nonEmpty)
              LocalReject.TransferInRejects.AlreadyCompleted.Reject("")(targetProtocolVersion.v)
            else
              throw new RuntimeException(
                withRequestId(requestId, s"Unexpected activeness result $activenessResult")
              )
          for {
            transferResponse <- EitherT.fromEither[Future](
              MediatorResponse
                .create(
                  requestId,
                  participantId,
                  Some(txInRequest.viewHash),
                  localVerdict,
                  txInRequest.toBeSigned,
                  validationResult.confirmingParties,
                  domainId,
                  targetProtocolVersion.v,
                )
                .leftMap(e => FailedToCreateResponse(e): TransferProcessorError)
            )
            causalityMessages <- checkCausalityState(validationResult.confirmingParties)
          } yield Seq(transferResponse -> Recipients.cc(mediatorId)) -> causalityMessages
      }

      (responses, causalityMessage) = responsesAndCausalityMessages
    } yield {
      StorePendingDataAndSendResponseAndCreateTimeout(
        entry,
        responses,
        causalityMessage,
        RejectionArgs(
          entry,
          LocalReject.TimeRejects.LocalTimeout.Reject()(targetProtocolVersion.v),
        ),
      )
    }
  }

  private[this] def withRequestId(requestId: RequestId, message: String) =
    s"Transfer-in $requestId: $message"

  @VisibleForTesting
  private[transfer] def validateTransferInRequest(
      tsIn: CantonTimestamp,
      transferInRequest: FullTransferInTree,
      transferDataO: Option[TransferData],
      targetCrypto: DomainSnapshotSyncCryptoApi,
      transferringParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Option[TransferInValidationResult]] = {
    val txOutResultEvent = transferInRequest.transferOutResultEvent.result

    val transferId = transferInRequest.transferOutResultEvent.transferId

    def checkSubmitterIsStakeholder: Either[TransferProcessorError, Unit] =
      condUnitE(
        transferInRequest.stakeholders.contains(transferInRequest.submitter),
        SubmittingPartyMustBeStakeholder(
          Some(transferId),
          transferInRequest.submitter,
          transferInRequest.stakeholders,
        ),
      )

    val targetIps = targetCrypto.ipsSnapshot

    transferDataO match {
      case Some(transferData) =>
        val sourceDomain = transferData.transferOutRequest.sourceDomain
        val transferOutTimestamp = transferData.transferOutTimestamp
        for {
          _ready <- {
            logger.info(
              s"Waiting for topology state at ${transferOutTimestamp} on transfer-out domain $sourceDomain ..."
            )
            EitherT(
              transferCoordination
                .awaitTransferOutTimestamp(sourceDomain, transferOutTimestamp)
                .sequence
            )
          }

          sourceCrypto <- transferCoordination.cryptoSnapshot(sourceDomain, transferOutTimestamp)
          // TODO(M40): Check the signatures of the mediator and the sequencer

          _ <- condUnitET[Future](
            txOutResultEvent.content.timestamp <= transferData.transferOutDecisionTime,
            ResultTimestampExceedsDecisionTime(
              transferId,
              timestamp = txOutResultEvent.content.timestamp,
              decisionTime = transferData.transferOutDecisionTime,
            ),
          )

          // TODO(M40): Validate the shipped transfer-out result w.r.t. stakeholders
          // TODO(M40): Validate that the transfer-out result received matches the transfer-out result in transferData

          _ <- condUnitET[Future](
            transferInRequest.contract == transferData.contract,
            ContractDataMismatch(transferId),
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          transferOutSubmitter = transferData.transferOutRequest.submitter
          exclusivityBaseline = transferData.transferOutRequest.targetTimeProof.timestamp

          // TODO(M40): Check that transferData.transferOutRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshot <- transferCoordination
            .cryptoSnapshot(
              transferData.targetDomain,
              transferData.transferOutRequest.targetTimeProof.timestamp,
            )

          /*
            We use `findDynamicDomainParameters` rather than `findDynamicDomainParametersOrDefault`
            because it makes no sense to progress if we don't manage to fetch domain parameters.
            Also, the `findDynamicDomainParametersOrDefault` method expected protocol version
            that we don't have here.
           */
          domainParameters <- EitherT(
            cryptoSnapshot.ipsSnapshot
              .findDynamicDomainParameters()
              .map(
                _.toRight(
                  DomainNotReady(transferData.targetDomain, "Unable to fetch domain parameters")
                )
              )
          )

          exclusivityLimit = domainParameters.transferExclusivityLimitFor(exclusivityBaseline)

          _ <- condUnitET[Future](
            tsIn >= exclusivityLimit
              || transferOutSubmitter == transferInRequest.submitter,
            NonInitiatorSubmitsBeforeExclusivityTimeout(
              transferId,
              transferInRequest.submitter,
              currentTimestamp = tsIn,
              timeout = exclusivityLimit,
            ),
          )
          _ <- condUnitET[Future](
            transferData.creatingTransactionId == transferInRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              transferId,
              transferInRequest.creatingTransactionId,
              transferData.creatingTransactionId,
            ),
          )
          sourceIps = sourceCrypto.ipsSnapshot
          confirmingParties <- EitherT.right(
            transferInRequest.stakeholders.toList.parTraverseFilter { stakeholder =>
              for {
                source <- sourceIps.canConfirm(participantId, stakeholder)
                target <- targetIps.canConfirm(participantId, stakeholder)
              } yield if (source && target) Some(stakeholder) else None
            }
          )

        } yield Some(TransferInValidationResult(confirmingParties.toSet))
      case None =>
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (transferringParticipant) {
              val targetIps = targetCrypto.ipsSnapshot
              val confirmingPartiesF = transferInRequest.stakeholders.toList
                .parTraverseFilter { stakeholder =>
                  targetIps
                    .canConfirm(participantId, stakeholder)
                    .map(if (_) Some(stakeholder) else None)
                }
                .map(_.toSet)
              EitherT(confirmingPartiesF.map { confirmingParties =>
                Right(Some(TransferInValidationResult(confirmingParties))): Either[
                  TransferProcessorError,
                  Option[TransferInValidationResult],
                ]
              })
            } else EitherT.rightT[Future, TransferProcessorError](None)
        } yield res
    }
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
      message: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, TransferInResult],
      pendingRequestData: PendingTransferIn,
      pendingSubmissionMap: PendingSubmissions,
      tracker: SingleDomainCausalTracker,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    // TODO(M40): Check that the notification tree is as expected
    val PendingTransferIn(
      requestId,
      requestCounter,
      requestSequencerCounter,
      rootHash,
      contract,
      submitterMetadata,
      workflowId,
      creatingTransactionId,
      transferringParticipant,
      transferId,
      hostedStakeholders,
    ) = pendingRequestData

    import scala.util.Either.MergeableEither
    MergeableEither[MediatorResult](result).merge.verdict match {
      case _: Verdict.Approve =>
        val commitSet = CommitSet(
          archivals = Map.empty,
          creations = Map.empty,
          transferOuts = Map.empty,
          transferIns = Map(
            contract.contractId -> WithContractHash
              .fromContract(contract, WithContractMetadata(transferId, contract.metadata))
          ),
          keyUpdates = Map.empty,
        )
        val commitSetO = Some(Future.successful(commitSet))
        val contractsToBeStored = Set(contract.contractId)

        for {
          event <- createTransferredIn(
            contract,
            creatingTransactionId,
            requestId.unwrap,
            submitterMetadata,
            transferId,
            rootHash,
            createTransactionAccepted = !transferringParticipant,
            workflowId,
          )
          timestampEvent = Some(
            TimestampedEvent(event, requestCounter.asLocalOffset, Some(requestSequencerCounter))
          )
        } yield CommitAndStoreContractsAndPublishEvent(
          commitSetO,
          contractsToBeStored,
          timestampEvent,
          Some(
            TransferInUpdate(
              hostedStakeholders,
              pendingRequestData.requestId.unwrap,
              domainId,
              requestCounter,
              transferId,
              targetProtocolVersion,
            )
          ),
        )

      case Verdict.ParticipantReject(_) | (_: Verdict.MediatorReject) =>
        EitherT.pure(CommitAndStoreContractsAndPublishEvent(None, Set(), None, None))
    }
  }

  private[transfer] def createTransferredIn(
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      recordTime: CantonTimestamp,
      submitterMetadata: TransferSubmitterMetadata,
      transferOutId: TransferId,
      rootHash: RootHash,
      createTransactionAccepted: Boolean,
      workflowId: Option[LfWorkflowId],
  ): EitherT[Future, TransferProcessorError, LedgerSyncEvent.TransferredIn] = {
    val targetDomain = domainId
    val contractInst = contract.contractInstance.unversioned
    val createNode: LfNodeCreate =
      LfNodeCreate(
        contract.contractId,
        contractInst.template,
        contractInst.arg,
        contract.rawContractInstance.agreementText.v,
        contract.metadata.signatories,
        contract.metadata.stakeholders,
        key = contract.metadata.maybeKeyWithMaintainers.map(keyWithMaintainers =>
          KeyWithMaintainers(keyWithMaintainers.globalKey.key, keyWithMaintainers.maintainers)
        ),
        contract.contractInstance.version,
      )
    val driverContractMetadata = contract.contractSalt
      .map { salt =>
        DriverContractMetadata(salt).toLfBytes(targetProtocolVersion.v)
      }
      .getOrElse(Bytes.Empty)

    for {
      updateId <- EitherT.fromEither[Future](
        rootHash.asLedgerTransactionId.leftMap[TransferProcessorError](
          FieldConversionError("Transaction id (root hash)", _)
        )
      )

      ledgerCreatingTransactionId <- EitherT.fromEither[Future](
        creatingTransactionId.asLedgerTransactionId.leftMap[TransferProcessorError](
          FieldConversionError("Transaction id (creating transaction)", _)
        )
      )

      completionInfo =
        Option.when(participantId.toLf == submitterMetadata.submittingParticipant)(
          CompletionInfo(
            actAs = List(submitterMetadata.submitter),
            applicationId = submitterMetadata.applicationId,
            commandId = Ref.CommandId.assertFromString("command-id"),
            optDeduplicationPeriod = None,
            submissionId = submitterMetadata.submissionId,
            statistics = None,
          )
        )
    } yield LedgerSyncEvent.TransferredIn(
      updateId = updateId,
      optCompletionInfo = completionInfo,
      submitter = submitterMetadata.submitter,
      recordTime = recordTime.toLf,
      ledgerCreateTime = contract.ledgerCreateTime.toLf,
      createNode = createNode,
      creatingTransactionId = ledgerCreatingTransactionId,
      contractMetadata = driverContractMetadata,
      transferOutId = transferOutId,
      targetDomain = targetDomain,
      createTransactionAccepted = createTransactionAccepted,
      workflowId = workflowId,
    )
  }
}

object TransferInProcessingSteps {

  case class SubmissionParam(
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      workflowId: Option[LfWorkflowId],
      sourceProtocolVersion: SourceProtocolVersion,
  ) {
    val submitterLf: LfPartyId = submitterMetadata.submitter
  }

  case class SubmissionResult(transferInCompletionF: Future[com.google.rpc.status.Status])

  case class PendingTransferIn(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      rootHash: RootHash,
      contract: SerializableContract,
      submitterMetadata: TransferSubmitterMetadata,
      workflowId: Option[LfWorkflowId],
      creatingTransactionId: TransactionId,
      transferringParticipant: Boolean,
      transferId: TransferId,
      hostedStakeholders: Set[LfPartyId],
  ) extends PendingTransfer
      with PendingRequestData {
    override def pendingContracts: Set[LfContractId] = Set(contract.contractId)
  }

  case class TransferInValidationResult(confirmingParties: Set[LfPartyId])

  private[transfer] def makeFullTransferInTree(
      pureCrypto: CryptoPureApi,
      seed: SaltSeed,
      submitterMetadata: TransferSubmitterMetadata,
      workflowId: Option[LfWorkflowId],
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      transferOutResult: DeliveredTransferOutResult,
      transferInUuid: UUID,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
  ): FullTransferInTree = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)
    val commonData = TransferInCommonData.create(pureCrypto)(
      commonDataSalt,
      targetDomain,
      targetMediator,
      stakeholders,
      transferInUuid,
      targetProtocolVersion,
    )
    val view = TransferInView.create(pureCrypto)(
      viewSalt,
      submitterMetadata,
      workflowId,
      contract,
      creatingTransactionId,
      transferOutResult,
      sourceProtocolVersion,
      targetProtocolVersion,
    )
    val tree = TransferInViewTree(commonData, view)(pureCrypto)
    FullTransferInTree(tree)
  }

  case class PendingDataAndResponseArgs(
      txInRequest: FullTransferInTree,
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      targetCrypto: DomainSnapshotSyncCryptoApi,
      transferringParticipant: Boolean,
  )

  private[transfer] sealed trait TransferInProcessorError extends TransferProcessorError

  case class NoTransferData(transferId: TransferId, lookupError: TransferStore.TransferLookupError)
      extends TransferInProcessorError

  case class TransferOutIncomplete(transferId: TransferId, participant: ParticipantId)
      extends TransferInProcessorError

  case class PartyNotHosted(transferId: TransferId, party: LfPartyId, participant: ParticipantId)
      extends TransferInProcessorError

  case class NoParticipantForReceivingParty(transferId: TransferId, party: LfPartyId)
      extends TransferInProcessorError

  case class UnexpectedDomain(transferId: TransferId, targetDomain: DomainId, receivedOn: DomainId)
      extends TransferInProcessorError

  case class ResultTimestampExceedsDecisionTime(
      transferId: TransferId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  ) extends TransferInProcessorError

  case class NonInitiatorSubmitsBeforeExclusivityTimeout(
      transferId: TransferId,
      submitter: LfPartyId,
      currentTimestamp: CantonTimestamp,
      timeout: CantonTimestamp,
  ) extends TransferInProcessorError

  case class IdentityStateNotAvailable(
      transferId: TransferId,
      sourceDomain: DomainId,
      timestamp: CantonTimestamp,
  ) extends TransferInProcessorError

  case class ContractDataMismatch(transferId: TransferId) extends TransferProcessorError

  case class CreatingTransactionIdMismatch(
      transferId: TransferId,
      transferInTransactionId: TransactionId,
      localTransactionId: TransactionId,
  ) extends TransferProcessorError

}
