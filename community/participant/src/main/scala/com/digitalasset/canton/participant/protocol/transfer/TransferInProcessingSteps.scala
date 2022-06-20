// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.alternative._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.daml.ledger.participant.state.v2.TransactionMeta
import com.daml.lf.CantonOnly
import com.daml.lf.data.ImmArray
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.interpretation.{Error => LfInterpretationError}
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DecryptionError => _, EncryptionError => _, _}
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data._
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.PendingRequestDataOrReplayData
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
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps._
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps._
import com.digitalasset.canton.participant.protocol.{
  ProtocolProcessor,
  SingleDomainCausalTracker,
  TransferInUpdate,
}
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.sync.{LedgerEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.{LedgerSyncEvent, RequestCounter}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import scala.collection.concurrent
import scala.collection.immutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

class TransferInProcessingSteps(
    domainId: DomainId,
    val participantId: ParticipantId,
    val engine: DAMLe,
    transferCoordination: TransferCoordination,
    seedGenerator: SeedGenerator,
    causalityTracking: Boolean,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransferProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      TransferInViewType,
      TransferInResult,
    ]
    with NamedLogging {

  import TransferInProcessingSteps.createUpdateForTransferIn

  override def requestKind: String = "TransferIn"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submitterLf}, transferId ${param.transferId}"

  override type PendingRequestData = PendingTransferIn

  override type SubmissionResultArgs = PendingTransferSubmission

  override type PendingDataAndResponseArgs = TransferInProcessingSteps.PendingDataAndResponseArgs

  override type RejectionArgs = Unit

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

    val SubmissionParam(submitterLf, transferId) = param
    val ipsSnapshot = recentSnapshot.ipsSnapshot
    val pureCrypto = recentSnapshot.pureCrypto

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
        stakeholders.contains(submitterLf),
        SubmittingPartyMustBeStakeholder(Some(transferId), submitterLf, stakeholders),
      )

      submitterRelationship <- EitherT(
        ipsSnapshot
          .hostedOn(submitterLf, participantId)
          .map(_.toRight(PartyNotHosted(transferId, submitterLf, participantId)))
      )

      _ <- condUnitET[Future](
        submitterRelationship.permission == ParticipantPermission.Submission,
        NoSubmissionPermission(Some(transferId), submitterLf, participantId),
      )

      transferInUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()
      fullTree = makeFullTransferInTree(
        pureCrypto,
        seed,
        submitterLf,
        stakeholders,
        transferData.contract,
        transferData.creatingTransactionId,
        targetDomain,
        mediatorId,
        transferOutResult,
        transferInUuid,
        protocolVersion,
      )
      rootHash = fullTree.rootHash
      mediatorMessage = fullTree.mediatorMessage
      recipientsSet <- {
        import cats.syntax.traverse._
        stakeholders.toSeq
          .traverse(activeParticipantsOfParty)
          .map(_.foldLeft(Set.empty[Member])(_ ++ _))
      }
      recipients <- EitherT.fromEither[Future](
        Recipients
          .ofSet(recipientsSet)
          .toRight(NoStakeholders.logAndCreate(transferData.contract.contractId, logger))
      )

      viewMessage <- EncryptedViewMessageFactory
        .create(TransferInViewType)(fullTree, recentSnapshot, protocolVersion)
        .leftMap[TransferProcessorError](EncryptionError)
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          domainId,
          protocolVersion,
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
      TransferSubmission(Batch.of(protocolVersion, messages: _*), rootHash)
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
      .decryptFor(snapshot, envelope.protocolMessage, participantId, protocolVersion) { bytes =>
        FullTransferInTree
          .fromByteString(snapshot.pureCrypto)(bytes)
          .leftMap(e => DeserializationError(e.toString, bytes))
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
        ): TransferProcessorError,
      )

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
          ): TransferProcessorError,
        )
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
            confirmForClocks.traverse { clock =>
              val hostedBy =
                targetCrypto.ipsSnapshot.activeParticipantsOf(clock.partyId).map(_.keySet)
              hostedBy.map(ptps => (clock, ptps))
            }
          }

          causalityMessages = {
            recipients.flatMap { case (clock, hostedBy) =>
              val msg = CausalityMessage(domainId, protocolVersion, transferId, clock)
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
            if (activenessResult.isSuccessful) LocalApprove
            else if (contractResult.notFree.nonEmpty) {
              contractResult.notFree.toSeq match {
                case Seq((coid, state)) =>
                  if (state == ActiveContractStore.Archived)
                    LocalReject.TransferInRejects.ContractAlreadyArchived.Reject(show"coid=$coid")
                  else
                    LocalReject.TransferInRejects.ContractAlreadyActive.Reject(show"coid=$coid")
                case coids =>
                  throw new RuntimeException(
                    s"Activeness result for a transfer-in fails for multiple contract IDs $coids"
                  )
              }
            } else if (contractResult.alreadyLocked.nonEmpty)
              LocalReject.TransferInRejects.ContractIsLocked.Reject("")
            else if (activenessResult.inactiveTransfers.nonEmpty)
              LocalReject.TransferInRejects.AlreadyCompleted.Reject("")
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
                  protocolVersion,
                )
                .leftMap(e => FailedToCreateResponse(e): TransferProcessorError)
            )
            causalityMessages <- checkCausalityState(validationResult.confirmingParties)
          } yield Seq(transferResponse -> Recipients.cc(mediatorId)) -> causalityMessages
      }

      (responses, causalityMessage) = responsesAndCausalityMessages
    } yield {
      StorePendingDataAndSendResponseAndCreateTimeout(entry, responses, causalityMessage, ())
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
        val originDomain = transferData.transferOutRequest.originDomain
        val transferOutTimestamp = transferData.transferOutTimestamp
        for {
          _ready <- {
            logger.info(
              s"Waiting for topology state at ${transferOutTimestamp} on transfer-out domain $originDomain ..."
            )
            EitherT(
              transferCoordination
                .awaitTransferOutTimestamp(originDomain, transferOutTimestamp)
                .sequence
            )
          }

          originCrypto <- transferCoordination.cryptoSnapshot(originDomain, transferOutTimestamp)
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
            ContractDataMismatch(transferId): TransferProcessorError,
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          transferOutSubmitter = transferData.transferOutRequest.submitter
          exclusivityBaseline = transferData.transferOutRequest.targetTimeProof.timestamp

          // TODO(M40): Check that transferData.transferOutRequest.targetTimeProof.timestamp is in the past
          domainParameters <- transferCoordination
            .cryptoSnapshot(
              transferData.targetDomain,
              transferData.transferOutRequest.targetTimeProof.timestamp,
            )
            .semiflatMap(_.ipsSnapshot.findDynamicDomainParametersOrDefault())

          exclusivityLimit = domainParameters.transferExclusivityLimitFor(exclusivityBaseline)

          _ <- condUnitET[Future](
            tsIn > exclusivityLimit
              || transferOutSubmitter == transferInRequest.submitter,
            NonInitiatorSubmitsBeforeExclusivityTimeout(
              transferId,
              transferInRequest.submitter,
              currentTimestamp = tsIn,
              timeout = exclusivityLimit,
            ): TransferProcessorError,
          )
          _ <- condUnitET[Future](
            transferData.creatingTransactionId == transferInRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              transferId,
              transferInRequest.creatingTransactionId,
              transferData.creatingTransactionId,
            ): TransferProcessorError,
          )
          originIps = originCrypto.ipsSnapshot
          confirmingParties <- EitherT.right(transferInRequest.stakeholders.toList.traverseFilter {
            stakeholder =>
              for {
                origin <- originIps.canConfirm(participantId, stakeholder)
                target <- targetIps.canConfirm(participantId, stakeholder)
              } yield if (origin && target) Some(stakeholder) else None
          })

        } yield Some(TransferInValidationResult(confirmingParties.toSet))
      case None =>
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (transferringParticipant) {
              val targetIps = targetCrypto.ipsSnapshot
              val confirmingPartiesF = transferInRequest.stakeholders.toList
                .traverseFilter { stakeholder =>
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

  override def pendingRequestMap
      : SyncDomainEphemeralState => concurrent.Map[RequestId, PendingRequestDataOrReplayData[
        PendingTransferIn
      ]] =
    _.pendingTransferIns

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
      creatingTransactionId,
      transferringParticipant,
      transferId,
      hostedStakeholders,
    ) = pendingRequestData

    import scala.util.Either.MergeableEither
    MergeableEither[MediatorResult](result).merge.verdict match {
      case Verdict.Approve =>
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

        /* Poor-man's heuristic for deciding when to signal the transfer-in to the ledger API server:
         * Emit an event if the participant is not transferring, as a transferring participant
         * must have known the contract already before the transfer.
         *
         * However, a non-transferring participant might have already known the contract,
         * for example, if the participant hosts distinct stakeholders on the two domains.
         * TODO(#4027): Generate an Enter event
         */
        val maybeEvent =
          if (transferringParticipant) None
          else {
            val event = createUpdateForTransferIn(contract, creatingTransactionId, requestId.unwrap)
            Some(TimestampedEvent(event, requestCounter, Some(requestSequencerCounter)))
          }

        EitherT.pure(
          CommitAndStoreContractsAndPublishEvent(
            commitSetO,
            contractsToBeStored,
            maybeEvent,
            Some(
              TransferInUpdate(
                hostedStakeholders,
                pendingRequestData.requestId.unwrap,
                domainId,
                requestCounter,
                transferId,
              )
            ),
          )
        )

      case Verdict.RejectReasons(_) | (_: Verdict.MediatorReject) | Verdict.Timeout =>
        EitherT.pure(CommitAndStoreContractsAndPublishEvent(None, Set(), None, None))
    }
  }
}

object TransferInProcessingSteps {

  case class SubmissionParam(submitterLf: LfPartyId, transferId: TransferId)

  case class SubmissionResult(transferInCompletionF: Future[com.google.rpc.status.Status])

  case class PendingTransferIn(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      rootHash: RootHash,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      transferringParticipant: Boolean,
      transferId: TransferId,
      hostedStakeholders: Set[LfPartyId],
  ) extends PendingTransfer
      with PendingRequestData {
    override def pendingContracts: Set[LfContractId] = Set(contract.contractId)
  }

  case class TransferInValidationResult(confirmingParties: Set[LfPartyId])

  /** Workaround to create an update for informing the ledger API server about a transferred-in contract.
    * Creates a TransactionAccepted event consisting of a single create action that creates the given contract.
    *
    * The transaction has the same ledger time and transaction id as the creation of the contract.
    */
  def createUpdateForTransferIn(
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      recordTime: CantonTimestamp,
  ): LedgerSyncEvent.TransactionAccepted = {
    val nodeId = LfNodeId(0)
    val contractInst = contract.contractInstance.unversioned
    val createNode =
      LfNodeCreate(
        contract.contractId,
        contractInst.template,
        contractInst.arg,
        contractInst.agreementText,
        contract.metadata.signatories,
        contract.metadata.stakeholders,
        key = None,
        contract.contractInstance.version,
      )
    val committedTransaction = LfCommittedTransaction(
      CantonOnly.lfVersionedTransaction(
        version = createNode.version,
        nodes = HashMap((nodeId, createNode)),
        roots = ImmArray(nodeId),
      )
    )
    val lfTransactionId = creatingTransactionId.tryAsLedgerTransactionId

    LedgerSyncEvent.TransactionAccepted(
      optCompletionInfo = None,
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = contract.ledgerCreateTime.toLf,
        workflowId = None,
        submissionTime =
          contract.ledgerCreateTime.toLf, // TODO(Andreas): Upstream mismatch, replace with enter/leave view
        submissionSeed = LedgerEvent.noOpSeed,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = committedTransaction,
      transactionId = lfTransactionId,
      recordTime = recordTime.toLf,
      divulgedContracts = List.empty,
      blindingInfo = None,
    )
  }

  def makeFullTransferInTree(
      pureCrypto: CryptoPureApi,
      seed: SaltSeed,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      transferOutResult: DeliveredTransferOutResult,
      transferInUuid: UUID,
      protocolVersion: ProtocolVersion,
  ): FullTransferInTree = {
    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)
    val commonData = TransferInCommonData.create(pureCrypto)(
      commonDataSalt,
      targetDomain,
      targetMediator,
      stakeholders,
      transferInUuid,
      protocolVersion,
    )
    val view =
      TransferInView.create(pureCrypto)(
        viewSalt,
        submitter,
        contract,
        creatingTransactionId,
        transferOutResult,
        protocolVersion,
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

  sealed trait TransferInProcessorError extends TransferProcessorError

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
      originDomain: DomainId,
      timestamp: CantonTimestamp,
  ) extends TransferInProcessorError

  case class ContractDataMismatch(transferId: TransferId) extends TransferProcessorError

  case class CreatingTransactionIdMismatch(
      transferId: TransferId,
      transferInTransactionId: TransactionId,
      localTransactionId: TransactionId,
  ) extends TransferProcessorError

}
