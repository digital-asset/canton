// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.api.v2.event.{Event, CreatedEvent as ScalaCreatedEvent}
import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.state_service.ActiveContract
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.EventFormat
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi.data.{CreatedEvent as JavaCreatedEvent, Identifier as JavaIdentifier}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.{LedgerClient, LedgerClientUtils}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.AdminWorkflowService
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationAdminWorkflow.*
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandResult,
  CommandSubmitterWithRetry,
  LedgerConnection,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, SingleUseCell}
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

/** Daml admin workflow reacting to party management proposals and agreements among participants.
  */
class AcsReplicationAdminWorkflow(
    ledgerClient: LedgerClient,
    participantId: ParticipantId,
    syncService: CantonSyncService,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends AdminWorkflowService
    with FlagCloseable
    with NamedLogging {

  private val acsReplicationTransactionHandler = new SingleUseCell[AcsReplicationTransactionHandler]

  def registerAcsReplicationTransactionHandler(handler: AcsReplicationTransactionHandler): Unit =
    acsReplicationTransactionHandler
      .putIfAbsent(handler)
      .foreach(_ =>
        throw new IllegalStateException("ACS replication transaction handler already set")
      )

  /** Have the target/current participant submit a Daml PartyReplication.PartyReplicationProposal
    * contract to agree on with the source participant.
    */
  private[admin] def proposeAcsReplication(
      partyReplicationId: Hash,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      sequencerCandidates: NonEmpty[Seq[SequencerId]],
      serial: PositiveInt, // TODO(#35267) replace with CantonTimestamp
      participantPermission: ParticipantPermission,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val partyReplicationIdS = partyReplicationId.toHexString
    val proposal = new M.acsreplication.AcsReplicationProposal(
      partyReplicationIdS,
      partyId.toProtoPrimitive,
      sourceParticipantId.adminParty.toProtoPrimitive,
      participantId.adminParty.toProtoPrimitive,
      sequencerCandidates.forgetNE.map(_.uid.toProtoPrimitive).asJava,
      serial.unwrap, // TODO(#35267) replace with CantonTimestamp
      PartyParticipantPermission.toDaml(participantPermission),
      "acs-replication", // TODO(#35267) pass the operation name from outside
    )
    EitherT(
      retrySubmitter
        .submitCommands(
          Commands(
            workflowId = "",
            userId = userId,
            commandId = s"proposal-$partyReplicationIdS",
            commands = proposal.create.commands.asScala.toSeq
              .map(LedgerClientUtils.javaCodegenToScalaProto),
            deduplicationPeriod =
              DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
            minLedgerTimeAbs = None,
            minLedgerTimeRel = None,
            actAs = Seq(participantId.adminParty.toProtoPrimitive),
            readAs = Nil,
            submissionId = "",
            disclosedContracts = Nil,
            synchronizerId = synchronizerId.toProtoPrimitive,
            packageIdSelectionPreference = Nil,
            prefetchContractKeys = Nil,
            tapsMaxPasses = None,
          ),
          timeouts.default.asFiniteApproximation,
        )
        .map(handleCommandResult(s"propose $partyReplicationIdS to replicate party"))
    ).mapK(FutureUnlessShutdown.outcomeK)
  }

  override private[admin] def eventFormat: EventFormat = {
    val templates = Seq(
      M.acsreplication.AcsReplicationProposal.TEMPLATE_ID,
      M.acsreplication.AcsReplicationAgreement.TEMPLATE_ID,
    )

    LedgerConnection.eventFormatByParty(
      Map(participantId.adminParty -> templates.map(LedgerConnection.mapTemplateIds))
    )
  }

  override private[admin] def processTransaction(tx: Transaction): Unit = {
    implicit val traceContext: TraceContext =
      LedgerClient.traceContextFromLedgerApi(tx.traceContext)

    tx.events
      .foreach {
        case Event(Event.Event.Created(createdEvent))
            if createdEvent.templateId.exists(isTemplateAcsReplicationRelated) =>
          createEventHandler(
            processProposalAtSourceParticipant(tx.synchronizerId, _),
            { case (agreedAt, agreement) =>
              processAgreementAtSourceOrTargetParticipant(
                tx.synchronizerId,
                agreement,
                agreedAt,
                mightNotRememberAgreement = false,
              )
            },
          )(createdEvent)
        case Event(Event.Event.Archived(archivedEvent))
            if archivedEvent.templateId.contains(agreementTemplate) =>
          archivedEvent.templateId match {
            case Some(`agreementTemplate`) =>
              processAgreementArchive(archivedEvent.contractId)
            case _ => ()
          }
        case _ => ()
      }
  }

  private def processProposalAtSourceParticipant(
      synchronizerIdS: String,
      contract: M.acsreplication.AcsReplicationProposal.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Received ACS replication proposal ${contract.data.acsReplicationId} for party ${contract.data.partyId} on synchronizer $synchronizerIdS" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )

    def respondToProposal(
        eitherErrorOrSequencerId: Either[String, AcsReplicationAgreementParams]
    ): Unit = {
      val (exercise, commandId) = eitherErrorOrSequencerId.fold(
        err => {
          logger.warn(err)
          (
            contract.id.exerciseReject(err).commands,
            // Upon reject use the contract id as the party-replication-id might be an invalid
            // command id if the party replication proposal contract was created by hand.
            s"proposal-reject-${contract.id.contractId}",
          )
        },
        agreementParams =>
          (
            contract.id.exerciseAccept(agreementParams.sequencerId.uid.toProtoPrimitive).commands,
            s"proposal-accept-${contract.data.acsReplicationId}",
          ),
      )
      val commandResultF = synchronizeWithClosingF(s"submit $commandId")(
        retrySubmitter.submitCommands(
          Commands(
            workflowId = "",
            userId = userId,
            commandId = commandId,
            commands = exercise.asScala.toSeq.map(LedgerClientUtils.javaCodegenToScalaProto),
            deduplicationPeriod =
              DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
            minLedgerTimeAbs = None,
            minLedgerTimeRel = None,
            actAs = Seq(participantId.adminParty.toProtoPrimitive),
            readAs = Nil,
            submissionId = "",
            disclosedContracts = Nil,
            synchronizerId = synchronizerIdS,
            packageIdSelectionPreference = Nil,
            prefetchContractKeys = Nil,
            tapsMaxPasses = None,
          ),
          timeouts.default.asFiniteApproximation,
        )
      )
      superviseBackgroundSubmission(
        s"Accept or reject proposal ${contract.data.acsReplicationId}",
        commandResultF,
      )
    }

    if (contract.data.sourceParticipant == participantId.adminParty.toProtoPrimitive) {
      val proposalOrError =
        AcsReplicationProposalParams.fromDaml(contract.data, synchronizerIdS)
      acsReplicationTransactionHandler.get.foreach(
        _.processAcsReplicationProposalAtSourceParticipant(
          proposalOrError,
          respondToProposal,
        ).discard
      )
    }
  }

  // TODO(#35267) Add more parameters: (synchronizerId, contract, sourceParticipant, targetParticipant, partyId...) for client verifications
  private def processAgreementArchive(
      contractId: String
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Received archival of ACS replication agreement $contractId.")
    LfContractId
      .fromString(contractId)
      .fold(
        err => logger.warn(s"Malformed ACS replication agreement contract id: $err"),
        lfContractId =>
          acsReplicationTransactionHandler.get
            .foreach(_.processAcsReplicationAgreementArchival(lfContractId)),
      )
  }

  private def processAgreementAtSourceOrTargetParticipant(
      synchronizerIdS: String,
      contract: M.acsreplication.AcsReplicationAgreement.Contract,
      agreedAt: CantonTimestamp,
      mightNotRememberAgreement: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Received agreement for ACS ${contract.data.partyId} on synchronizer $synchronizerIdS" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )
    participantId.adminParty.toProtoPrimitive match {
      case `contract`.data.sourceParticipant | `contract`.data.targetParticipant =>
        (for {
          lfContractId <- LfContractId.fromString(contract.id.contractId)
          params <- AcsReplicationAgreementParams.fromDaml(contract.data, synchronizerIdS)
        } yield acsReplicationTransactionHandler.get.foreach(
          _.processAcsReplicationAgreement(lfContractId, agreedAt, mightNotRememberAgreement)(
            params
          )
        ))
          .valueOr(err => logger.warn(s"Malformed party replication agreement: $err"))
      case nonStakeholder =>
        logger.warn(
          s"Received unexpected party replication agreement between source ${contract.data.sourceParticipant}" +
            s"and target ${contract.data.targetParticipant} on non-stakeholder participant $nonStakeholder"
        )
    }
  }

  override private[admin] def processReassignment(tx: Reassignment): Unit =
    if (
      tx.events
        .flatMap(_.event.assigned)
        .exists(
          _.createdEvent
            .exists(_.templateId.exists(isTemplateAcsReplicationRelated))
        ) ||
      tx.events
        .flatMap(_.event.unassigned)
        .exists(
          _.templateId.exists(isTemplateAcsReplicationRelated)
        )
    ) {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(tx.traceContext)
      // TODO(#20638): Should we archive unexpectedly reassigned party replication contracts or only warn?
      logger.warn(
        s"Received unexpected reassignment of party replication related contract: ${tx.events}"
      )
    }

  override private[admin] def processAcs(acs: Seq[ActiveContract])(implicit
      traceContext: TraceContext
  ): Unit = {
    val createdEvents = acs
      .collect {
        case ActiveContract(Some(createdEvent), synchronizerIdS, _)
            if createdEvent.templateId.exists(isTemplateAcsReplicationRelated) =>
          createdEvent -> synchronizerIdS
      }

    // Upon source participant restart, check for party replications agreements that
    // may indicate an interrupted OnPR.
    // TODO(#20636): Once OnPR no longer pauses indexing, remove this eager action on the
    //  part of the SP, and have the TP renegotiate resuming OnPR via a new proposal.
    if (createdEvents.nonEmpty) {
      logger.info(
        s"Found ${createdEvents.length} active party replication agreement contracts ${createdEvents
            .map(_._1.contractId)} upon participant start"
      )
      createdEvents.foreach { case (createdEvent, synchronizerIdS) =>
        createEventHandler(
          processProposalAtSourceParticipant(synchronizerIdS, _),
          {
            case (agreedAt, onPRAgreement)
                if onPRAgreement.data.sourceParticipant == participantId.adminParty.toProtoPrimitive =>
              processAgreementAtSourceOrTargetParticipant(
                synchronizerIdS,
                onPRAgreement,
                agreedAt,
                // maybe unknown due to the participant restart and lack of SP-side persistence:
                mightNotRememberAgreement = true,
              )
            case _ => ()
          },
        )(createdEvent)
      }
    }
  }

  // Event handler sharable outside create event handling, e.g. for acs handling
  private def createEventHandler(
      handleProposal: M.acsreplication.AcsReplicationProposal.Contract => Unit,
      handleAgreement: (
          CantonTimestamp,
          M.acsreplication.AcsReplicationAgreement.Contract,
      ) => Unit,
  )(created: ScalaCreatedEvent)(implicit traceContext: TraceContext): Unit = created match {
    case event
        if event.templateId
          .contains(proposalTemplate) =>
      val contract =
        M.acsreplication.AcsReplicationProposal.COMPANION
          .fromCreatedEvent(JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event)))
      handleProposal(contract)
    case event
        if event.templateId
          .contains(agreementTemplate) =>
      val contract =
        M.acsreplication.AcsReplicationAgreement.COMPANION
          .fromCreatedEvent(JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event)))
      val createdAt = event.createdAt.map(CantonTimestamp.fromProtoTimestamp) match {
        case None =>
          ErrorUtil.invalidState(s"Agreement ${contract.data.partyId} without createdAt")
        case Some(Left(conversionErr)) =>
          ErrorUtil.invalidState(
            s"Agreement ${contract.data.partyId} createdAt protoTime failed to convert: $conversionErr."
          )
        case Some(Right(createdAt)) => createdAt
      }
      handleAgreement(createdAt, contract)
    case _ => ()
  }

  /** Attempts to mark an active agreement as done when the TP deems that OnPR has progressed
    * sufficiently far that the SP's involvement is no longer needed. As a result of the agreement
    * being archived, the SP will no longer act on the agreement's behalf particularly after the SP
    * restarts.
    *
    * @return
    *   `true` if the agreement was successfully marked as done, `false` otherwise.
    */
  private[party] def markAcsReplicationAgreementDone(
      ap: AcsReplicationAgreementParams,
      damlAgreementCid: LfContractId,
      tc: TraceContext,
  ): FutureUnlessShutdown[Boolean] = {
    implicit val traceContext: TraceContext = tc
    (for {
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(ap.synchronizerId)
            .toRight {
              s"Synchronizer ${ap.synchronizerId} not connected when marking agreement done for ${ap.requestId}"
                .tap(logger.debug(_))
            }
        )
      // Use the ActiveContractStore to query the activeness of the daml agreement contract as
      // that is significantly faster than querying the ACS via the ledger api and more robust than
      // monitoring for the archival which may be missed in case the participant is restarted.
      isAgreementActive <- EitherT.right[String](
        connectedSynchronizer.synchronizerHandle.syncPersistentState.activeContractStore
          .fetchStates(Seq(damlAgreementCid))
          .map(_.values.exists(_.status.isActive))
      )
      _ <- EitherTUtil.ifThenET(isAgreementActive)(
        exerciseAgreementDoneOnTargetParticipant(ap, damlAgreementCid)
      )
    } yield !isAgreementActive).getOrElse(
      false // when unable to check or submit changes, return false to have caller check again
    )
  }

  /** Archive the agreement contract by exercising the done choice. */
  private def exerciseAgreementDoneOnTargetParticipant(
      ap: AcsReplicationAgreementParams,
      damlAgreementCid: LfContractId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Marking agreement done for ${ap.requestId} on target participant")
    val commandId = s"agreement-done-${ap.requestId}"
    val agreementCid =
      new M.acsreplication.AcsReplicationAgreement.ContractId(damlAgreementCid.coid)
    val exercise = agreementCid.exerciseDone(participantId.adminParty.uid.toProtoPrimitive).commands
    val operation = s"submit $commandId"
    EitherTUtil.ifThenET(participantId == ap.targetParticipantId) {
      EitherT(
        synchronizeWithClosingF(operation)(
          retrySubmitter
            .submitCommands(
              Commands(
                workflowId = "",
                userId = userId,
                commandId = commandId,
                commands = exercise.asScala.toSeq.map(LedgerClientUtils.javaCodegenToScalaProto),
                deduplicationPeriod =
                  DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
                minLedgerTimeAbs = None,
                minLedgerTimeRel = None,
                actAs = Seq(participantId.adminParty.toProtoPrimitive),
                readAs = Nil,
                submissionId = "",
                disclosedContracts = Nil,
                synchronizerId = ap.synchronizerId.toProtoPrimitive,
                packageIdSelectionPreference = Nil,
                prefetchContractKeys = Nil,
                tapsMaxPasses = None,
              ),
              timeouts.default.asFiniteApproximation,
            )
            .map(handleCommandResult(operation))
        )
      )
    }
  }

  private def superviseBackgroundSubmission(
      operation: String,
      submission: FutureUnlessShutdown[CommandResult],
  )(implicit traceContext: TraceContext): Unit =
    futureSupervisor
      .supervisedUS(operation)(submission)
      .failOnShutdownToAbortException(operation)
      .foreach(handleCommandResult(operation))

  // Effectively called from AdminWorkflowServices. Subscribers manage their own lifecycle separately.
  override def onClosed(): Unit =
    // Note that we can not time out requests nicely here on shutdown as the admin
    // server is closed first, which means that our requests will never
    // return properly on shutdown abort.
    LifeCycle.close(retrySubmitter, ledgerClient)(logger)

  private val retrySubmitter = new CommandSubmitterWithRetry(
    ledgerClient.commandService,
    clock,
    futureSupervisor,
    timeouts,
    loggerFactory,
    decideRetry = _ => None,
  )
}

object AcsReplicationAdminWorkflow {

  /** Handles ACS replication Daml transactions during sequencer channel negotiations.
    *
    * The trait is mainly useful for hiding circular dependency between
    * [[AcsReplicationAdminWorkflow]] and [[AcsReplicator]]. The dependency is still there but at
    * least the specific class is not used here.
    */
  trait AcsReplicationTransactionHandler {

    /** Processes sequencer channel proposal at source participant.
      * @param proposalOrError
      *   proposal if deserialized successfully, error otherwise
      * @param respondToProposal
      *   function that responds to proposal or error
      */
    private[party] def processAcsReplicationProposalAtSourceParticipant(
        proposalOrError: Either[String, AcsReplicationProposalParams],
        respondToProposal: Either[String, AcsReplicationAgreementParams] => Unit,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Either[String, Unit]]

    /** Processes sequencer channel agreement on both source and target participant.
      * @param damlAgreementCid
      *   agreement contract id
      * @param agreedAt
      *   The ledger effective time of the agreement contract creation for human consumption of when
      *   it was agreed to replicate the ACS.
      * @param mightNotRememberProposal
      *   whether it's allowed for handler not to be aware of ACS replication
      * @param agreementParams
      *   deserialized agreement
      */
    private[party] def processAcsReplicationAgreement(
        damlAgreementCid: LfContractId,
        agreedAt: CantonTimestamp,
        mightNotRememberProposal: Boolean,
    )(
        agreementParams: AcsReplicationAgreementParams
    )(implicit traceContext: TraceContext): Unit

    /** Processes archival of sequencer channel agreement on both source and target participant.
      * @param contractId
      *   agreement contract id
      */
    private[party] def processAcsReplicationAgreementArchival(
        contractId: LfContractId
    )(implicit traceContext: TraceContext): Unit
  }

  private def userId = "AcsReplicationAdminWorkflow"

  private def apiIdentifierFromJavaIdentifier(javaIdentifier: JavaIdentifier): Identifier =
    Identifier(
      packageId = javaIdentifier.getPackageId,
      moduleName = javaIdentifier.getModuleName,
      entityName = javaIdentifier.getEntityName,
    )

  @VisibleForTesting
  lazy val proposalTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.acsreplication.AcsReplicationProposal.TEMPLATE_ID_WITH_PACKAGE_ID
    )
  private lazy val agreementTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.acsreplication.AcsReplicationAgreement.TEMPLATE_ID_WITH_PACKAGE_ID
    )

  @VisibleForTesting
  lazy val proposalTemplatePkgName: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.acsreplication.AcsReplicationProposal.TEMPLATE_ID
    )
  lazy val agreementTemplatePkgName: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.acsreplication.AcsReplicationAgreement.TEMPLATE_ID
    )

  private def isTemplateAcsReplicationRelated(id: Identifier) =
    id == proposalTemplate || id == agreementTemplate
}
