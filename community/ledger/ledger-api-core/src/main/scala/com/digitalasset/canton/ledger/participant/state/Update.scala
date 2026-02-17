// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.logging.entries.{LoggingEntry, LoggingValue, ToLoggingValue}
import com.digitalasset.base.error.GrpcStatuses
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.RejectionReasonTemplate
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.platform.indexer.TransactionTraversalUtils
import com.digitalasset.canton.protocol.{LfHash, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.canton.util.ShowUtil
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.{
  BlindingInfo,
  CommittedTransaction,
  TransactionNodeStatistics,
}
import com.digitalasset.daml.lf.value.Value
import com.google.rpc.status.Status as RpcStatus

import java.util.UUID
import scala.concurrent.Promise

/** An update to the (abstract) participant state.
  *
  * [[Update]]'s are used in to communicate changes to abstract participant state to consumers.
  *
  * We describe the possible updates in the comments of each of the case classes implementing
  * [[Update]].
  *
  * Deduplication guarantee: Let there be a [[Update.TransactionAccepted]] with [[CompletionInfo]]
  * or a [[Update.CommandRejected]] with [[CompletionInfo]] at offset `off2`. If `off2`'s
  * [[CompletionInfo.optDeduplicationPeriod]] is a
  * [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset]], let `off1` be the
  * first offset after the deduplication offset. If the deduplication period is a
  * [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration]], let `off1` be the
  * first offset whose record time is at most the duration before `off2`'s record time (inclusive).
  * Then there is no other [[Update.TransactionAccepted]] with [[CompletionInfo]] for the same
  * [[CompletionInfo.changeId]] between the offsets `off1` and `off2` inclusive.
  *
  * So if a command submission has resulted in a [[Update.TransactionAccepted]], other command
  * submissions with the same [[SubmitterInfo.changeId]] must be deduplicated if the earlier's
  * [[Update.TransactionAccepted]] falls within the latter's
  * [[CompletionInfo.optDeduplicationPeriod]].
  *
  * Implementations MAY extend the deduplication period from [[SubmitterInfo]] arbitrarily and
  * reject a command submission as a duplicate even if its deduplication period does not include the
  * earlier's [[Update.TransactionAccepted]]. A [[Update.CommandRejected]] completion does not
  * trigger deduplication and implementations SHOULD process such resubmissions normally.
  */
sealed trait Update extends Product with Serializable with PrettyPrinting with HasTraceContext

/** Update which defines a recordTime, synchronizerId and SynchronizerIndex.
  */
sealed trait SynchronizerUpdate extends Update {

  /** The record time at which the state change was committed. */
  def recordTime: CantonTimestamp

  def synchronizerId: SynchronizerId

  def synchronizerIndex: SynchronizerIndex
}

sealed trait SequencedUpdate extends SynchronizerUpdate {
  final override def synchronizerIndex: SynchronizerIndex =
    SynchronizerIndex.forSequencedUpdate(recordTime)
}

sealed trait FloatingUpdate extends SynchronizerUpdate {
  final override def synchronizerIndex: SynchronizerIndex =
    SynchronizerIndex.forFloatingUpdate(recordTime)
}

sealed trait SequencedEventUpdate extends SequencedUpdate

sealed trait RepairUpdate extends SynchronizerUpdate {
  def repairCounter: RepairCounter

  final override def synchronizerIndex: SynchronizerIndex =
    SynchronizerIndex.forRepairUpdate(
      RepairIndex(
        timestamp = recordTime,
        counter = repairCounter,
      )
    )
}

object Update {

  /** Produces a constant dummy transaction seed for transactions in which we cannot expose a seed.
    * Essentially all of them. TransactionMeta.submissionSeed can no longer be set to None starting
    * with Daml 1.3
    */
  def noOpSeed: LfHash =
    LfHash.assertFromString("00" * LfHash.underlyingHashLength)

  final case class TopologyTransactionEffective(
      updateId: UpdateId,
      events: Set[TopologyTransactionEffective.TopologyEvent],
      synchronizerId: SynchronizerId,
      effectiveTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends FloatingUpdate {

    // Topology transactions emitted to the update stream at effective time
    override def recordTime: CantonTimestamp = effectiveTime

    override def pretty: Pretty[TopologyTransactionEffective] =
      TopologyTransactionEffective.pretty
  }

  object TopologyTransactionEffective extends PrettyUtil {

    sealed trait AuthorizationLevel
    object AuthorizationLevel {
      final case object Submission extends AuthorizationLevel

      final case object Confirmation extends AuthorizationLevel

      final case object Observation extends AuthorizationLevel
    }

    sealed trait AuthorizationEvent
    object AuthorizationEvent {
      sealed trait ActiveAuthorization extends AuthorizationEvent {
        def level: AuthorizationLevel
      }

      final case class Added(level: AuthorizationLevel) extends ActiveAuthorization
      final case class ChangedTo(level: AuthorizationLevel) extends ActiveAuthorization
      final case object Revoked extends AuthorizationEvent
    }

    sealed trait TopologyEvent

    object TopologyEvent {
      final case class PartyToParticipantAuthorization(
          party: Ref.Party,
          participant: Ref.ParticipantId,
          authorizationEvent: AuthorizationEvent,
      ) extends TopologyEvent
    }
    implicit val `TopologyTransactionEffective to LoggingValue`
        : ToLoggingValue[TopologyTransactionEffective] = { topologyTransactionEffective =>
      LoggingValue.Nested.fromEntries(
        Logging.updateId(topologyTransactionEffective.updateId),
        Logging.recordTime(topologyTransactionEffective.recordTime.toLf),
        Logging.synchronizerId(topologyTransactionEffective.synchronizerId),
      )
    }

    val pretty: Pretty[TopologyTransactionEffective] =
      prettyOfClass(
        param("effectiveTime", _.effectiveTime),
        param("synchronizerId", _.synchronizerId),
        param("updateId", _.updateId.tryAsLedgerTransactionId),
        indicateOmittedFields,
      )
  }

  sealed trait AcsChangeSequencedUpdate extends SynchronizerUpdate {
    def acsChangeFactory: AcsChangeFactory
  }

  /** Signal the acceptance of a transaction.
    */
  trait TransactionAccepted extends SynchronizerUpdate {

    /** The information provided by the submitter of the command that created this transaction. It
      * must be provided if this participant hosts one of the [[SubmitterInfo.actAs]] parties and
      * shall output a completion event for this transaction. This in particular applies if this
      * participant has submitted the command to the [[SyncService]].
      *
      * The Offset-order of Updates must ensure that command deduplication guarantees are met.
      */
    def completionInfoO: Option[CompletionInfo]

    /** The metadata of the transaction that was provided by the submitter. It is visible to all
      * parties that can see the transaction.
      */
    def transactionMeta: TransactionMeta

    def transactionInfo: TransactionAccepted.TransactionInfo

    def updateId: UpdateId

    def externalTransactionHash: Option[Hash]

    def isAcsDelta(contractId: Value.ContractId): Boolean

    /** Maps each contract id (of created or archived events of the transaction) to the
      * corresponding [[ContractInfo]].
      */
    def contractInfos: Map[Value.ContractId, ContractInfo]
  }

  object TransactionAccepted {
    implicit val `TransactionAccepted to LoggingValue`: ToLoggingValue[TransactionAccepted] = {
      case txAccepted: TransactionAccepted =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(txAccepted.recordTime.toLf),
          Logging.completionInfo(txAccepted.completionInfoO),
          Logging.updateId(txAccepted.updateId),
          Logging.ledgerTime(txAccepted.transactionMeta.ledgerEffectiveTime),
          Logging.workflowIdOpt(txAccepted.transactionMeta.workflowId),
          Logging.preparationTime(txAccepted.transactionMeta.preparationTime),
          Logging.synchronizerId(txAccepted.synchronizerId),
        )
    }

    /** For each contract created in a transaction, a representative package exists in the
      * Participant package store that is guaranteed to type-check the contract's argument. Such a
      * package-id guarantee is required for ensuring correct rendering of contract create values on
      * the gRPC/JSON Ledger API read queries.
      */
    sealed trait RepresentativePackageId extends Product with Serializable
    object RepresentativePackageId {

      /** Signals that the representative package-id of the created contract referenced in this
        * transaction are the same as the contract's creation package-id.
        */
      case object SameAsContractPackageId extends RepresentativePackageId

      final case class DedicatedRepresentativePackageId(
          representativePackageId: Ref.PackageId
      ) extends RepresentativePackageId
    }

    final case class TransactionInfo(
        blindingInfo: BlindingInfo,
        executionOrder: Seq[TransactionTraversalUtils.NodeInfo],
        statistics: TransactionNodeStatistics,
        noOfNodes: Int,
        noOfRootNodes: Int,
    )
    object TransactionInfo {
      def apply(transaction: CommittedTransaction): TransactionInfo = TransactionInfo(
        blindingInfo = Blinding.blind(transaction),
        executionOrder = TransactionTraversalUtils
          .executionOrderTraversalForIngestion(transaction.transaction)
          .toVector,
        statistics = TransactionNodeStatistics(
          transaction,
          Set.empty[Ref.PackageId],
        ),
        noOfNodes = transaction.nodes.size,
        noOfRootNodes = transaction.roots.length,
      )
    }
  }

  /** Information about a contract needed for indexing.
    *
    * @param internalContractId
    *   The internal contract id assigned to the contract.
    * @param contractAuthenticationData
    *   Contract authentication data assigned by the ledger implementation. This data is opaque and
    *   can only be used in [[com.digitalasset.daml.lf.transaction.FatContractInstance]]s when
    *   submitting transactions through the [[SyncService]].
    * @param representativePackageId
    *   The representative package-id for the contract, if the contract is created in this
    *   transaction. See [[TransactionAccepted.RepresentativePackageId]] for more details.
    */
  final case class ContractInfo(
      internalContractId: Long,
      contractAuthenticationData: Bytes,
      representativePackageId: RepresentativePackageId,
  )

  final case class SequencedTransactionAccepted(
      completionInfoO: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transactionInfo: TransactionAccepted.TransactionInfo,
      updateId: UpdateId,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      acsChangeFactory: AcsChangeFactory,
      contractInfos: Map[Value.ContractId, ContractInfo],
      externalTransactionHash: Option[Hash] = None,
  )(implicit override val traceContext: TraceContext)
      extends TransactionAccepted
      with SequencedEventUpdate
      with AcsChangeSequencedUpdate {
    override def isAcsDelta(contractId: Value.ContractId): Boolean =
      acsChangeFactory.contractActivenessChanged(contractId)

    override protected def pretty: Pretty[TransactionAccepted] =
      SequencedTransactionAccepted.pretty
  }

  object SequencedTransactionAccepted extends PrettyUtil {
    val pretty: Pretty[TransactionAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("updateId", _.updateId.tryAsLedgerTransactionId),
        param("transactionMeta", _.transactionMeta),
        paramIfDefined("completion", _.completionInfoO),
        param("nodes", _.transactionInfo.noOfNodes),
        param("roots", _.transactionInfo.noOfRootNodes),
        indicateOmittedFields,
      )
  }

  final case class RepairTransactionAccepted(
      transactionMeta: TransactionMeta,
      transactionInfo: TransactionAccepted.TransactionInfo,
      updateId: UpdateId,
      synchronizerId: SynchronizerId,
      repairCounter: RepairCounter,
      recordTime: CantonTimestamp,
      contractInfos: Map[Value.ContractId, ContractInfo],
  )(implicit override val traceContext: TraceContext)
      extends TransactionAccepted
      with RepairUpdate {

    override def externalTransactionHash: Option[Hash] = None
    override def completionInfoO: Option[CompletionInfo] = None

    // Repair transactions have only contracts which affect the ACS.
    override def isAcsDelta(contractId: Value.ContractId): Boolean = true

    override protected def pretty: Pretty[RepairTransactionAccepted] =
      RepairTransactionAccepted.pretty
  }

  object RepairTransactionAccepted extends PrettyUtil {
    val pretty: Pretty[RepairTransactionAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("repairCounter", _.repairCounter),
        param("updateId", _.updateId.tryAsLedgerTransactionId),
        param("transactionMeta", _.transactionMeta),
        paramIfDefined("completion", _.completionInfoO),
        param("nodes", _.transactionInfo.noOfNodes),
        param("roots", _.transactionInfo.noOfRootNodes),
        indicateOmittedFields,
      )
  }

  trait ReassignmentAccepted extends SynchronizerUpdate {

    /** The information provided by the submitter of the command that created this reassignment. It
      * must be provided if this participant hosts the submitter and shall output a completion event
      * for this reassignment. This in particular applies if this participant has submitted the
      * command to the [[SyncService]].
      */
    def optCompletionInfo: Option[CompletionInfo]

    /** A submitter-provided identifier used for monitoring and to traffic-shape the work handled by
      * Daml applications
      */
    def workflowId: Option[Ref.WorkflowId]

    /** A unique identifier for this update assigned by the ledger.
      */
    def updateId: UpdateId

    /** Common part of all type of reassignments.
      */
    def reassignmentInfo: ReassignmentInfo

    def reassignment: Reassignment.Batch

    def kind: String = if (reassignmentInfo.sourceSynchronizer.unwrap == synchronizerId)
      "unassignment"
    else "assignment"
  }

  final case class SequencedReassignmentAccepted(
      optCompletionInfo: Option[CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      updateId: UpdateId,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment.Batch,
      recordTime: CantonTimestamp,
      override val synchronizerId: SynchronizerId,
      acsChangeFactory: AcsChangeFactory,
  )(implicit override val traceContext: TraceContext)
      extends ReassignmentAccepted
      with SequencedEventUpdate
      with AcsChangeSequencedUpdate {

    override protected def pretty: Pretty[SequencedReassignmentAccepted] =
      SequencedReassignmentAccepted.pretty
  }

  object SequencedReassignmentAccepted extends PrettyUtil with ShowUtil {
    val pretty: Pretty[SequencedReassignmentAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("updateId", _.updateId.tryAsLedgerTransactionId),
        paramIfDefined("completion", _.optCompletionInfo),
        param("source", _.reassignmentInfo.sourceSynchronizer),
        param("target", _.reassignmentInfo.targetSynchronizer),
        param("kind", _.kind.unquoted),
        indicateOmittedFields,
      )
  }

  final case class RepairReassignmentAccepted(
      workflowId: Option[Ref.WorkflowId],
      updateId: UpdateId,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment.Batch,
      repairCounter: RepairCounter,
      recordTime: CantonTimestamp,
      override val synchronizerId: SynchronizerId,
  )(implicit override val traceContext: TraceContext)
      extends ReassignmentAccepted
      with RepairUpdate {
    override def optCompletionInfo: Option[CompletionInfo] = None

    override protected def pretty: Pretty[RepairReassignmentAccepted] =
      RepairReassignmentAccepted.pretty
  }

  object RepairReassignmentAccepted extends PrettyUtil with ShowUtil {
    val pretty: Pretty[RepairReassignmentAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("repairCounter", _.repairCounter),
        param("updateId", _.updateId.tryAsLedgerTransactionId),
        paramIfDefined("completion", _.optCompletionInfo),
        param("source", _.reassignmentInfo.sourceSynchronizer),
        param("target", _.reassignmentInfo.targetSynchronizer),
        param("kind", _.kind.unquoted),
        indicateOmittedFields,
      )
  }

  final case class OnPRReassignmentAccepted(
      workflowId: Option[Ref.WorkflowId],
      updateId: UpdateId,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment.Batch,
      repairCounter: RepairCounter,
      recordTime: CantonTimestamp,
      override val synchronizerId: SynchronizerId,
      acsChangeFactory: AcsChangeFactory,
  )(implicit override val traceContext: TraceContext)
      extends ReassignmentAccepted
      with RepairUpdate
      with AcsChangeSequencedUpdate {
    override def optCompletionInfo: Option[CompletionInfo] = None

    override protected def pretty: Pretty[OnPRReassignmentAccepted] =
      OnPRReassignmentAccepted.pretty
  }

  object OnPRReassignmentAccepted extends PrettyUtil with ShowUtil {
    val pretty: Pretty[OnPRReassignmentAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("repairCounter", _.repairCounter),
        param("updateId", _.updateId.tryAsLedgerTransactionId),
        paramIfDefined("completion", _.optCompletionInfo),
        param("source", _.reassignmentInfo.sourceSynchronizer),
        param("target", _.reassignmentInfo.targetSynchronizer),
        param("kind", _.kind.unquoted),
        indicateOmittedFields,
      )
  }

  object ReassignmentAccepted {
    implicit val `ReassignmentAccepted to LoggingValue`: ToLoggingValue[ReassignmentAccepted] = {
      case reassignmentAccepted: ReassignmentAccepted =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(reassignmentAccepted.recordTime.toLf),
          Logging.completionInfo(reassignmentAccepted.optCompletionInfo),
          Logging.updateId(reassignmentAccepted.updateId),
          Logging.workflowIdOpt(reassignmentAccepted.workflowId),
        )
    }
  }

  /** Signal that a command submitted via [[SyncService]] was rejected.
    */
  sealed trait CommandRejected extends SynchronizerUpdate {

    /** The completion information for the submission
      */
    def completionInfo: CompletionInfo

    /** A template for generating the gRPC status code with error details. See ``error.proto`` for
      * the status codes of common rejection reasons.
      */
    def reasonTemplate: RejectionReasonTemplate

    /** If true, the deduplication guarantees apply to this rejection. The participant state
      * implementations should strive to set this flag to true as often as possible so that
      * applications get better guarantees.
      */
    final def definiteAnswer: Boolean = reasonTemplate.definiteAnswer

    override protected def pretty: Pretty[CommandRejected] =
      CommandRejected.pretty
  }

  final case class SequencedCommandRejected(
      completionInfo: CompletionInfo,
      reasonTemplate: RejectionReasonTemplate,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends CommandRejected
      with SequencedEventUpdate

  final case class UnSequencedCommandRejected(
      completionInfo: CompletionInfo,
      reasonTemplate: RejectionReasonTemplate,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      messageUuid: UUID,
  )(implicit override val traceContext: TraceContext)
      extends CommandRejected
      with FloatingUpdate

  object CommandRejected extends PrettyUtil with ShowUtil {

    implicit val `CommandRejected to LoggingValue`: ToLoggingValue[CommandRejected] = {
      case commandRejected: CommandRejected =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(commandRejected.recordTime.toLf),
          Logging.submitter(commandRejected.completionInfo.actAs),
          Logging.userId(commandRejected.completionInfo.userId),
          Logging.commandId(commandRejected.completionInfo.commandId),
          Logging.deduplicationPeriod(commandRejected.completionInfo.optDeduplicationPeriod),
          Logging.rejectionReason(commandRejected.reasonTemplate),
          Logging.synchronizerId(commandRejected.synchronizerId),
        )
    }

    val pretty: Pretty[CommandRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("completion", _.completionInfo),
        paramIfTrue("definiteAnswer", _.definiteAnswer),
        param("reason", _.reasonTemplate.message.singleQuoted),
        param("synchronizerId", _.synchronizerId.uid),
      )

    /** A template for generating gRPC status codes.
      */
    sealed trait RejectionReasonTemplate {

      /** A human-readable description of the error */
      def message: String

      /** A gRPC status code representing the error. */
      def code: Int

      /** A protobuf gRPC status representing the error. */
      def status: RpcStatus

      /** Whether the rejection is a definite answer for the deduplication guarantees specified for
        * [[Update]].
        */
      def definiteAnswer: Boolean
    }

    object RejectionReasonTemplate {
      implicit val `RejectionReasonTemplate to LoggingValue`
          : ToLoggingValue[RejectionReasonTemplate] =
        reason =>
          LoggingValue.Nested.fromEntries(
            "code" -> reason.code,
            "message" -> reason.message,
            "definiteAnswer" -> reason.definiteAnswer,
          )
    }

    /** The status code for the command rejection. */
    final case class FinalReason(override val status: RpcStatus) extends RejectionReasonTemplate {
      override def message: String = status.message

      override def code: Int = status.code

      override def definiteAnswer: Boolean = GrpcStatuses.isDefiniteAnswer(status)
    }
  }

  final case class SequencerIndexMoved(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends SequencedUpdate {
    override protected def pretty: Pretty[SequencerIndexMoved] =
      prettyOfClass(
        param("synchronizerId", _.synchronizerId.uid),
        param("sequencerTimestamp", _.recordTime),
      )
  }

  object SequencerIndexMoved extends PrettyUtil {
    implicit val `SequencerIndexMoved to LoggingValue`: ToLoggingValue[SequencerIndexMoved] =
      seqIndexMoved =>
        LoggingValue.Nested.fromEntries(
          Logging.synchronizerId(seqIndexMoved.synchronizerId),
          "sequencerTimestamp" -> seqIndexMoved.recordTime.toInstant,
        )

    val pretty: Pretty[SequencerIndexMoved] =
      prettyOfClass(
        param("synchronizerId", _.synchronizerId.uid),
        param("sequencerTimestamp", _.recordTime),
      )
  }

  final case class LsuTimeReached(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends FloatingUpdate {
    override protected def pretty: Pretty[LsuTimeReached] =
      LsuTimeReached.pretty
  }

  object LsuTimeReached extends PrettyUtil {
    implicit val `LsuTimeReached to LoggingValue`: ToLoggingValue[LsuTimeReached] =
      lsuTimeReached =>
        LoggingValue.Nested.fromEntries(
          Logging.synchronizerId(lsuTimeReached.synchronizerId),
          "sequencerTimestamp" -> lsuTimeReached.recordTime.toInstant,
        )

    val pretty: Pretty[LsuTimeReached] =
      prettyOfClass(
        param("synchronizerId", _.synchronizerId.uid),
        param("sequencerTimestamp", _.recordTime),
      )
  }

  final case class EmptyAcsPublicationRequired(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends FloatingUpdate {
    override protected def pretty: Pretty[EmptyAcsPublicationRequired] =
      EmptyAcsPublicationRequired.pretty
  }

  object EmptyAcsPublicationRequired extends PrettyUtil {
    implicit val `EmptyAcsPublicationRequired to LoggingValue`
        : ToLoggingValue[EmptyAcsPublicationRequired] =
      emptyAcsPublicationRequired =>
        LoggingValue.Nested.fromEntries(
          Logging.synchronizerId(emptyAcsPublicationRequired.synchronizerId),
          "sequencerTimestamp" -> emptyAcsPublicationRequired.recordTime.toInstant,
        )

    val pretty: Pretty[EmptyAcsPublicationRequired] =
      prettyOfClass(
        param("synchronizerId", _.synchronizerId.uid),
        param("sequencerTimestamp", _.recordTime),
      )
  }

  final case class CommitRepair()(implicit override val traceContext: TraceContext) extends Update {
    val persisted: Promise[Unit] = Promise()

    override protected def pretty: Pretty[CommitRepair] = prettyOfClass()
  }

  implicit val `Update to LoggingValue`: ToLoggingValue[Update] = {
    case update: TopologyTransactionEffective =>
      TopologyTransactionEffective.`TopologyTransactionEffective to LoggingValue`.toLoggingValue(
        update
      )
    case update: TransactionAccepted =>
      TransactionAccepted.`TransactionAccepted to LoggingValue`.toLoggingValue(update)
    case update: CommandRejected =>
      CommandRejected.`CommandRejected to LoggingValue`.toLoggingValue(update)
    case update: ReassignmentAccepted =>
      ReassignmentAccepted.`ReassignmentAccepted to LoggingValue`.toLoggingValue(update)
    case update: EmptyAcsPublicationRequired =>
      EmptyAcsPublicationRequired.`EmptyAcsPublicationRequired to LoggingValue`.toLoggingValue(
        update
      )
    case update: LsuTimeReached =>
      LsuTimeReached.`LsuTimeReached to LoggingValue`
        .toLoggingValue(
          update
        )
    case update: SequencerIndexMoved =>
      SequencerIndexMoved.`SequencerIndexMoved to LoggingValue`.toLoggingValue(update)
    case _: CommitRepair =>
      LoggingValue.Empty
  }

  private object Logging {
    def recordTime(timestamp: Timestamp): LoggingEntry =
      "recordTime" -> timestamp.toInstant

    def submissionId(id: Ref.SubmissionId): LoggingEntry =
      "submissionId" -> id

    def submissionIdOpt(id: Option[Ref.SubmissionId]): LoggingEntry =
      "submissionId" -> id

    def participantId(id: Ref.ParticipantId): LoggingEntry =
      "participantId" -> id

    def commandId(id: Ref.CommandId): LoggingEntry =
      "commandId" -> id

    def party(party: Ref.Party): LoggingEntry =
      "party" -> party

    def updateId(id: UpdateId): LoggingEntry =
      "updateId" -> id.toHexString

    def userId(id: Ref.UserId): LoggingEntry =
      "userId" -> id

    def workflowIdOpt(id: Option[Ref.WorkflowId]): LoggingEntry =
      "workflowId" -> id

    def ledgerTime(time: Timestamp): LoggingEntry =
      "ledgerTime" -> time.toInstant

    def preparationTime(time: Timestamp): LoggingEntry =
      "preparationTime" -> time.toInstant

    def deduplicationPeriod(period: Option[DeduplicationPeriod]): LoggingEntry =
      "deduplicationPeriod" -> period

    def rejectionReason(rejectionReason: String): LoggingEntry =
      "rejectionReason" -> rejectionReason

    def rejectionReason(
        rejectionReasonTemplate: CommandRejected.RejectionReasonTemplate
    ): LoggingEntry =
      "rejectionReason" -> rejectionReasonTemplate

    def submitter(parties: List[Ref.Party]): LoggingEntry =
      "submitter" -> parties

    def completionInfo(info: Option[CompletionInfo]): LoggingEntry =
      "completion" -> info

    def synchronizerId(synchronizerId: SynchronizerId): LoggingEntry =
      "synchronizerId" -> synchronizerId.toString
  }

}
