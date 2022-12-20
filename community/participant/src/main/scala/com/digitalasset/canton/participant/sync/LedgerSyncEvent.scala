// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.ledger.participant.state.v2.{
  CompletionInfo,
  DivulgedContract,
  TransactionMeta,
  Update,
}
import com.daml.lf.CantonOnly
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{
  LfCommittedTransaction,
  LfContractId,
  LfHash,
  LfNodeCreate,
  LfNodeId,
  TransferId,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{
  LedgerConfiguration,
  LedgerParticipantId,
  LedgerSubmissionId,
  LedgerTransactionId,
  LfPartyId,
  LfTimestamp,
}
import com.google.rpc.status.Status as RpcStatus
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*

import scala.collection.immutable.HashMap

/** This a copy of [[com.daml.ledger.participant.state.v2.Update]].
  * Refer to [[com.daml.ledger.participant.state.v2.Update]] documentation for more information.
  */
sealed trait LedgerSyncEvent extends Product with Serializable with PrettyPrinting {
  def description: String
  def recordTime: LfTimestamp
  def toDamlUpdate: Option[Update]

  def setTimestamp(timestamp: LfTimestamp): LedgerSyncEvent =
    this match {
      case ta: LedgerSyncEvent.TransactionAccepted =>
        ta.copy(
          recordTime = timestamp,
          transactionMeta = ta.transactionMeta.copy(submissionTime = timestamp),
        )
      case ev: LedgerSyncEvent.PublicPackageUpload => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PublicPackageUploadRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.CommandRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PartyAddedToParticipant => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PartyAllocationRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ConfigurationChanged => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ConfigurationChangeRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.TransferredOut => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.TransferredIn => ev.copy(recordTime = timestamp)
    }
}

object LedgerSyncEvent {

  /** Produces a constant dummy transaction seed for transactions in which we cannot expose a seed. Essentially all of
    * them. TransactionMeta.submissionSeed can no longer be set to None starting with Daml 1.3
    */
  def noOpSeed: LfHash =
    LfHash.assertFromString("00" * LfHash.underlyingHashLength)

  final case class ConfigurationChanged(
      recordTime: LfTimestamp,
      submissionId: LedgerSubmissionId,
      participantId: LedgerParticipantId,
      newConfiguration: LedgerConfiguration,
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String =
      s"Configuration change '$submissionId' from participant '$participantId' accepted with configuration: $newConfiguration"

    override def pretty: Pretty[ConfigurationChanged] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("newConfiguration", _.newConfiguration),
      )
    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.ConfigurationChanged])
  }

  final case class ConfigurationChangeRejected(
      recordTime: LfTimestamp,
      submissionId: LedgerSubmissionId,
      participantId: LedgerParticipantId,
      proposedConfiguration: LedgerConfiguration,
      rejectionReason: String,
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String = {
      s"Configuration change '$submissionId' from participant '$participantId' was rejected: $rejectionReason"
    }

    override def pretty: Pretty[ConfigurationChangeRejected] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("rejectionReason", _.rejectionReason.doubleQuoted),
        param("proposedConfiguration", _.proposedConfiguration),
      )
    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.ConfigurationChangeRejected])
  }

  final case class PartyAddedToParticipant(
      party: LfPartyId,
      displayName: String,
      participantId: LedgerParticipantId,
      recordTime: LfTimestamp,
      submissionId: Option[LedgerSubmissionId],
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String =
      s"Add party '$party' to participant"

    override def pretty: Pretty[PartyAddedToParticipant] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId.showValueOrNone),
        param("party", _.party),
        param("displayName", _.displayName.singleQuoted),
      )
    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.PartyAddedToParticipant])
  }

  final case class PartyAllocationRejected(
      submissionId: LedgerSubmissionId,
      participantId: LedgerParticipantId,
      recordTime: LfTimestamp,
      rejectionReason: String,
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override val description: String =
      s"Request to add party to participant with submissionId '$submissionId' failed"

    override def pretty: Pretty[PartyAllocationRejected] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("rejectionReason", _.rejectionReason.doubleQuoted),
      )

    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.PartyAllocationRejected])
  }

  final case class PublicPackageUpload(
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
      recordTime: LfTimestamp,
      submissionId: Option[LedgerSubmissionId],
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String =
      s"Public package upload: ${archives.map(_.getHash).mkString(", ")}"

    override def pretty: Pretty[PublicPackageUpload] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId.showValueOrNone),
        param("sourceDescription", _.sourceDescription.map(_.doubleQuoted).showValueOrNone),
        paramWithoutValue("archives"),
      )

    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.PublicPackageUpload])
  }

  final case class PublicPackageUploadRejected(
      submissionId: LedgerSubmissionId,
      recordTime: LfTimestamp,
      rejectionReason: String,
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String =
      s"Public package upload rejected, correlationId=$submissionId reason='$rejectionReason'"

    override def pretty: Pretty[PublicPackageUploadRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("rejectionReason", _.rejectionReason.doubleQuoted),
      )

    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.PublicPackageUploadRejected])
  }

  final case class TransactionAccepted(
      optCompletionInfo: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: LedgerTransactionId,
      recordTime: LfTimestamp,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
      contractMetadata: Map[LfContractId, Bytes],
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String = s"Accept transaction $transactionId"

    override def pretty: Pretty[TransactionAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("transactionId", _.transactionId),
        paramIfDefined("completion info", _.optCompletionInfo),
        param("transactionMeta", _.transactionMeta),
        paramWithoutValue("transaction"),
        paramWithoutValue("divulgedContracts"),
        paramWithoutValue("blindingInfo"),
      )
    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.TransactionAccepted])
  }

  final case class CommandRejected(
      recordTime: LfTimestamp,
      completionInfo: CompletionInfo,
      reasonTemplate: CommandRejected.FinalReason,
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String =
      s"Reject command ${completionInfo.commandId}${if (definiteAnswer)
          " (definite answer)"}: ${reasonTemplate.message}"

    def definiteAnswer: Boolean = reasonTemplate.definiteAnswer

    override def pretty: Pretty[CommandRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("completionInfo", _.completionInfo),
        param("reason", _.reasonTemplate),
      )

    def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.CommandRejected])
  }

  object CommandRejected {

    final case class FinalReason(status: RpcStatus) extends PrettyPrinting {
      def message: String = status.message
      def code: Int = status.code
      def definiteAnswer: Boolean = GrpcStatuses.isDefiniteAnswer(status)
      override def pretty: Pretty[FinalReason] = prettyOfClass(
        unnamedParam(_.status)
      )
    }
    object FinalReason {
      implicit val toDamlFinalReason
          : Transformer[FinalReason, Update.CommandRejected.RejectionReasonTemplate] =
        (finalReason: FinalReason) => Update.CommandRejected.FinalReason(finalReason.status)
    }
  }

  /** Signal the transfer-out of a contract from source to target domain.
    *
    * @param updateId              Uniquely identifies the update.
    * @param optCompletionInfo     Must be provided for the participant that submitted the transfer-out.
    * @param submitter             The partyId of the transfer submitter.
    * @param recordTime            The ledger-provided timestamp at which the contract was transferred away.
    * @param contractId            The contract-id that's being transfer-out.
    * @param sourceDomainId        The source domain of the transfer.
    * @param targetDomainId        The target domain of the transfer.
    * @param transferInExclusivity The timestamp of the timeout before which only the submitter can initiate the
    *                              corresponding transfer-in. Must be provided for the participant that submitted the transfer-out.
    */
  final case class TransferredOut(
      updateId: LedgerTransactionId,
      optCompletionInfo: Option[CompletionInfo],
      submitter: LfPartyId,
      recordTime: LfTimestamp,
      contractId: LfContractId,
      sourceDomainId: DomainId,
      targetDomainId: DomainId,
      transferInExclusivity: Option[LfTimestamp],
  ) extends LedgerSyncEvent
      with PrettyPrinting {

    override def description: String =
      s"transfer out ${contractId} from $sourceDomainId to $targetDomainId"

    override def pretty: Pretty[TransferredOut] = prettyOfClass(
      param("updateId", _.updateId),
      paramIfDefined("completionInfo", _.optCompletionInfo),
      param("submitter", _.submitter),
      param("recordTime", _.recordTime),
      param("contractId", _.contractId),
      param("source", _.sourceDomainId),
      param("target", _.targetDomainId),
      paramIfDefined("transferInExclusivity", _.transferInExclusivity),
    )

    def toDamlUpdate: Option[Update] = None
  }

  /**  Signal the transfer-in of a contract from the source domain to the target domain.
    *
    * @param updateId                  Uniquely identifies the update.
    * @param optCompletionInfo         Must be provided for the participant that submitted the transferIn.
    * @param submitter                 The partyId of the transfer submitter.
    * @param recordTime                The ledger-provided timestamp at which the contract was transferred in.
    * @param ledgerCreateTime          The ledger time of the transaction '''creating''' the contract
    * @param createNode                Denotes the creation of the contract being transferred-in.
    * @param contractMetadata          Contains contract metadata of the contract transferred assigned by the ledger implementation
    * @param transferOutId             Uniquely identifies the transfer-out. See [[com.digitalasset.canton.protocol.TransferId]].
    * @param targetDomain              The target domain of the transfer.
    * @param createTransactionAccepted We used to create a TransactionAccepted for the transferIn.
    *                                  This param is used to keep the same behavior.
    */

  final case class TransferredIn(
      updateId: LedgerTransactionId,
      optCompletionInfo: Option[CompletionInfo],
      submitter: LfPartyId,
      recordTime: LfTimestamp,
      ledgerCreateTime: LfTimestamp,
      createNode: LfNodeCreate,
      contractMetadata: Bytes,
      transferOutId: TransferId,
      targetDomain: DomainId,
      createTransactionAccepted: Boolean,
  ) extends LedgerSyncEvent
      with PrettyPrinting {
    override def description: String =
      s"transfer in ${createNode.coid} from $targetDomain to ${transferOutId.sourceDomain}"
    override def pretty: Pretty[TransferredIn] = prettyOfClass(
      param("updateId", _.updateId),
      param("ledgerCreateTime", _.ledgerCreateTime),
      paramIfDefined("optCompletionInfo", _.optCompletionInfo),
      param("submitter", _.submitter),
      param("recordTime", _.recordTime),
      param("transferOutId", _.transferOutId),
      param("transferOutId", _.transferOutId),
      param("target", _.targetDomain),
      paramWithoutValue("createNode"),
      paramWithoutValue("contractMetadata"),
      paramWithoutValue("createdEvent"),
    )

    lazy val transactionMeta = TransactionMeta(
      ledgerEffectiveTime = ledgerCreateTime,
      workflowId = None,
      submissionTime = recordTime, // TODO(M41): Upstream mismatch, replace with enter/leave view
      submissionSeed = LedgerSyncEvent.noOpSeed,
      optUsedPackages = None,
      optNodeSeeds = None,
      optByKeyNodes = None,
    )

    /** Workaround to create an update for informing the ledger API server about a transferred-in contract.
      * Creates a TransactionAccepted event consisting of a single create action that creates the given contract.
      *
      * The transaction has the same ledger time and transaction id as the creation of the contract.
      */
    def toDamlUpdate: Option[Update] =
      Option.when(createTransactionAccepted) {
        val nodeId = LfNodeId(0)
        val committedTransaction = LfCommittedTransaction(
          CantonOnly.lfVersionedTransaction(
            version = createNode.version,
            nodes = HashMap((nodeId, createNode)),
            roots = ImmArray(nodeId),
          )
        )

        Update.TransactionAccepted(
          optCompletionInfo = optCompletionInfo,
          transactionMeta = transactionMeta,
          transaction = committedTransaction,
          transactionId = updateId,
          recordTime = recordTime,
          divulgedContracts = Nil,
          blindingInfo = None,
          contractMetadata = Map(createNode.coid -> contractMetadata),
        )
      }
  }
}
