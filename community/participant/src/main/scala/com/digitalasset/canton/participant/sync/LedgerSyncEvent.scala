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
import com.daml.lf.data.Bytes
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfContractId, LfHash}
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

/** This a copy of [[com.daml.ledger.participant.state.v2.Update]].
  * Refer to [[com.daml.ledger.participant.state.v2.Update]] documentation for more information.
  */
sealed trait LedgerSyncEvent extends Product with Serializable with PrettyPrinting {
  def description: String
  def recordTime: LfTimestamp
  def toDamlUpdate: Update =
    this.into[Update].transform

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
}
