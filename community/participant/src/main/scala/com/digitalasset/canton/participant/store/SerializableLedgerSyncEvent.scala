// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.configuration.*
import com.daml.ledger.participant.state.v2.*
import com.daml.lf.crypto.Hash as LfHash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes as LfBytes, ImmArray, Ref, StringModule}
import com.daml.lf.transaction.{BlindingInfo, TransactionOuterClass}
import com.digitalasset.canton
import com.digitalasset.canton.ProtoDeserializationError.{
  SubmissionIdConversionError,
  TimeModelConversionError,
  ValueConversionError,
}
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.participant.protocol.v0
import com.digitalasset.canton.participant.store.DamlLfSerializers.*
import com.digitalasset.canton.participant.sync.LedgerSyncEvent
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{
  AgreementText,
  LfCommittedTransaction,
  LfNodeId,
  SerializableDeduplicationPeriod,
  TransferId,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{
  DurationConverter,
  InstantConverter,
  ParsingResult,
  protoParser,
  required,
}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbSerializationException}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionedCompanionDbHelpers,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerParticipantId,
  LedgerSubmissionId,
  LedgerTransactionId,
  LfPackageId,
  LfWorkflowId,
  ProtoDeserializationError,
  checked,
}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status as RpcStatus
import slick.jdbc.{GetResult, SetParameter}

/** Wrapper for converting a [[com.digitalasset.canton.participant.sync.LedgerSyncEvent]] to its protobuf companion.
  * Currently only Intended only for storage due to the unusual exceptions which are thrown that are only permitted in a storage context.
  *
  * @throws canton.store.db.DbSerializationException if transactions or contracts fail to serialize
  * @throws canton.store.db.DbDeserializationException if transactions or contracts fail to deserialize
  */
final case class SerializableLedgerSyncEvent(event: LedgerSyncEvent)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[SerializableLedgerSyncEvent]
) extends HasProtocolVersionedWrapper[SerializableLedgerSyncEvent] {

  override protected def companionObj = SerializableLedgerSyncEvent

  def toProtoV0: v0.LedgerSyncEvent = {
    val SyncEventP = v0.LedgerSyncEvent.Value

    v0.LedgerSyncEvent(
      event match {
        case configurationChanged: LedgerSyncEvent.ConfigurationChanged =>
          SyncEventP.ConfigurationChanged(
            SerializableConfigurationChanged(configurationChanged).toProtoV0
          )
        case configurationChangeRejected: LedgerSyncEvent.ConfigurationChangeRejected =>
          SyncEventP.ConfigurationChangeRejected(
            SerializableConfigurationChangeRejected(configurationChangeRejected).toProtoV0
          )
        case partyAddedToParticipant: LedgerSyncEvent.PartyAddedToParticipant =>
          SyncEventP.PartyAddedToParticipant(
            SerializablePartyAddedToParticipant(partyAddedToParticipant).toProtoV0
          )
        case partyAllocationRejected: LedgerSyncEvent.PartyAllocationRejected =>
          SyncEventP.PartyAllocationRejected(
            SerializablePartyAllocationRejected(partyAllocationRejected).toProtoV0
          )
        case publicPackageUpload: LedgerSyncEvent.PublicPackageUpload =>
          SyncEventP.PublicPackageUpload(
            SerializablePublicPackageUpload(publicPackageUpload).toProtoV0
          )
        case publicPackageUploadRejected: LedgerSyncEvent.PublicPackageUploadRejected =>
          SyncEventP.PublicPackageUploadRejected(
            SerializablePublicPackageUploadRejected(publicPackageUploadRejected).toProtoV0
          )
        case transactionAccepted: LedgerSyncEvent.TransactionAccepted =>
          SyncEventP.TransactionAccepted(
            SerializableTransactionAccepted(transactionAccepted).toProtoV0
          )
        case commandRejected: LedgerSyncEvent.CommandRejected =>
          SyncEventP.CommandRejected(SerializableCommandRejected(commandRejected).toProtoV0)

        case transferOut: LedgerSyncEvent.TransferredOut =>
          SyncEventP.TransferredOut(SerializableTransferredOut(transferOut).toProtoV0)

        case transferIn: LedgerSyncEvent.TransferredIn =>
          SyncEventP.TransferredIn(SerializableTransferredIn(transferIn).toProtoV0)
      }
    )
  }
}

object SerializableLedgerSyncEvent
    extends HasProtocolVersionedCompanion[SerializableLedgerSyncEvent]
    with ProtocolVersionedCompanionDbHelpers[SerializableLedgerSyncEvent] {
  override val name: String = "SerializableLedgerSyncEvent"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.LedgerSyncEvent)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      event: LedgerSyncEvent,
      protocolVersion: ProtocolVersion,
  ): SerializableLedgerSyncEvent =
    SerializableLedgerSyncEvent(event)(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV0(
      ledgerSyncEventP: v0.LedgerSyncEvent
  ): ParsingResult[SerializableLedgerSyncEvent] = {
    val SyncEventP = v0.LedgerSyncEvent.Value
    val ledgerSyncEvent = ledgerSyncEventP.value match {
      case SyncEventP.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("LedgerSyncEvent.value"))
      case SyncEventP.ConfigurationChanged(configurationChanged) =>
        SerializableConfigurationChanged.fromProtoV0(configurationChanged)
      case SyncEventP.ConfigurationChangeRejected(configurationChangeRejected) =>
        SerializableConfigurationChangeRejected.fromProtoV0(configurationChangeRejected)
      case SyncEventP.PartyAddedToParticipant(partyAddedToParticipant) =>
        SerializablePartyAddedToParticipant.fromProtoV0(partyAddedToParticipant)
      case SyncEventP.PartyAllocationRejected(partyAllocationRejected) =>
        SerializablePartyAllocationRejected.fromProtoV0(partyAllocationRejected)
      case SyncEventP.PublicPackageUpload(publicPackageUpload) =>
        SerializablePublicPackageUpload.fromProtoV0(publicPackageUpload)
      case SyncEventP.PublicPackageUploadRejected(publicPackageUploadRejected) =>
        SerializablePublicPackageUploadRejected.fromProtoV0(publicPackageUploadRejected)
      case SyncEventP.TransactionAccepted(transactionAccepted) =>
        SerializableTransactionAccepted.fromProtoV0(transactionAccepted)
      case SyncEventP.CommandRejected(commandRejected) =>
        SerializableCommandRejected.fromProtoV0(commandRejected)
      case SyncEventP.TransferredOut(transferOut) =>
        SerializableTransferredOut.fromProtoV0(transferOut)
      case SyncEventP.TransferredIn(transferIn) =>
        SerializableTransferredIn.fromProtoV0(transferIn)
    }

    ledgerSyncEvent.map(
      SerializableLedgerSyncEvent(_)(
        protocolVersionRepresentativeFor(ProtoVersion(0))
      )
    )
  }
}

trait ConfigurationParamsDeserializer {
  def fromProtoV0(
      recordTimeP: Option[com.google.protobuf.timestamp.Timestamp],
      submissionIdP: String,
      participantIdP: String,
      configurationP: (String, Option[v0.Configuration]),
  ): Either[
    ProtoDeserializationError,
    (Timestamp, LedgerSubmissionId, LedgerParticipantId, Configuration),
  ] =
    configurationP match {
      case (field, configP) =>
        for {
          recordTime <- required("recordTime", recordTimeP).flatMap(
            SerializableLfTimestamp.fromProtoPrimitive
          )
          submissionId <- SerializableSubmissionId.fromProtoPrimitive(submissionIdP)
          participantId <- SerializableParticipantId.fromProtoPrimitive(participantIdP)
          configuration <- required(field, configP).flatMap(SerializableConfiguration.fromProtoV0)
        } yield (recordTime, submissionId, participantId, configuration)
    }
}

case class SerializableConfigurationChanged(
    configurationChanged: LedgerSyncEvent.ConfigurationChanged
) {
  def toProtoV0: v0.ConfigurationChanged = {
    val LedgerSyncEvent.ConfigurationChanged(
      recordTime,
      submissionId,
      participantId,
      newConfiguration,
    ) =
      configurationChanged
    v0.ConfigurationChanged(
      submissionId,
      Some(SerializableConfiguration(newConfiguration).toProtoV0),
      participantId,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
    )
  }
}

object SerializableConfigurationChanged extends ConfigurationParamsDeserializer {
  def fromProtoV0(
      configurationChangedP: v0.ConfigurationChanged
  ): ParsingResult[LedgerSyncEvent.ConfigurationChanged] = {
    val v0.ConfigurationChanged(submissionIdP, configurationP, participantIdP, recordTimeP) =
      configurationChangedP
    for {
      cfg <- fromProtoV0(
        recordTimeP,
        submissionIdP,
        participantIdP,
        ("configuration", configurationP),
      )
      (recordTime, submissionId, participantId, configuration) = cfg
    } yield LedgerSyncEvent.ConfigurationChanged(
      recordTime,
      submissionId,
      participantId,
      configuration,
    )
  }
}

case class SerializableConfigurationChangeRejected(
    configurationChangeRejected: LedgerSyncEvent.ConfigurationChangeRejected
) {
  def toProtoV0: v0.ConfigurationChangeRejected = {
    val LedgerSyncEvent.ConfigurationChangeRejected(
      recordTime,
      submissionId,
      participantId,
      proposedConfiguration,
      reason,
    ) =
      configurationChangeRejected
    v0.ConfigurationChangeRejected(
      submissionId,
      reason,
      participantId,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      Some(SerializableConfiguration(proposedConfiguration).toProtoV0),
    )
  }
}

object SerializableConfigurationChangeRejected extends ConfigurationParamsDeserializer {
  def fromProtoV0(
      configurationChangeRejected: v0.ConfigurationChangeRejected
  ): Either[canton.ProtoDeserializationError, LedgerSyncEvent.ConfigurationChangeRejected] = {
    val v0.ConfigurationChangeRejected(
      submissionIdP,
      reason,
      participantIdP,
      recordTimeP,
      proposedConfigurationP,
    ) =
      configurationChangeRejected
    for {
      cfg <- fromProtoV0(
        recordTimeP,
        submissionIdP,
        participantIdP,
        ("proposedConfiguration", proposedConfigurationP),
      )
      (recordTime, submissionId, participantId, proposedConfiguration) = cfg
    } yield LedgerSyncEvent.ConfigurationChangeRejected(
      recordTime,
      submissionId,
      participantId,
      proposedConfiguration,
      reason,
    )
  }
}

case class SerializablePartyAddedToParticipant(
    partyAddedToParticipant: LedgerSyncEvent.PartyAddedToParticipant
) {
  def toProtoV0: v0.PartyAddedToParticipant = {
    val LedgerSyncEvent.PartyAddedToParticipant(
      party,
      displayName,
      participantId,
      recordTime,
      submissionId,
    ) =
      partyAddedToParticipant
    v0.PartyAddedToParticipant(
      party,
      displayName,
      participantId,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      submissionId.fold("")(_.toString),
    )
  }
}

object SerializablePartyAddedToParticipant {
  def fromProtoV0(
      partyAddedToParticipant: v0.PartyAddedToParticipant
  ): ParsingResult[LedgerSyncEvent.PartyAddedToParticipant] = {
    val v0.PartyAddedToParticipant(partyP, displayName, participantIdP, recordTime, submissionIdP) =
      partyAddedToParticipant
    for {
      party <- ProtoConverter.parseLfPartyId(partyP)
      participantId <- SerializableParticipantId.fromProtoPrimitive(participantIdP)
      recordTime <- required("recordTime", recordTime).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      // submission id can be empty when the PartyAdded event is sent to non-submitting participants
      submissionId <- {
        if (submissionIdP.isEmpty) Right(None)
        else SerializableSubmissionId.fromProtoPrimitive(submissionIdP).map(Some(_))
      }
    } yield LedgerSyncEvent.PartyAddedToParticipant(
      party,
      displayName,
      participantId,
      recordTime,
      submissionId,
    )
  }
}

case class SerializablePartyAllocationRejected(
    partyAllocationRejected: LedgerSyncEvent.PartyAllocationRejected
) {
  def toProtoV0: v0.PartyAllocationRejected = {
    val LedgerSyncEvent.PartyAllocationRejected(
      submissionId,
      participantId,
      recordTime,
      rejectionReason,
    ) =
      partyAllocationRejected
    v0.PartyAllocationRejected(
      submissionId,
      participantId,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      rejectionReason,
    )
  }
}

object SerializablePartyAllocationRejected {
  def fromProtoV0(
      partyAllocationRejected: v0.PartyAllocationRejected
  ): ParsingResult[LedgerSyncEvent.PartyAllocationRejected] = {
    val v0.PartyAllocationRejected(submissionIdP, participantIdP, recordTime, rejectionReason) =
      partyAllocationRejected
    for {
      submissionId <- SerializableSubmissionId.fromProtoPrimitive(submissionIdP)
      participantId <- SerializableParticipantId.fromProtoPrimitive(participantIdP)
      recordTime <- required("recordTime", recordTime).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
    } yield LedgerSyncEvent.PartyAllocationRejected(
      submissionId,
      participantId,
      recordTime,
      rejectionReason,
    )
  }
}

case class SerializablePublicPackageUpload(
    publicPackageUpload: LedgerSyncEvent.PublicPackageUpload
) {
  def toProtoV0: v0.PublicPackageUpload = {
    val LedgerSyncEvent.PublicPackageUpload(archives, sourceDescription, recordTime, submissionId) =
      publicPackageUpload
    v0.PublicPackageUpload(
      archives.map(_.toByteString),
      sourceDescription,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      submissionId.fold("")(_.toString),
    )
  }
}

object SerializablePublicPackageUpload {
  import cats.syntax.traverse.*

  def fromProtoV0(
      publicPackageUploadP: v0.PublicPackageUpload
  ): ParsingResult[LedgerSyncEvent.PublicPackageUpload] = {
    val v0.PublicPackageUpload(archivesP, sourceDescription, recordTime, submissionIdP) =
      publicPackageUploadP
    for {
      archives <- archivesP.toList.traverse(protoParser(Archive.parseFrom))
      recordTime <- required("recordTime", recordTime).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      // submission id can be empty when the PublicPackageUpload event is sent to non-submitting participants
      submissionId <- {
        if (submissionIdP.isEmpty) Right(None)
        else SerializableSubmissionId.fromProtoPrimitive(submissionIdP).map(Some(_))
      }
    } yield LedgerSyncEvent.PublicPackageUpload(
      archives,
      sourceDescription,
      recordTime,
      submissionId,
    )
  }
}

case class SerializablePublicPackageUploadRejected(
    publicPackageUploadRejected: LedgerSyncEvent.PublicPackageUploadRejected
) {
  def toProtoV0: v0.PublicPackageUploadRejected = {
    val LedgerSyncEvent.PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =
      publicPackageUploadRejected
    v0.PublicPackageUploadRejected(
      submissionId,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      rejectionReason,
    )
  }
}

object SerializablePublicPackageUploadRejected {
  def fromProtoV0(
      publicPackageUploadRejectedP: v0.PublicPackageUploadRejected
  ): ParsingResult[LedgerSyncEvent.PublicPackageUploadRejected] = {
    val v0.PublicPackageUploadRejected(submissionIdP, recordTime, rejectionReason) =
      publicPackageUploadRejectedP
    for {
      submissionId <- SerializableSubmissionId.fromProtoPrimitive(submissionIdP)
      recordTime <- required("recordTime", recordTime).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
    } yield LedgerSyncEvent.PublicPackageUploadRejected(submissionId, recordTime, rejectionReason)
  }
}

case class SerializableTransactionAccepted(
    transactionAccepted: LedgerSyncEvent.TransactionAccepted
) {
  def toProtoV0: v0.TransactionAccepted = {
    val LedgerSyncEvent.TransactionAccepted(
      optCompletionInfo,
      transactionMeta,
      committedTransaction,
      transactionId,
      recordTime,
      divulgedContracts,
      blindingInfo,
      contractMetadata,
    ) = transactionAccepted
    val contractMetadataP = contractMetadata.view.map { case (contractId, bytes) =>
      contractId.toProtoPrimitive -> bytes.toByteString
    }.toMap
    v0.TransactionAccepted(
      optCompletionInfo.map(SerializableCompletionInfo(_).toProtoV0),
      Some(SerializableTransactionMeta(transactionMeta).toProtoV0),
      serializeTransaction(
        committedTransaction
      ) // LfCommittedTransaction implicitly turned into LfVersionedTransaction by LF
        .valueOr(err =>
          throw new DbSerializationException(
            s"Failed to serialize versioned transaction: ${err.errorMessage}"
          )
        ),
      transactionId,
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      divulgedContracts.map(SerializableDivulgedContract(_).toProtoV0),
      blindingInfo.map(SerializableBlindingInfo(_).toProtoV0),
      contractMetadata = contractMetadataP,
    )
  }
}

object SerializableTransactionAccepted {
  def fromProtoV0(
      transactionAcceptedP: v0.TransactionAccepted
  ): ParsingResult[LedgerSyncEvent.TransactionAccepted] = {
    val v0.TransactionAccepted(
      completionInfoP,
      transactionMetaP,
      transactionP,
      transactionIdP,
      recordTimeP,
      divulgedContractsP,
      blindingInfoP,
      contractMetadataP,
    ) = transactionAcceptedP
    for {
      optCompletionInfo <- completionInfoP.traverse(SerializableCompletionInfo.fromProtoV0)
      transactionMeta <- required("transactionMeta", transactionMetaP)
        .flatMap(SerializableTransactionMeta.fromProtoV0)
      committedTransaction = deserializeTransaction(transactionP)
        .leftMap(err =>
          new DbDeserializationException(
            s"Failed to deserialize versioned transaction: ${err.errorMessage}"
          )
        )
        .fold(throw _, LfCommittedTransaction(_))
      transactionId <- SerializableTransactionId.fromProtoPrimitive(transactionIdP)
      recordTime <- required("recordTime", recordTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      divulgedContracts <- divulgedContractsP.toList.traverse(
        SerializableDivulgedContract.fromProtoV0
      )
      blindingInfo <- blindingInfoP.fold(
        Right(None): ParsingResult[Option[BlindingInfo]]
      )(SerializableBlindingInfo.fromProtoV0(_).map(Some(_)))
      contractMetadataSeq <- contractMetadataP.toList.traverse {
        case (contractIdP, driverContractMetadataBytes) =>
          ProtoConverter
            .parseLfContractId(contractIdP)
            .map(_ -> LfBytes.fromByteString(driverContractMetadataBytes))
      }
      contractMetadata = contractMetadataSeq.toMap
    } yield LedgerSyncEvent.TransactionAccepted(
      optCompletionInfo,
      transactionMeta,
      committedTransaction,
      transactionId,
      recordTime,
      divulgedContracts,
      blindingInfo,
      contractMetadata = contractMetadata,
    )
  }
}

case class SerializableDivulgedContract(divulgedContract: DivulgedContract) {
  def toProtoV0: v0.DivulgedContract = {
    val DivulgedContract(contractId, contractInst) = divulgedContract
    v0.DivulgedContract(
      contractId = contractId.toProtoPrimitive,
      // This is fine to use empty agreement text for divulged contract
      contractInst = serializeContract(contractInst, AgreementText.empty)
        .valueOr(err =>
          throw new DbSerializationException(
            s"Failed to serialize contract: ${err.errorMessage}"
          )
        ),
    )
  }
}

object SerializableDivulgedContract {
  def fromProtoV0(
      divulgedContract: v0.DivulgedContract
  ): ParsingResult[DivulgedContract] = {
    val v0.DivulgedContract(contractIdP, contractInstP) = divulgedContract
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      contractInstAndAgreementText <- deserializeContract(contractInstP).leftMap(err =>
        ValueConversionError("contractInst", err.errorMessage)
      )
      contractInst = contractInstAndAgreementText.map(_.contractInstance)
    } yield DivulgedContract(contractId, contractInst)
  }
}

case class SerializableCommandRejected(commandRejected: LedgerSyncEvent.CommandRejected) {
  def toProtoV0: v0.CommandRejected = {
    val LedgerSyncEvent.CommandRejected(recordTime, completionInfo, reason) = commandRejected
    v0.CommandRejected(
      Some(SerializableCompletionInfo(completionInfo).toProtoV0),
      Some(SerializableLfTimestamp(recordTime).toProtoV0),
      Some(SerializableRejectionReasonTemplate(reason).toProtoV0),
    )
  }
}

object SerializableCommandRejected {
  def fromProtoV0(
      commandRejectedP: v0.CommandRejected
  ): ParsingResult[LedgerSyncEvent.CommandRejected] = {
    val v0.CommandRejected(completionInfoP, recordTimeP, rejectionReasonP) = commandRejectedP
    for {
      recordTime <- required("recordTime", recordTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      completionInfo <- required("completionInfo", completionInfoP).flatMap(
        SerializableCompletionInfo.fromProtoV0
      )
      rejectionReason <- required("rejectionReason", rejectionReasonP).flatMap(
        SerializableRejectionReasonTemplate.fromProtoV0
      )
    } yield LedgerSyncEvent.CommandRejected(recordTime, completionInfo, rejectionReason)
  }
}

case class SerializableLfTimestamp(timestamp: Timestamp) {
  def toProtoV0: com.google.protobuf.timestamp.Timestamp =
    InstantConverter.toProtoPrimitive(timestamp.toInstant)
}

object SerializableLfTimestamp {
  def fromProtoPrimitive(
      timestampP: com.google.protobuf.timestamp.Timestamp
  ): ParsingResult[Timestamp] =
    for {
      instant <- InstantConverter.fromProtoPrimitive(timestampP)
      // we prefer sticking to our fromProto convention which prevents passing the field name
      // hence the fieldName is unknown at this point. We may decide to invest in richer
      // error context information passing in the future if deemed valuable.
      timestamp <- Timestamp.fromInstant(instant).left.map(ValueConversionError("<unknown>", _))
    } yield timestamp
}

case class SerializableSubmissionId(submissionId: LedgerSubmissionId) {
  def toProtoPrimitive: String = submissionId
  def toLengthLimitedString: String255 =
    checked(String255.tryCreate(submissionId)) // LedgerSubmissionId is limited to 255 chars
}

object SerializableSubmissionId {
  def fromProtoPrimitive(
      submissionIdP: String
  ): ParsingResult[LedgerSubmissionId] =
    LedgerSubmissionId.fromString(submissionIdP).leftMap(SubmissionIdConversionError)

  implicit val setParameterSubmissionId: SetParameter[SerializableSubmissionId] = (v, pp) =>
    pp >> v.toLengthLimitedString

  implicit val getResultSubmissionId: GetResult[SerializableSubmissionId] = GetResult { r =>
    deserializeFromPrimitive(r.nextString())
  }

  implicit val getResultOptionSubmissionId: GetResult[Option[SerializableSubmissionId]] =
    GetResult { r =>
      r.nextStringOption().map(deserializeFromPrimitive)
    }

  implicit val setParameterOptionSubmissionId: SetParameter[Option[SerializableSubmissionId]] =
    (v, pp) => pp >> v.map(_.toLengthLimitedString)

  private def deserializeFromPrimitive(serialized: String): SerializableSubmissionId = {
    val submissionId = SerializableSubmissionId
      .fromProtoPrimitive(serialized)
      .valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize submission id: $err")
      )
    SerializableSubmissionId(submissionId)
  }
}

/** Provides the `fromProto` conversion method typically provided by the companion object of a HasProtoV0 class
  * for converting daml-lf string instances that use [[StringModule]]. As their native value is a string
  * a `toProto` equivalent is currently unnecessary.
  */
private[store] class SerializableStringModule[V, M <: StringModule[V]](module: M) {
  def fromProtoPrimitive(valueP: String): ParsingResult[V] =
    // see note about unknown field naming in SerializableLfTimestamp
    module.fromString(valueP).leftMap(ValueConversionError("<unknown>", _))
}

object SerializableParticipantId
    extends SerializableStringModule[LedgerParticipantId, LedgerParticipantId.type](
      LedgerParticipantId
    )
object SerializableTransactionId
    extends SerializableStringModule[LedgerTransactionId, LedgerTransactionId.type](
      LedgerTransactionId
    )
object SerializableApplicationId
    extends SerializableStringModule[LedgerApplicationId, Ref.ApplicationId.type](
      LedgerApplicationId
    )
object SerializableCommandId
    extends SerializableStringModule[Ref.CommandId, Ref.CommandId.type](Ref.CommandId)
object SerializableWorkflowId
    extends SerializableStringModule[LfWorkflowId, Ref.WorkflowId.type](LfWorkflowId)
object SerializablePackageId
    extends SerializableStringModule[LfPackageId, LfPackageId.type](LfPackageId)

case class SerializableConfiguration(configuration: Configuration) {
  def toProtoV0: v0.Configuration = configuration match {
    case Configuration(generation, timeModel, maxDeduplicationDuration) =>
      v0.Configuration(
        generation,
        Some(SerializableTimeModel(timeModel).toProtoV0),
        Some(DurationConverter.toProtoPrimitive(maxDeduplicationDuration)),
      )
  }
}

object SerializableConfiguration {
  def fromProtoV0(
      configuration: v0.Configuration
  ): ParsingResult[Configuration] = {
    val v0.Configuration(generationP, timeModelP, maxDeduplicationDurationP) = configuration
    for {
      timeModel <- required("timeModel", timeModelP).flatMap(SerializableTimeModel.fromProtoV0)
      maxDeduplicationDuration <- required("maxDeduplicationDuration", maxDeduplicationDurationP)
        .flatMap(
          DurationConverter.fromProtoPrimitive
        )
    } yield Configuration(generationP, timeModel, maxDeduplicationDuration)
  }
}

case class SerializableTimeModel(timeModel: LedgerTimeModel) {
  def toProtoV0: v0.TimeModel =
    // uses direct field access as TimeModel is a trait rather than interface
    v0.TimeModel(
      Some(DurationConverter.toProtoPrimitive(timeModel.avgTransactionLatency)),
      Some(DurationConverter.toProtoPrimitive(timeModel.minSkew)),
      Some(DurationConverter.toProtoPrimitive(timeModel.maxSkew)),
    )
}

object SerializableTimeModel {
  def fromProtoV0(timeModelP: v0.TimeModel): ParsingResult[LedgerTimeModel] = {
    val v0.TimeModel(avgTransactionLatencyP, minSkewP, maxSkewP) =
      timeModelP
    for {
      // abbreviations are due to not being able to use full names as they'd be considered accessors in the time model definition below
      atl <- deserializeDuration("avgTransactionLatencyP", avgTransactionLatencyP)
      mis <- deserializeDuration("minSkewP", minSkewP)
      mas <- deserializeDuration("maxSkewP", maxSkewP)
      // this is quite sketchy however there is no current way to use the values persisted for all fields rather than potentially different new defaults
      // (without adjusting upstream)
      timeModel <- LedgerTimeModel(atl, mis, mas)
        .fold(t => Left(TimeModelConversionError(t.getMessage)), Right(_))
    } yield timeModel
  }

  private def deserializeDuration(
      field: String,
      optDurationP: Option[com.google.protobuf.duration.Duration],
  ): ParsingResult[java.time.Duration] =
    required(field, optDurationP).flatMap(DurationConverter.fromProtoPrimitive)
}

case class SerializableCompletionInfo(completionInfo: CompletionInfo) {
  def toProtoV0: v0.CompletionInfo = {
    val CompletionInfo(
      actAs,
      applicationId,
      commandId,
      deduplicateUntil,
      submissionId,
      statistics,
    ) =
      completionInfo
    require(
      statistics.isEmpty,
      "Statistics are only set before emitting CompletionInfo in CantonSyncService",
    )
    v0.CompletionInfo(
      actAs,
      applicationId,
      commandId,
      deduplicateUntil.map(SerializableDeduplicationPeriod(_).toProtoV0),
      submissionId.getOrElse(""),
    )
  }
}

object SerializableCompletionInfo {
  def fromProtoV0(
      completionInfoP: v0.CompletionInfo
  ): ParsingResult[CompletionInfo] = {
    val v0.CompletionInfo(actAsP, applicationIdP, commandIdP, deduplicateUntilP, submissionIdP) =
      completionInfoP
    for {
      actAs <- actAsP.toList.traverse(ProtoConverter.parseLfPartyId(_))
      applicationId <- SerializableApplicationId.fromProtoPrimitive(applicationIdP)
      commandId <- SerializableCommandId.fromProtoPrimitive(commandIdP)
      deduplicateUntil <- deduplicateUntilP.traverse(SerializableDeduplicationPeriod.fromProtoV0(_))
      submissionId <-
        if (submissionIdP.nonEmpty)
          SerializableSubmissionId.fromProtoPrimitive(submissionIdP).map(Some(_))
        else Right(None)
    } yield CompletionInfo(
      actAs,
      applicationId,
      commandId,
      deduplicateUntil,
      submissionId,
      statistics = None,
    )
  }
}

case class SerializableNodeSeed(nodeId: LfNodeId, seedHash: LfHash) {
  def toProtoV0: v0.NodeSeed =
    v0.NodeSeed(nodeId.index, ByteString.copyFrom(seedHash.bytes.toByteArray))
}

object SerializableNodeSeed {
  def fromProtoV0(nodeSeed: v0.NodeSeed): ParsingResult[(LfNodeId, LfHash)] = {
    val v0.NodeSeed(nodeIndex, seedHashP) = nodeSeed
    for {
      nodeId <- Right(LfNodeId(nodeIndex))
      nodeSeedHash <- LfHash
        .fromBytes(LfBytes.fromByteString(seedHashP))
        .leftMap(ValueConversionError("nodeSeed", _))
    } yield (nodeId, nodeSeedHash)
  }
}

case class SerializableTransactionMeta(transactionMeta: TransactionMeta) {
  def toProtoV0: v0.TransactionMeta = {
    val TransactionMeta(
      ledgerTime,
      workflowId,
      submissionTime,
      submissionSeed,
      optUsedPackages,
      optNodeSeeds,
      optByKeyNodes,
    ) = transactionMeta
    v0.TransactionMeta(
      ledgerTime = Some(InstantConverter.toProtoPrimitive(ledgerTime.toInstant)),
      workflowId = workflowId,
      submissionTime = Some(InstantConverter.toProtoPrimitive(submissionTime.toInstant)),
      submissionSeed = ByteString.copyFrom(submissionSeed.bytes.toByteArray),
      usedPackages = optUsedPackages.fold(Seq.empty[String])(_.map(_.toString).toSeq),
      nodeSeeds = optNodeSeeds.fold(Seq.empty[v0.NodeSeed])(_.map { case (nodeId, seedHash) =>
        SerializableNodeSeed(nodeId, seedHash).toProtoV0
      }.toSeq),
      byKeyNodes = optByKeyNodes.map(byKeyNodes =>
        v0.TransactionMeta.ByKeyNodes(byKeyNodes.map(_.index).toSeq)
      ),
    )
  }
}

object SerializableTransactionMeta {

  def fromProtoV0(
      transactionMetaP: v0.TransactionMeta
  ): ParsingResult[TransactionMeta] = {
    val v0.TransactionMeta(
      ledgerTimeP,
      workflowIdP,
      submissionTimeP,
      submissionSeedP,
      usedPackagesP,
      nodeSeedsP,
      byKeyNodesP,
    ) =
      transactionMetaP
    for {
      ledgerTime <- required("ledger_time", ledgerTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      workflowId <- workflowIdP.traverse(SerializableWorkflowId.fromProtoPrimitive)
      submissionTime <- required("submissionTime", submissionTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      submissionSeed <- LfHash
        .fromBytes(LfBytes.fromByteString(submissionSeedP))
        .leftMap(ValueConversionError("submissionSeed", _))
      optUsedPackages <- {
        if (usedPackagesP.isEmpty) Right(None)
        else
          usedPackagesP.toList
            .traverse(LfPackageId.fromString(_).leftMap(ValueConversionError("usedPackages", _)))
            .map(packageList => Some(packageList.toSet))
      }
      optNodeSeeds <- nodeSeedsP
        .traverse(SerializableNodeSeed.fromProtoV0)
        .map(list => Some(list.to(ImmArray)))
      optByKeyNodes = byKeyNodesP.map(byKeyNodes =>
        byKeyNodes.byKeyNode.map(LfNodeId(_)).to(ImmArray)
      )
    } yield TransactionMeta(
      ledgerTime,
      workflowId,
      submissionTime,
      submissionSeed,
      optUsedPackages,
      optNodeSeeds,
      optByKeyNodes,
    )
  }
}

case class SerializableBlindingInfo(blindingInfo: BlindingInfo) {
  def toProtoV0: v0.BlindingInfo = {
    val BlindingInfo(disclosure, divulgence) = blindingInfo

    v0.BlindingInfo(
      disclosure.map { case (LfNodeId(nodeId), parties) => nodeId -> v0.Parties(parties.toSeq) },
      divulgence.map { case (contractId, parties) => contractId.coid -> v0.Parties(parties.toSeq) },
    )
  }
}

object SerializableBlindingInfo {
  def fromProtoV0(
      blindingInfoP: v0.BlindingInfo
  ): ParsingResult[BlindingInfo] = {
    val v0.BlindingInfo(disclosureP, divulgenceP) = blindingInfoP
    for {
      disclosure <- disclosureP.toList
        .traverse { case (nodeIdAsInt, parties) =>
          parties.parties.toList
            .traverse(ProtoConverter.parseLfPartyId)
            .map(parties => LfNodeId(nodeIdAsInt) -> parties.toSet)
        }
        .map(_.toMap)
      divulgence <- divulgenceP.toList
        .traverse { case (contractIdP, parties) =>
          ProtoConverter
            .parseLfContractId(contractIdP)
            .flatMap(contractId =>
              parties.parties
                .traverse(ProtoConverter.parseLfPartyId)
                .map(parties => contractId -> parties.toSet)
            )
        }
        .map(_.toMap)
    } yield BlindingInfo(disclosure, divulgence)
  }
}

case class SerializableRejectionReasonTemplate(
    rejectionReason: LedgerSyncEvent.CommandRejected.FinalReason
) {
  def toProtoV0: v0.CommandRejected.GrpcRejectionReasonTemplate =
    v0.CommandRejected.GrpcRejectionReasonTemplate(rejectionReason.status.toByteString)
}

object SerializableRejectionReasonTemplate {
  def fromProtoV0(
      reasonP: v0.CommandRejected.GrpcRejectionReasonTemplate
  ): ParsingResult[LedgerSyncEvent.CommandRejected.FinalReason] = {
    for {
      rpcStatus <- ProtoConverter.protoParser(RpcStatus.parseFrom)(reasonP.status)
    } yield LedgerSyncEvent.CommandRejected.FinalReason(rpcStatus)
  }
}

private[store] final case class SerializableTransferredOut(
    transferOut: LedgerSyncEvent.TransferredOut
) {
  def toProtoV0: v0.TransferredOut = {
    val LedgerSyncEvent.TransferredOut(
      updateId,
      optCompletionInfo,
      submitter,
      recordTime,
      contractId,
      contractStakeholders,
      source,
      target,
      transferInExclusivity,
    ) = transferOut
    v0.TransferredOut(
      updateId = updateId,
      completionInfo = optCompletionInfo.map(SerializableCompletionInfo(_).toProtoV0),
      submitter = submitter,
      recordTime = Some(SerializableLfTimestamp(recordTime).toProtoV0),
      contractId = contractId.toProtoPrimitive,
      contractStakeholders = contractStakeholders.toSeq,
      sourceDomain = source.toProtoPrimitive,
      targetDomain = target.toProtoPrimitive,
      transferInExclusivity = transferInExclusivity.map(SerializableLfTimestamp(_).toProtoV0),
    )
  }
}

private[store] object SerializableTransferredOut {
  def fromProtoV0(
      transferOutP: v0.TransferredOut
  ): ParsingResult[LedgerSyncEvent.TransferredOut] = {
    val v0.TransferredOut(
      updateIdP,
      optCompletionInfoP,
      submitterP,
      recordTimeP,
      contractIdP,
      contractStakeholdersP,
      sourceDomainIdP,
      targetDomainIdP,
      transferInExclusivityP,
    ) = transferOutP

    for {
      updateId <- ProtoConverter.parseLedgerTransactionId(updateIdP)
      optCompletionInfo <- optCompletionInfoP.traverse(SerializableCompletionInfo.fromProtoV0)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      recordTime <- required("record_time", recordTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      contractStakeholders <- contractStakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      sourceDomainId <- DomainId.fromProtoPrimitive(sourceDomainIdP, "source_domain")
      targetDomainId <- DomainId.fromProtoPrimitive(targetDomainIdP, "target_domain")
      transferInExclusivity <- transferInExclusivityP
        .traverse(SerializableLfTimestamp.fromProtoPrimitive)

    } yield LedgerSyncEvent.TransferredOut(
      updateId = updateId,
      optCompletionInfo = optCompletionInfo,
      submitter = submitter,
      recordTime = recordTime,
      contractId = contractId,
      contractStakeholders = contractStakeholders.toSet,
      sourceDomainId = sourceDomainId,
      targetDomainId = targetDomainId,
      transferInExclusivity = transferInExclusivity,
    )
  }
}

final case class SerializableTransferredIn(transferIn: LedgerSyncEvent.TransferredIn) {
  def toProtoV0: v0.TransferredIn = {
    val LedgerSyncEvent.TransferredIn(
      updateId,
      optCompletionInfo,
      submitter,
      recordTime,
      ledgerCreateTime,
      createNode,
      creatingTransactionId,
      contractMetadata,
      transferOutId,
      targetDomain,
      createTransactionAccepted,
    ) = transferIn
    val contractMetadataP = contractMetadata.toByteString
    val createNodeByteString = DamlLfSerializers
      .serializeCreateNode(createNode)
      .valueOr(err =>
        throw new DbSerializationException(
          s"Failed to serialize versioned CreateNode: ${err.errorMessage}"
        )
      )
    v0.TransferredIn(
      updateId = updateId,
      completionInfo = optCompletionInfo.map(SerializableCompletionInfo(_).toProtoV0),
      submitter = submitter,
      recordTime = Some(SerializableLfTimestamp(recordTime).toProtoV0),
      ledgerCreateTime = Some(SerializableLfTimestamp(ledgerCreateTime).toProtoV0),
      contractMetadata = contractMetadataP,
      createNode = createNodeByteString,
      creatingTransactionId = creatingTransactionId,
      transferOutId = Some(transferOutId.toProtoV0),
      targetDomain = targetDomain.toProtoPrimitive,
      createTransactionAccepted = createTransactionAccepted,
    )

  }
}

object SerializableTransferredIn {
  def fromProtoV0(transferInP: v0.TransferredIn): ParsingResult[LedgerSyncEvent.TransferredIn] = {
    val v0.TransferredIn(
      updateIdP,
      optCompletionInfoP,
      submitterP,
      recordTimeP,
      ledgerCreateTimeP,
      createNodeP,
      creatingTransactionIdP,
      contractMetadataP,
      transferOutIdP,
      targetDomainIdP,
      createTransactionAcceptedP,
    ) = transferInP

    for {
      updateId <- ProtoConverter.parseLedgerTransactionId(updateIdP)
      optCompletionInfo <- optCompletionInfoP.traverse(SerializableCompletionInfo.fromProtoV0)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      recordTime <- required("record_time", recordTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      ledgerCreateTime <- required("ledger_create_time", ledgerCreateTimeP).flatMap(
        SerializableLfTimestamp.fromProtoPrimitive
      )
      contractMetadata = LfBytes.fromByteString(contractMetadataP)
      transferOutId <- ProtoConverter.parseRequired(
        TransferId.fromProtoV0,
        "transfer_id",
        transferOutIdP,
      )
      createNode <- ProtoConverter.parse(
        TransactionOuterClass.Node.parseFrom,
        (proto: TransactionOuterClass.Node) =>
          DamlLfSerializers.deserializeCreateNode(proto).leftMap { err =>
            ValueConversionError("create_node", err.errorMessage)
          },
        createNodeP,
      )
      creatingTransactionId <- ProtoConverter.parseLedgerTransactionId(creatingTransactionIdP)
      targetDomainId <- DomainId.fromProtoPrimitive(targetDomainIdP, "target_domain")
    } yield LedgerSyncEvent.TransferredIn(
      updateId = updateId,
      optCompletionInfo = optCompletionInfo,
      submitter = submitter,
      recordTime = recordTime,
      ledgerCreateTime = ledgerCreateTime,
      createNode = createNode,
      creatingTransactionId = creatingTransactionId,
      contractMetadata = contractMetadata,
      transferOutId = transferOutId,
      targetDomain = targetDomainId,
      createTransactionAccepted = createTransactionAcceptedP,
    )
  }
}
