// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.messages.TransferOutMediatorMessage
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfTemplateId,
  RootHash,
  SourceDomainId,
  TargetDomainId,
  ViewHash,
  v0,
  v1,
  v2,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  HasVersionedToByteString,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerCommandId,
  LedgerParticipantId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
  TransferCounter,
}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-out requests */
final case class TransferOutViewTree private (
    commonData: MerkleTree[TransferOutCommonData],
    view: MerkleTree[TransferOutView],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TransferOutViewTree.type
    ],
    hashOps: HashOps,
) extends GenTransferViewTree[
      TransferOutCommonData,
      TransferOutView,
      TransferOutViewTree,
      TransferOutMediatorMessage,
    ](commonData, view)(hashOps)
    with HasRepresentativeProtocolVersion {

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferOutViewTree] =
    TransferOutViewTree(
      commonData.doBlind(optimizedBlindingPolicy),
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps)

  protected[this] override def createMediatorMessage(
      blindedTree: TransferOutViewTree
  ): TransferOutMediatorMessage =
    TransferOutMediatorMessage(blindedTree)

  override def pretty: Pretty[TransferOutViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: TransferOutViewTree.type =
    TransferOutViewTree
}

object TransferOutViewTree
    extends HasProtocolVersionedWithContextCompanion[TransferOutViewTree, HashOps] {

  override val name: String = "TransferOutViewTree"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferViewTree)(
      supportedProtoVersion(_)((hashOps, proto) => fromProtoV0(hashOps)(proto)),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferViewTree)(
      supportedProtoVersion(_)((hashOps, proto) => fromProtoV1(hashOps)(proto)),
      _.toProtoV1.toByteString,
    ),
  )

  def apply(
      commonData: MerkleTree[TransferOutCommonData],
      view: MerkleTree[TransferOutView],
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): TransferOutViewTree =
    TransferOutViewTree(commonData, view)(
      TransferOutViewTree.protocolVersionRepresentativeFor(protocolVersion),
      hashOps,
    )

  def fromProtoV0(hashOps: HashOps)(
      transferOutViewTreeP: v0.TransferViewTree
  ): ParsingResult[TransferOutViewTree] =
    GenTransferViewTree.fromProtoV0(
      TransferOutCommonData.fromByteString(hashOps),
      TransferOutView.fromByteString(hashOps),
    )((commonData, view) =>
      TransferOutViewTree(commonData, view)(
        protocolVersionRepresentativeFor(ProtoVersion(0)),
        hashOps,
      )
    )(transferOutViewTreeP)

  def fromProtoV1(hashOps: HashOps)(
      transferOutViewTreeP: v1.TransferViewTree
  ): ParsingResult[TransferOutViewTree] =
    GenTransferViewTree.fromProtoV1(
      TransferOutCommonData.fromByteString(hashOps),
      TransferOutView.fromByteString(hashOps),
    )((commonData, view) =>
      TransferOutViewTree(commonData, view)(
        protocolVersionRepresentativeFor(ProtoVersion(1)),
        hashOps,
      )
    )(transferOutViewTreeP)
}

/** Aggregates the data of a transfer-out request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param sourceDomain The domain to which the transfer-out request is sent
  * @param sourceMediator The mediator that coordinates the transfer-out request on the source domain
  * @param stakeholders The stakeholders of the contract to be transferred
  * @param adminParties The admin parties of transferring transfer-out participants
  * @param uuid The request UUID of the transfer-out
  * @param transferCounter The [[com.digitalasset.canton.TransferCounter]] of the contract
  */
final case class TransferOutCommonData private (
    override val salt: Salt,
    sourceDomain: SourceDomainId,
    sourceMediator: MediatorId,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    uuid: UUID,
    transferCounter: TransferCounter,
)(
    hashOps: HashOps,
    val protocolVersion: SourceProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferOutCommonData](hashOps)
    with HasProtocolVersionedWrapper[TransferOutCommonData]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: TransferOutCommonData.type =
    TransferOutCommonData

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferOutCommonData.type] =
    TransferOutCommonData.protocolVersionRepresentativeFor(protocolVersion.v)

  protected def toProtoV0: v0.TransferOutCommonData =
    v0.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      originDomain = sourceDomain.toProtoPrimitive,
      originMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    )

  protected def toProtoV1: v1.TransferOutCommonData =
    v1.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      sourceDomain = sourceDomain.toProtoPrimitive,
      sourceMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      sourceProtocolVersion = protocolVersion.v.toProtoPrimitive,
    )

  protected def toProtoV2: v2.TransferOutCommonData =
    v2.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      sourceDomain = sourceDomain.toProtoPrimitive,
      sourceMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      sourceProtocolVersion = protocolVersion.v.toProtoPrimitive,
      transferCounter = transferCounter.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutCommonData

  def confirmingParties: Set[Informee] = (stakeholders ++ adminParties).map(ConfirmingParty(_, 1))

  override def pretty: Pretty[TransferOutCommonData] = prettyOfClass(
    param("source domain", _.sourceDomain),
    param("source mediator", _.sourceMediator),
    param("stakeholders", _.stakeholders),
    param("admin parties", _.adminParties),
    param("uuid", _.uuid),
    param("salt", _.salt),
    param("transfer counter", _.transferCounter),
  )
}

object TransferOutCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransferOutCommonData,
      HashOps,
    ] {
  override val name: String = "TransferOutCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferOutCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferOutCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.TransferOutCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      sourceDomain: SourceDomainId,
      sourceMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
      transferCounter: TransferCounter,
      protocolVersion: SourceProtocolVersion,
  ): TransferOutCommonData =
    TransferOutCommonData(
      salt,
      sourceDomain,
      sourceMediator,
      stakeholders,
      adminParties,
      uuid,
      transferCounter,
    )(hashOps, protocolVersion, None)

  private[this] def fromProtoV0(hashOps: HashOps, transferOutCommonDataP: v0.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v0.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
    ) = transferOutCommonDataP
    for {
      commonData <- ParsedDataV0V1V2.fromProto(
        saltP,
        sourceDomainP,
        mediatorIdP,
        stakeholdersP,
        adminPartiesP,
        uuidP,
      )
    } yield TransferOutCommonData(
      commonData.salt,
      commonData.sourceDomain,
      commonData.sourceMediator,
      commonData.stakeholders,
      commonData.adminParties,
      commonData.uuid,
      TransferCounter.Genesis,
    )(
      hashOps,
      SourceProtocolVersion(
        protocolVersionRepresentativeFor(ProtoVersion(0)).representative
      ),
      Some(bytes),
    )
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferOutCommonDataP: v1.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v1.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
      protocolVersionP,
    ) = transferOutCommonDataP
    for {
      commonData <- ParsedDataV0V1V2.fromProto(
        saltP,
        sourceDomainP,
        mediatorIdP,
        stakeholdersP,
        adminPartiesP,
        uuidP,
      )
      protocolVersion = ProtocolVersion.fromProtoPrimitive(protocolVersionP)
    } yield TransferOutCommonData(
      commonData.salt,
      commonData.sourceDomain,
      commonData.sourceMediator,
      commonData.stakeholders,
      commonData.adminParties,
      commonData.uuid,
      TransferCounter.Genesis,
    )(hashOps, SourceProtocolVersion(protocolVersion), Some(bytes))
  }

  private[this] def fromProtoV2(hashOps: HashOps, transferOutCommonDataP: v2.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v2.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
      protocolVersionP,
      transferCounterP,
    ) = transferOutCommonDataP
    for {
      commonData <- ParsedDataV0V1V2.fromProto(
        saltP,
        sourceDomainP,
        mediatorIdP,
        stakeholdersP,
        adminPartiesP,
        uuidP,
      )
      protocolVersion = ProtocolVersion.fromProtoPrimitive(protocolVersionP)
      transferCounter = TransferCounter(transferCounterP)
    } yield TransferOutCommonData(
      commonData.salt,
      commonData.sourceDomain,
      commonData.sourceMediator,
      commonData.stakeholders,
      commonData.adminParties,
      commonData.uuid,
      transferCounter,
    )(hashOps, SourceProtocolVersion(protocolVersion), Some(bytes))
  }

  final case class ParsedDataV0V1V2(
      salt: Salt,
      sourceDomain: SourceDomainId,
      sourceMediator: MediatorId,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
  )
  private[this] object ParsedDataV0V1V2 {
    def fromProto(
        salt: Option[com.digitalasset.canton.crypto.v0.Salt],
        sourceDomain: String,
        mediatorId: String,
        stakeholders: Seq[String],
        adminParties: Seq[String],
        uuid: String,
    ): ParsingResult[ParsedDataV0V1V2] =
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", salt)
        sourceDomain <- DomainId.fromProtoPrimitive(sourceDomain, "source_domain")
        sourceMediator <- MediatorId.fromProtoPrimitive(mediatorId, "source_mediator")
        stakeholders <- stakeholders.traverse(ProtoConverter.parseLfPartyId)
        adminParties <- adminParties.traverse(ProtoConverter.parseLfPartyId)
        uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuid)
      } yield ParsedDataV0V1V2(
        salt,
        SourceDomainId(sourceDomain),
        sourceMediator,
        stakeholders.toSet,
        adminParties.toSet,
        uuid,
      )
  }

}

/** Aggregates the data of a transfer-out request that is only sent to the involved participants
  *
  * @param salt The salt to blind the Merkle hash
  * @param submitter The submitter of the transfer-out request
  * @param contractId The contract ID to be transferred
  * @param templateId The template ID of the contract to be transferred
  * @param targetDomain The target domain to which the contract is to be transferred
  * @param targetTimeProof The sequenced event from the target domain
  *                        whose timestamp defines the baseline for measuring time periods on the target domain
  */
final case class TransferOutView private (
    override val salt: Salt,
    submitterMetadata: TransferSubmitterMetadata,
    contractId: LfContractId,
    templateId: LfTemplateId,
    targetDomain: TargetDomainId,
    targetTimeProof: TimeProof,
    targetProtocolVersion: TargetProtocolVersion,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutView.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferOutView](hashOps)
    with HasProtocolVersionedWrapper[TransferOutView]
    with ProtocolVersionedMemoizedEvidence {

  val submitter: LfPartyId = submitterMetadata.submitter
  val submittingParticipant: LedgerParticipantId = submitterMetadata.submittingParticipant
  val applicationId: LedgerApplicationId = submitterMetadata.applicationId
  val submissionId: Option[LedgerSubmissionId] = submitterMetadata.submissionId
  val commandId: LedgerCommandId = submitterMetadata.commandId
  val workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutView

  @transient override protected lazy val companionObj: TransferOutView.type = TransferOutView

  protected def toProtoV0: v0.TransferOutView =
    v0.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
    )

  protected def toProtoV1: v1.TransferOutView =
    v1.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
    )

  protected def toProtoV2: v2.TransferOutView =
    v2.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
      submittingParticipant = submittingParticipant,
      applicationId = applicationId,
      submissionId = submissionId.getOrElse(""),
      workflowId = workflowId.getOrElse(""),
      commandId = commandId,
      templateId = templateId.toString,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferOutView] = prettyOfClass(
    param("submitter", _.submitter),
    param("contract id", _.contractId),
    param("template id", _.templateId),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("submitting participant", _.submittingParticipant),
    param("application id", _.applicationId),
    paramIfDefined("submission id", _.submissionId),
    paramIfDefined("workflow id", _.workflowId),
    param("salt", _.salt),
  )
}

object TransferOutView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferOutView, HashOps] {
  override val name: String = "TransferOutView"
  private val noParticipantId = LedgerParticipantId.assertFromString("no-participant-id")
  private val noApplicationId = LedgerApplicationId.assertFromString("no-application-id")
  private val unknownTemplateId = LfTemplateId.assertFromString("unknown:template:id")
  private val noCommandId = LedgerCommandId.assertFromString("no-command-id")

  private[TransferOutView] final case class ParsedDataV0V1V2(
      salt: Salt,
      submitter: LfPartyId,
      contractId: LfContractId,
      targetDomain: TargetDomainId,
      targetDomainPV: TargetProtocolVersion,
      targetTimeProof: TimeProof,
  )
  private[TransferOutView] object ParsedDataV0V1V2 {
    def fromProto(
        hashOps: HashOps,
        saltP: Option[com.digitalasset.canton.crypto.v0.Salt],
        submitterP: String,
        contractIdP: String,
        targetDomainP: String,
        targetTimeProofP: Option[com.digitalasset.canton.time.v0.TimeProof],
        targetProtocolVersion: ProtocolVersion,
    ): ParsingResult[ParsedDataV0V1V2] = {
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
        submitter <- ProtoConverter.parseLfPartyId(submitterP)
        contractId <- ProtoConverter.parseLfContractId(contractIdP)
        targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")

        targetTimeProof <- ProtoConverter
          .required("targetTimeProof", targetTimeProofP)
          .flatMap(TimeProof.fromProtoV0(targetProtocolVersion, hashOps))
      } yield ParsedDataV0V1V2(
        salt,
        submitter,
        contractId,
        TargetDomainId(targetDomain),
        TargetProtocolVersion(targetProtocolVersion), // TODO(#12626)
        targetTimeProof,
      )
    }
  }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      submitterMetadata: TransferSubmitterMetadata,
      contractId: LfContractId,
      templateId: LfTemplateId,
      targetDomain: TargetDomainId,
      targetTimeProof: TimeProof,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
  ): TransferOutView =
    TransferOutView(
      salt,
      submitterMetadata,
      contractId,
      templateId,
      targetDomain,
      targetTimeProof,
      targetProtocolVersion,
    )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.v), None)

  private[this] def fromProtoV0(hashOps: HashOps, transferOutViewP: v0.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutView] = {
    val v0.TransferOutView(saltP, submitterP, contractIdP, targetDomainP, targetTimeProofP) =
      transferOutViewP
    for {
      commonData <- ParsedDataV0V1V2.fromProto(
        hashOps,
        saltP,
        submitterP,
        contractIdP,
        targetDomainP,
        targetTimeProofP,
        ProtocolVersion.v3,
      )
    } yield TransferOutView(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        noApplicationId,
        noParticipantId,
        noCommandId,
        submissionId = None,
        workflowId = None,
      ),
      commonData.contractId,
      unknownTemplateId,
      commonData.targetDomain,
      commonData.targetTimeProof,
      commonData.targetDomainPV,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)), // TODO(#12626)
      Some(bytes),
    )
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferOutViewP: v1.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutView] = {
    val v1.TransferOutView(
      saltP,
      submitterP,
      contractIdP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
    ) = transferOutViewP

    for {
      commonData <- ParsedDataV0V1V2.fromProto(
        hashOps,
        saltP,
        submitterP,
        contractIdP,
        targetDomainP,
        targetTimeProofP,
        ProtocolVersion.fromProtoPrimitive(targetProtocolVersionP),
      )
    } yield TransferOutView(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        noApplicationId,
        noParticipantId,
        noCommandId,
        submissionId = None,
        workflowId = None,
      ),
      commonData.contractId,
      unknownTemplateId,
      commonData.targetDomain,
      commonData.targetTimeProof,
      commonData.targetDomainPV,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(1)), // TODO(#12626)
      Some(bytes),
    )
  }

  private[this] def fromProtoV2(hashOps: HashOps, transferOutViewP: v2.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutView] = {
    val v2.TransferOutView(
      saltP,
      submitterP,
      contractIdP,
      templateIdP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
      submittingParticipantP,
      applicationIdP,
      submissionIdP,
      worfklowIdP,
      commandIdP,
    ) = transferOutViewP

    for {
      commonData <- ParsedDataV0V1V2.fromProto(
        hashOps,
        saltP,
        submitterP,
        contractIdP,
        targetDomainP,
        targetTimeProofP,
        ProtocolVersion.fromProtoPrimitive(targetProtocolVersionP),
      )
      submittingParticipantId <-
        ProtoConverter.parseLfParticipantId(submittingParticipantP)
      applicationId <- ProtoConverter.parseLFApplicationId(applicationIdP)
      submissionId <- ProtoConverter.parseLFSubmissionIdO(submissionIdP)
      workflowId <- ProtoConverter.parseLFWorkflowIdO(worfklowIdP)
      commandId <- ProtoConverter.parseCommandId(commandIdP)
      templateId <- ProtoConverter.parseTemplateId(templateIdP)
    } yield TransferOutView(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        applicationId,
        submittingParticipantId,
        commandId,
        submissionId,
        workflowId,
      ),
      commonData.contractId,
      templateId,
      commonData.targetDomain,
      commonData.targetTimeProof,
      commonData.targetDomainPV,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(2)), // TODO(#12626)
      Some(bytes),
    )

  }
}

/** A fully unblinded [[TransferOutViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
final case class FullTransferOutTree(tree: TransferOutViewTree)
    extends TransferViewTree
    with HasVersionedToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "A transfer-out request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  override val viewPosition: ViewPosition =
    ViewPosition.root // Use a dummy value, as there is only one view.

  def submitter: LfPartyId = view.submitter

  def submitterMetadata: TransferSubmitterMetadata = view.submitterMetadata
  def workflowId: Option[LfWorkflowId] = view.workflowId

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def adminParties: Set[LfPartyId] = commonData.adminParties

  def contractId: LfContractId = view.contractId

  def sourceDomain: SourceDomainId = commonData.sourceDomain

  def targetDomain: TargetDomainId = view.targetDomain

  def targetTimeProof: TimeProof = view.targetTimeProof

  def mediatorMessage: TransferOutMediatorMessage = tree.mediatorMessage

  override def domainId: DomainId = sourceDomain.unwrap

  override def mediatorId: MediatorId = commonData.sourceMediator

  override def informees: Set[Informee] = commonData.confirmingParties

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[FullTransferOutTree] = prettyOfClass(unnamedParam(_.tree))

  override def toByteString(version: ProtocolVersion): ByteString = tree.toByteString(version)
}

object FullTransferOutTree {
  def fromByteString(
      crypto: CryptoPureApi
  )(bytes: ByteString): ParsingResult[FullTransferOutTree] =
    for {
      tree <- TransferOutViewTree.fromByteString(crypto)(bytes)
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-out request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferOutTree(tree)
}
