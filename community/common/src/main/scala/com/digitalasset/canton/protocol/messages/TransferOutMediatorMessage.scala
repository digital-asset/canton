// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{Informee, TransferOutViewTree, ViewType}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasProtocolVersionedWithContextCompanion,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

import java.util.UUID

/** Message sent to the mediator as part of a transfer-out request
  *
  * @param tree The transfer-out view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException if the common data is blinded or the view is not blinded
  */
case class TransferOutMediatorMessage(
    tree: TransferOutViewTree
) extends MediatorRequest
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtoV0[v0.TransferOutMediatorMessage] {
  require(tree.commonData.isFullyUnblinded, "The transfer-out common data must be unblinded")
  require(tree.view.isBlinded, "The transfer-out view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutMediatorMessage] =
    TransferOutMediatorMessage.protocolVersionRepresentativeFor(
      commonData.representativeProtocolVersion.unwrap
    )

  override def domainId: DomainId = commonData.originDomain

  override def mediatorId: MediatorId = commonData.originMediator

  override def requestUuid: UUID = commonData.uuid

  override def informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] = {
    val confirmingParties = commonData.confirmingParties
    val threshold = NonNegativeInt.tryCreate(confirmingParties.size)
    Map(tree.viewHash -> ((confirmingParties, threshold)))
  }

  override def confirmationPolicy: ConfirmationPolicy = ConfirmationPolicy.Full

  override def createMediatorResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): MediatorResult with SignedProtocolMessageContent = {
    val informees = commonData.stakeholders ++ commonData.adminParties
    require(
      recipientParties.subsetOf(informees),
      "Recipient parties of the transfer-out result are neither stakeholders nor admin parties",
    )
    TransferResult.create(
      requestId,
      informees,
      TransferOutDomainId(domainId),
      verdict,
      representativeProtocolVersion.unwrap,
    )
  }

  override def toProtoV0: v0.TransferOutMediatorMessage =
    v0.TransferOutMediatorMessage(tree = Some(tree.toProtoV0))

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV0))

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.TransferOutMediatorMessage(toProtoV0))

  override def rootHash: Option[RootHash] = Some(tree.rootHash)

  override def viewType: ViewType = ViewType.TransferOutViewType
}

object TransferOutMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[TransferOutMediatorMessage, HashOps] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.TransferOutMediatorMessage)((hashOps, proto) =>
        fromProtoV0(hashOps)(proto)
      ),
      _.toProtoV0.toByteString,
    )
  )

  def fromProtoV0(hashOps: HashOps)(
      transferOutMediatorMessageP: v0.TransferOutMediatorMessage
  ): ParsingResult[TransferOutMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferOutMediatorMessage.tree", transferOutMediatorMessageP.tree)
        .flatMap(TransferOutViewTree.fromProtoV0(hashOps))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-out common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-out view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferOutMediatorMessage(tree)

  override protected def name: String = "TransferOutMediatorMessage"
}
