// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{Informee, TransferInViewTree, ViewType}
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

/** Message sent to the mediator as part of a transfer-in request
  *
  * @param tree The transfer-in view tree blinded for the mediator
  * @throws java.lang.IllegalArgumentException if the common data is blinded or the view is not blinded
  */
case class TransferInMediatorMessage(tree: TransferInViewTree)
    extends MediatorRequest
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtoV0[v0.TransferInMediatorMessage] {

  require(tree.commonData.isFullyUnblinded, "The transfer-in common data must be unblinded")
  require(tree.view.isBlinded, "The transfer-out view must be blinded")

  private[this] val commonData = tree.commonData.tryUnwrap

  // Align the protocol version with the common data's protocol version
  lazy val representativeProtocolVersion: RepresentativeProtocolVersion[TransferInMediatorMessage] =
    TransferInMediatorMessage.protocolVersionRepresentativeFor(
      commonData.representativeProtocolVersion.unwrap
    )

  override def domainId: DomainId = commonData.targetDomain

  override def mediatorId: MediatorId = commonData.targetMediator

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
    val informees = commonData.stakeholders
    require(
      recipientParties.subsetOf(informees),
      "Recipient parties of the transfer-in result must be stakeholders.",
    )
    TransferResult.create(
      requestId,
      informees,
      TransferInDomainId(domainId),
      verdict,
      representativeProtocolVersion.unwrap,
    )
  }

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      someEnvelopeContent =
        v0.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      someEnvelopeContent =
        v1.EnvelopeContent.SomeEnvelopeContent.TransferInMediatorMessage(toProtoV0)
    )

  override def toProtoV0: v0.TransferInMediatorMessage =
    v0.TransferInMediatorMessage(tree = Some(tree.toProtoV0))

  override def rootHash: Option[RootHash] = Some(tree.rootHash)

  override def viewType: ViewType = ViewType.TransferInViewType
}

object TransferInMediatorMessage
    extends HasProtocolVersionedWithContextCompanion[TransferInMediatorMessage, HashOps] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.TransferInMediatorMessage)((hashOps, proto) =>
        fromProtoV0(hashOps)(proto)
      ),
      _.toProtoV0.toByteString,
    )
  )

  def fromProtoV0(hashOps: HashOps)(
      transferInMediatorMessageP: v0.TransferInMediatorMessage
  ): ParsingResult[TransferInMediatorMessage] =
    for {
      tree <- ProtoConverter
        .required("TransferInMediatorMessage.tree", transferInMediatorMessageP.tree)
        .flatMap(TransferInViewTree.fromProtoV0(hashOps, _))
      _ <- EitherUtil.condUnitE(
        tree.commonData.isFullyUnblinded,
        OtherError(s"Transfer-in common data is blinded in request ${tree.rootHash}"),
      )
      _ <- EitherUtil.condUnitE(
        tree.view.isBlinded,
        OtherError(s"Transfer-in view data is not blinded in request ${tree.rootHash}"),
      )
    } yield TransferInMediatorMessage(tree)

  override protected def name: String = "TransferInMediatorMessage"
}
