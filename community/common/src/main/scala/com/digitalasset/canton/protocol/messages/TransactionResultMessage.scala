// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.bifunctor._
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{CantonTimestamp, InformeeTree}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version._
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.google.protobuf.ByteString

/** Transaction result message that the mediator sends to all stakeholders of a confirmation request with its verdict.
  * https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/transactions.html#phase-6-broadcast-of-result
  *
  * @param requestId        identifier of the confirmation request
  * @param verdict          the finalized verdict on the request
  * @param notificationTree the informee tree unblinded for the parties hosted by the receiving participant
  */
case class TransactionResultMessage private (
    override val requestId: RequestId,
    override val verdict: Verdict,
    notificationTree: InformeeTree,
)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[TransactionResultMessage],
    val deserializedFrom: Option[ByteString],
) extends RegularMediatorResult
    with HasProtocolVersionedWrapper[TransactionResultMessage]
    with PrettyPrinting {

  override def domainId: DomainId = notificationTree.domainId

  override def viewType: TransactionViewType = TransactionViewType

  def informees: Set[LfPartyId] = notificationTree.informeesByView.values.flatten.map(_.party).toSet

  /** Computes the serialization of the object as a [[com.google.protobuf.ByteString]].
    *
    * Must meet the contract of [[com.digitalasset.canton.serialization.HasCryptographicEvidence.getCryptographicEvidence]]
    * except that when called several times, different [[com.google.protobuf.ByteString]]s may be returned.
    */
  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def companionObj = TransactionResultMessage

  protected def toProtoV0: v0.TransactionResultMessage =
    v0.TransactionResultMessage(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      verdict = Some(verdict.toProtoV0),
      notificationTree = Some(notificationTree.toProtoV0),
    )

  protected def toProtoV1: v1.TransactionResultMessage =
    v1.TransactionResultMessage(
      requestId = Some(requestId.toProtoPrimitive),
      verdict = Some(verdict.toProtoV1),
      notificationTree = Some(notificationTree.toProtoV0),
    )

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransactionResult =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransactionResult(getCryptographicEvidence)

  override def hashPurpose: HashPurpose = HashPurpose.TransactionResultSignature

  override def pretty: Pretty[TransactionResultMessage] =
    prettyOfClass(
      param("requestId", _.requestId.unwrap),
      param("verdict", _.verdict),
      param("informees", _.informees),
      paramWithoutValue("notificationTree"),
    )
}

object TransactionResultMessage
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransactionResultMessage,
      HashOps,
    ] {
  override val name: String = "TransactionResultMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.TransactionResultMessage)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersionMemoized(v1.TransactionResultMessage)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def apply(
      requestId: RequestId,
      verdict: Verdict,
      notificationTree: InformeeTree,
      protocolVersion: ProtocolVersion,
  ): TransactionResultMessage =
    TransactionResultMessage(requestId, verdict, notificationTree)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV0(hashOps: HashOps, protoResultMessage: v0.TransactionResultMessage)(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] =
    for {
      requestId <- ProtoConverter
        .required("request_id", protoResultMessage.requestId)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      transactionResult <- ProtoConverter
        .required("verdict", protoResultMessage.verdict)
        .flatMap(Verdict.fromProtoV0)
      protoNotificationTree <- ProtoConverter
        .required("notification_tree", protoResultMessage.notificationTree)
        .leftWiden[ProtoDeserializationError]
      notificationTree <- InformeeTree.fromProtoV0(hashOps, protoNotificationTree)
    } yield TransactionResultMessage(requestId, transactionResult, notificationTree)(
      protocolVersionRepresentativeFor(ProtobufVersion(0)),
      Some(bytes),
    )

  private def fromProtoV1(hashOps: HashOps, protoResultMessage: v1.TransactionResultMessage)(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] = {
    val v1.TransactionResultMessage(requestIdPO, verdictPO, notificationTreePO) = protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV1)
      notificationTree <- ProtoConverter
        .required("notification_tree", notificationTreePO)
        .flatMap(InformeeTree.fromProtoV0(hashOps, _))
    } yield TransactionResultMessage(requestId, transactionResult, notificationTree)(
      protocolVersionRepresentativeFor(ProtobufVersion(1)),
      Some(bytes),
    )
  }

  implicit val transactionResultMessageCast: SignedMessageContentCast[TransactionResultMessage] = {
    case m: TransactionResultMessage => Some(m)
    case _ => None
  }
}
