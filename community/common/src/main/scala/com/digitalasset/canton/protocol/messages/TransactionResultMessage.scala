// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.bifunctor._
import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{CantonTimestamp, InformeeTree}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedVersionedMessageWithContextCompanion,
  HasProtoV0,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
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
)(val deserializedFrom: Option[ByteString])
    extends RegularMediatorResult
    with NoCopy
    with HasVersionedWrapper[VersionedMessage[TransactionResultMessage]]
    with HasProtoV0[v0.TransactionResultMessage]
    with PrettyPrinting {

  override def domainId: DomainId = notificationTree.domainId

  override def viewType: TransactionViewType = TransactionViewType

  def informees: Set[LfPartyId] = notificationTree.informeesByView.values.flatten.map(_.party).toSet

  /** Computes the serialization of the object as a [[com.google.protobuf.ByteString]].
    *
    * Must meet the contract of [[com.digitalasset.canton.serialization.HasCryptographicEvidence.getCryptographicEvidence]]
    * except that when called several times, different [[com.google.protobuf.ByteString]]s may be returned.
    */
  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[TransactionResultMessage] = VersionedMessage(toProtoV0.toByteString, 0)

  override protected def toProtoV0: v0.TransactionResultMessage =
    v0.TransactionResultMessage(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      verdict = Some(verdict.toProtoV0),
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
    extends HasMemoizedVersionedMessageWithContextCompanion[
      TransactionResultMessage,
      HashOps,
    ] {
  override val name: String = "TransactionResultMessage"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersionMemoized(v0.TransactionResultMessage) { case (hashOps, proto) =>
      fromProtoV0(proto, hashOps)
    }
  )

  private[this] def apply(requestId: RequestId, verdict: Verdict, notificationTree: InformeeTree)(
      deseializedFrom: Option[ByteString]
  ): TransactionResultMessage =
    throw new UnsupportedOperationException("Use the public apply method instead")

  def apply(
      requestId: RequestId,
      verdict: Verdict,
      notificationTree: InformeeTree,
  ): TransactionResultMessage =
    new TransactionResultMessage(requestId, verdict, notificationTree)(None)

  private def fromProtoV0(protoResultMessage: v0.TransactionResultMessage, hashOps: HashOps)(
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
    } yield new TransactionResultMessage(requestId, transactionResult, notificationTree)(
      Some(bytes)
    )

  implicit val transactionResultMessageCast: SignedMessageContentCast[TransactionResultMessage] = {
    case m: TransactionResultMessage => Some(m)
    case _ => None
  }
}
