// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.crypto.{HashOps, HashPurpose}
import com.digitalasset.canton.data.InformeeTree
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, v0, v2, v3}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Transaction result message that the mediator sends to all stakeholders of a confirmation request with its verdict.
  * https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/transactions.html#phase-6-broadcast-of-result
  *
  * @param requestId        identifier of the confirmation request
  * @param verdict          the finalized verdict on the request
  * @param notificationTree the informee tree unblinded for the parties hosted by the receiving participant
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class TransactionResultMessage private (
    override val requestId: RequestId,
    override val verdict: Verdict,
    rootHash: RootHash,
    override val domainId: DomainId,
    notificationTree: Option[InformeeTree], // TODO(i12171): remove in 3.0
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TransactionResultMessage.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends RegularMediatorResult
    with HasProtocolVersionedWrapper[TransactionResultMessage]
    with PrettyPrinting {

  def copy(
      requestId: RequestId = this.requestId,
      verdict: Verdict = this.verdict,
      rootHash: RootHash = this.rootHash,
      domainId: DomainId = this.domainId,
      notificationTree: Option[InformeeTree] = this.notificationTree,
  ): TransactionResultMessage =
    TransactionResultMessage(requestId, verdict, rootHash, domainId, notificationTree)(
      representativeProtocolVersion,
      None,
    )

  override def viewType: TransactionViewType = TransactionViewType

  /** Computes the serialization of the object as a [[com.google.protobuf.ByteString]].
    *
    * Must meet the contract of [[com.digitalasset.canton.serialization.HasCryptographicEvidence.getCryptographicEvidence]]
    * except that when called several times, different [[com.google.protobuf.ByteString]]s may be returned.
    */
  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @transient override protected lazy val companionObj: TransactionResultMessage.type =
    TransactionResultMessage

  protected def toProtoV2: v2.TransactionResultMessage =
    v2.TransactionResultMessage(
      requestId = Some(requestId.toProtoPrimitive),
      verdict = Some(verdict.toProtoV2),
      rootHash = rootHash.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
    )

  protected def toProtoV3: v3.TransactionResultMessage =
    v3.TransactionResultMessage(
      requestId = Some(requestId.toProtoPrimitive),
      verdict = Some(verdict.toProtoV3),
      rootHash = rootHash.toProtoPrimitive,
      domainId = domainId.toProtoPrimitive,
    )

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransactionResult =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransactionResult(getCryptographicEvidence)

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v0.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.TransactionResult(
      getCryptographicEvidence
    )

  override def hashPurpose: HashPurpose = HashPurpose.TransactionResultSignature

  override def pretty: Pretty[TransactionResultMessage] =
    prettyOfClass(
      param("requestId", _.requestId.unwrap),
      param("verdict", _.verdict),
      param("rootHash", _.rootHash),
      param("domainId", _.domainId),
    )
}

object TransactionResultMessage
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransactionResultMessage,
      HashOps,
    ] {
  override val name: String = "TransactionResultMessage"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.v5)(v2.TransactionResultMessage)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
    ProtoVersion(3) -> VersionedProtoConverter(ProtocolVersion.v6)(v3.TransactionResultMessage)(
      supportedProtoVersionMemoized(_)(fromProtoV3),
      _.toProtoV3.toByteString,
    ),
  )

  def apply(
      requestId: RequestId,
      verdict: Verdict,
      rootHash: RootHash,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): TransactionResultMessage =
    TransactionResultMessage(requestId, verdict, rootHash, domainId, None)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  // TODO(i12171): Remove in 3.0 and drop context HashOps
  def apply(
      requestId: RequestId,
      verdict: Verdict,
      notificationTree: InformeeTree,
      protocolVersion: ProtocolVersion,
  ): TransactionResultMessage =
    TransactionResultMessage(
      requestId,
      verdict,
      notificationTree.tree.rootHash,
      notificationTree.domainId,
      Some(notificationTree),
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV2(_hashOps: HashOps, protoResultMessage: v2.TransactionResultMessage)(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] = {
    val v2.TransactionResultMessage(requestIdPO, verdictPO, rootHashP, domainIdP) =
      protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV2)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
    } yield TransactionResultMessage(requestId, transactionResult, rootHash, domainId, None)(
      protocolVersionRepresentativeFor(ProtoVersion(2)),
      Some(bytes),
    )
  }

  private def fromProtoV3(_hashOps: HashOps, protoResultMessage: v3.TransactionResultMessage)(
      bytes: ByteString
  ): ParsingResult[TransactionResultMessage] = {
    val v3.TransactionResultMessage(requestIdPO, verdictPO, rootHashP, domainIdP) =
      protoResultMessage
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      transactionResult <- ProtoConverter
        .required("verdict", verdictPO)
        .flatMap(Verdict.fromProtoV3)
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
    } yield TransactionResultMessage(requestId, transactionResult, rootHash, domainId, None)(
      protocolVersionRepresentativeFor(ProtoVersion(3)),
      Some(bytes),
    )
  }

  implicit val transactionResultMessageCast: SignedMessageContentCast[TransactionResultMessage] =
    SignedMessageContentCast.create[TransactionResultMessage]("TransactionResultMessage") {
      case m: TransactionResultMessage => Some(m)
      case _ => None
    }
}
