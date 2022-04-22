// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.syntax.either._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult.InvalidTransferOutResult
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.messages.TransferDomainId.TransferDomainIdCast
import com.digitalasset.canton.protocol.{RequestId, TransferId, v0}
import com.digitalasset.canton.sequencing.RawProtocolEvent
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedVersionedMessageCompanion,
  HasProtoV0,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.topology.DomainId
import com.google.protobuf.ByteString

/** Mediator result for a transfer-out request
  *
  * @param requestId timestamp of the corresponding [[TransferOutRequest]] on the origin domain
  */
case class TransferResult[+Domain <: TransferDomainId] private (
    override val requestId: RequestId,
    informees: Set[LfPartyId],
    domain: Domain, // For transfer-out, this is the origin domain. For transfer-in, this is the target domain.
    override val verdict: Verdict,
)(override val deserializedFrom: Option[ByteString])
    extends RegularMediatorResult
    with HasVersionedWrapper[VersionedMessage[TransferResult[Domain]]]
    with HasProtoV0[v0.TransferResult]
    with NoCopy
    with PrettyPrinting {

  override def domainId: DomainId = domain.unwrap

  override def viewType: ViewType = domain.toViewType

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  def isTransferIn: Boolean =
    domain.isInstanceOf[TransferInDomainId]

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransferResult =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.TransferResult(getCryptographicEvidence)

  override def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[TransferResult[Domain]] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.TransferResult = {
    val domainP = (domain: @unchecked) match {
      case TransferOutDomainId(domainId) =>
        v0.TransferResult.Domain.OriginDomain(domainId.toProtoPrimitive)
      case TransferInDomainId(domainId) =>
        v0.TransferResult.Domain.TargetDomain(domainId.toProtoPrimitive)
    }
    v0.TransferResult(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      domain = domainP,
      informees = informees.toSeq,
      verdict = Some(verdict.toProtoV0),
    )
  }

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override def hashPurpose: HashPurpose = HashPurpose.TransferResultSignature

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[TransferResult] def traverse[F[_], Domain2 <: TransferDomainId](
      f: Domain => F[Domain2]
  )(implicit F: Functor[F]): F[TransferResult[Domain2]] =
    F.map(f(domain)) { newDomain =>
      if (newDomain eq domain) this.asInstanceOf[TransferResult[Domain2]]
      else if (newDomain == domain)
        new TransferResult(requestId, informees, newDomain, verdict)(deserializedFrom)
      else TransferResult.create(requestId, informees, newDomain, verdict)
    }

  override def pretty: Pretty[TransferResult[_ <: TransferDomainId]] =
    prettyOfClass(
      param("requestId", _.requestId.unwrap),
      param("verdict", _.verdict),
      param("informees", _.informees),
      param("domain", _.domain),
    )
}

object TransferResult
    extends HasMemoizedVersionedMessageCompanion[
      TransferResult[TransferDomainId],
    ] {
  override val name: String = "TransferResult"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersionMemoized(v0.TransferResult)(fromProtoV0)
  )

  private def apply[Domain <: TransferDomainId](
      requestId: RequestId,
      informees: Set[LfPartyId],
      domain: Domain,
      verdict: Verdict,
  )(
      deserializedFrom: Option[ByteString]
  ): TransferResult[Domain] = throw new UnsupportedOperationException(
    "Use the create method instead"
  )

  def create[Domain <: TransferDomainId](
      requestId: RequestId,
      informees: Set[LfPartyId],
      domain: Domain,
      verdict: Verdict,
  ) =
    new TransferResult[Domain](requestId, informees, domain, verdict)(None)

  private def fromProtoV0(transferResultP: v0.TransferResult)(
      bytes: ByteString
  ): ParsingResult[TransferResult[TransferDomainId]] =
    transferResultP match {
      case v0.TransferResult(maybeRequestIdP, domainP, informeesP, maybeVerdictP) =>
        import v0.TransferResult.Domain
        for {
          requestId <- ProtoConverter
            .required("TransferOutResult.requestId", maybeRequestIdP)
            .flatMap(CantonTimestamp.fromProtoPrimitive)
            .map(RequestId(_))
          domain <- domainP match {
            case Domain.OriginDomain(originDomain) =>
              DomainId
                .fromProtoPrimitive(originDomain, "TransferResult.originDomain")
                .map(TransferOutDomainId(_))
            case Domain.TargetDomain(targetDomain) =>
              DomainId
                .fromProtoPrimitive(targetDomain, "TransferResult.targetDomain")
                .map(TransferInDomainId(_))
            case Domain.Empty => Left(FieldNotSet("TransferResponse.domain"))
          }
          informees <- informeesP.traverse(ProtoConverter.parseLfPartyId)
          verdict <- ProtoConverter
            .required("TransferResult.verdict", maybeVerdictP)
            .flatMap(Verdict.fromProtoV0)
        } yield new TransferResult(requestId, informees.toSet, domain, verdict)(Some(bytes))
    }

  implicit def transferResultCast[Kind <: TransferDomainId](implicit
      cast: TransferDomainIdCast[Kind]
  ): SignedMessageContentCast[TransferResult[Kind]] = {
    case result: TransferResult[TransferDomainId] => result.traverse(cast.toKind)
    case _ => None
  }
}

case class DeliveredTransferOutResult(result: SignedContent[Deliver[DefaultOpenEnvelope]])
    extends PrettyPrinting {

  val unwrap: TransferOutResult = result.content match {
    case Deliver(_, _, _, _, Batch(envelopes)) =>
      val transferOutResults =
        envelopes.mapFilter(ProtocolMessage.select[SignedProtocolMessage[TransferOutResult]])
      val size = transferOutResults.size
      if (size != 1)
        throw InvalidTransferOutResult(
          result,
          s"The deliver event must contain exactly one transfer-out result, but found $size.",
        )
      transferOutResults(0).protocolMessage.message
  }

  if (unwrap.verdict != Verdict.Approve)
    throw InvalidTransferOutResult(result, "The transfer-out result must be approving.")

  def transferId: TransferId = TransferId(unwrap.domainId, unwrap.requestId.unwrap)

  override def pretty: Pretty[DeliveredTransferOutResult] = prettyOfParam(_.unwrap)
}

object DeliveredTransferOutResult {

  case class InvalidTransferOutResult(
      transferOutResult: SignedContent[RawProtocolEvent],
      message: String,
  ) extends RuntimeException(s"$message: $transferOutResult")

  def create(
      result: SignedContent[RawProtocolEvent]
  ): Either[InvalidTransferOutResult, DeliveredTransferOutResult] =
    for {
      castToDeliver <- result
        .traverse(Deliver.fromSequencedEvent)
        .toRight(
          InvalidTransferOutResult(result, "Only a Deliver event contains a transfer-out result.")
        )
      deliveredTransferOutResult <- Either.catchOnly[InvalidTransferOutResult] {
        DeliveredTransferOutResult(castToDeliver)
      }
    } yield deliveredTransferOutResult
}
