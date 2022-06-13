// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.protocol.messages.MediatorResponse.InvalidMediatorResponse
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtoV0,
  HasProtocolVersionedWrapper,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedMessage,
}
import com.digitalasset.canton.LfPartyId
import com.google.protobuf.ByteString

/** Payload of a response sent to the mediator in reaction to a request.
  *
  * @param requestId The unique identifier of the request.
  * @param sender The identity of the sender.
  * @param viewHash The value of the field "view hash" in the corresponding view message.
  *                 May be empty if the [[localVerdict]] is [[com.digitalasset.canton.protocol.messages.LocalReject.Malformed]].
  * @param localVerdict The participant's verdict on the request's view.
  * @param rootHash The root hash of the request if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                 or [[com.digitalasset.canton.protocol.messages.LocalReject]]. [[scala.None$]] otherwise.
  * @param confirmingParties The non-empty set of confirming parties of the view hosted by the sender if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                          or [[com.digitalasset.canton.protocol.messages.LocalReject]]. Empty otherwise.
  * @param domainId The domain ID over which the request is sent.
  */
// This class is a reference example of serialization best practices, demonstrating:
// - handling of object invariants (i.e., the construction of an instance may fail with an exception)
// Please consult the team if you intend to change the design of serialization.
//
// The constructor and `fromProto...` methods are private to ensure that clients cannot create instances with an incorrect `deserializedFrom` field.
//
// Optional parameters are strongly discouraged, as each parameter needs to be consciously set in a production context.
case class MediatorResponse private (
    requestId: RequestId,
    sender: ParticipantId,
    viewHash: Option[ViewHash],
    localVerdict: LocalVerdict,
    rootHash: Option[RootHash],
    confirmingParties: Set[LfPartyId],
    override val domainId: DomainId,
)(
    val representativeProtocolVersion: RepresentativeProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[MediatorResponse]
    with HasProtoV0[v0.MediatorResponse]
    with HasDomainId
    with NoCopy {

  // If an object invariant is violated, throw an exception specific to the class.
  // Thus, the exception can be caught during deserialization and translated to a human readable error message.
  localVerdict match {
    case _: LocalReject.Malformed =>
      if (confirmingParties.nonEmpty)
        throw InvalidMediatorResponse("Confirming parties must be empty for verdict Malformed.")
    case LocalApprove | _: LocalReject =>
      if (confirmingParties.isEmpty)
        throw InvalidMediatorResponse(
          show"Confirming parties must not be empty for verdict $localVerdict"
        )
      if (rootHash.isEmpty)
        throw InvalidMediatorResponse(show"Root hash must not be empty for verdict $localVerdict")
      if (viewHash.isEmpty)
        throw InvalidMediatorResponse(show"View mash must not be empty for verdict $localVerdict")
  }

  protected override def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected def toProtoVersioned: VersionedMessage[MediatorResponse] =
    MediatorResponse.toProtoVersioned(this)

  override protected def toProtoV0: v0.MediatorResponse =
    v0.MediatorResponse(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      sender = sender.toProtoPrimitive,
      viewHash = viewHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      localVerdict = Some(localVerdict.toProtoV0),
      rootHash = rootHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      confirmingParties = confirmingParties.toList,
      domainId = domainId.toProtoPrimitive,
    )

  override def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.MediatorResponse =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.MediatorResponse(getCryptographicEvidence)

  override def hashPurpose: HashPurpose = HashPurpose.MediatorResponseSignature
}

object MediatorResponse extends HasMemoizedProtocolVersionedWrapperCompanion[MediatorResponse] {
  override val name: String = "MediatorResponse"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.MediatorResponse)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  case class InvalidMediatorResponse(msg: String) extends RuntimeException(msg)

  // Make the auto-generated apply method inaccessible to prevent clients from creating instances with an incorrect
  // `deserializedFrom` field.
  private[this] def apply(
      requestId: RequestId,
      sender: ParticipantId,
      viewHash: Option[ViewHash],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
  )(
      representativeProtocolVersion: RepresentativeProtocolVersion,
      deserializedFrom: Option[ByteString],
  ) =
    throw new UnsupportedOperationException("Use the public apply method")

  // Variant of "tryCreate" that returns Left(...) instead of throwing an exception.
  // This is for callers who *do not know up front* whether the parameters meet the object invariants.
  //
  // Optional method, feel free to omit it.
  def create(
      requestId: RequestId,
      sender: ParticipantId,
      viewHash: Option[ViewHash],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): Either[InvalidMediatorResponse, MediatorResponse] =
    Either.catchOnly[InvalidMediatorResponse](
      tryCreate(
        requestId,
        sender,
        viewHash,
        localVerdict,
        rootHash,
        confirmingParties,
        domainId,
        protocolVersion,
      )
    )

  // This method is tailored to the case that the caller already knows that the parameters meet the object invariants.
  // Consequently, the method throws an exception on invalid parameters.
  //
  // The "tryCreate" method has the following advantage over the auto-generated "apply" method:
  // - The deserializedFrom field cannot be set; so it cannot be set incorrectly.
  //
  // The method is called "tryCreate" instead of "apply" for two reasons:
  // - to emphasize that this method may throw an exception
  // - to not confuse the Idea compiler by overloading "apply".
  //   (This is not a problem with this particular class, but it has been a problem with other classes.)
  //
  // The "tryCreate" method is optional.
  // Feel free to omit "tryCreate", if the auto-generated "apply" method is good enough.
  def tryCreate(
      requestId: RequestId,
      sender: ParticipantId,
      viewHash: Option[ViewHash],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): MediatorResponse =
    new MediatorResponse(
      requestId,
      sender,
      viewHash,
      localVerdict,
      rootHash,
      confirmingParties,
      domainId,
    )(protocolVersionRepresentativeFor(protocolVersion), None)

  private def fromProtoV0(mediatorResponseP: v0.MediatorResponse)(
      bytes: ByteString
  ): ParsingResult[MediatorResponse] = {
    val v0.MediatorResponse(
      requestIdP,
      senderP,
      viewHashP,
      localVerdictP,
      rootHashP,
      confirmingPartiesP,
      domainIdP,
    ) =
      mediatorResponseP
    for {
      requestId <- ProtoConverter
        .required("MediatorResponse.request_id", requestIdP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      sender <- ParticipantId.fromProtoPrimitive(senderP, "MediatorResponse.sender")
      viewHash <- ViewHash.fromProtoPrimitiveOption(viewHashP)
      localVerdict <- ProtoConverter
        .required("MediatorResponse.local_verdict", localVerdictP)
        .flatMap(LocalVerdict.fromProtoV0)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      confirmingParties <- confirmingPartiesP.traverse(ProtoConverter.parseLfPartyId)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      response <- Either
        .catchOnly[InvalidMediatorResponse](
          new MediatorResponse(
            requestId,
            sender,
            viewHash,
            localVerdict,
            rootHashO,
            confirmingParties.toSet,
            domainId,
          )(
            supportedProtoVersions.protocolVersionRepresentativeFor(ProtobufVersion(0)),
            Some(bytes),
          )
        )
        .leftMap(err => InvariantViolation(err.toString))
    } yield response
  }

  implicit val mediatorResponseSignedMessageContentCast
      : SignedMessageContentCast[MediatorResponse] = {
    case response: MediatorResponse => Some(response)
    case _ => None
  }
}
