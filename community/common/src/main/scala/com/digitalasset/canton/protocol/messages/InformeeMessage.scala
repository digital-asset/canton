// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{FullInformeeTree, Informee, ViewType}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{ConfirmationPolicy, RequestId, RootHash, ViewHash, v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasProtocolVersionedWithContextCompanion,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

import java.util.UUID

/** The informee message to be sent to the mediator.
  */
// This class is a reference example of serialization best practices.
// It is a simple example for getting started with serialization.
// Please consult the team if you intend to change the design of serialization.
case class InformeeMessage(fullInformeeTree: FullInformeeTree)
    extends MediatorRequest
    // By default, we use ProtoBuf for serialization.
    // Serializable classes that have a corresponding Protobuf message should inherit from this trait to inherit common code and naming conventions.
    // If the corresponding Protobuf message of a class has multiple versions (e.g. `v0.InformeeMessage` and `v1.InformeeMessage`),
    // it should implement traits for each (e.g. InformeeMessage should implement HasProtoV0 and HasProtoV1)
    with HasProtoV0[v0.InformeeMessage]
    with ProtocolMessageV0
    with ProtocolMessageV1 {

  val representativeProtocolVersion: RepresentativeProtocolVersion[InformeeMessage] =
    InformeeMessage.protocolVersionRepresentativeFor(fullInformeeTree.protocolVersion)

  override def requestUuid: UUID = fullInformeeTree.transactionUuid

  override def domainId: DomainId = fullInformeeTree.domainId

  override def mediatorId: MediatorId = fullInformeeTree.mediatorId

  override def informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] =
    fullInformeeTree.informeesAndThresholdByView

  override def createMediatorResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): TransactionResultMessage =
    TransactionResultMessage(
      requestId,
      verdict,
      fullInformeeTree.informeeTreeUnblindedFor(recipientParties),
      representativeProtocolVersion.unwrap,
    )

  // Implementing a `toProto<version>` method allows us to compose serializable classes.
  // You should define the toProtoV0 method on the serializable class, because then it is easiest to find and use.
  // (Conversely, you should not define a separate proto converter class.)
  override def toProtoV0: v0.InformeeMessage =
    // The proto generated version of InformeeMessage is referenced with a package prefix (preferably the version of the corresponding
    // Protobuf package, e.g., "v0") so that it can easily be distinguished from the hand written version of the
    // InformeeMessage class and other versions of the protobuf message InformeeMessage.
    // Try to avoid using renaming imports, as they are more difficult to maintain.
    //
    // To go a step further, we could even give the proto class a different name (e.g. InformeeMessageP),
    // but we have not yet agreed on a naming convention.
    //
    // Unless in special cases, you shouldn't embed an `UntypedVersionedMessage` wrapper inside a Protobuf message but should explicitly
    // indicate the version of the nested Protobuf message via calling `toProto<version>
    v0.InformeeMessage(fullInformeeTree = Some(fullInformeeTree.toProtoV0))

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.InformeeMessage(toProtoV0))

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(v1.EnvelopeContent.SomeEnvelopeContent.InformeeMessage(toProtoV0))

  override def confirmationPolicy: ConfirmationPolicy = fullInformeeTree.confirmationPolicy

  override def rootHash: Option[RootHash] = Some(fullInformeeTree.transactionId.toRootHash)

  override def viewType: ViewType = ViewType.TransactionViewType
}

object InformeeMessage extends HasProtocolVersionedWithContextCompanion[InformeeMessage, HashOps] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.InformeeMessage)((hashOps, proto) => fromProtoV0(hashOps)(proto)),
      _.toProtoV0.toByteString,
    )
  )

  // The inverse of "toProto<version>".
  //
  // On error, it returns `Left(...)` as callers cannot predict whether the conversion would succeed.
  // So the caller is forced to handle failing conversion.
  // Conversely, the method absolutely must not throw an exception, because this will likely kill the calling thread.
  // So it would be a DOS vulnerability.
  //
  // There is no agreed convention on which type to use for errors. In this class it is "ProtoDeserializationError",
  // but other classes use something else (e.g. "String").
  // In the end, it is most important that the errors are informative and this can be achieved in different ways.
  def fromProtoV0(
      hashOps: HashOps
  )(informeeMessageP: v0.InformeeMessage): ParsingResult[InformeeMessage] = {
    // Use pattern matching to access the fields of v0.InformeeMessage,
    // because this will break if a field is forgotten.
    val v0.InformeeMessage(maybeFullInformeeTreeP) = informeeMessageP
    for {
      // Keep in mind that all fields of a proto class are optional. So the existence must be checked explicitly.
      fullInformeeTreeP <- ProtoConverter.required(
        "InformeeMessage.informeeTree",
        maybeFullInformeeTreeP,
      )
      fullInformeeTree <- FullInformeeTree.fromProtoV0(hashOps, fullInformeeTreeP)
    } yield new InformeeMessage(fullInformeeTree)
  }

  // The inverse of "toByteString".
  //
  // Again, the method must return "Left(...)" (and not throw an exception), if deserialization fails.
  //
  // Optional method, feel free to omit it.
  override def fromByteString(
      hashOps: HashOps
  )(bytes: ByteString): ParsingResult[InformeeMessage] = {
    for {
      // Do not directly call "v0.InformeeMessage.parseFrom", as this may throw an exception.
      informeeMessageP <- ProtoConverter.protoParser(v0.InformeeMessage.parseFrom)(bytes)
      informeeMessage <- fromProtoV0(hashOps)(informeeMessageP)
    } yield informeeMessage
  }

  implicit val informeeMessageCast: ProtocolMessageContentCast[InformeeMessage] = {
    case im: InformeeMessage => Some(im)
    case _ => None
  }

  override protected def name: String = "InformeeMessage"
}
