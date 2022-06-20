// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  HasProtocolVersionedWrapperCompanion,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

case class SubmissionRequest private (
    sender: Member,
    messageId: MessageId,
    isRequest: Boolean,
    batch: Batch[ClosedEnvelope],
    maxSequencingTime: CantonTimestamp,
    timestampOfSigningKey: Option[CantonTimestamp],
)(val representativeProtocolVersion: RepresentativeProtocolVersion[SubmissionRequest])
    extends HasProtocolVersionedWrapper[SubmissionRequest]
    with HasProtoV0[v0.SubmissionRequest] {

  override val companionObj: HasProtocolVersionedWrapperCompanion[SubmissionRequest] =
    SubmissionRequest

  override def toProtoV0: v0.SubmissionRequest = v0.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    isRequest = isRequest,
    batch = Some(batch.toProtoV0),
    maxSequencingTime = Some(maxSequencingTime.toProtoPrimitive),
    timestampOfSigningKey = timestampOfSigningKey.map(_.toProtoPrimitive),
  )

  def copy(
      sender: Member = sender,
      messageId: MessageId = messageId,
      isRequest: Boolean = isRequest,
      batch: Batch[ClosedEnvelope] = batch,
      maxSequencingTime: CantonTimestamp = maxSequencingTime,
      timestampOfSigningKey: Option[CantonTimestamp] = timestampOfSigningKey,
  ) = SubmissionRequest(
    sender,
    messageId,
    isRequest,
    batch,
    maxSequencingTime,
    timestampOfSigningKey,
  )(representativeProtocolVersion)

  def isConfirmationRequest(mediator: Member): Boolean =
    batch.envelopes.exists(_.recipients.allRecipients == Set(mediator)) && batch.envelopes.exists(
      e => e.recipients.allRecipients != Set(mediator)
    )

  def isConfirmationResponse(mediator: Member): Boolean =
    batch.envelopes.nonEmpty && batch.envelopes.forall(_.recipients.allRecipients == Set(mediator))
}

object SubmissionRequest extends HasProtocolVersionedCompanion[SubmissionRequest] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersion(v0.SubmissionRequest)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override protected def name: String = "submission request"

  def apply(
      sender: Member,
      messageId: MessageId,
      isRequest: Boolean,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      timestampOfSigningKey: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SubmissionRequest =
    SubmissionRequest(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
    )(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV0(
      requestP: v0.SubmissionRequest
  ): ParsingResult[SubmissionRequest] = {
    val v0.SubmissionRequest(
      senderP,
      messageIdP,
      isRequest,
      batchP,
      maxSequencingTimeP,
      timestampOfSigningKey,
    ) =
      requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- ProtoConverter
        .required("SubmissionRequest.maxSequencingTime", maxSequencingTimeP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      batch <- ProtoConverter
        .required("SubmissionRequest.batch", batchP)
        .flatMap(Batch.fromProtoV0(ClosedEnvelope.fromProtoV0))
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield new SubmissionRequest(sender, messageId, isRequest, batch, maxSequencingTime, ts)(
      protocolVersionRepresentativeFor(ProtobufVersion(0))
    )
  }
}
