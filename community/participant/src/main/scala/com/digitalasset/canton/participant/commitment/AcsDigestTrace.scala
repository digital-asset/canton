// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ShowUtil
import com.digitalasset.canton.version.{
  HasVersionedJsonMessageCompanion,
  HasVersionedJsonMessageCompanionDbHelpers,
  HasVersionedJsonWrapper,
  ProtoVersion,
  ProtocolVersion,
  ReleaseProtocolVersion,
}
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Bytes
import slick.jdbc.SetParameter

/** Contains trace data about changes that contribute to an ACS digest.
  */
final case class AcsDigestTrace(traces: Seq[TraceElement])
    extends HasVersionedJsonWrapper[AcsDigestTrace]
    with PrettyPrinting {

  override protected def companionObj: AcsDigestTrace.type = AcsDigestTrace

  def toProtoV30: v30.AcsDigestTrace =
    v30.AcsDigestTrace(traces.map {
      case group: TraceGroup =>
        v30.TraceElement(v30.TraceElement.Trace.Group(group.toProtoV30))
      case single: SingleTrace =>
        v30.TraceElement(v30.TraceElement.Trace.Single(single.toProtoV30))
    })

  override protected def pretty: Pretty[AcsDigestTrace] = AcsDigestTrace.pretty
}

object AcsDigestTrace
    extends HasVersionedJsonMessageCompanion[AcsDigestTrace]
    with HasVersionedJsonMessageCompanionDbHelpers[AcsDigestTrace]
    with PrettyUtil {
  override def name: String = "AcsDigestTrace"

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(-1) -> unsupportedProtoCodec(ProtocolVersion.v34),
      ProtoVersion(30) -> ProtoCodec(
        // TODO(#33849) replace with the stable protocol version that introduces the feature
        ReleaseProtocolVersion.acsCommitmentRedesignStorage.v,
        supportedProtoVersion(fromProtoV30),
        _.toProtoV30,
      ),
    )

  private def fromProtoV30(proto: v30.AcsDigestTrace): ParsingResult[AcsDigestTrace] =
    proto.traces
      .map(_.trace)
      .traverse {
        case v30.TraceElement.Trace.Group(group) =>
          group.traces
            .traverse(SingleTrace.fromProtoV30)
            .map(TraceGroup(group.description, _, group.addedToHash))
        case v30.TraceElement.Trace.Single(singleChange) =>
          SingleTrace.fromProtoV30(singleChange)
        case v30.TraceElement.Trace.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("data"))
      }
      .map(AcsDigestTrace(_))

  implicit val setParameterAcsDigestTrace: SetParameter[AcsDigestTrace] =
    // TODO(#33849): replace with ReleaseProtocolversion.latest
    AcsDigestTrace.getVersionedSetParameter(ReleaseProtocolVersion.acsCommitmentRedesignStorage.v)
  implicit val setParameterAcsDigestTraceO: SetParameter[Option[AcsDigestTrace]] =
    // TODO(#33849): replace with ReleaseProtocolversion.latest
    AcsDigestTrace.getVersionedSetParameterO(ReleaseProtocolVersion.acsCommitmentRedesignStorage.v)

  val pretty: Pretty[AcsDigestTrace] = prettyOfParam(_.traces)
}

/** Base type for trace groups and single traces.
  */
sealed trait TraceElement extends Product with Serializable with PrettyPrinting

/** Represents a group of changes that were applied to an ACS digest (e.g. in case of party on- or
  * offboarding). The traces describe the state of the digest that was added to or subtracted from
  * the traced digest.
  */
final case class TraceGroup(description: String, traces: Seq[SingleTrace], addedToHash: Boolean)
    extends TraceElement {

  def toProtoV30: v30.TraceElement.TraceGroup =
    v30.TraceElement.TraceGroup(description, traces.map(_.toProtoV30), addedToHash)

  override protected def pretty: Pretty[TraceGroup] = TraceGroup.pretty
}

object TraceGroup extends PrettyUtil with ShowUtil {
  val pretty: Pretty[TraceGroup] =
    prettyOfClass(
      param("description", _.description.unquoted),
      paramIfTrue("added", _.addedToHash),
      paramIfTrue("subtracted", !_.addedToHash),
      unnamedParamIfNonEmpty(_.traces),
    )
}

/** Represents an individual change that can be applied to an ACS digest.
  */
final case class SingleTrace(
    contractId: LfContractId,
    reassignmentCounter: ReassignmentCounter,
    partyId1: LfPartyId,
    partyId2: LfPartyId,
    isActivation: Boolean,
) extends TraceElement {
  def toProtoV30: v30.TraceElement.SingleTrace =
    v30.TraceElement.SingleTrace(
      contractId = contractId.toBytes.toByteString,
      reassignmentCounter = reassignmentCounter.v,
      party1 = partyId1,
      party2 = partyId2,
      isActivation = isActivation,
    )

  override protected def pretty: Pretty[SingleTrace] = SingleTrace.pretty
}

object SingleTrace extends PrettyUtil {
  def fromProtoV30(
      proto: v30.TraceElement.SingleTrace
  ): ParsingResult[SingleTrace] =
    for {
      cid <- LfContractId
        .fromBytes(Bytes.fromByteString(proto.contractId))
        .leftMap(ProtoDeserializationError.ValueDeserializationError("contract_id", _))
      rc = ReassignmentCounter(proto.reassignmentCounter)
      p1 <- LfPartyId
        .fromString(proto.party1)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("party1", _))
      p2 <- LfPartyId
        .fromString(proto.party2)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("party2", _))
      isActivation = proto.isActivation
    } yield SingleTrace(cid, rc, p1, p2, isActivation)

  val pretty: Pretty[SingleTrace] = prettyOfString[SingleTrace](st =>
    s"${st.contractId}|${st.reassignmentCounter}|${st.partyId1}|${st.partyId2}|${if (st.isActivation) "+"
      else "-"}"
  )

}
