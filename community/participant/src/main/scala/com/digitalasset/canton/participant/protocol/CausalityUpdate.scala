// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0.CausalityUpdate.Tag
import com.digitalasset.canton.protocol.{TransferId, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionedCompanionDbHelpers,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError, RequestCounter}

/** Represents the causal dependencies of a given request.
  */
sealed trait CausalityUpdate
    extends HasProtocolVersionedWrapper[CausalityUpdate]
    with PrettyPrinting {

  override protected def companionObj = CausalityUpdate

  val ts: CantonTimestamp
  val domain: DomainId
  val rc: RequestCounter
  val hostedInformeeStakeholders: Set[LfPartyId]

  def toProtoV0: v0.CausalityUpdate
}

/** A transaction is causally dependant on all earlier events in the same domain.
  */
case class TransactionUpdate(
    hostedInformeeStakeholders: Set[LfPartyId],
    ts: CantonTimestamp,
    domain: DomainId,
    rc: RequestCounter,
)(val representativeProtocolVersion: RepresentativeProtocolVersion[CausalityUpdate])
    extends CausalityUpdate {

  override def pretty: Pretty[TransactionUpdate] =
    prettyOfClass(
      param("domain", _.domain),
      param("timestamp", _.ts),
      param("request counter", _.rc),
      param("hosted informee stakeholders", _.hostedInformeeStakeholders),
    )

  override def toProtoV0: v0.CausalityUpdate =
    v0.CausalityUpdate(
      hostedInformeeStakeholders.toList,
      Some(ts.toProtoPrimitive),
      domain.toProtoPrimitive,
      rc.toProtoPrimitive,
      v0.CausalityUpdate.Tag.TransactionUpdate(v0.TransactionUpdate()),
    )
}

object TransactionUpdate {
  def apply(
      hostedInformeeStakeholders: Set[LfPartyId],
      ts: CantonTimestamp,
      domain: DomainId,
      rc: RequestCounter,
      protocolVersion: ProtocolVersion,
  ): TransactionUpdate = TransactionUpdate(hostedInformeeStakeholders, ts, domain, rc)(
    CausalityUpdate.protocolVersionRepresentativeFor(protocolVersion)
  )
}

/** A transfer-out is causally dependant on all earlier events in the same domain.
  */
case class TransferOutUpdate(
    hostedInformeeStakeholders: Set[LfPartyId],
    ts: CantonTimestamp,
    transferId: TransferId,
    rc: RequestCounter,
)(val representativeProtocolVersion: RepresentativeProtocolVersion[CausalityUpdate])
    extends CausalityUpdate {

  override val domain: DomainId = transferId.sourceDomain

  override def pretty: Pretty[TransferOutUpdate] =
    prettyOfClass(
      param("domain", _.domain),
      param("timestamp", _.ts),
      param("request counter", _.rc),
      param("transfer id", _.transferId),
      param("hosted informee stakeholders", _.hostedInformeeStakeholders),
    )

  override def toProtoV0: v0.CausalityUpdate =
    v0.CausalityUpdate(
      hostedInformeeStakeholders.toList,
      Some(ts.toProtoPrimitive),
      domain.toProtoPrimitive,
      rc.toProtoPrimitive,
      Tag.TransferOutUpdate(v0.TransferOutUpdate(Some(transferId.toProtoV0))),
    )
}

object TransferOutUpdate {
  def apply(
      hostedInformeeStakeholders: Set[LfPartyId],
      ts: CantonTimestamp,
      transferId: TransferId,
      rc: RequestCounter,
      protocolVersion: SourceProtocolVersion,
  ): TransferOutUpdate = TransferOutUpdate(hostedInformeeStakeholders, ts, transferId, rc)(
    CausalityUpdate.protocolVersionRepresentativeFor(protocolVersion.v)
  )
}

/** A transfer-in is causally dependant on all earlier events in the same domain, as well as all events causally observed
  * by `hostedInformeeStakeholders` at the time of the transfer-out on the target domain.
  */
case class TransferInUpdate(
    hostedInformeeStakeholders: Set[LfPartyId],
    ts: CantonTimestamp,
    domain: DomainId,
    rc: RequestCounter,
    transferId: TransferId,
)(val representativeProtocolVersion: RepresentativeProtocolVersion[CausalityUpdate])
    extends CausalityUpdate {
  override def pretty: Pretty[TransferInUpdate] =
    prettyOfClass(
      param("domain", _.domain),
      param("timestamp", _.ts),
      param("request counter", _.rc),
      param("transfer id", _.transferId),
      param("hosted informee stakeholders", _.hostedInformeeStakeholders),
    )

  override def toProtoV0: v0.CausalityUpdate =
    v0.CausalityUpdate(
      hostedInformeeStakeholders.toList,
      Some(ts.toProtoPrimitive),
      domain.toProtoPrimitive,
      rc.toProtoPrimitive,
      Tag.TransferInUpdate(v0.TransferInUpdate(Some(transferId.toProtoV0))),
    )
}

object TransferInUpdate {
  def apply(
      hostedInformeeStakeholders: Set[LfPartyId],
      ts: CantonTimestamp,
      domain: DomainId,
      rc: RequestCounter,
      transferId: TransferId,
      protocolVersion: TargetProtocolVersion,
  ): TransferInUpdate = TransferInUpdate(hostedInformeeStakeholders, ts, domain, rc, transferId)(
    CausalityUpdate.protocolVersionRepresentativeFor(protocolVersion.v)
  )
}

object CausalityUpdate
    extends HasProtocolVersionedCompanion[CausalityUpdate]
    with ProtocolVersionedCompanionDbHelpers[CausalityUpdate] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(v0.CausalityUpdate)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override protected def name: String = "causality update"

  def fromProtoV0(p: v0.CausalityUpdate): ParsingResult[CausalityUpdate] = {
    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(0))

    for {
      domainId <- DomainId.fromProtoPrimitive(p.domainId, "domain_id")
      informeeStksL <- p.informeeStakeholders.traverse { p =>
        ProtoConverter.parseLfPartyId(p)
      }
      informeeStks = informeeStksL.toSet
      ts <- ProtoConverter.parseRequired(CantonTimestamp.fromProtoPrimitive, "ts", p.ts)
      rc = RequestCounter(p.requestCounter)

      updateE: Either[ProtoDeserializationError, CausalityUpdate] = p.tag match {
        case Tag.Empty => Left(ProtoDeserializationError.FieldNotSet(s"tag"))
        case Tag.TransactionUpdate(_value) =>
          Right(TransactionUpdate(informeeStks, ts, domainId, rc)(representativeProtocolVersion))
        case Tag.TransferOutUpdate(v0.TransferOutUpdate(transferIdO)) =>
          for {
            tid <- ProtoConverter.parseRequired(
              TransferId.fromProtoV0,
              "transfer_id",
              transferIdO,
            )
          } yield TransferOutUpdate(informeeStks, ts, tid, rc)(representativeProtocolVersion)
        case Tag.TransferInUpdate(v0.TransferInUpdate(transferIdO)) =>
          for {
            tid <- ProtoConverter.parseRequired(
              TransferId.fromProtoV0,
              "transfer_id",
              transferIdO,
            )
          } yield TransferInUpdate(informeeStks, ts, domainId, rc, tid)(
            representativeProtocolVersion
          )
      }

      update <- updateE

    } yield update
  }
}
