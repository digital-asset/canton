// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.implicits._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.v0.CausalityUpdate.Tag
import com.digitalasset.canton.protocol.v0.{
  CausalityUpdate => CausalityUpdateProto,
  TransactionUpdate => TransactionUpdateProto,
  TransferInUpdate => TransferInUpdateProto,
  TransferOutUpdate => TransferOutUpdateProto,
}
import com.digitalasset.canton.protocol.version.VersionedCausalityUpdate
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, HasVersionedWrapperCompanion}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainId, LfPartyId, ProtoDeserializationError}

/** Represents the causal dependencies of a given request.
  */
sealed trait CausalityUpdate
    extends HasProtoV0[CausalityUpdateProto]
    with HasVersionedWrapper[VersionedCausalityUpdate]
    with PrettyPrinting {

  val ts: CantonTimestamp
  val domain: DomainId
  val rc: RequestCounter
  val hostedInformeeStakeholders: Set[LfPartyId]

  def toProtoV0: CausalityUpdateProto

  override protected def toProtoVersioned(version: ProtocolVersion): VersionedCausalityUpdate = {
    VersionedCausalityUpdate(VersionedCausalityUpdate.Version.V0(toProtoV0))
  }
}

/** A transaction is causally dependant on all earlier events in the same domain.
  */
case class TransactionUpdate(
    hostedInformeeStakeholders: Set[LfPartyId],
    ts: CantonTimestamp,
    domain: DomainId,
    rc: RequestCounter,
) extends CausalityUpdate {

  override def pretty: Pretty[TransactionUpdate] =
    prettyOfClass(
      param("domain", _.domain),
      param("timestamp", _.ts),
      param("request counter", _.rc),
      param("hosted informee stakeholders", _.hostedInformeeStakeholders),
    )

  override def toProtoV0: CausalityUpdateProto =
    CausalityUpdateProto(
      hostedInformeeStakeholders.toList,
      Some(ts.toProtoPrimitive),
      domain.toProtoPrimitive,
      rc,
      CausalityUpdateProto.Tag.TransactionUpdate(TransactionUpdateProto()),
    )

}

/** A transfer-out is causally dependant on all earlier events in the same domain.
  */
case class TransferOutUpdate(
    hostedInformeeStakeholders: Set[LfPartyId],
    ts: CantonTimestamp,
    transferId: TransferId,
    rc: RequestCounter,
) extends CausalityUpdate {

  override val domain: DomainId = transferId.originDomain

  override def pretty: Pretty[TransferOutUpdate] =
    prettyOfClass(
      param("domain", _.domain),
      param("timestamp", _.ts),
      param("request counter", _.rc),
      param("transfer id", _.transferId),
      param("hosted informee stakeholders", _.hostedInformeeStakeholders),
    )

  override def toProtoV0: CausalityUpdateProto =
    CausalityUpdateProto(
      hostedInformeeStakeholders.toList,
      Some(ts.toProtoPrimitive),
      domain.toProtoPrimitive,
      rc,
      Tag.TransferOutUpdate(TransferOutUpdateProto(Some(transferId.toProtoV0))),
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
) extends CausalityUpdate {
  override def pretty: Pretty[TransferInUpdate] =
    prettyOfClass(
      param("domain", _.domain),
      param("timestamp", _.ts),
      param("request counter", _.rc),
      param("transfer id", _.transferId),
      param("hosted informee stakeholders", _.hostedInformeeStakeholders),
    )

  override def toProtoV0: CausalityUpdateProto =
    CausalityUpdateProto(
      hostedInformeeStakeholders.toList,
      Some(ts.toProtoPrimitive),
      domain.toProtoPrimitive,
      rc,
      Tag.TransferInUpdate(TransferInUpdateProto(Some(transferId.toProtoV0))),
    )
}

object CausalityUpdate
    extends HasVersionedWrapperCompanion[VersionedCausalityUpdate, CausalityUpdate] {

  override protected def ProtoClassCompanion: VersionedCausalityUpdate.type =
    VersionedCausalityUpdate

  override protected def name: String = "causality update"

  override protected def fromProtoVersioned(
      proto: VersionedCausalityUpdate
  ): ParsingResult[CausalityUpdate] =
    proto.version match {
      case VersionedCausalityUpdate.Version.Empty =>
        Left(FieldNotSet("VersionedCausalityUpdate.version"))
      case VersionedCausalityUpdate.Version.V0(parameters) => fromProtoV0(parameters)
    }

  def fromProtoV0(p: CausalityUpdateProto): ParsingResult[CausalityUpdate] = {
    for {
      domainId <- DomainId.fromProtoPrimitive(p.domainId, "domain_id")
      informeeStksL <- p.informeeStakeholders.traverse { p =>
        ProtoConverter.parseLfPartyId(p)
      }
      informeeStks = informeeStksL.toSet
      ts <- ProtoConverter.parseRequired(CantonTimestamp.fromProtoPrimitive, "ts", p.ts)
      rc = p.requestCounter
      update <- p.tag match {
        case Tag.Empty =>
          Left(ProtoDeserializationError.FieldNotSet(s"tag")): Either[
            ProtoDeserializationError,
            CausalityUpdate,
          ]
        case Tag.TransactionUpdate(value) =>
          Right(TransactionUpdate(informeeStks, ts, domainId, rc)): Either[
            ProtoDeserializationError,
            CausalityUpdate,
          ]
        case Tag.TransferOutUpdate(value) =>
          (for {
            tid <- ProtoConverter.parseRequired(
              TransferId.fromProtoV0,
              "transfer_id",
              value.transferId,
            )
          } yield {
            TransferOutUpdate(informeeStks, ts, tid, rc)
          }): ParsingResult[CausalityUpdate]
        case Tag.TransferInUpdate(value) =>
          (for {
            tid <- ProtoConverter.parseRequired(
              TransferId.fromProtoV0,
              "transfer_id",
              value.transferId,
            )
          } yield {
            TransferInUpdate(informeeStks, ts, domainId, rc, tid)
          }): ParsingResult[CausalityUpdate]
      }

    } yield update
  }
}
