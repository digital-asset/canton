// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.implicits._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v0.EnvelopeContent
import com.digitalasset.canton.protocol.{TransferId, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.topology.DomainId

/** Causality messages are sent along with a transfer-in response. They propagate causality information on
  * the events a participant has "seen" for a party at the time of the transfer-out.
  * TODO(i5352): consider whether this should be encrypted (probably).*
  *
  * @param domainId The domain ID that the causality message is addressed to
  * @param transferId The ID of the transfer for which we are propagating causality information
  * @param clock The vector clock specifying causality information at the time of the transfer out
  */
case class CausalityMessage(domainId: DomainId, transferId: TransferId, clock: VectorClock)
    extends ProtocolMessage
    with HasProtoV0[v0.CausalityMessage]
    with PrettyPrinting {

  override def toProtoV0: v0.CausalityMessage = v0.CausalityMessage(
    targetDomainId = domainId.toProtoPrimitive,
    transferId = Some(transferId.toProtoV0),
    clock = Some(clock.toProtoV0),
  )

  override def toProtoEnvelopeContentV0(version: ProtocolVersion): EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.CausalityMessage(toProtoV0))

  override def pretty: Pretty[CausalityMessage.this.type] =
    prettyOfClass(
      param("Message domain ", _.domainId),
      param("Transfer ID ", _.transferId),
      param("Vector clock", _.clock),
    )
}

object CausalityMessage {

  implicit val causalityMessageCast: ProtocolMessageContentCast[CausalityMessage] = {
    case cm: CausalityMessage => Some(cm)
    case _ => None
  }

  def fromProtoV0(cmP: v0.CausalityMessage): ParsingResult[CausalityMessage] = {
    val v0.CausalityMessage(domainIdP, transferIdP, clockPO) = cmP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "target_domain_id")
      clocks <- ProtoConverter.parseRequired(VectorClock.fromProtoV0, "clock", clockPO)
      tid <- ProtoConverter.parseRequired(TransferId.fromProtoV0, "transfer_id", transferIdP)
    } yield CausalityMessage(domainId, tid, clocks)
  }
}

/** A vector clock represents the causal constraints that must be respected for a party at a certain point in time.
  * Vector clocks are maintained per-domain
  *
  * @param originDomainId The domain of the vector clock
  * @param localTs The timestamp on `originDomainId` specifying the time at which the causal constraints are valid
  * @param partyId The party who has seen the causal information specified by `clock`
  * @param clock The most recent timestamp on each domain that `partyId` has causally observed
  */
case class VectorClock(
    originDomainId: DomainId,
    localTs: CantonTimestamp,
    partyId: LfPartyId,
    clock: Map[DomainId, CantonTimestamp],
) extends HasProtoV0[v0.VectorClock]
    with PrettyPrinting {

  override def pretty: Pretty[VectorClock.this.type] =
    prettyOfClass(
      param("Domain for constraints ", _.originDomainId),
      param("Most recent timestamps", _.clock),
      param("Local timestamp", _.localTs),
      param("Party", _.partyId),
    )

  override def toProtoV0: v0.VectorClock = {
    v0.VectorClock(
      originDomainId = originDomainId.toProtoPrimitive,
      localTs = Some(localTs.toProtoPrimitive),
      partyId = partyId,
      clock = clock.map { case (did, cts) => did.toProtoPrimitive -> cts.toProtoPrimitive },
    )
  }
}

object VectorClock {
  def fromProtoV0(vc: v0.VectorClock): ParsingResult[VectorClock] = {
    val v0.VectorClock(did, ts, partyid, clock) = vc
    for {
      localTs <- ProtoConverter.parseRequired(CantonTimestamp.fromProtoPrimitive, "local_ts", ts)
      domainId <- DomainId.fromProtoPrimitive(did, "origin_domain_id")
      party <- ProtoConverter.parseLfPartyId(partyid)
      domainTimestamps <- clock.toList.traverse { case (kProto, vProto) =>
        for {
          k <- DomainId.fromProtoPrimitive(kProto, "clock (key: DomainId)")
          v <- CantonTimestamp.fromProtoPrimitive(vProto)
        } yield k -> v
      }
    } yield {
      VectorClock(domainId, localTs, party, domainTimestamps.toMap)
    }
  }
}
