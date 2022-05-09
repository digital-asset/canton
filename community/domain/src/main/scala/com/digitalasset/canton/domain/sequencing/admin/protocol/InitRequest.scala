// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.protocol

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.protocol.{StaticDomainParameters, v0 => protocolV0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.HasProtoV0

case class InitRequest private (
    domainId: DomainId,
    topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
    domainParameters: StaticDomainParameters,
    sequencerSnapshot: Option[SequencerSnapshot] = None,
) extends HasProtoV0[v0.InitRequest] {

  /* We allow serializing this message to a ByteArray despite it implementing ProtoNonSerializable because the serialization
 is (and should) only used in the HttpSequencerClient.
If you need to save this message in a database, please add an UntypedVersionedMessage message as documented in contributing.md  */
  def toByteArrayV0: Array[Byte] = toProtoV0.toByteArray

  override def toProtoV0: v0.InitRequest = v0.InitRequest(
    domainId.toProtoPrimitive,
    Some(topologySnapshot.toProtoV0),
    Some(domainParameters.toProtoV0),
    sequencerSnapshot.map(_.toProtoV0),
  )
}

object InitRequest {

  def convertTopologySnapshot(
      transactionsP: protocolV0.TopologyTransactions
  ): ParsingResult[StoredTopologyTransactions[TopologyChangeOp.Positive]] = {
    StoredTopologyTransactions.fromProtoV0(transactionsP).flatMap { topologySnapshot =>
      val topologySnapshotPositive = topologySnapshot.collectOfType[TopologyChangeOp.Positive]
      if (topologySnapshot.result.sizeCompare(topologySnapshotPositive.result) == 0)
        Right(topologySnapshotPositive)
      else
        Left(
          ProtoDeserializationError.InvariantViolation(
            "InitRequest should contain only positive transactions"
          )
        )
    }
  }

  def fromProtoV0(request: v0.InitRequest): ParsingResult[InitRequest] =
    for {
      domainId <- UniqueIdentifier
        .fromProtoPrimitive(request.domainId, "domainId")
        .map(DomainId(_))
      domainParametersP <- request.domainParameters.toRight(
        ProtoDeserializationError.FieldNotSet("domainParameters")
      )
      domainParameters <- StaticDomainParameters.fromProtoV0(domainParametersP)
      topologySnapshotAddO <- request.topologySnapshot.traverse(convertTopologySnapshot)
      snapshotO <- request.snapshot.traverse(SequencerSnapshot.fromProtoV0)
    } yield InitRequest(
      domainId,
      topologySnapshotAddO.getOrElse(StoredTopologyTransactions.empty),
      domainParameters,
      snapshotO,
    )
}
