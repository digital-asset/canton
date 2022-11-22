// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.admin.{v0, v1}
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.protocol.{StaticDomainParameters, v0 as protocolV0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

final case class InitRequest(
    domainId: DomainId,
    topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
    domainParameters: StaticDomainParameters,
    sequencerSnapshot: Option[SequencerSnapshot] = None,
) extends HasProtocolVersionedWrapper[InitRequest] {

  val representativeProtocolVersion: RepresentativeProtocolVersion[InitRequest] =
    InitRequest.protocolVersionRepresentativeFor(domainParameters.protocolVersion)

  override protected def companionObj = InitRequest

  def toProtoV0: v0.InitRequest = v0.InitRequest(
    domainId.toProtoPrimitive,
    Some(topologySnapshot.toProtoV0),
    Some(domainParameters.toProtoV0),
    sequencerSnapshot.map(_.toProtoV0),
  )

  def toProtoV1: v1.InitRequest = v1.InitRequest(
    domainId.toProtoPrimitive,
    Some(topologySnapshot.toProtoV0),
    Some(domainParameters.toProtoV1),
    sequencerSnapshot.map(_.toProtoV0),
  )
}

object InitRequest extends HasProtocolVersionedCompanion[InitRequest] {
  override val name: String = "InitRequest"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> LegacyProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(v0.InitRequest)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.InitRequest)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

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

  private[sequencing] def fromProtoV0(request: v0.InitRequest): ParsingResult[InitRequest] =
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

  private[sequencing] def fromProtoV1(request: v1.InitRequest): ParsingResult[InitRequest] =
    for {
      domainId <- UniqueIdentifier
        .fromProtoPrimitive(request.domainId, "domainId")
        .map(DomainId(_))
      domainParametersP <- request.domainParameters.toRight(
        ProtoDeserializationError.FieldNotSet("domainParameters")
      )
      domainParameters <- StaticDomainParameters.fromProtoV1(domainParametersP)
      topologySnapshotAddO <- request.topologySnapshot.traverse(convertTopologySnapshot)
      snapshotO <- request.snapshot.traverse(SequencerSnapshot.fromProtoV0)
    } yield InitRequest(
      domainId,
      topologySnapshotAddO.getOrElse(StoredTopologyTransactions.empty),
      domainParameters,
      snapshotO,
    )
}
