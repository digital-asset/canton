// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.admin.{v0, v2}
import com.digitalasset.canton.domain.sequencing.admin.grpc.InitializeSequencerRequest
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainId, MediatorId, SequencerId, UniqueIdentifier}

final case class InitializeMediatorRequest(
    domainId: DomainId,
    mediatorId: MediatorId,
    topologyState: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
    domainParameters: StaticDomainParameters,
    sequencerConnections: SequencerConnections,
) {
  def toProtoV0: v0.InitializeMediatorRequest =
    v0.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      mediatorId.uid.toProtoPrimitive,
      topologyState.map(_.toProtoV0),
      Some(domainParameters.toProtoV0),
      // Non-BFT domain is only supporting a single sequencer connection
      Some(sequencerConnections.default.toProtoV0),
    )
}

object InitializeMediatorRequest {
  def fromProtoV0(
      requestP: v0.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequest] = {
    val v0.InitializeMediatorRequest(
      domainIdP,
      mediatorIdP,
      topologyStateP,
      domainParametersP,
      sequencerConnectionP,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      mediatorId <- UniqueIdentifier
        .fromProtoPrimitive(mediatorIdP, "mediator_id")
        .map(MediatorId(_))
      topologyState <- topologyStateP.traverse(InitializeSequencerRequest.convertTopologySnapshot)
      domainParameters <- ProtoConverter
        .required("domain_parameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV0)
      sequencerConnection <- ProtoConverter.parseRequired(
        SequencerConnection.fromProtoV0,
        "sequencer_connection",
        sequencerConnectionP,
      )
    } yield InitializeMediatorRequest(
      domainId,
      mediatorId,
      topologyState,
      domainParameters,
      SequencerConnections.default(sequencerConnection),
    )
  }
}

final case class InitializeMediatorRequestX(
    domainId: DomainId,
    domainParameters: StaticDomainParameters,
    sequencerId: SequencerId,
    sequencerConnections: SequencerConnections,
) {
  def toProtoV2: v2.InitializeMediatorRequest =
    v2.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      Some(domainParameters.toProtoV1),
      sequencerId.toProtoPrimitive,
      sequencerConnections.toProtoV0,
    )
}

object InitializeMediatorRequestX {
  def fromProtoV2(
      requestP: v2.InitializeMediatorRequest
  ): ParsingResult[InitializeMediatorRequestX] = {
    val v2.InitializeMediatorRequest(
      domainIdP,
      domainParametersP,
      sequencerIdP,
      sequencerConnectionP,
    ) = requestP
    for {
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      domainParameters <- ProtoConverter
        .required("domain_parameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV1)
      sequencerId <- SequencerId.fromProtoPrimitive(sequencerIdP, "sequencer_id")
      sequencerConnections <- SequencerConnections.fromProtoV0(sequencerConnectionP)
    } yield InitializeMediatorRequestX(
      domainId,
      domainParameters,
      sequencerId,
      sequencerConnections,
    )
  }
}
