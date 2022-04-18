// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.admin.gprc

import cats.syntax.traverse._
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.sequencing.admin.protocol.InitRequest
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, MediatorId, UniqueIdentifier}
import com.digitalasset.canton.version.HasProtoV0

case class InitializeMediatorRequest(
    domainId: DomainId,
    mediatorId: MediatorId,
    topologyState: Option[StoredTopologyTransactions[TopologyChangeOp.Positive]],
    domainParameters: StaticDomainParameters,
    sequencerConnection: SequencerConnection,
) extends HasProtoV0[v0.InitializeMediatorRequest] {
  override def toProtoV0: v0.InitializeMediatorRequest =
    v0.InitializeMediatorRequest(
      domainId.toProtoPrimitive,
      mediatorId.uid.toProtoPrimitive,
      topologyState.map(_.toProtoV0),
      Some(domainParameters.toProtoV0),
      Some(sequencerConnection.toProtoV0),
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
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domainId")
      mediatorId <- UniqueIdentifier
        .fromProtoPrimitive(mediatorIdP, "mediatorId")
        .map(MediatorId(_))
      topologyState <- topologyStateP.traverse(InitRequest.convertTopologySnapshot)
      domainParameters <- ProtoConverter
        .required("domainParameters", domainParametersP)
        .flatMap(StaticDomainParameters.fromProtoV0)
      sequencerConnection <- ProtoConverter.parseRequired(
        SequencerConnection.fromProtoV0,
        "sequencerConnection",
        sequencerConnectionP,
      )
    } yield InitializeMediatorRequest(
      domainId,
      mediatorId,
      topologyState,
      domainParameters,
      sequencerConnection,
    )
  }
}
