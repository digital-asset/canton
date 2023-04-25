// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.protocol.TopologyStateForInitRequest
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.TopologyStoreX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait DomainTopologyInitializationCallback {
  def callback(
      topologyClient: DomainTopologyClientWithInit,
      clientTransport: SequencerClientTransport,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit]
}

class StoreBasedDomainTopologyInitializationCallback(
    member: Member,
    topologyStore: TopologyStoreX[DomainStore],
) extends DomainTopologyInitializationCallback {
  override def callback(
      topologyClient: DomainTopologyClientWithInit,
      transport: SequencerClientTransport,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] = {
    for {
      response <- transport.downloadTopologyStateForInit(
        TopologyStateForInitRequest(
          member,
          protocolVersion,
        )
      )
      _ <- EitherT.liftF(topologyStore.bootstrap(response.topologyTransactions.value))
    } yield {
      response.topologyTransactions.value.lastChangeTimestamp.foreach { timestamp =>
        val tsNext = EffectiveTime(timestamp.immediateSuccessor)
        topologyClient.updateHead(
          tsNext,
          tsNext.toApproximate,
          potentialTopologyChange = true,
        )
      }
    }
  }
}
