// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps

import scala.concurrent.{ExecutionContext, Future}

final case class DomainConnectionInfo(
    connection: SequencerConnection,
    domainId: DomainId,
    parameters: StaticDomainParameters,
)

object DomainConnectionInfo {

  def fromConfig(
      sequencerConnectClientBuilder: SequencerConnectClient.Builder
  )(config: DomainConnectionConfig)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SequencerConnectClient.Error, DomainConnectionInfo] = {
    def getDomainInfo(
        alias: DomainAlias,
        sequencerConnectClient: SequencerConnectClient,
    ): EitherT[Future, SequencerConnectClient.Error, (DomainId, StaticDomainParameters)] =
      (for {
        domainIdSequencerId <- sequencerConnectClient.getDomainIdSequencerId(alias)
        (domainId, _) = domainIdSequencerId
        staticDomainParameters <- sequencerConnectClient.getDomainParameters(alias)
      } yield (domainId, staticDomainParameters))

    for {
      sequencerConnectClient <- sequencerConnectClientBuilder(config.sequencerConnection)
      domainInfo <- getDomainInfo(config.domain, sequencerConnectClient).thereafter(_ =>
        sequencerConnectClient.close()
      )
      (domainId, staticDomainParameters) = domainInfo
    } yield DomainConnectionInfo(config.sequencerConnection, domainId, staticDomainParameters)
  }

}
