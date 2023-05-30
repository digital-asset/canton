// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.SequencerConnectClient
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps

import scala.concurrent.{ExecutionContext, Future}

final case class DomainConnectionInfo(
    sequencerConnections: SequencerConnections,
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
      for {
        domainClientBootstrapInfo <- sequencerConnectClient.getDomainClientBootstrapInfo(alias)
        staticDomainParameters <- sequencerConnectClient.getDomainParameters(alias)
      } yield (domainClientBootstrapInfo.domainId, staticDomainParameters)

    for {
      sequencerConnectClient <- sequencerConnectClientBuilder(
        config.sequencerConnections.default
      )
      domainInfo <- getDomainInfo(config.domain, sequencerConnectClient).thereafter(_ =>
        sequencerConnectClient.close()
      )
      // TODO(i12076): Support multiple sequencers, check all sequencers to be aligned on domainId and parameters
      (domainId, staticDomainParameters) = domainInfo
    } yield DomainConnectionInfo(config.sequencerConnections, domainId, staticDomainParameters)
  }

}
