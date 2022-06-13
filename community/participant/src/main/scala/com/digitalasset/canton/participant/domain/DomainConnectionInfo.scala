// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.topology.DomainId
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
      loggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SequencerConnectClient.Error, DomainConnectionInfo] = {
    implicit val traceContext = loggingContext.traceContext
    def getDomainInfo(
        alias: DomainAlias,
        sequencerConnectClient: SequencerConnectClient,
    ): EitherT[Future, SequencerConnectClient.Error, (DomainId, StaticDomainParameters)] =
      (for {
        domainId <- sequencerConnectClient.getDomainId(alias)
        staticDomainParameters <- sequencerConnectClient.getDomainParameters(alias)
      } yield (domainId, staticDomainParameters))

    for {
      sequencerConnectClient <- sequencerConnectClientBuilder(config, loggingContext)
      domainInfo <- getDomainInfo(config.domain, sequencerConnectClient).thereafter(_ =>
        sequencerConnectClient.close()
      )
      (domainId, staticDomainParameters) = domainInfo
    } yield DomainConnectionInfo(config.sequencerConnection, domainId, staticDomainParameters)
  }

}
