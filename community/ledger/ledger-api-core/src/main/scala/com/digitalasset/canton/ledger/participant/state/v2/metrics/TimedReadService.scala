// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2.metrics

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.configuration.LedgerInitialConditions
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{ReadService, Update}

final class TimedReadService(delegate: ReadService, metrics: Metrics) extends ReadService {

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Timed.source(
      metrics.daml.services.read.getLedgerInitialConditions,
      delegate.ledgerInitialConditions(),
    )

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit loggingContext: LoggingContext): Source[(Offset, Update), NotUsed] =
    Timed.source(metrics.daml.services.read.stateUpdates, delegate.stateUpdates(beginAfter))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}