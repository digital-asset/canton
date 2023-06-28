// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.canton.ledger.configuration.{
  Configuration,
  LedgerId,
  LedgerInitialConditions,
  LedgerTimeModel,
}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.{ReadService, Update}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import java.time.Duration
import scala.concurrent.blocking

class BridgeReadService(
    ledgerId: LedgerId,
    maximumDeduplicationDuration: Duration,
    stateUpdatesSource: Source[(Offset, Traced[Update]), NotUsed],
    val loggerFactory: NamedLoggerFactory,
) extends ReadService
    with NamedLogging {
  private var stateUpdatesWasCalledAlready = false

  logger.info("Starting Sandbox-on-X read service...")(TraceContext.empty)

  override def ledgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    Source.single(
      LedgerInitialConditions(
        ledgerId = ledgerId,
        config = Configuration(
          generation = 1L,
          timeModel = LedgerTimeModel.reasonableDefault,
          maxDeduplicationDuration = maximumDeduplicationDuration,
        ),
        initialRecordTime = Timestamp.now(),
      )
    )

  override def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit traceContext: TraceContext): Source[(Offset, Traced[Update]), NotUsed] = {
    // For PoC purposes:
    //   This method may only be called once, either with `beginAfter` set or unset.
    //   A second call will result in an error unless the server is restarted.
    //   Bootstrapping the bridge from indexer persistence is supported.
    blocking(synchronized {
      if (stateUpdatesWasCalledAlready)
        throw new IllegalStateException("not allowed to call this twice")
      else stateUpdatesWasCalledAlready = true
    })
    logger.info("Indexer subscribed to state updates.")
    beginAfter.foreach(offset =>
      logger.warn(
        s"Indexer subscribed from a specific offset $offset. This offset is not taking into consideration, and does not change the behavior of the ReadWriteServiceBridge. Only valid use case supported: service starting from an already ingested database, and indexer subscribes from exactly the ledger-end."
      )
    )

    stateUpdatesSource
  }

  override def currentHealth(): HealthStatus = Healthy
}
