// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.RestartSource

import java.util.concurrent.atomic.AtomicReference

sealed trait DigestProcessor extends BaseDigestProcessor

trait ReinitializingDigestProcessor extends DigestProcessor {
  def reinitializingTimepoint: Timepoint
}

trait RunningDigestProcessor extends DigestProcessor

object DigestProcessor {
  def acsUpdatesWithRetries(
      indexService: InternalIndexService,
      synchronizerId: SynchronizerId,
      startingOffset: Option[Offset],
  )(implicit errorLoggingContext: ErrorLoggingContext) = {
    implicit val traceContext: TraceContext = errorLoggingContext.traceContext
    import scala.concurrent.duration.DurationInt
    val restartSettings = RestartSettings(
      minBackoff = 1.millisecond,
      maxBackoff = 10.milliseconds,
      randomFactor = 0.2,
    ).withRestartOn {
      // TODO(#35251) More robust restarts needed here.
      case ex: com.digitalasset.base.error.ErrorCode.LoggedApiException
          if ex.getMessage.contains(
            com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning.code.id
          ) && ex.getMessage.contains("Ledger API offset dispatcher") =>
        errorLoggingContext.info("ACS update sources has failed. Restarting the source.", ex)
        true
      case _ => false
    }
    val startingOffsetRef = new AtomicReference[Option[Offset]](startingOffset)
    RestartSource.withBackoff(restartSettings) { () =>
      indexService
        .acsUpdates(synchronizerId, startingOffsetRef.get)
        .map { element =>
          startingOffsetRef.set(Some(element.offset))
          element
        }
    }
  }

}
