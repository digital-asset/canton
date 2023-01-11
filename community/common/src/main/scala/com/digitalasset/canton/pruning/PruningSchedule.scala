// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pruning

import cats.syntax.either.*
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.scheduler.Cron

case class PruningSchedule(
    cron: Cron,
    maxDuration: PositiveDurationSeconds,
    retention: PositiveDurationSeconds,
) {
  def toProtoV0: v0.PruningSchedule = v0.PruningSchedule(
    cron = cron.toProtoPrimitive,
    maxDuration = Some(maxDuration.toProtoPrimitive),
    retention = Some(retention.toProtoPrimitive),
  )
}

object PruningSchedule {
  def fromProtoV0(scheduleP: v0.PruningSchedule): Either[String, PruningSchedule] =
    for {
      cron <- Cron.fromProtoPrimitive(scheduleP.cron).leftMap(_.message)
      maxDurationP <- scheduleP.maxDuration.toRight("Missing max_duration")
      maxDuration <- PositiveDurationSeconds.fromProtoPrimitive(maxDurationP)
      retentionP <- scheduleP.retention.toRight("Missing retention")
      retention <- PositiveDurationSeconds.fromProtoPrimitive(retentionP)
    } yield PruningSchedule(cron, maxDuration, retention)

}
