// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.time

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.digitalasset.canton.util.DelayUtil

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait DelayMechanism {
  def delayBy(duration: FiniteDuration): Future[Unit]
}

class TimeDelayMechanism() extends DelayMechanism {
  override def delayBy(duration: FiniteDuration): Future[Unit] = DelayUtil.delay(duration)
}

class StaticTimeDelayMechanism(ledger: ParticipantTestContext)(implicit
    ec: ExecutionContext
) extends DelayMechanism {
  override def delayBy(duration: FiniteDuration): Future[Unit] =
    ledger
      .time()
      .flatMap { currentTime =>
        ledger.setTime(currentTime, currentTime.plusMillis(duration.toMillis))
      }
}
