// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.interpretation

import com.digitalasset.daml.lf.transaction.metrics.TransactionNodeMetrics

final case class LimitConfig(
    limits: Limits,
    metrics: Option[TransactionNodeMetrics],
)

object LimitConfig {
  val Off = new LimitConfig(Limits.Lenient, None)

  def apply(limits: Limits, metrics: TransactionNodeMetrics): LimitConfig =
    new LimitConfig(limits, Some(metrics))
}
