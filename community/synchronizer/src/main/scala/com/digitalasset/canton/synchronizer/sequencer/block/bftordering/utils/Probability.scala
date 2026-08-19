// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import scala.util.Random

final case class Probability(prob: Double) {
  require(0 <= prob, "Probability must be at least 0")
  require(prob <= 1, "Probability must be at most 1")
  def flipCoin(rng: Random): Boolean =
    rng.nextDouble() < prob
}
