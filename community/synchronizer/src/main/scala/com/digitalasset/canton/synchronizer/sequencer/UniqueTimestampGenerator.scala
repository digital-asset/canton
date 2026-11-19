// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.Clock

import java.util.concurrent.atomic.AtomicLong

/** Generates strictly increasing, and therefore unique, timestamps based on the clock, even if the
  * clock does not advance or moves backwards.
  */
class UniqueTimestampGenerator(clock: Clock) {
  private val lastRef = new AtomicLong(
    // The `SequencerWriter` initialization waits after a restart until the clock has reached the highest watermark
    // written before a crash. So even if the clock jumps backwards across restarts,
    // it will have advanced sufficiently by the time the next timestamp is generated.
    clock.monotonicTime().toMicros
  )

  def generateNext: CantonTimestamp = {
    val nowMicros = clock.monotonicTime().toMicros
    val micros = lastRef.updateAndGet(latest => Math.max(nowMicros, latest + 1))
    CantonTimestamp.assertFromLong(micros)
  }

}
