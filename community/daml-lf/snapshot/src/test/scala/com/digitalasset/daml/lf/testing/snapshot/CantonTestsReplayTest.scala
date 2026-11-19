// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.canton.logging.SuppressingLogging
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Replays and benchmarks the canton-tests corpus, which covers what the cn-apps one doesn't. */
class CantonTestsReplayTest extends AnyWordSpec with Matchers with SuppressingLogging {

  private lazy val corpus = CantonTestsCorpus.read()
  private lazy val replay = new CorpusReplay(corpus, loggerFactory)

  "CorpusReplay" should {
    "replay and verify every canton-tests unit, each on a fresh engine" in {
      val failures = corpus.units.flatMap { unit =>
        replay.verify(replay.newEngine(), unit).left.toOption.map(err => s"$unit: $err")
      }.toSeq
      failures shouldBe empty
    }

    "need the recorded key lookups" in {
      val withKeys = corpus.units.filter(_.entry.getKeyLookupsCount > 0).toSeq
      withKeys should not be empty
      withKeys.foreach { unit =>
        val tampered = unit.copy(entry = unit.entry.toBuilder.clearKeyLookups().build())
        replay.verify(replay.newEngine(), tampered) shouldBe a[Left[?, ?]]
      }
    }
  }

  "CorpusBenchmark" should {
    "replay the canton-tests units" in {
      val benchmark = new CorpusBenchmark
      benchmark.corpus = CantonTestsCorpus.dir.toString
      benchmark.select = "canton-tests/"
      benchmark.init()
      val counters = new CorpusBenchmark.EventCounter
      benchmark.bench(counters)
      counters.transactionNodeCount should be > 0L
    }
  }
}
