// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsLastPointers,
  AchsState,
}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AchsStateCacheSpec extends AnyFlatSpec with Matchers {

  private val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  implicit val traceContext: TraceContext = TraceContext.empty

  behavior of "AchsStateCache"

  it should "start with empty state" in {
    val cache = new AchsStateCache(loggerFactory)
    cache.get() shouldBe AchsState(
      validAt = 0,
      lastPointers = AchsLastPointers(lastRemoved = 0, lastPopulated = 0),
    )
  }

  it should "set and get state" in {
    val cache = new AchsStateCache(loggerFactory)
    val state = AchsState(
      validAt = 10L,
      lastPointers = AchsLastPointers(lastRemoved = 5L, lastPopulated = 3L),
    )
    cache.set(state)
    cache.get() shouldBe state
  }

  it should "updateLastPointers" in {
    val cache = new AchsStateCache(loggerFactory)
    cache.set(
      AchsState(
        validAt = 100L,
        lastPointers = AchsLastPointers(lastRemoved = 10L, lastPopulated = 5L),
      )
    )
    cache.updateLastPointers(AchsLastPointers(lastRemoved = 50L, lastPopulated = 30L))
    cache.get() shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 50L, lastPopulated = 30L),
    )
  }

  it should "updateValidAt preserves lastRemoved and lastPopulated" in {
    val cache = new AchsStateCache(loggerFactory)
    cache.set(
      AchsState(
        validAt = 10L,
        lastPointers = AchsLastPointers(lastRemoved = 5L, lastPopulated = 3L),
      )
    )
    cache.updateValidAt(99L)
    val state = cache.get()
    state.lastPointers.lastRemoved shouldBe 5L
    state.lastPointers.lastPopulated shouldBe 3L
  }

  it should "handle sequence of updateValidAt and updateLastPointers" in {
    val cache = new AchsStateCache(loggerFactory)
    cache.updateValidAt(100L)
    cache.get() shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
    )

    cache.updateLastPointers(AchsLastPointers(lastRemoved = 60L, lastPopulated = 40L))
    cache.get() shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 60L, lastPopulated = 40L),
    )

    cache.updateValidAt(200L)
    cache.get() shouldBe AchsState(
      validAt = 200L,
      lastPointers = AchsLastPointers(lastRemoved = 60L, lastPopulated = 40L),
    )

    cache.updateLastPointers(AchsLastPointers(lastRemoved = 160L, lastPopulated = 140L))
    cache.get() shouldBe AchsState(
      validAt = 200L,
      lastPointers = AchsLastPointers(lastRemoved = 160L, lastPopulated = 140L),
    )
  }
}
