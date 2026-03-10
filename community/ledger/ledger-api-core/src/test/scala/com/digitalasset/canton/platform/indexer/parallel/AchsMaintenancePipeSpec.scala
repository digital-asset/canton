// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.indexer.parallel.AchsMaintenancePipe.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsAddActivationsParams,
  AchsLastPointers,
  AchsRemoveDeactivatedParams,
  AchsState,
}
import com.digitalasset.canton.platform.store.cache.AchsStateCache
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class AchsMaintenancePipeSpec extends AnyFlatSpec with BaseTest with HasExecutionContext {

  private lazy val executionContext = implicitly[ExecutionContext]

  private def workRange(
      popStart: Long,
      popEnd: Long,
      remStart: Long,
      remEnd: Long,
  ): AchsWorkRange =
    AchsWorkRange(
      activationsPopulation = EventSeqIdRange(startExclusive = popStart, endInclusive = popEnd),
      deactivatedRemoval = EventSeqIdRange(startExclusive = remStart, endInclusive = remEnd),
    )

  behavior of "AchsWorkDistance.+"

  it should "sum two work distances" in {
    val a = AchsWorkDistance(populate = 10L, remove = 20L)
    val b = AchsWorkDistance(populate = 5L, remove = 22L)
    (a + b) shouldBe AchsWorkDistance(populate = 15L, remove = 42L)
  }

  behavior of "AchsWorkDistance.-"

  it should "subtract two work distances" in {
    val a = AchsWorkDistance(populate = 15L, remove = 23L)
    val b = AchsWorkDistance(populate = 5L, remove = 3L)
    (a - b) shouldBe AchsWorkDistance(populate = 10L, remove = 20L)
  }

  behavior of "AchsWorkDistance.cap"

  it should "return threshold for dimensions at or above it, and 0 for dimensions below (populate)" in {
    val work = AchsWorkDistance(populate = 25L, remove = 8L)
    val chunk = work.cap(10L)
    chunk shouldBe AchsWorkDistance(populate = 10L, remove = 0L)
    (work - chunk) shouldBe AchsWorkDistance(populate = 15L, remove = 8L)
  }

  it should "return threshold for dimensions at or above it, and 0 for dimensions below (remove)" in {
    val work = AchsWorkDistance(populate = 5L, remove = 10L)
    val chunk = work.cap(10L)
    chunk shouldBe AchsWorkDistance(populate = 0L, remove = 10L)
    (work - chunk) shouldBe AchsWorkDistance(populate = 5L, remove = 0L)
  }

  it should "return 0 for both dimensions if both are under the threshold" in {
    val work = AchsWorkDistance(populate = 5L, remove = 3L)
    val chunk = work.cap(10L)
    chunk shouldBe AchsWorkDistance(populate = 0L, remove = 0L)
    (work - chunk) shouldBe AchsWorkDistance(populate = 5L, remove = 3L)
  }

  it should "return threshold for populate and 0 for a negative remove" in {
    val work = AchsWorkDistance(populate = 35L, remove = -5000L)
    val chunk = work.cap(10L)
    chunk shouldBe AchsWorkDistance(populate = 10L, remove = 0L)
    (work - chunk) shouldBe AchsWorkDistance(populate = 25L, remove = -5000L)
  }

  behavior of "drain"

  it should "produce no work ranges when below threshold" in {
    val state = AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 80L, lastPopulated = 60L),
    )
    val work = AchsWorkDistance(populate = 5L, remove = 5L)
    val (newState, remaining, ranges) = AchsMaintenancePipe.drain(
      aggregationThreshold = 10L,
      state = state,
      remaining = work,
      acc = Vector.empty,
    )
    ranges shouldBe empty
    newState shouldBe state
    remaining shouldBe work
  }

  it should "produce one work range when exactly at threshold for both" in {
    val state = AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 80L, lastPopulated = 60L),
    )
    val work = AchsWorkDistance(populate = 10L, remove = 10L)
    val (newState, remaining, ranges) = AchsMaintenancePipe.drain(
      aggregationThreshold = 10L,
      state = state,
      remaining = work,
      acc = Vector.empty,
    )
    ranges shouldBe Vector(workRange(popStart = 60L, popEnd = 70L, remStart = 80L, remEnd = 90L))
    newState shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 90L, lastPopulated = 70L),
    )
    remaining shouldBe AchsWorkDistance(populate = 0L, remove = 0L)
  }

  it should "produce one work range when exactly at threshold for one" in {
    val state = AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 80L, lastPopulated = 60L),
    )
    val work = AchsWorkDistance(populate = 9L, remove = 10L)
    val (newState, remaining, ranges) = AchsMaintenancePipe.drain(
      aggregationThreshold = 10L,
      state = state,
      remaining = work,
      acc = Vector.empty,
    )
    ranges shouldBe Vector(workRange(popStart = 60L, popEnd = 60L, remStart = 80L, remEnd = 90L))
    newState shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 90L, lastPopulated = 60L),
    )
    remaining shouldBe AchsWorkDistance(populate = 9L, remove = 0L)
  }

  it should "produce multiple work ranges when work is greater than a multiple of threshold" in {
    val state = AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
    )
    val work = AchsWorkDistance(populate = 25L, remove = 25L)
    val (newState, remaining, ranges) = AchsMaintenancePipe.drain(
      aggregationThreshold = 10L,
      state = state,
      remaining = work,
      acc = Vector.empty,
    )
    ranges shouldBe Vector(
      workRange(popStart = 0L, popEnd = 10L, remStart = 0L, remEnd = 10L),
      workRange(popStart = 10L, popEnd = 20L, remStart = 10L, remEnd = 20L),
    )
    newState shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 20L, lastPopulated = 20L),
    )
    remaining shouldBe AchsWorkDistance(populate = 5L, remove = 5L)
  }

  it should "produce multiple work ranges with non zero last pointers" in {
    val state = AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 42L, lastPopulated = 5L),
    )
    val work = AchsWorkDistance(populate = 35L, remove = 13L)
    val (newState, remaining, ranges) = AchsMaintenancePipe.drain(
      aggregationThreshold = 10L,
      state = state,
      remaining = work,
      acc = Vector.empty,
    )
    ranges shouldBe Vector(
      workRange(popStart = 5L, popEnd = 15L, remStart = 42L, remEnd = 52L),
      workRange(popStart = 15L, popEnd = 25L, remStart = 52L, remEnd = 52L),
      workRange(popStart = 25L, popEnd = 35L, remStart = 52L, remEnd = 52L),
    )
    newState shouldBe AchsState(
      validAt = 100L,
      lastPointers = AchsLastPointers(lastRemoved = 52L, lastPopulated = 35L),
    )
    remaining shouldBe AchsWorkDistance(populate = 5L, remove = 3L)
  }

  behavior of "bumpAchsValidAt"

  private def storeAchsValidAt(achsStateRef: AtomicReference[AchsState])(
      validAt: Long
  ): Future[Unit] =
    Future.successful {
      achsStateRef.updateAndGet(curr => curr.copy(validAt = validAt))
      ()
    }

  it should "bump the validAt" in {
    val state0 = AchsState(
      validAt = 30L,
      lastPointers = AchsLastPointers(lastRemoved = 30L, lastPopulated = 10L),
    )

    val achsStateRef = new AtomicReference[AchsState](state0)
    val achsStateCache = new AchsStateCache(loggerFactory)
    achsStateCache.set(state0)

    achsStateRef.get() shouldBe state0
    achsStateCache.get() shouldBe state0

    val inputWorkRange = workRange(
      popStart = 0L,
      popEnd = 0L,
      remStart = 10L,
      remEnd = 60L,
    )

    AchsMaintenancePipe
      .bumpAchsValidAt(
        storeAchsValidAt = storeAchsValidAt(achsStateRef),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue shouldBe inputWorkRange

    val state1 = AchsState(
      validAt = 60L,
      lastPointers = AchsLastPointers(lastRemoved = 30L, lastPopulated = 10L),
    )
    achsStateRef.get() shouldBe state1
    achsStateCache.get() shouldBe state1
  }

  it should "not bump the validAt if lagging behind" in {
    val currState = AchsState(
      validAt = 60L,
      lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
    )

    val achsStateRef = new AtomicReference[AchsState](currState)
    val achsStateCache = new AchsStateCache(loggerFactory)
    achsStateCache.set(currState)

    val inputWorkRange = workRange(
      popStart = 0L,
      popEnd = 0L,
      remStart = 10L,
      remEnd = 50L,
    )

    AchsMaintenancePipe
      .bumpAchsValidAt(
        storeAchsValidAt = storeAchsValidAt(achsStateRef),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue shouldBe inputWorkRange

    achsStateRef.get() shouldBe currState
    achsStateCache.get() shouldBe currState
  }

  behavior of "storeAchsLastPointersF"

  private val zeroLastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L)

  it should "update lastRemoved and lastPopulated when both are positive" in {
    val dbRef = new AtomicReference[AchsLastPointers](zeroLastPointers)
    val achsStateCache = new AchsStateCache(loggerFactory)
    val validAt = 150L
    achsStateCache.set(
      AchsState(
        validAt = validAt,
        lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
      )
    )

    val inputWorkRange = workRange(
      popStart = 65L,
      popEnd = 70L,
      remStart = 75L,
      remEnd = 80L,
    )

    AchsMaintenancePipe
      .storeAchsLastPointersF(
        persistAchsLastPointersF = lastPointers => Future.successful(dbRef.set(lastPointers)),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue

    dbRef.get() shouldBe AchsLastPointers(lastRemoved = 80L, lastPopulated = 70L)
    achsStateCache.get() shouldBe AchsState(
      validAt = validAt,
      lastPointers = AchsLastPointers(
        lastRemoved = 80L,
        lastPopulated = 70L,
      ),
    )
  }

  it should "skip when lastRemoved computes to zero" in {
    val dbRef = new AtomicReference[AchsLastPointers](zeroLastPointers)
    val achsStateCache = new AchsStateCache(loggerFactory)
    val validAt = 100L
    achsStateCache.set(
      AchsState(
        validAt = validAt,
        lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
      )
    )

    val inputWorkRange = workRange(
      popStart = 0L,
      popEnd = 0L,
      remStart = 0L,
      remEnd = 0L,
    )

    AchsMaintenancePipe
      .storeAchsLastPointersF(
        persistAchsLastPointersF = lastPointers => Future.successful(dbRef.set(lastPointers)),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue

    dbRef.get() shouldBe zeroLastPointers
    achsStateCache.get() shouldBe AchsState(
      validAt = validAt,
      lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
    )
  }

  it should "handle zero lastPopulated while lastRemoved is positive" in {
    val dbRef = new AtomicReference[AchsLastPointers](zeroLastPointers)
    val achsStateCache = new AchsStateCache(loggerFactory)
    val validAt = 100L
    achsStateCache.set(
      AchsState(
        validAt = validAt,
        lastPointers = AchsLastPointers(lastRemoved = 0L, lastPopulated = 0L),
      )
    )

    val inputWorkRange = workRange(
      popStart = 0L,
      popEnd = 0L,
      remStart = 0L,
      remEnd = 5L,
    )

    AchsMaintenancePipe
      .storeAchsLastPointersF(
        persistAchsLastPointersF = lastPointers => Future.successful(dbRef.set(lastPointers)),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue

    dbRef.get() shouldBe AchsLastPointers(lastRemoved = 5L, lastPopulated = 0L)
    achsStateCache.get() shouldBe
      AchsState(
        validAt = validAt,
        lastPointers = AchsLastPointers(lastRemoved = 5L, lastPopulated = 0L),
      )
  }

  behavior of "populateAchsActivations"

  private val zeroAddParams =
    AchsAddActivationsParams(startExclusive = 0L, endInclusive = 0L, activeAt = 0L)

  it should "populate activations in the correct range" in {
    val dbRef = new AtomicReference[AchsAddActivationsParams](zeroAddParams)

    val inputWorkRange = workRange(
      popStart = 20L,
      popEnd = 70L,
      remStart = 30L,
      remEnd = 80L,
    )

    AchsMaintenancePipe
      .populateAchsActivations(
        persistActivationsF = params => _ => Future.successful(dbRef.set(params)),
        logger = loggerFactory.getTracedLogger(this.getClass),
        executionContext = executionContext,
      )(inputWorkRange)
      .futureValue shouldBe inputWorkRange

    dbRef.get() shouldBe AchsAddActivationsParams(
      startExclusive = 20L,
      endInclusive = 70L,
      activeAt = 80L,
    )
  }

  it should "skip population when no sequenced events are in the batch" in {
    val dbRef = new AtomicReference[AchsAddActivationsParams](zeroAddParams)

    val inputWorkRange = workRange(
      popStart = 70L,
      popEnd = 70L,
      remStart = 80L,
      remEnd = 90L,
    )

    AchsMaintenancePipe
      .populateAchsActivations(
        persistActivationsF = params => _ => Future.successful(dbRef.set(params)),
        logger = loggerFactory.getTracedLogger(this.getClass),
        executionContext = executionContext,
      )(inputWorkRange)
      .futureValue shouldBe inputWorkRange

    dbRef.get() shouldBe zeroAddParams
  }

  behavior of "removeDeactivatedFromAchsStage"

  private val zeroRemoveParams = AchsRemoveDeactivatedParams(startExclusive = 0L, endInclusive = 0L)

  it should "remove deactivated entries in the correct range" in {
    val dbRef = new AtomicReference[AchsRemoveDeactivatedParams](zeroRemoveParams)

    val inputWorkRange = workRange(
      popStart = 20L,
      popEnd = 70L,
      remStart = 30L,
      remEnd = 80L,
    )

    AchsMaintenancePipe
      .removeDeactivatedFromAchs(
        removeDeactivatedF =
          params => (_: LoggingContextWithTrace) => Future.successful(dbRef.set(params)),
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue shouldBe inputWorkRange

    dbRef.get() shouldBe AchsRemoveDeactivatedParams(
      startExclusive = 30L,
      endInclusive = 80L,
    )
  }

  it should "skip removal when no sequenced events are in the batch" in {
    val dbRef = new AtomicReference[AchsRemoveDeactivatedParams](zeroRemoveParams)

    val inputWorkRange = workRange(
      popStart = 50L,
      popEnd = 70L,
      remStart = 80L,
      remEnd = 80L,
    )

    AchsMaintenancePipe
      .removeDeactivatedFromAchs(
        removeDeactivatedF =
          params => (_: LoggingContextWithTrace) => Future.successful(dbRef.set(params)),
        executionContext = executionContext,
        logger = loggerFactory.getTracedLogger(this.getClass),
      )(inputWorkRange)
      .futureValue shouldBe inputWorkRange

    dbRef.get() shouldBe zeroRemoveParams
  }

  behavior of "initialWork"

  it should "compute zero work when ACHS state and ledger end match config distances" in {
    val achsState =
      AchsState(validAt = 95, lastPointers = AchsLastPointers(lastRemoved = 90, lastPopulated = 80))
    val lastEventSeqId = 100L
    val config =
      AchsConfig(
        validAtDistanceTarget = NonNegativeLong.tryCreate(10L),
        lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(10L),
      )
    // populate = lastEventSeqId - validAtDistanceTarget - lastPopulatedDistanceTarget = (100 - 10 - 10) - 80 = 80 - 80 = 0
    // remove   = lastEventSeqId - validAtDistanceTarget = (100 - 10) - 90 = 90 - 90 = 0
    initialWork(
      achsState = achsState,
      lastEventSeqId = lastEventSeqId,
      achsConfig = config,
    ) shouldBe AchsWorkDistance(
      populate = 0L,
      remove = 0L,
    )
  }

  it should "compute positive work when ledger end is ahead of ACHS state" in {
    val achsState =
      AchsState(validAt = 55, lastPointers = AchsLastPointers(lastRemoved = 50, lastPopulated = 40))
    val lastEventSeqId = 100L
    val config =
      AchsConfig(
        validAtDistanceTarget = NonNegativeLong.tryCreate(10L),
        lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(10L),
      )
    // populate = (100 - 10 - 10) - 40 = 80 - 40 = 40
    // remove   = (100 - 10) - 50 = 90 - 50 = 40
    initialWork(
      achsState = achsState,
      lastEventSeqId = lastEventSeqId,
      achsConfig = config,
    ) shouldBe AchsWorkDistance(
      populate = 40L,
      remove = 40L,
    )
  }

  it should "compute negative work when ACHS state is ahead of distance targets" in {
    val achsState =
      AchsState(validAt = 55, lastPointers = AchsLastPointers(lastRemoved = 50, lastPopulated = 40))
    val lastEventSeqId = 100L
    val config =
      AchsConfig(
        validAtDistanceTarget = NonNegativeLong.tryCreate(70L),
        lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(20L),
      )
    // populate = (100 - 70 - 20) - 40 = 10 - 40 = -30
    // remove   = (100 - 70) - 50 = 30 - 50 = -20
    initialWork(
      achsState = achsState,
      lastEventSeqId = lastEventSeqId,
      achsConfig = config,
    ) shouldBe AchsWorkDistance(
      populate = -30,
      remove = -20,
    )
  }

}
