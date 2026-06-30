// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.ContractStateCaches.{
  applyContractStateEvent,
  computeKeyStateChange,
  resolveKeyUpdatesFor,
}
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ValueInt64
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicLong

class ContractStateCachesSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with OptionValues
    with TestEssentials
    with HasExecutionContext {
  behavior of classOf[ContractStateCaches].getSimpleName

  "build" should "set the cache index to the initialization index" in {
    val cacheInitializationEventSeqId = 1337L
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    val contractStateCaches = ContractStateCaches.build(
      cacheInitializationEventSeqId,
      maxContractsCacheSize = 1L,
      maxKeyCacheSize = 1L,
      metrics = LedgerApiServerMetrics.ForTesting,
      loggerFactory,
    )

    contractStateCaches.keyState.cacheEventSeqIdIndex shouldBe cacheInitializationEventSeqId
    contractStateCaches.contractState.cacheEventSeqIdIndex shouldBe cacheInitializationEventSeqId
  }

  "push" should "update the caches with a batch of events" in new TestScope {
    val previousCreate = createEvent(withKey = true)

    val create1 = createEvent(withKey = false)
    val create2 = createEvent(withKey = true)
    val archive1 = archiveEvent(create1)
    val archivedPrevious = archiveEvent(previousCreate)

    val batch = NonEmptyVector.of(create1, create2, archive1, archivedPrevious)

    val expectedContractStateUpdates = Map(
      create1.contractId -> ContractStateStatus.Archived,
      create2.contractId -> ContractStateStatus.Active,
      previousCreate.contractId -> ContractStateStatus.Archived,
    )
    val expectedKeyStateResults = Map[Key, Option[ContractKeyStateValue]](
      create2.globalKey.value -> Some(keyAssigned(create2)),
      previousCreate.globalKey.value -> None,
    )

    contractStateCaches.push(batch, 4)
    verify(contractStateCache).putBatch(4, expectedContractStateUpdates)

    val (capturedValidAt, capturedKeys, capturedChange) = captureChange()
    capturedValidAt shouldBe 4L
    capturedKeys shouldBe expectedKeyStateResults.keySet
    val (upserts, invalidations) = capturedChange(Map.empty)
    // Per-key expected outcome: Some -> in `upserts`, None -> in `invalidations`
    expectedKeyStateResults.foreach {
      case (key, Some(value)) => upserts.get(key) shouldBe Some(value)
      case (key, None) => invalidations should contain(key)
    }
  }

  "push" should "update the key state cache even if no key updates" in new TestScope {
    val create1 = createEvent(withKey = false)

    val batch = NonEmptyVector.of(create1)
    val expectedContractStateUpdates = Map(create1.contractId -> ContractStateStatus.Active)

    contractStateCaches.push(batch, 2)
    verify(contractStateCache).putBatch(2, expectedContractStateUpdates)
    val (capturedValidAt, capturedKeys, capturedChange) = captureChange()
    capturedValidAt shouldBe 2L
    capturedKeys shouldBe Set.empty[Key]
    capturedChange(Map.empty) shouldBe (Map.empty, Set.empty)
  }

  "push" should "only update contract state cache for Activated events with isInitial=true" in new TestScope {
    val initialCreate = createEvent(withKey = false)
    val nonInitialCreate = createEvent(withKey = false).copy(isInitial = false)

    val batch = NonEmptyVector.of(initialCreate, nonInitialCreate)

    contractStateCaches.push(eventsBatch = batch, lastEventSeqId = 3)

    // Only the initial create should appear in contract state updates
    val expectedContractStateUpdates = Map(
      initialCreate.contractId -> ContractStateStatus.Active
    )
    verify(contractStateCache).putBatch(validAtEventSeqId = 3, batch = expectedContractStateUpdates)
  }

  "push" should "only update contract state cache for Deactivated events with isFinal=true" in new TestScope {
    val create1 = createEvent(withKey = false)
    val create2 = createEvent(withKey = false)
    val nonFinalArchive = archiveEvent(create1).copy(isFinal = false)
    val finalArchive = archiveEvent(create2)

    val batch = NonEmptyVector.of(nonFinalArchive, finalArchive)

    contractStateCaches.push(batch, 4)

    // Only the final archive should appear in contract state updates
    val expectedContractStateUpdates = Map(
      create2.contractId -> ContractStateStatus.Archived
    )
    verify(contractStateCache).putBatch(4, expectedContractStateUpdates)
  }

  "push" should "not touch the key state cache when no events carry a key" in new TestScope {
    val keyless1 = createEvent(withKey = false)
    val keyless2 = archiveEvent(keyless1)
    val batch = NonEmptyVector.of(keyless1, keyless2)

    contractStateCaches.push(batch, lastEventSeqId = 7)

    // The contract state cache must still be updated.
    val expectedContractStateUpdates = Map(
      keyless1.contractId -> ContractStateStatus.Archived
    )
    verify(contractStateCache).putBatch(7, expectedContractStateUpdates)

    // The key state cache must observe an empty change for an empty key set.
    val (capturedValidAt, capturedKeys, capturedChange) = captureChange()
    capturedValidAt shouldBe 7L
    capturedKeys shouldBe Set.empty[Key]
    capturedChange(Map.empty) shouldBe (Map.empty, Set.empty)
  }

  "push" should "group multiple events for the same key and apply them in order through the change function" in new TestScope {
    val activate1 = createEvent(withKey = true)
    val sameKey = activate1.globalKey.value
    val activate2 = createEventWithKey(Some(sameKey))
    val deactivate1 = archiveEvent(activate1)

    val batch = NonEmptyVector.of(activate1, activate2, deactivate1)

    contractStateCaches.push(batch, lastEventSeqId = 10)

    val (capturedValidAt, capturedKeys, capturedChange) = captureChange()
    capturedValidAt shouldBe 10L
    capturedKeys shouldBe Set(sameKey)

    // Starting from a cache miss, the events are folded in the order they appeared in the batch.
    // activate1: None -> Last(cid1, _, true)
    // activate2: Last(cid1, _, true) -> Last(cid2, _, true)
    // deactivate1 (targets cid1 at activate1.eventSequentialId): cid does not match cid2 (different seqId)
    //   -> Last(cid2, _, true) stays
    val (upserts, invalidations) = capturedChange(Map.empty)
    upserts shouldBe Map[Key, ContractKeyStateValue](
      sameKey -> ContractKeyStateValue.Last(
        contractId = activate2.contractId,
        eventSequentialId = activate2.eventSequentialId,
        thereMightBeMore = true,
      )
    )
    invalidations shouldBe empty
  }

  "push" should "request only keys carried by events from the cache when building the change" in new TestScope {
    val withKey1 = createEvent(withKey = true)
    val withKey2 = createEvent(withKey = true)
    val keyless = createEvent(withKey = false)
    val batch = NonEmptyVector.of(withKey1, keyless, withKey2)

    contractStateCaches.push(batch, lastEventSeqId = 8)

    val (_, capturedKeys, _) = captureChange()
    capturedKeys shouldBe Set(withKey1.globalKey.value, withKey2.globalKey.value)
  }

  "push" should "propagate lastEventSeqId to both caches in a mixed batch" in new TestScope {
    val create = createEvent(withKey = true)
    val assign = createEvent(withKey = false).copy(isInitial = false)
    val unassign = archiveEvent(createEvent(withKey = true)).copy(isFinal = false)
    val archive = archiveEvent(createEvent(withKey = false))

    val batch = NonEmptyVector.of(create, assign, unassign, archive)

    contractStateCaches.push(batch, lastEventSeqId = 99)

    // Contract state cache: only initial activations (isInitial=true) and final
    // deactivations (isFinal=true) are forwarded.
    val expectedContractStateUpdates = Map(
      create.contractId -> ContractStateStatus.Active,
      archive.contractId -> ContractStateStatus.Archived,
    )
    verify(contractStateCache).putBatch(99, expectedContractStateUpdates)

    // Key state cache: receives the same lastEventSeqId.
    val (capturedValidAt, capturedKeys, _) = captureChange()
    capturedValidAt shouldBe 99L
    capturedKeys shouldBe Set(create.globalKey.value, unassign.globalKey.value)
  }

  "reset" should "reset the caches on `reset`" in new TestScope {
    private val someOffset = Some(
      LedgerEnd(
        lastOffset = Offset.tryFromLong(112243L),
        lastEventSeqId = 125,
        lastStringInterningId = 0,
        lastPublicationTime = CantonTimestamp.MinValue,
        synchronizerIndices = Map.empty,
      )
    )

    contractStateCaches.reset(someOffset)
    verify(keyStateCache).reset(125)
    verify(contractStateCache).reset(125)
  }

  "computeKeyStateChange" should "return empty for empty input" in {
    val (upserts, invalidations) =
      computeKeyStateChange(
        keyEvents = Map.empty,
        cached = Map.empty,
        logger = logger,
      )
    upserts shouldBe empty
    invalidations shouldBe empty
  }

  it should "return empty when keyEvents is empty even if cached is non-empty" in {
    val cachedKey: Key = key(777L)
    val cachedValue: ContractKeyStateValue.Last =
      ContractKeyStateValue.Last(contractId(7L), 7L, thereMightBeMore = true)

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map.empty,
      cached = Map(
        cachedKey -> cachedValue,
        key(778L) -> ContractKeyStateValue.Empty,
      ),
      logger = logger,
    )

    upserts shouldBe empty
    invalidations shouldBe empty
  }

  it should "ignore cached entries for keys not present in keyEvents" in new TestScope {
    val keyEvent: ContractStateEvent.Activated = createEvent(withKey = true)
    val unrelatedKey: Key = key(999L)
    val unrelatedValue: ContractKeyStateValue.Last =
      ContractKeyStateValue.Last(
        contractId = contractId(42L),
        eventSequentialId = 42L,
        thereMightBeMore = false,
      )

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(keyEvent.globalKey.value -> Vector(keyEvent)),
      cached = Map(
        keyEvent.globalKey.value -> ContractKeyStateValue.Empty,
        unrelatedKey -> unrelatedValue,
      ),
      logger = logger,
    )

    upserts shouldBe Map[Key, ContractKeyStateValue](
      keyEvent.globalKey.value -> ContractKeyStateValue.Last(
        keyEvent.contractId,
        keyEvent.eventSequentialId,
        thereMightBeMore = false,
      )
    )
    invalidations shouldBe empty
    upserts.keySet should not contain unrelatedKey
    invalidations should not contain unrelatedKey
  }

  it should "route unrelated keys to independent outputs" in new TestScope {
    val keyAEvent: ContractStateEvent.Activated = createEvent(withKey = true)
    val keyBEvent: ContractStateEvent.Activated = createEvent(withKey = true)

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(
        keyAEvent.globalKey.value -> Vector(keyAEvent),
        keyBEvent.globalKey.value -> Vector(keyBEvent),
      ),
      cached = Map.empty,
      logger = logger,
    )

    invalidations shouldBe empty
    upserts shouldBe Map[Key, ContractKeyStateValue](
      keyAEvent.globalKey.value -> ContractKeyStateValue.Last(
        keyAEvent.contractId,
        keyAEvent.eventSequentialId,
        thereMightBeMore = true,
      ),
      keyBEvent.globalKey.value -> ContractKeyStateValue.Last(
        keyBEvent.contractId,
        keyBEvent.eventSequentialId,
        thereMightBeMore = true,
      ),
    )
  }

  it should "handle two activations for two different keys, one cached and one on a cache miss" in new TestScope {
    val cachedKeyEvent: ContractStateEvent.Activated = createEvent(withKey = true)
    val missKeyEvent: ContractStateEvent.Activated = createEvent(withKey = true)

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(
        cachedKeyEvent.globalKey.value -> Vector(cachedKeyEvent),
        missKeyEvent.globalKey.value -> Vector(missKeyEvent),
      ),
      // Only the first key is present in the cache.
      cached = Map(cachedKeyEvent.globalKey.value -> ContractKeyStateValue.Empty),
      logger = logger,
    )

    invalidations shouldBe empty
    upserts shouldBe Map[Key, ContractKeyStateValue](
      // Cache hit on Empty -> thereMightBeMore = false.
      cachedKeyEvent.globalKey.value -> ContractKeyStateValue.Last(
        cachedKeyEvent.contractId,
        cachedKeyEvent.eventSequentialId,
        thereMightBeMore = false,
      ),
      // Cache miss -> thereMightBeMore = true.
      missKeyEvent.globalKey.value -> ContractKeyStateValue.Last(
        missKeyEvent.contractId,
        missKeyEvent.eventSequentialId,
        thereMightBeMore = true,
      ),
    )
  }

  it should "guarantee upserts and invalidations are disjoint across mixed scenarios" in new TestScope {
    // Key A: Activate then Archive (invalidation from cache miss).
    val aActivate: ContractStateEvent.Activated = createEvent(withKey = true)
    val aArchive: ContractStateEvent.Deactivated = archiveEvent(aActivate)
    // Key B: plain Activate (upsert).
    val bActivate: ContractStateEvent.Activated = createEvent(withKey = true)
    // Key C: Archive only on a cache miss (invalidation: deactivate on None -> None).
    val cActivate: ContractStateEvent.Activated = createEvent(withKey = true)
    val cArchive: ContractStateEvent.Deactivated = archiveEvent(cActivate)

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(
        aActivate.globalKey.value -> Vector(aActivate, aArchive),
        bActivate.globalKey.value -> Vector(bActivate),
        cArchive.globalKey.value -> Vector(cArchive),
      ),
      cached = Map.empty,
      logger = logger,
    )

    (upserts.keySet intersect invalidations) shouldBe empty
    // Key A: Activate on miss -> Last(_, _, true); matching Archive with thereMightBeMore=true -> invalidate.
    // Key B: Activate on miss -> Last(_, _, true).
    // Key C: Archive on miss -> None -> invalidate.
    upserts shouldBe Map[Key, ContractKeyStateValue](
      bActivate.globalKey.value -> ContractKeyStateValue.Last(
        bActivate.contractId,
        bActivate.eventSequentialId,
        thereMightBeMore = true,
      )
    )
    invalidations shouldBe Set(aActivate.globalKey.value, cArchive.globalKey.value)
  }

  "computeKeyStateChange (within-batch)" should "handle Create(C1), Archive(C1) on cache miss as invalidation" in new TestScope {
    val c1: ContractStateEvent.Activated = createEvent(withKey = true)
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(c1.globalKey.value -> Vector(c1, a1)),
      cached = Map.empty,
      logger = logger,
    )
    // Activate -> Last(_, _, true). Deactivate matching with thereMightBeMore=true -> invalidate.
    upserts shouldBe empty
    invalidations shouldBe Set(c1.globalKey.value)
  }

  it should "handle Create(C1), Archive(C1) on Some(Empty) as upsert Empty" in new TestScope {
    val c1: ContractStateEvent.Activated = createEvent(withKey = true)
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(c1.globalKey.value -> Vector(c1, a1)),
      cached = Map(c1.globalKey.value -> ContractKeyStateValue.Empty),
      logger = logger,
    )
    // Activate on Empty -> Last(_, _, false). Deactivate matching with thereMightBeMore=false -> Empty.
    upserts shouldBe Map[Key, ContractKeyStateValue](
      c1.globalKey.value -> ContractKeyStateValue.Empty
    )
    invalidations shouldBe empty
  }

  it should "handle Create(C1), Create(C2), Archive(C2) on cache miss as invalidation" in new TestScope {
    val sharedKey: Key = key(43L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))
    val a2: ContractStateEvent.Deactivated = archiveEvent(c2)

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(sharedKey -> Vector(c1, c2, a2)),
      cached = Map.empty,
      logger = logger,
    )
    // Last(C1, _, true) -> Last(C2, _, true) -> invalidate (matching cid, thereMightBeMore=true).
    upserts shouldBe empty
    invalidations shouldBe Set(sharedKey)
  }

  it should "handle Create(C1), Create(C2), Archive(C1) on cache miss as upsert Last(C2, _, true)" in new TestScope {
    val sharedKey: Key = key(42L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(sharedKey -> Vector(c1, c2, a1)),
      cached = Map.empty,
      logger = logger,
    )
    // Last(C1, _, true) -> Last(C2, _, true) -> non-matching deactivate keeps Last(C2, _, true).
    upserts shouldBe Map[Key, ContractKeyStateValue](
      sharedKey -> ContractKeyStateValue.Last(
        c2.contractId,
        c2.eventSequentialId,
        thereMightBeMore = true,
      )
    )
    invalidations shouldBe empty
  }

  it should "handle evict-then-reactivate (Create C1, Archive C1, Create C2) on cache miss" in new TestScope {
    val sharedKey: Key = key(77L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(sharedKey -> Vector(c1, a1, c2)),
      cached = Map.empty,
      logger = logger,
    )
    // Last(C1, _, true) -> None (intermediate invalidation) -> Last(C2, _, true) on cache miss.
    // Final result is Some -> upsert; same key does not also appear in invalidations.
    upserts shouldBe Map[Key, ContractKeyStateValue](
      sharedKey -> ContractKeyStateValue.Last(
        c2.contractId,
        c2.eventSequentialId,
        thereMightBeMore = true,
      )
    )
    invalidations shouldBe empty
  }

  it should "handle evict-then-reactivate (Create C1, Archive C1, Create C2) on Some(Empty)" in new TestScope {
    val sharedKey: Key = key(78L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(sharedKey))

    val (upserts, invalidations) = computeKeyStateChange(
      keyEvents = Map(sharedKey -> Vector(c1, a1, c2)),
      cached = Map(sharedKey -> ContractKeyStateValue.Empty),
      logger = logger,
    )
    // Last(C1, _, false) -> Empty -> Last(C2, _, false). Final is Some -> upsert.
    upserts shouldBe Map[Key, ContractKeyStateValue](
      sharedKey -> ContractKeyStateValue.Last(
        c2.contractId,
        c2.eventSequentialId,
        thereMightBeMore = false,
      )
    )
    invalidations shouldBe empty
  }

  "applyContractStateEvent on Activated" should "upsert Last(thereMightBeMore=true) on cache miss" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    applyContractStateEvent(current = None, event = c, logger = logger) shouldBe
      Some(ContractKeyStateValue.Last(c.contractId, c.eventSequentialId, thereMightBeMore = true))
  }

  it should "upsert Last(thereMightBeMore=false) when cache holds Empty" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    applyContractStateEvent(
      current = Some(ContractKeyStateValue.Empty),
      event = c,
      logger = logger,
    ) shouldBe
      Some(ContractKeyStateValue.Last(c.contractId, c.eventSequentialId, thereMightBeMore = false))
  }

  it should "upsert Last(thereMightBeMore=true) when cache holds Last(otherCid, otherSeqId, false)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val otherCid: ContractId = contractId(999)
    val otherSeqId: Long = c.eventSequentialId + 1
    applyContractStateEvent(
      current = Some(ContractKeyStateValue.Last(otherCid, otherSeqId, thereMightBeMore = false)),
      event = c,
      logger = logger,
    ) shouldBe
      Some(ContractKeyStateValue.Last(c.contractId, c.eventSequentialId, thereMightBeMore = true))
  }

  it should "upsert Last(thereMightBeMore=true) when cache holds Last(otherCid, otherSeqId, true)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val otherCid: ContractId = contractId(999)
    val otherSeqId: Long = c.eventSequentialId + 1
    applyContractStateEvent(
      current = Some(ContractKeyStateValue.Last(otherCid, otherSeqId, thereMightBeMore = true)),
      event = c,
      logger = logger,
    ) shouldBe
      Some(ContractKeyStateValue.Last(c.contractId, c.eventSequentialId, thereMightBeMore = true))
  }

  it should "invalidate (and warn) when cache already holds Last with same eventSeqId (thereMightBeMore=false)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    loggerFactory.assertLogs(
      within = applyContractStateEvent(
        current = Some(
          ContractKeyStateValue.Last(contractId(999), c.eventSequentialId, thereMightBeMore = false)
        ),
        event = c,
        logger = logger,
      ) shouldBe None,
      assertions = _.warningMessage should include("skipping update"),
    )
  }

  it should "invalidate (and warn) when cache already holds Last with same eventSeqId (thereMightBeMore=true)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    loggerFactory.assertLogs(
      within = applyContractStateEvent(
        current = Some(
          ContractKeyStateValue.Last(contractId(999), c.eventSequentialId, thereMightBeMore = true)
        ),
        event = c,
        logger = logger,
      ) shouldBe None,
      assertions = _.warningMessage should include("skipping update"),
    )
  }

  it should "use the passed cid and eventSeqId in the upserted Last value (not the cached ones)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val otherCid: ContractId = contractId(12345)
    val cachedSeqId: Long = c.eventSequentialId + 999
    applyContractStateEvent(
      current = Some(ContractKeyStateValue.Last(otherCid, cachedSeqId, thereMightBeMore = true)),
      event = c,
      logger = logger,
    ) shouldBe Some(
      ContractKeyStateValue.Last(c.contractId, c.eventSequentialId, thereMightBeMore = true)
    )
  }

  "applyContractStateEvent on Deactivated" should "invalidate on cache miss (no upsert, no warning)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    applyContractStateEvent(
      current = None,
      event = a,
      logger = logger,
    ) shouldBe None
  }

  it should "keep Empty (and warn) when cache already holds Empty" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    loggerFactory.assertLogs(
      within = applyContractStateEvent(
        current = Some(ContractKeyStateValue.Empty),
        event = a,
        logger = logger,
      ) shouldBe Some(ContractKeyStateValue.Empty),
      assertions = _.warningMessage should include(
        s"Deactivation with deactivatedEventSequentialId=${a.deactivatedEventSequentialId} encountered, " +
          s"but this entry is already deactivated in the cache"
      ),
    )
  }

  it should "transition Last(matching, _, false) to Empty" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    applyContractStateEvent(
      current = Some(
        ContractKeyStateValue.Last(
          c.contractId,
          a.deactivatedEventSequentialId,
          thereMightBeMore = false,
        )
      ),
      event = a,
      logger = logger,
    ) shouldBe Some(ContractKeyStateValue.Empty)
  }

  it should "invalidate Last(matching, _, true)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    applyContractStateEvent(
      current = Some(
        ContractKeyStateValue.Last(
          c.contractId,
          a.deactivatedEventSequentialId,
          thereMightBeMore = true,
        )
      ),
      event = a,
      logger = logger,
    ) shouldBe None
  }

  it should "keep Last(non-matching, _, true) unchanged" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    val cached: ContractKeyStateValue.Last =
      ContractKeyStateValue.Last(
        c.contractId,
        a.deactivatedEventSequentialId + 1,
        thereMightBeMore = true,
      )
    applyContractStateEvent(
      current = Some(cached),
      event = a,
      logger = logger,
    ) shouldBe Some(cached)
  }

  it should "keep Last(non-matching, _, false) unchanged (and warn)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    val cached: ContractKeyStateValue.Last =
      ContractKeyStateValue.Last(
        c.contractId,
        a.deactivatedEventSequentialId + 1,
        thereMightBeMore = false,
      )
    loggerFactory.assertLogs(
      within = applyContractStateEvent(
        current = Some(cached),
        event = a,
        logger = logger,
      ) shouldBe Some(cached),
      assertions = _.warningMessage should include(
        s"Deactivation with deactivatedEventSequentialId=${a.deactivatedEventSequentialId} on cache entry assigned to " +
          s"contract ${c.contractId} with sequentialId=${a.deactivatedEventSequentialId + 1} while thereMightBeMore was false"
      ),
    )
  }

  "resolveKeyUpdatesFor" should "return the initial value unchanged for an empty event vector" in new TestScope {
    val cached: ContractKeyStateValue.Last =
      ContractKeyStateValue.Last(contractId(1), 1L, thereMightBeMore = true)
    resolveKeyUpdatesFor(Vector.empty, Some(cached), logger) shouldBe Some(cached)
    resolveKeyUpdatesFor(Vector.empty, None, logger) shouldBe None
    resolveKeyUpdatesFor(
      Vector.empty,
      Some(ContractKeyStateValue.Empty),
      logger,
    ) shouldBe Some(ContractKeyStateValue.Empty)
  }

  it should "handle a single on cache miss" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    resolveKeyUpdatesFor(Vector(c), None, logger) shouldBe
      Some(ContractKeyStateValue.Last(c.contractId, c.eventSequentialId, thereMightBeMore = true))
  }

  it should "handle a single Deactivated on cache miss" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    resolveKeyUpdatesFor(Vector(a), None, logger) shouldBe None
  }

  it should "invalidate on [Activate(C1), Archive(C1)] starting from cache miss" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    resolveKeyUpdatesFor(Vector(c, a), None, logger) shouldBe None
  }

  it should "settle to Empty on [Activate(C1), Archive(C1)] starting from Some(Empty)" in new TestScope {
    val c: ContractStateEvent.Activated = createEvent(withKey = true)
    val a: ContractStateEvent.Deactivated = archiveEvent(c)
    resolveKeyUpdatesFor(
      Vector(c, a),
      Some(ContractKeyStateValue.Empty),
      logger,
    ) shouldBe Some(ContractKeyStateValue.Empty)
  }

  it should "settle to Last(C2, _, true) on [Activate(C1), Activate(C2)] starting from cache miss" in new TestScope {
    val shared: Key = key(100L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    resolveKeyUpdatesFor(Vector(c1, c2), None, logger) shouldBe
      Some(ContractKeyStateValue.Last(c2.contractId, c2.eventSequentialId, thereMightBeMore = true))
  }

  it should "settle to Last(C2, _, true) on [Activate(C1), Activate(C2), Archive(C1)] starting from cache miss" in new TestScope {
    val shared: Key = key(101L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    resolveKeyUpdatesFor(Vector(c1, c2, a1), None, logger) shouldBe
      Some(ContractKeyStateValue.Last(c2.contractId, c2.eventSequentialId, thereMightBeMore = true))
  }

  it should "settle to Last(C2, _, true) on [Activate(C1), Activate(C2), Archive(C1)] starting from Some(Empty)" in new TestScope {
    val shared: Key = key(105L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    resolveKeyUpdatesFor(
      Vector(c1, c2, a1),
      Some(ContractKeyStateValue.Empty),
      logger,
    ) shouldBe
      Some(ContractKeyStateValue.Last(c2.contractId, c2.eventSequentialId, thereMightBeMore = true))
  }

  it should "invalidate on [Activate(C1), Activate(C2), Archive(C2)] starting from cache miss" in new TestScope {
    val shared: Key = key(102L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val a2: ContractStateEvent.Deactivated = archiveEvent(c2)
    resolveKeyUpdatesFor(Vector(c1, c2, a2), None, logger) shouldBe None
  }

  it should "recover from mid-fold invalidation: [Activate(C1), Archive(C1), Activate(C2)] starting from cache miss" in new TestScope {
    val shared: Key = key(103L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    resolveKeyUpdatesFor(Vector(c1, a1, c2), None, logger) shouldBe
      Some(ContractKeyStateValue.Last(c2.contractId, c2.eventSequentialId, thereMightBeMore = true))
  }

  it should "preserve authoritative state through evict-then-reactivate starting from Some(Empty)" in new TestScope {
    val shared: Key = key(104L)
    val c1: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val c2: ContractStateEvent.Activated = createEventWithKey(Some(shared))
    // Last(C1, _, false) -> Empty -> Last(C2, _, false)
    resolveKeyUpdatesFor(
      Vector(c1, a1, c2),
      Some(ContractKeyStateValue.Empty),
      logger,
    ) shouldBe
      Some(
        ContractKeyStateValue.Last(c2.contractId, c2.eventSequentialId, thereMightBeMore = false)
      )
  }

  it should "leave None unchanged across multiple deactivations on cache miss" in new TestScope {
    val c1: ContractStateEvent.Activated = createEvent(withKey = true)
    val a1: ContractStateEvent.Deactivated = archiveEvent(c1)
    val c2: ContractStateEvent.Activated = createEvent(withKey = true)
    val a2: ContractStateEvent.Deactivated = archiveEvent(c2)
    resolveKeyUpdatesFor(Vector(a1, a2), None, logger) shouldBe None
  }

  "push" should "see cached values produced by a previous batch" in new TestScope {
    val create1: ContractStateEvent.Activated = createEvent(withKey = true)
    val archive1: ContractStateEvent.Deactivated = archiveEvent(create1)

    // First batch: a single activation
    contractStateCaches.push(NonEmptyVector.of(create1), lastEventSeqId = 5)
    val firstUpsert: ContractKeyStateValue =
      ContractKeyStateValue.Last(
        create1.contractId,
        create1.eventSequentialId,
        thereMightBeMore = true,
      )

    // Second batch: the deactivation for the same key.
    // We push it under a different lastEventSeqId so we can disambiguate captures.
    contractStateCaches.push(NonEmptyVector.of(archive1), lastEventSeqId = 10)

    val secondChangeCaptor: ArgumentCaptor[KeyStateChange] =
      ArgumentCaptor.forClass(classOf[KeyStateChange])
    verify(keyStateCache).putBatchCond(
      eqTo(10L),
      eqTo(Set(create1.globalKey.value)),
      secondChangeCaptor.capture(),
    )(any[TraceContext])
    val secondChange: KeyStateChange = secondChangeCaptor.getValue

    // Simulate the cache returning what the first batch upserted.
    val (upserts, invalidations) =
      secondChange(Map(create1.globalKey.value -> firstUpsert))
    // matching seq id with thereMightBeMore=true -> invalidate
    upserts shouldBe Map.empty[Key, ContractKeyStateValue]
    invalidations shouldBe Set(create1.globalKey.value)
  }

  private trait TestScope {
    private val contractIdx: AtomicLong = new AtomicLong(0)
    private val keyIdx: AtomicLong = new AtomicLong(0)

    val keyStateCache: StateCache[Key, ContractKeyStateValue] =
      mock[StateCache[Key, ContractKeyStateValue]]
    val contractStateCache: StateCache[ContractId, ContractStateStatus] =
      mock[StateCache[ContractId, ContractStateStatus]]

    val contractStateCaches = new ContractStateCaches(
      keyStateCache,
      contractStateCache,
      loggerFactory,
    )

    def createEvent(
        withKey: Boolean
    ): ContractStateEvent.Activated = {
      val cId = contractId(contractIdx.incrementAndGet())
      val templateId = Identifier.assertFromString(s"some:template:name")
      val packageName = Ref.PackageName.assertFromString("pkg-name")
      val keyValue = keyIdx.incrementAndGet()
      val key = Option.when(withKey)(
        Key.assertBuild(
          templateId,
          packageName,
          ValueInt64(keyValue),
          crypto.Hash.hashPrivateKey(keyValue.toString),
        )
      )
      ContractStateEvent.Activated(
        contractId = cId,
        globalKey = key,
        eventSequentialId = contractIdx.get(),
        isInitial = true,
      )
    }

    def archiveEvent(
        create: ContractStateEvent.Activated
    ): ContractStateEvent.Deactivated =
      ContractStateEvent.Deactivated(
        contractId = create.contractId,
        globalKey = create.globalKey,
        deactivatedEventSequentialId = create.eventSequentialId,
        isFinal = true,
      )

    def createEventWithKey(
        key: Option[Key]
    ): ContractStateEvent.Activated = {
      val cId: ContractId = contractId(contractIdx.incrementAndGet())
      ContractStateEvent.Activated(
        contractId = cId,
        globalKey = key,
        eventSequentialId = contractIdx.get(),
        isInitial = true,
      )
    }

    def captureChange(): (Long, Set[Key], KeyStateChange) = {
      val validAtCaptor: ArgumentCaptor[java.lang.Long] =
        ArgumentCaptor.forClass(classOf[java.lang.Long])
      val keysCaptor: ArgumentCaptor[Set[Key]] =
        ArgumentCaptor.forClass(classOf[Set[Key]])
      val changeCaptor: ArgumentCaptor[KeyStateChange] =
        ArgumentCaptor.forClass(classOf[KeyStateChange])
      verify(keyStateCache).putBatchCond(
        validAtCaptor.capture(),
        keysCaptor.capture(),
        changeCaptor.capture(),
      )(any[TraceContext])
      (validAtCaptor.getValue, keysCaptor.getValue, changeCaptor.getValue)
    }
  }

  private def keyAssigned(create: ContractStateEvent.Activated) =
    ContractKeyStateValue.Last(
      create.contractId,
      create.eventSequentialId,
      thereMightBeMore = true,
    )

  private def contractId(id: Long): ContractId =
    ContractId.V1(Hash.hashPrivateKey(id.toString))

  private def key(value: Long): Key =
    Key.assertBuild(
      Identifier.assertFromString("some:template:name"),
      Ref.PackageName.assertFromString("pkg-name"),
      ValueInt64(value),
      crypto.Hash.hashPrivateKey(value.toString),
    )

  type KeyStateChange =
    Map[Key, ContractKeyStateValue] => (Map[Key, ContractKeyStateValue], Set[Key])
}
