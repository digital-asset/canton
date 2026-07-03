// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import cats.data.NonEmptyVector
import com.daml.ledger.resources.Resource
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.ledger.participant.state.index.{
  ContractKeyPage,
  ContractState,
  ContractStateStatus,
}
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  SuppressionRule,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.cache.ContractKeyStateValue.Last
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStore.resolveFromCache
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStoreSpec.*
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.{
  ContractRef,
  KeyLookupPageResult,
  LedgerDaoContractsReader,
}
import com.digitalasset.canton.platform.store.{LedgerApiContractStore, LedgerApiContractStoreImpl}
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{HasExecutionContext, TestEssentials}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueText}
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

class MutableCacheBackedContractStoreSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with TestEssentials
    with HasExecutionContext {

  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  "push" should {
    "update the contract state caches" in {
      val contractStateCaches = mock[ContractStateCaches]
      val contractStore = new MutableCacheBackedContractStore(
        contractsReader = mock[LedgerDaoContractsReader],
        contractStateCaches = contractStateCaches,
        loggerFactory = loggerFactory,
        contractStore = mock[LedgerApiContractStore],
        ledgerEndCache = MutableLedgerEndCache(),
        maxLookupLimit = 10,
      )

      val event1 = ContractStateEvent.Deactivated(
        contractId = ContractId.V1(Hash.hashPrivateKey("cid")),
        globalKey = None,
        deactivatedEventSequentialId = 1L,
        isFinal = true,
      )
      val event2 = event1
      val updateBatch = NonEmptyVector.of(event1, event2)

      contractStore.contractStateCaches.push(updateBatch, 10)
      verify(contractStateCaches).push(updateBatch, 10)

      succeed
    }
  }

  "lookupContractKey" should {
    "cap the limit and log when the requested limit exceeds the configured max" in {
      val maxLimit = 5
      val requestedLimit = 20
      val contractsReader = mock[LedgerDaoContractsReader]
      val mockContractStore = mock[LedgerApiContractStore]
      val ledgerEndCache = MutableLedgerEndCache()

      when(
        contractsReader.lookupNonUniqueKey(any[Key], any[Long], any[Option[Long]], any[Int])(
          any[LoggingContextWithTrace]
        )
      ).thenReturn(
        Future.successful(
          KeyLookupPageResult(contracts = Vector.empty, nextPageToken = None)
        )
      )

      when(
        mockContractStore.lookupBatchedContractIdsNonReadThrough(any[Vector[Long]])(
          any[TraceContext]
        )
      )
        .thenReturn(Future.successful(Map.empty[Long, ContractId]))

      val mockKeyStateCache = mock[StateCache[Key, ContractKeyStateValue]]
      when(mockKeyStateCache.get(any[Key])(any[TraceContext])).thenReturn(None)
      when(
        mockKeyStateCache.putAsync(any[Key], any[Long => Future[ContractKeyStateValue]])(
          any[TraceContext]
        )
      )
        .thenAnswer((_: Key, fetchAsync: Long => Future[ContractKeyStateValue], _: TraceContext) =>
          fetchAsync(0L)
        )
      val mockContractStateCaches = mock[ContractStateCaches]
      when(mockContractStateCaches.keyState).thenReturn(mockKeyStateCache)

      val store = new MutableCacheBackedContractStore(
        contractsReader = contractsReader,
        contractStateCaches = mockContractStateCaches,
        loggerFactory = loggerFactory,
        contractStore = mockContractStore,
        ledgerEndCache = ledgerEndCache,
        maxLookupLimit = maxLimit,
      )

      loggerFactory.assertLogs(SuppressionRule.Level(Level.INFO))(
        store
          .lookupContractKey(
            readers = Set(party("alice")),
            key = globalKey("some-key"),
            pageToken = None,
            limit = requestedLimit,
          )
          .futureValue,
        _.infoMessage should include(
          s"Lookup limit $requestedLimit exceeds configured cap of $maxLimit"
        ),
      )

      // Verify the capped limit was passed to the reader
      verify(contractsReader).lookupNonUniqueKey(
        any[Key],
        any[Long],
        eqTo(None),
        eqTo(maxLimit),
      )(any[LoggingContextWithTrace])

      succeed
    }

    "return empty result when cache has Unassigned" in {
      for {
        store <- contractStore(cachesSize = 1L, loggerFactory).asFuture
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId2,
          Map(someKey -> ContractKeyStateValue.Empty),
        )
        result <- store.lookupContractKey(Set(alice), someKey, pageToken = None, limit = 1)
        resultLimit10 <- store.lookupContractKey(
          Set(alice),
          someKey,
          pageToken = None,
          limit = 10,
        )
      } yield {
        result shouldBe ContractKeyPage(contracts = Vector.empty, nextPageToken = None)
        resultLimit10 shouldBe result
      }
    }

    "return single page when cache has Last(_, _, false) and no token" in {
      for {
        store <- contractStore(cachesSize = 1L, loggerFactory).asFuture
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId2,
          Map(
            someKey -> ContractKeyStateValue.Last(cId_1, eventSeqId1, thereMightBeMore = false)
          ),
        )
        result <- store.lookupContractKey(Set(alice), someKey, pageToken = None, limit = 1)
        resultLimit10 <- store.lookupContractKey(
          Set(alice),
          someKey,
          pageToken = None,
          limit = 10,
        )
      } yield {
        result.contracts.map(_.templateId) shouldBe Vector(contract1.inst.templateId)
        result.nextPageToken shouldBe None
        resultLimit10 shouldBe result
      }
    }

    "return contract when cache has Last(_, seqId, false) and token > seqId" in {
      for {
        store <- contractStore(cachesSize = 1L, loggerFactory).asFuture
        // Cache: Assigned at seqId=2 (eventSeqId1), token=3 (eventSeqId2) means seqId < token, so contract should be returned
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId3,
          Map(
            someKey -> ContractKeyStateValue.Last(cId_1, eventSeqId1, thereMightBeMore = false)
          ),
        )
        result <- store.lookupContractKey(
          Set(alice),
          someKey,
          pageToken = Some(eventSeqId2),
          limit = 1,
        )
      } yield {
        result.contracts.map(_.templateId) shouldBe Vector(contract1.inst.templateId)
        result.nextPageToken shouldBe None
      }
    }

    "return empty when cache has Last(_, seqId, false) and token <= seqId" in {
      for {
        store <- contractStore(cachesSize = 10L, loggerFactory).asFuture
        // Cache: Assigned at seqId=3 (eventSeqId2), token=2 (eventSeqId1) means seqId >= token, so contract already seen
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId3,
          Map(
            someKey -> ContractKeyStateValue.Last(cId_1, eventSeqId2, thereMightBeMore = false)
          ),
        )
        result <- store.lookupContractKey(
          Set(alice),
          someKey,
          pageToken = Some(eventSeqId1),
          limit = 1,
        )
        resultLimit10 <- store.lookupContractKey(
          Set(alice),
          someKey,
          pageToken = Some(eventSeqId1),
          limit = 10,
        )
      } yield {
        result shouldBe ContractKeyPage(contracts = Vector.empty, nextPageToken = None)
        resultLimit10 shouldBe result
      }
    }

    "serve from cache when cache has Last(_, _, true) and no token" in {
      val nuckKey = globalKey("nuck-key")
      val spyContractsReader = spy(ContractsReaderFixtureWithNuck(nuckKey))
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId2,
          Map(
            nuckKey -> ContractKeyStateValue.Last(cId_1, eventSeqId1, thereMightBeMore = true)
          ),
        )
        result <- store.lookupContractKey(Set(alice), nuckKey, pageToken = None, limit = 1)
      } yield {
        verify(spyContractsReader, never).lookupNonUniqueKey(
          any[Key],
          any[Long],
          any[Option[Long]],
          any[Int],
        )(any[LoggingContextWithTrace])
        result.contracts.map(_.contractId) shouldBe Vector(cId_1)
        result.nextPageToken shouldBe Some(eventSeqId1)
      }
    }

    "serve from cache when cache has Last(_, _, true) and token > seqId" in {
      val nuckKey = globalKey("nuck-key2")
      val spyContractsReader = spy(ContractsReaderFixtureWithNuck(nuckKey))
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId3,
          Map(
            nuckKey -> ContractKeyStateValue.Last(cId_1, eventSeqId1, thereMightBeMore = true)
          ),
        )
        result <- store.lookupContractKey(
          Set(alice),
          nuckKey,
          pageToken = Some(eventSeqId2),
          limit = 1,
        )
      } yield {
        verify(spyContractsReader, never).lookupNonUniqueKey(
          any[Key],
          any[Long],
          any[Option[Long]],
          any[Int],
        )(any[LoggingContextWithTrace])
        result.contracts.map(_.contractId) shouldBe Vector(cId_1)
        result.nextPageToken shouldBe Some(eventSeqId1)
      }
    }

    "fall through to DB when cache has Last(_, _, true) and token < seqId" in {
      val nuckKey = globalKey("nuck-key3")
      val spyContractsReader = spy(ContractsReaderFixtureWithNuck(nuckKey))
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.keyState.putBatch(
          eventSeqId3,
          Map(
            nuckKey -> ContractKeyStateValue.Last(cId_1, eventSeqId2, thereMightBeMore = true)
          ),
        )
        result <- store.lookupContractKey(
          Set(alice),
          nuckKey,
          pageToken = Some(eventSeqId1),
          limit = 1,
        )
      } yield {
        verify(spyContractsReader, atLeastOnce).lookupNonUniqueKey(
          key = nuckKey,
          validAtEventSeqId = 0L,
          nextPageToken = Some(eventSeqId1),
          limit = 1,
        )
        succeed
      }
    }

    "read-through the key state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      val unassignedKey = globalKey("unassigned")

      for {
        store <- contractStore(
          cachesSize = 1L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        activated_firstLookup <- store.lookupContractKey(
          readers = Set(alice),
          key = someKey,
          pageToken = None,
          limit = 1,
        )
        activated_secondLookup <- store.lookupContractKey(
          readers = Set(alice),
          key = someKey,
          pageToken = None,
          limit = 1,
        )

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId1
        empty_firstLookup <- store.lookupContractKey(
          readers = Set(alice),
          key = unassignedKey,
          pageToken = None,
          limit = 1,
        )
        empty_secondLookup <- store.lookupContractKey(
          readers = Set(alice),
          key = unassignedKey,
          pageToken = None,
          limit = 1,
        )
      } yield {
        activated_firstLookup.contracts.map(_.contractId) shouldBe Vector(cId_1)
        activated_secondLookup.contracts.map(_.contractId) shouldBe Vector(cId_1)

        empty_firstLookup.contracts shouldBe empty
        empty_secondLookup.contracts shouldBe empty

        verify(spyContractsReader).lookupNonUniqueKey(someKey, eventSeqId0, None, 1)(loggingContext)
        verify(spyContractsReader).lookupNonUniqueKey(unassignedKey, eventSeqId1, None, 1)(
          loggingContext
        )
        verifyNoMoreInteractions(spyContractsReader)
        succeed
      }
    }

    "present the key state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        key_lookup0 <- store.lookupContractKey(
          readers = Set(alice),
          key = someKey,
          pageToken = None,
          limit = 1,
        )

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId1
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        key_lookup1 <- store.lookupContractKey(
          readers = Set(alice),
          key = someKey,
          pageToken = None,
          limit = 1,
        )

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId2
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId2
        key_lookup2 <- store.lookupContractKey(
          readers = Set(bob),
          key = someKey,
          pageToken = None,
          limit = 1,
        )
        key_lookup2_notVisible <- store.lookupContractKey(
          readers = Set(charlie),
          key = someKey,
          pageToken = None,
          limit = 1,
        )

        _ = store.contractStateCaches.keyState.cacheEventSeqIdIndex = eventSeqId3
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId3
        key_lookup3 <- store.lookupContractKey(
          readers = Set(bob),
          key = someKey,
          pageToken = None,
          limit = 1,
        )
      } yield {
        key_lookup0.contracts.map(_.contractId) shouldBe Vector(cId_1)
        key_lookup1.contracts shouldBe empty
        key_lookup2.contracts.map(_.contractId) shouldBe Vector(cId_2)
        key_lookup2_notVisible.contracts shouldBe empty
        key_lookup3.contracts shouldBe empty
      }
    }
  }

  "readThroughKeyCache" should {
    "read-through and populate the cache via putAsync when pageToken is None" in {
      val key = globalKey("read-through-key")
      // DB returns a single contract and no continuation: cache should be populated with Last(_, _, false).
      val spyReader =
        spy(
          ConfigurableContractsReaderFixture(
            KeyLookupPageResult(
              Vector(ContractRef(contractId = cId_1, eventSequentialId = eventSeqId1)),
              None,
            )
          )
        )
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyReader,
        ).asFuture
        // Sanity check: nothing cached yet for this key.
        _ = store.contractStateCaches.keyState.get(key) shouldBe None
        result <- store.lookupContractKey(Set(alice), key, pageToken = None, limit = 10)
      } yield {
        // The DB was queried.
        verify(spyReader, atLeastOnce).lookupNonUniqueKey(
          key = key,
          notEarlierThanEventSeqId = eventSeqId0,
          nextPageToken = None,
          limit = 10,
        )
        // The cache has been populated.
        store.contractStateCaches.keyState.get(key) shouldBe Some(
          Last(cId_1, eventSeqId1, thereMightBeMore = false)
        )
        result.contracts.map(_.contractId) shouldBe Vector(cId_1)
        result.nextPageToken shouldBe None
      }
    }

    "query the DB directly without populating the cache when pageToken is Some(...)" in {
      val key = globalKey("no-read-through-key")
      val spyReader =
        spy(
          ConfigurableContractsReaderFixture(
            KeyLookupPageResult(
              Vector(ContractRef(contractId = cId_2, eventSequentialId = eventSeqId3)),
              None,
            )
          )
        )
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyReader,
        ).asFuture
        result <- store.lookupContractKey(
          Set(alice),
          key,
          pageToken = Some(eventSeqId2),
          limit = 1,
        )
      } yield {
        // The DB was queried directly with the provided pageToken.
        // For a non-initial page the validAt is derived from the (unset) ledger end cache, hence 0L.
        verify(spyReader, atLeastOnce).lookupNonUniqueKey(
          key = key,
          notEarlierThanEventSeqId = 0L,
          nextPageToken = Some(eventSeqId2),
          limit = 1,
        )
        // The cache is NOT populated for a non-initial page request.
        store.contractStateCaches.keyState.get(key) shouldBe None
        result.contracts.map(_.contractId) shouldBe Vector(cId_2)
        result.nextPageToken shouldBe None
      }
    }

    "cache Last with thereMightBeMore = false when the DB returns a single contract and no continuation" in {
      val key = globalKey("might-be-more-false-single")
      val spyReader =
        spy(
          ConfigurableContractsReaderFixture(
            KeyLookupPageResult(
              Vector(ContractRef(contractId = cId_1, eventSequentialId = eventSeqId1)),
              None,
            )
          )
        )
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyReader,
        ).asFuture
        _ <- store.lookupContractKey(Set(alice), key, pageToken = None, limit = 1)
      } yield store.contractStateCaches.keyState.get(key) shouldBe Some(
        Last(cId_1, eventSeqId1, thereMightBeMore = false)
      )
    }

    "cache Last with thereMightBeMore = true when the DB returns more than one contract" in {
      val key = globalKey("might-be-more-true-multi")
      val spyReader =
        spy(
          ConfigurableContractsReaderFixture(
            KeyLookupPageResult(
              Vector(
                ContractRef(contractId = cId_1, eventSequentialId = eventSeqId1),
                ContractRef(contractId = cId_2, eventSequentialId = eventSeqId2),
              ),
              None,
            )
          )
        )
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyReader,
        ).asFuture
        result <- store.lookupContractKey(Set(alice), key, pageToken = None, limit = 10)
      } yield {
        // The cached entry tracks the head contract and signals more may exist.
        store.contractStateCaches.keyState.get(key) shouldBe Some(
          Last(cId_1, eventSeqId1, thereMightBeMore = true)
        )
        // The full DB result is returned to the caller, not just the cached subset.
        result.contracts.map(_.contractId) shouldBe Vector(cId_1, cId_2)
      }
    }

    "cache Last with thereMightBeMore = true when the DB returns a single contract but a continuation token" in {
      val key = globalKey("might-be-more-true-token")
      val spyReader =
        spy(
          ConfigurableContractsReaderFixture(
            KeyLookupPageResult(
              Vector(ContractRef(contractId = cId_1, eventSequentialId = eventSeqId1)),
              Some(eventSeqId1),
            )
          )
        )
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyReader,
        ).asFuture
        result <- store.lookupContractKey(Set(alice), key, pageToken = None, limit = 1)
      } yield {
        store.contractStateCaches.keyState.get(key) shouldBe Some(
          Last(cId_1, eventSeqId1, thereMightBeMore = true)
        )
        result.contracts.map(_.contractId) shouldBe Vector(cId_1)
        result.nextPageToken shouldBe Some(eventSeqId1)
      }
    }

    "cache Empty when the DB returns no contracts" in {
      val key = globalKey("empty-key")
      val spyReader =
        spy(ConfigurableContractsReaderFixture(KeyLookupPageResult(Vector.empty, None)))
      for {
        store <- contractStore(
          cachesSize = 10L,
          loggerFactory = loggerFactory,
          spyReader,
        ).asFuture
        result <- store.lookupContractKey(Set(alice), key, pageToken = None, limit = 1)
      } yield {
        store.contractStateCaches.keyState.get(key) shouldBe Some(ContractKeyStateValue.Empty)
        result.contracts shouldBe empty
        result.nextPageToken shouldBe None
      }
    }
  }

  "resolveFromCache" should {
    val cid: ContractId = ContractId.V1(Hash.hashPrivateKey("resolveFromCacheTest"))

    "return an empty page (no continuation) for Empty regardless of pageToken" in {
      resolveFromCache(
        value = ContractKeyStateValue.Empty,
        pageToken = None,
      ) shouldBe Some((Vector.empty, None))
      resolveFromCache(
        ContractKeyStateValue.Empty,
        pageToken = Some(42L),
      ) shouldBe Some((Vector.empty, None))
      resolveFromCache(
        ContractKeyStateValue.Empty,
        pageToken = Some(Long.MaxValue),
      ) shouldBe Some((Vector.empty, None))
    }

    "return the cached contract with no continuation for Last with no more contracts and no pageToken" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 5L, thereMightBeMore = false),
        pageToken = None,
      ) shouldBe Some((Vector(cid), None))
    }

    "return the cached contract and expose its seqId as the next pageToken for Last with potentially more contracts and no pageToken" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 5L, thereMightBeMore = true),
        pageToken = None,
      ) shouldBe Some((Vector(cid), Some(5L)))
    }

    "return the cached contract with no continuation when seqId < pageToken and thereMightBeMore = false" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 5L, thereMightBeMore = false),
        pageToken = Some(10L),
      ) shouldBe Some((Vector(cid), None))
    }

    "return the cached contract with its seqId as next pageToken when seqId < pageToken and thereMightBeMore = true" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 5L, thereMightBeMore = true),
        pageToken = Some(10L),
      ) shouldBe Some((Vector(cid), Some(5L)))
    }

    "return an empty page when seqId == pageToken and thereMightBeMore = false (already seen, nothing older)" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 5L, thereMightBeMore = false),
        pageToken = Some(5L),
      ) shouldBe Some((Vector.empty, None))
    }

    "fall through to DB (None) when seqId == pageToken and thereMightBeMore = true" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 5L, thereMightBeMore = true),
        pageToken = Some(5L),
      ) shouldBe None
    }

    "return an empty page when seqId > pageToken and thereMightBeMore = false" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 10L, thereMightBeMore = false),
        pageToken = Some(5L),
      ) shouldBe Some((Vector.empty, None))
    }

    "fall through to DB (None) when seqId > pageToken and thereMightBeMore = true" in {
      resolveFromCache(
        Last(contractId = cid, eventSequentialId = 10L, thereMightBeMore = true),
        pageToken = Some(5L),
      ) shouldBe None
    }
  }

  "lookupActiveContract" should {
    "read-through the contract state cache" in {
      val spyContractsReader = spy(ContractsReaderFixture())

      for {
        store <- contractStore(
          cachesSize = 1L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        cId2_lookup <- store.lookupActiveContract(Set(charlie), cId_2)
        another_cId2_lookup <- store.lookupActiveContract(Set(charlie), cId_2)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId2
        cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)
        another_cId3_lookup <- store.lookupActiveContract(Set(bob), cId_3)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId3
        nonExistentCId = cId_5
        nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
        another_nonExistentCId_lookup <- store.lookupActiveContract(Set.empty, nonExistentCId)
      } yield {
        cId2_lookup shouldBe Option.empty
        another_cId2_lookup shouldBe Option.empty

        cId3_lookup.map(_.templateId) shouldBe Some(contract3.inst.templateId)
        another_cId3_lookup.map(_.templateId) shouldBe Some(contract3.inst.templateId)

        nonExistentCId_lookup shouldBe Option.empty
        another_nonExistentCId_lookup shouldBe Option.empty

        // The cache is evicted BOTH on the number of entries AND memory pressure
        // So even though a read-through populates missing entries,
        // they can be immediately evicted by GCs and lead to subsequent misses.
        // Hence, verify atLeastOnce for LedgerDaoContractsReader.lookupContractState
        verify(spyContractsReader, atLeastOnce).lookupContractState(cId_2, eventSeqId1)
        verify(spyContractsReader, atLeastOnce).lookupContractState(cId_3, eventSeqId2)
        verify(spyContractsReader, atLeastOnce).lookupContractState(nonExistentCId, eventSeqId3)
        succeed
      }
    }

    "read-through the cache without storing negative lookups" in {
      val spyContractsReader = spy(ContractsReaderFixture())
      for {
        store <- contractStore(
          cachesSize = 1L,
          loggerFactory = loggerFactory,
          spyContractsReader,
        ).asFuture
        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        negativeLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
        positiveLookup_cId6 <- store.lookupActiveContract(Set(alice), cId_6)
      } yield {
        negativeLookup_cId6 shouldBe Option.empty
        positiveLookup_cId6 shouldBe Option.empty

        verify(spyContractsReader, times(wantedNumberOfInvocations = 1))
          .lookupContractState(cId_6, eventSeqId1)
        succeed
      }
    }

    "present the contract state if visible at specific cache offsets (with no cache)" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        cId1_lookup0 <- store.lookupActiveContract(Set(alice), cId_1)
        cId2_lookup0 <- store.lookupActiveContract(Set(bob), cId_2)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId1
        cId1_lookup1 <- store.lookupActiveContract(Set(alice), cId_1)
        cid1_lookup1_archivalNotDivulged <- store.lookupActiveContract(Set(charlie), cId_1)

        _ = store.contractStateCaches.contractState.cacheEventSeqIdIndex = eventSeqId2
        cId2_lookup2 <- store.lookupActiveContract(Set(bob), cId_2)
        cid2_lookup2_divulged <- store.lookupActiveContract(Set(charlie), cId_2)
        cid2_lookup2_nonVisible <- store.lookupActiveContract(Set(charlie), cId_2)
      } yield {
        cId1_lookup0.map(_.templateId) shouldBe Some(contract1.inst.templateId)
        cId2_lookup0 shouldBe Option.empty

        cId1_lookup1 shouldBe Option.empty
        cid1_lookup1_archivalNotDivulged shouldBe None

        cId2_lookup2.map(_.templateId) shouldBe Some(contract2.inst.templateId)
        cid2_lookup2_divulged shouldBe None
        cid2_lookup2_nonVisible shouldBe Option.empty
      }
    }
  }

  "lookupContractStateWithoutDivulgence" should {

    "resolve lookup from cache" in {
      for {
        store <- contractStore(cachesSize = 2L, loggerFactory).asFuture
        _ = store.contractStateCaches.contractState.putBatch(
          eventSeqId2,
          Map(
            // Populate the cache with an active contract
            cId_4 -> ContractStateStatus.Active,
            // Populate the cache with an archived contract
            cId_5 -> ContractStateStatus.Archived,
          ),
        )
        activeContractLookupResult <- store.lookupContractState(cId_4)
        archivedContractLookupResult <- store.lookupContractState(cId_5)
        nonExistentContractLookupResult <- store.lookupContractState(cId_7)
      } yield {
        activeContractLookupResult shouldBe ContractState.Active(contract4.inst)
        archivedContractLookupResult shouldBe ContractState.Archived
        nonExistentContractLookupResult shouldBe ContractState.NotFound
      }
    }

    "resolve lookup from the ContractsReader when not cached" in {
      for {
        store <- contractStore(cachesSize = 0L, loggerFactory).asFuture
        activeContractLookupResult <- store.lookupContractState(cId_4)
        archivedContractLookupResult <- store.lookupContractState(cId_5)
        nonExistentContractLookupResult <- store.lookupContractState(cId_7)
      } yield {
        activeContractLookupResult shouldBe ContractState.Active(contract4.inst)
        archivedContractLookupResult shouldBe ContractState.Archived
        nonExistentContractLookupResult shouldBe ContractState.NotFound
      }
    }
  }
}

@nowarn("msg=match may not be exhaustive")
object MutableCacheBackedContractStoreSpec {
  private val eventSeqId0 = 1L
  private val eventSeqId1 = 2L
  private val eventSeqId2 = 3L
  private val eventSeqId3 = 4L

  private val Seq(alice, bob, charlie) = Seq("alice", "bob", "charlie").map(party)

  private val someKey = globalKey("key1")

  private val exStakeholders = Set(bob, alice)
  private val exSignatories = Set(alice)
  private val exMaintainers = Set(alice)
  private val someKeyWithMaintainers = KeyWithMaintainers(someKey, exMaintainers)

  private val timeouts = ProcessingTimeout()

  private val Seq(t1, t2, t3, t4, t5, t6, t7) = (1 to 7).map { id =>
    Time.Timestamp.assertFromLong(id.toLong * 1000L)
  }

  private val (
    Seq(cId_1, cId_2, cId_3, cId_4, cId_5, cId_6, cId_7),
    Seq(contract1, contract2, contract3, contract4, _, contract6, _),
  ) =
    Seq(
      contract(Set(alice), t1),
      contract(exStakeholders, t2),
      contract(exStakeholders, t3),
      contract(exStakeholders, t4),
      contract(exStakeholders, t5),
      contract(Set(alice), t6),
      contract(exStakeholders, t7),
    ).map(c => c.contractId -> c).unzip

  private def contractStore(
      cachesSize: Long,
      loggerFactory: NamedLoggerFactory,
      readerFixture: LedgerDaoContractsReader = ContractsReaderFixture(),
  )(implicit ec: ExecutionContext, traceContext: TraceContext) = {
    val metrics = LedgerApiServerMetrics.ForTesting
    val startIndexExclusive = eventSeqId0
    val contractStore = new MutableCacheBackedContractStore(
      readerFixture,
      contractStateCaches = ContractStateCaches
        .build(startIndexExclusive, cachesSize, cachesSize, metrics, loggerFactory),
      loggerFactory = loggerFactory,
      contractStore = inMemoryContractStore(loggerFactory),
      ledgerEndCache = MutableLedgerEndCache(),
      maxLookupLimit = 10,
    )

    Resource.successful(contractStore)
  }

  @SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is spied in tests
  case class ContractsReaderFixture() extends LedgerDaoContractsReader {
    @volatile private var initialResultForCid6 =
      Future.successful(Option.empty[ExistingContractStatus])

    override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanOffset: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[Key, Long]] = ??? // not used in this test

    override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(
        implicit loggingContext: LoggingContextWithTrace
    ): Future[Option[ExistingContractStatus]] =
      (contractId, notEarlierThanEventSeqId) match {
        case (`cId_1`, `eventSeqId0`) => activeContract
        case (`cId_1`, validAt) if validAt > eventSeqId0 => archivedContract
        case (`cId_2`, validAt) if validAt >= eventSeqId1 =>
          activeContract
        case (`cId_3`, _) => activeContract
        case (`cId_4`, _) => activeContract
        case (`cId_5`, _) => archivedContract
        case (`cId_6`, _) =>
          // Simulate store being populated from one query to another
          val result = initialResultForCid6
          initialResultForCid6 = activeContract
          result
        case _ => Future.successful(Option.empty)
      }

    override def lookupNonUniqueKey(
        key: Key,
        notEarlierThanEventSeqId: Long,
        nextPageToken: Option[Long],
        limit: Int,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[KeyLookupPageResult] =
      (key, notEarlierThanEventSeqId) match {
        case (`someKey`, `eventSeqId0`) =>
          Future.successful(
            KeyLookupPageResult(
              Vector(ContractRef(contractId = cId_1, eventSequentialId = eventSeqId0)),
              None,
            )
          )
        case (`someKey`, `eventSeqId2`) =>
          Future.successful(
            KeyLookupPageResult(
              Vector(ContractRef(contractId = cId_2, eventSequentialId = eventSeqId2)),
              None,
            )
          )
        case _ => Future.successful(KeyLookupPageResult(Vector.empty, None))
      }

  }

  @SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is spied in tests
  case class ContractsReaderFixtureWithNuck(nuckKey: Key) extends LedgerDaoContractsReader {
    override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanOffset: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[Key, Long]] = ??? // not used in this test

    override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(
        implicit loggingContext: LoggingContextWithTrace
    ): Future[Option[ExistingContractStatus]] = Future.successful(Some(ContractStateStatus.Active))

    override def lookupNonUniqueKey(
        key: Key,
        validAtEventSeqId: Long,
        nextPageToken: Option[Long],
        limit: Int,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[KeyLookupPageResult] =
      if (key == nuckKey)
        Future.successful(
          KeyLookupPageResult(
            Vector(ContractRef(contractId = cId_1, eventSequentialId = eventSeqId0)),
            None,
          )
        )
      else
        Future.successful(KeyLookupPageResult(Vector.empty, None))
  }

  @SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is spied in tests
  case class ConfigurableContractsReaderFixture(
      dbResult: KeyLookupPageResult
  ) extends LedgerDaoContractsReader {
    override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanOffset: Long)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Map[Key, Long]] = ??? // not used in this test

    override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(
        implicit loggingContext: LoggingContextWithTrace
    ): Future[Option[ExistingContractStatus]] = Future.successful(Some(ContractStateStatus.Active))

    override def lookupNonUniqueKey(
        key: Key,
        notEarlierThanEventSeqId: Long,
        nextPageToken: Option[Long],
        limit: Int,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[KeyLookupPageResult] =
      Future.successful(dbResult)
  }

  def inMemoryContractStore(
      loggerFactory: NamedLoggerFactory
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): LedgerApiContractStore = {
    val store = new InMemoryContractStore(timeouts, loggerFactory)
    val contracts = Seq(
      contract1,
      contract2,
      contract3,
      contract4,
      contract6,
    )
    store.storeContracts(contracts).discard
    LedgerApiContractStoreImpl(store, loggerFactory, LedgerApiServerMetrics.ForTesting)
  }

  private def contract(
      stakeholders: Set[Party],
      ledgerEffectiveTime: Time.Timestamp,
      key: Option[KeyWithMaintainers] = Some(someKeyWithMaintainers),
  ) =
    ExampleContractFactory.build(
      createdAt = CreationTime.CreatedAt(ledgerEffectiveTime),
      signatories = exSignatories,
      stakeholders = stakeholders,
      keyOpt = key,
    )

  private val activeContract: Future[Option[ExistingContractStatus]] =
    Future.successful(Some(ContractStateStatus.Active))

  private val archivedContract: Future[Option[ExistingContractStatus]] =
    Future.successful(Some(ContractStateStatus.Archived))

  private def party(name: String): Party = Party.assertFromString(name)

  private def globalKey(desc: String): Key =
    Key(
      Identifier.assertFromString("some:template:name"),
      Ref.PackageName.assertFromString("pkg-name"),
      ValueText(desc),
      crypto.Hash.hashPrivateKey(desc),
    )
}
