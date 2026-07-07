// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.{Future, Promise}

final class PruningOffsetServiceSpec extends AsyncFlatSpec with Matchers {

  private implicit val tc: TraceContext = TraceContext.empty
  private val loggerFactory = SuppressingLogger(getClass)

  private def offset(n: Long): Option[Offset] = Some(Offset.tryFromLong(n))

  behavior of "PruningOffsetServiceImpl"

  it should "fetch from DB on first call and cache the result" in {
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(10))
      },
      loggerFactory = loggerFactory,
    )

    for {
      r1 <- service.pruningOffset
      r2 <- service.pruningOffset
      r3 <- service.pruningOffset
    } yield {
      r1 shouldBe offset(10)
      r2 shouldBe offset(10)
      r3 shouldBe offset(10)
      callCount.get() shouldBe 1
    }
  }

  it should "return None when DB returns None" in {
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => Future.successful(None),
      loggerFactory = loggerFactory,
    )

    for {
      result <- service.pruningOffset
    } yield result shouldBe None
  }

  it should "fetch from DB every time when disabled" in {
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(10))
      },
      loggerFactory = loggerFactory,
    )

    service.disableCache()

    for {
      r1 <- service.pruningOffset
      r2 <- service.pruningOffset
    } yield {
      r1 shouldBe offset(10)
      r2 shouldBe offset(10)
      callCount.get() shouldBe 2
    }
  }

  it should "re-fetch from DB after reEnableCache and then cache again" in {
    val callCount = new AtomicInteger(0)
    val currentValue = new AtomicInteger(10)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(currentValue.get().toLong))
      },
      loggerFactory = loggerFactory,
    )

    for {
      r1 <- service.pruningOffset
      _ = callCount.get() shouldBe 1
      _ = currentValue.set(20)
      _ = service.reEnableCache()
      r2 <- service.pruningOffset
      r3 <- service.pruningOffset
    } yield {
      r1 shouldBe offset(10)
      r2 shouldBe offset(20)
      r3 shouldBe offset(20)
      callCount.get() shouldBe 2
    }
  }

  it should "not cache when disabled, then cache after re-enable" in {
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(10))
      },
      loggerFactory = loggerFactory,
    )

    service.disableCache()

    for {
      r1 <- service.pruningOffset
      _ = callCount.get() shouldBe 1
      r2 <- service.pruningOffset
      _ = callCount.get() shouldBe 2
      _ = service.reEnableCache()
      r3 <- service.pruningOffset
      r4 <- service.pruningOffset
    } yield {
      r1 shouldBe offset(10)
      r2 shouldBe offset(10)
      r3 shouldBe offset(10)
      r4 shouldBe offset(10)
      callCount.get() shouldBe 3
    }
  }

  it should "reset cache to Undefined on fetch failure so next call retries" in {
    val callCount = new AtomicInteger(0)
    val shouldFail = new AtomicBoolean(true)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        if (shouldFail.get()) Future.failed(new RuntimeException("DB error"))
        else Future.successful(offset(10))
      },
      loggerFactory = loggerFactory,
    )
    callCount.get() shouldBe 0

    for {
      r1 <- service.pruningOffset.failed
      _ = r1 shouldBe a[RuntimeException]
      _ = callCount.get() shouldBe 1
      _ = shouldFail.set(false)
      r2 <- service.pruningOffset
      // after the failure, cache should have reset to Undefined and another fetch from db is triggered
      _ = r2 shouldBe offset(10)
      _ = callCount.get() shouldBe 2
      r3 <- service.pruningOffset
      _ = r3 shouldBe offset(10)
      _ = callCount.get() shouldBe 2 // cache is working again, no new db call
    } yield succeed
  }

  it should "not overwrite Disabled with Undefined on fetch failure" in {
    val fetchPromise = Promise[Option[Offset]]()
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        val count = callCount.incrementAndGet()
        if (count == 1) fetchPromise.future
        else Future.successful(offset(99))
      },
      loggerFactory = loggerFactory,
    )

    // Start a fetch (transitions Undefined -> Defined)
    val f1 = service.pruningOffset

    // Disable before the fetch completes
    service.disableCache()

    // Fail the fetch
    fetchPromise.failure(new RuntimeException("DB error"))

    for {
      // The failure handler should NOT reset to Undefined because
      // disableCache() already changed the state to Disabled.
      ex <- f1.failed
      _ = ex shouldBe a[RuntimeException]
      // Subsequent reads should still go to Disabled
      // returning fresh results each time (no caching)
      r2 <- service.pruningOffset
      r3 <- service.pruningOffset
    } yield {
      r2 shouldBe offset(99)
      r3 shouldBe offset(99)
      callCount.get() shouldBe 3
    }
  }

  it should "not overwrite Disabled with Defined on fetch success" in {
    val fetchPromise = Promise[Option[Offset]]()
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        val count = callCount.incrementAndGet()
        if (count == 1) fetchPromise.future
        else Future.successful(offset(99))
      },
      loggerFactory = loggerFactory,
    )

    // Start a long-running fetch (transitions Undefined -> Defined)
    val f1 = service.pruningOffset

    // Disable before the fetch completes
    service.disableCache()

    // Complete the fetch successfully
    fetchPromise.success(offset(10))

    for {
      // The success should NOT reset state, Disabled should be preserved.
      r1 <- f1
      _ = r1 shouldBe offset(10)
      // Each call fetches from DB since caching is disabled
      r2 <- service.pruningOffset
      r3 <- service.pruningOffset
    } yield {
      r2 shouldBe offset(99)
      r3 shouldBe offset(99)
      callCount.get() shouldBe 3
    }
  }

  it should "handle disable and reEnable during in-flight fetch" in {
    val fetchPromise = Promise[Option[Offset]]()
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        val count = callCount.incrementAndGet()
        if (count == 1) fetchPromise.future
        else Future.successful(offset(99))
      },
      loggerFactory = loggerFactory,
    )

    // Start a long-running fetch (Undefined -> Defined)
    val f1 = service.pruningOffset

    // Disable, then re-enable (Defined -> Disabled -> Undefined)
    service.disableCache()
    service.reEnableCache()

    // A new fetch should now be possible
    val f2 = service.pruningOffset

    // Complete original fetch
    fetchPromise.success(offset(1))

    for {
      r1 <- f1
      r2 <- f2
      _ = r1 shouldBe offset(1)
      _ = r2 shouldBe offset(99)
      _ = callCount.get() shouldBe 2
      r3 <- service.pruningOffset
      _ = callCount.get() shouldBe 2
      _ = r3 shouldBe offset(99)
    } yield succeed
  }

  it should "simulate the pruning transaction lifecycle correctly" in {
    val callCount = new AtomicInteger(0)
    val currentValue = new AtomicInteger(1)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(currentValue.get().toLong))
      },
      loggerFactory = loggerFactory,
    )

    for {
      // Initial state: no pruning yet
      r1 <- service.pruningOffset
      _ = r1 shouldBe offset(1)
      _ = callCount.get() shouldBe 1

      // Simulate pruning transaction: disable cache before commit
      _ = service.disableCache()

      // During transaction: reads go to DB (still sees old value)
      r2 <- service.pruningOffset
      _ = r2 shouldBe offset(1)
      _ = callCount.get() shouldBe 2

      // Transaction commits, update the DB value
      _ = currentValue.set(100)

      // Value is updated in the DB, but cache is disabled so reads go directly to DB
      r3 <- service.pruningOffset
      _ = r3 shouldBe offset(100)
      _ = callCount.get() shouldBe 3
      // Another read while disabled, still goes to DB (not cached)
      r4 <- service.pruningOffset
      _ = r4 shouldBe offset(100)
      _ = callCount.get() shouldBe 4

      // Re-enable cache after commit
      _ = service.reEnableCache()

      // First read after re-enable: fetches new value and caches
      r5 <- service.pruningOffset
      _ = r5 shouldBe offset(100)
      _ = callCount.get() shouldBe 5

      // Subsequent reads: served from cache
      r6 <- service.pruningOffset
    } yield {
      r6 shouldBe offset(100)
      callCount.get() shouldBe 5
    }
  }

  it should "handle multiple disable/reEnable cycles" in {
    val callCount = new AtomicInteger(0)
    val currentValue = new AtomicInteger(10)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(currentValue.get().toLong))
      },
      loggerFactory = loggerFactory,
    )

    for {
      r1 <- service.pruningOffset
      _ = {
        r1 shouldBe offset(10)
        service.disableCache()
        currentValue.set(20)
        service.reEnableCache()
      }
      r2 <- service.pruningOffset
      _ = {
        r2 shouldBe offset(20)
        service.disableCache()
        currentValue.set(30)
        service.reEnableCache()
      }
      r3 <- service.pruningOffset
    } yield {
      r3 shouldBe offset(30)
      callCount.get() shouldBe 3
    }
  }

  it should "disableCache from initial Undefined state" in {
    val callCount = new AtomicInteger(0)
    val service = new PruningOffsetServiceImpl(
      fetchFromDb = _ => {
        callCount.incrementAndGet()
        Future.successful(offset(5))
      },
      loggerFactory = loggerFactory,
    )

    service.disableCache()

    for {
      r1 <- service.pruningOffset
      r2 <- service.pruningOffset
    } yield {
      r1 shouldBe offset(5)
      r2 shouldBe offset(5)
      callCount.get() shouldBe 2
    }
  }

}
