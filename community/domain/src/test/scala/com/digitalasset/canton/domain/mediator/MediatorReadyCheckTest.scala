// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.tracing.Traced
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.Future

class MediatorReadyCheckTest extends AsyncWordSpec with BaseTest {
  class MockCheck {
    private val result = new AtomicBoolean()
    private val checkedCounter = new AtomicInteger()
    private val lastTimestampRef = new AtomicReference[Option[CantonTimestamp]](None)

    def isReady(ts: Traced[CantonTimestamp]): Future[Boolean] = {
      checkedCounter.getAndIncrement()
      lastTimestampRef.set(Some(ts.value))
      Future.successful(result.get())
    }

    def setResult(value: Boolean): Unit = result.set(value)

    def checkedCount: Int = checkedCounter.get()
    def lastTimestamp: Option[CantonTimestamp] = lastTimestampRef.get()
  }

  case class Env(readyCheck: MediatorReadyCheck, mockCheck: MockCheck)
  object Env {
    def apply(): Env = {
      val mockCheck = new MockCheck
      val readyCheck = new MediatorReadyCheck(mockCheck.isReady, loggerFactory)(
        directExecutionContext
      )
      Env(readyCheck, mockCheck)
    }
  }

  "returns check result" in {
    val Env(readyCheck, mockCheck) = Env()

    val ts = CantonTimestamp.now()
    mockCheck.setResult(true)

    for {
      result <- readyCheck.isReady(ts)
    } yield {
      result shouldBe true
      mockCheck.lastTimestamp.value shouldBe ts
    }
  }

  "checks on first access but then caches result" in {
    val Env(readyCheck, mockCheck) = Env()

    mockCheck.checkedCount shouldBe 0

    for {
      _ <- readyCheck.isReady(CantonTimestamp.Epoch)
      _ = mockCheck.checkedCount shouldBe 1
      _ <- readyCheck.isReady(CantonTimestamp.Epoch)
      _ <- readyCheck.isReady(CantonTimestamp.Epoch)
      _ <- readyCheck.isReady(CantonTimestamp.Epoch)
    } yield {
      mockCheck.checkedCount shouldBe 1
    }
  }

  "isReady result can be reset" in {
    val Env(readyCheck, mockCheck) = Env()

    mockCheck.checkedCount shouldBe 0
    mockCheck.setResult(false)

    for {
      result1 <- readyCheck.isReady(CantonTimestamp.Epoch)
      _ = {
        mockCheck.checkedCount shouldBe 1
        result1 shouldBe false
      }
      result2 <- readyCheck.isReady(CantonTimestamp.Epoch)
      _ = {
        mockCheck.checkedCount shouldBe 1
        result2 shouldBe false
      }
      _ = {
        mockCheck.setResult(true)
        readyCheck.reset()
      }
      result3 <- readyCheck.isReady(CantonTimestamp.Epoch)
    } yield {
      mockCheck.checkedCount shouldBe 2
      result3 shouldBe true
    }
  }
}
