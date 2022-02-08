// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import org.scalatest.wordspec.AsyncWordSpecLike

trait CursorPreheadStoreTest {
  this: AsyncWordSpecLike with BaseTest =>

  def cursorPreheadStore(mk: () => CursorPreheadStore[Long]): Unit = {
    val prehead5 = CursorPrehead(5L, CantonTimestamp.ofEpochSecond(5))
    val prehead10 = CursorPrehead(10L, CantonTimestamp.ofEpochSecond(10))
    val prehead20 = CursorPrehead(20L, CantonTimestamp.ofEpochSecond(20))
    val prehead30 = CursorPrehead(20L, CantonTimestamp.ofEpochSecond(30))

    "store and retrieve the prehead" in {
      val store = mk()
      for {
        cursor0 <- store.prehead
        _ <- store.advancePreheadTo(prehead10)
        cursor10 <- store.prehead
        _ <- store.advancePreheadTo(prehead20)
        cursor20 <- store.prehead
      } yield {
        cursor0 shouldBe None
        cursor10 shouldBe Some(prehead10)
        cursor20 shouldBe Some(prehead20)
      }
    }

    "advance" should {
      "only advance the prehead" in {
        val store = mk()
        for {
          cursor0 <- store.prehead
          _ <- store.advancePreheadTo(prehead10)
          cursor10 <- store.prehead
          _ <- store.advancePreheadTo(prehead5)
          cursor5 <- store.prehead
          _ <- store.advancePreheadTo(prehead20)
          cursor20 <- store.prehead
        } yield {
          cursor0 shouldBe None
          cursor10 shouldBe Some(prehead10)
          cursor5 shouldBe Some(prehead10)
          cursor20 shouldBe Some(prehead20)
        }
      }

      "not overwrite the timestamp" in {
        val store = mk()
        for {
          _ <- store.advancePreheadTo(prehead10)
          _ <- store.advancePreheadTo(prehead10.copy(timestamp = CantonTimestamp.Epoch))
          cursor10 <- store.prehead
        } yield {
          cursor10 shouldBe Some(prehead10)
        }
      }
    }

    "rewind" should {
      "only rewind the prehead" in {
        val store = mk()
        for {
          _ <- store.rewindPreheadTo(Some(prehead20))
          cursorNone <- store.prehead
          _ <- store.advancePreheadTo(prehead20)
          cursor20 <- store.prehead
          _ <- store.rewindPreheadTo(Some(prehead5))
          cursor5 <- store.prehead
          _ <- store.rewindPreheadTo(Some(prehead10))
          cursor10 <- store.prehead
          _ <- store.rewindPreheadTo(None)
          cursorNone2 <- store.prehead
        } yield {
          cursorNone shouldBe None
          cursor20 shouldBe Some(prehead20)
          cursor5 shouldBe Some(prehead5)
          cursor10 shouldBe Some(prehead5)
          cursorNone2 shouldBe None
        }
      }

      "not overwrite the timestamp" in {
        val store = mk()
        for {
          _ <- store.advancePreheadTo(prehead10)
          _ <- store.rewindPreheadTo(Some(prehead10.copy(timestamp = CantonTimestamp.Epoch)))
          cursor10 <- store.prehead
        } yield {
          cursor10 shouldBe Some(prehead10)
        }
      }
    }

    "override the prehead counter" in {
      val store = mk()
      for {
        cursor0 <- store.prehead
        _ <- store.advancePreheadTo(prehead10)
        cursor10 <- store.prehead
        _ <- store.overridePreheadUnsafe(None)
        cursorNone <- store.prehead
        _ <- store.overridePreheadUnsafe(Some(prehead30))
        cursor30 <- store.prehead
        _ <- store.overridePreheadUnsafe(Some(prehead20))
        cursor20 <- store.prehead
      } yield {
        cursor0 shouldBe None
        cursor10 shouldBe Some(prehead10)
        cursorNone shouldBe None
        cursor30 shouldBe Some(prehead30)
        cursor20 shouldBe Some(prehead20)
      }
    }
  }
}
