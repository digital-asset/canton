// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tea.projection.{
  TeaProjectionFactory,
  TeaProjectionTest,
  TeaTrafficStore,
}
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class MemoryTeaProjectionFactoryTest extends AnyWordSpec with BaseTest with TeaProjectionTest {

  // The in-memory projection uses the pekko TestProjection which does not persist offsets across
  // restarts, so a restarted projection replays from the beginning and relies on store-level
  // deduplication.
  override protected def offsetsArePersisted: Boolean = false

  override protected def createBackend()(implicit system: ActorSystem[?]): Backend = {
    implicit val ec: ExecutionContext = system.executionContext
    val memoryStore = new TeaMemoryTrafficStore()
    new Backend {
      override val store: TeaTrafficStore = memoryStore
      override def newProjection(): TeaProjectionFactory =
        new TeaMemoryProjectionFactory(loggerFactory, memoryStore)
    }
  }

  "MemoryTeaProjection" should {
    behave like teaProjection()
  }
}
