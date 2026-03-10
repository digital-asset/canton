// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsLastPointers,
  AchsState,
}

import java.util.concurrent.atomic.AtomicReference

/** In-memory cache for the ACHS (Active Contracts Head Snapshot) state. */
class AchsStateCache(val loggerFactory: NamedLoggerFactory) extends NamedLogging {
  private val achsState: AtomicReference[AchsState] =
    new AtomicReference(
      AchsState(validAt = 0, lastPointers = AchsLastPointers(lastRemoved = 0, lastPopulated = 0))
    )

  def set(state: AchsState): Unit = achsState.set(state)

  def get(): AchsState = achsState.get()

  def updateValidAt(validAt: Long): Unit =
    achsState.updateAndGet(_.copy(validAt = validAt)).discard

  def updateLastPointers(lastPointers: AchsLastPointers): Unit =
    achsState
      .getAndUpdate(
        _.copy(lastPointers = lastPointers)
      )
      .discard
}
