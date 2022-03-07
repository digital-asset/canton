// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.store.SequencerCounterTrackerStore

class InMemorySequencerCounterTrackerStore(override val loggerFactory: NamedLoggerFactory)
    extends SequencerCounterTrackerStore
    with NamedLogging {
  override protected[store] val cursorStore =
    new InMemoryCursorPreheadStore[SequencerCounter](loggerFactory)

  override def close(): Unit = ()
}
