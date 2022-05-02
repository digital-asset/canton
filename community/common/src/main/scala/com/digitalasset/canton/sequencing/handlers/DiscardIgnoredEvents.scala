// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.sequencing.{
  OrdinaryApplicationHandler,
  PossiblyIgnoredApplicationHandler,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.tracing.Traced

object DiscardIgnoredEvents {
  def apply[Env](handler: OrdinaryApplicationHandler[Env]): PossiblyIgnoredApplicationHandler[Env] =
    handler.replace(_.withTraceContext { batchTraceContext => events =>
      handler(
        Traced(events.collect { case e: OrdinarySequencedEvent[Env] => e })(batchTraceContext)
      )
    })
}
