// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.EventSeqIdRange

final case class EventsRange(
    offsetRange: OffsetRange,
    eventSeqIdRange: EventSeqIdRange,
)

final case class OffsetRange(startInclusive: Offset, endInclusive: Offset) extends PrettyPrinting {

  override protected def pretty: Pretty[OffsetRange] = PrettyUtil.prettyOfString(range =>
    s"offset range [${range.startInclusive.unwrap}, ${range.endInclusive.unwrap}]"
  )
}
