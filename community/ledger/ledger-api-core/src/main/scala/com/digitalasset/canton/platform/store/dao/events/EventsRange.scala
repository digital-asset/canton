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
  // TODO(#35882) Uncomment this assertion once all usages conform to it
  // assert(startInclusive <= endInclusive)

  def intersection(other: OffsetRange): Option[OffsetRange] = {
    val (earlier, later) =
      if (startInclusive > other.startInclusive) other -> this
      else this -> other
    if (earlier.endInclusive >= later.startInclusive)
      Some(
        OffsetRange(
          startInclusive = later.startInclusive,
          endInclusive = earlier.endInclusive min later.endInclusive,
        )
      )
    else None
  }

  def before(other: OffsetRange): Option[OffsetRange] =
    this
      .intersection(other)
      .filter(_.startInclusive > this.startInclusive)
      .flatMap(_.startInclusive.decrement)
      .map(newEnd => this.copy(endInclusive = newEnd))

  def after(other: OffsetRange): Option[OffsetRange] =
    this
      .intersection(other)
      .filter(_.endInclusive < this.endInclusive)
      .map(_.endInclusive.increment)
      .map(newStart => this.copy(startInclusive = newStart))

  override protected def pretty: Pretty[OffsetRange] = PrettyUtil.prettyOfString(range =>
    s"offset range [${range.startInclusive.unwrap}, ${range.endInclusive.unwrap}]"
  )
}
