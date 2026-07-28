// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

final case class Timepoint(offset: Offset)(val recordTime: CantonTimestamp) extends PrettyPrinting {
  override protected def pretty: Pretty[Timepoint] = prettyOfClass(
    param("offset", _.offset),
    param("recordTime", _.recordTime),
  )

  def tupled: (Offset, CantonTimestamp) = (offset, recordTime)
}

object Timepoint {
  implicit val orderingTimepoint: Ordering[Timepoint] = Ordering.by[Timepoint, Offset](_.offset)
}
