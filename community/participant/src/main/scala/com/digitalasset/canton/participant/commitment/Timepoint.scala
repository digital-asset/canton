// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import slick.jdbc.GetResult

final case class Timepoint(offset: Offset)(val recordTime: CantonTimestamp)
    extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[Timepoint] = Timepoint

  def tupled: (Offset, CantonTimestamp) = (offset, recordTime)
}

object Timepoint extends PrettyPrintingCompanion[Timepoint] {
  implicit val orderingTimepoint: Ordering[Timepoint] = Ordering.by[Timepoint, Offset](_.offset)

  implicit val timepointGetResult: GetResult[Timepoint] =
    GetResult[Timepoint] { rs =>
      val offset = rs.<<[Offset]
      val timestamp = rs.<<[CantonTimestamp]
      Timepoint(offset)(timestamp)
    }

  override protected val pretty: Pretty[Timepoint] = prettyOfClass(
    param("offset", _.offset),
    param("recordTime", _.recordTime),
  )
}
