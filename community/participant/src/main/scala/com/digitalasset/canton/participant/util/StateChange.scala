// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import slick.jdbc.GetResult

/** A status and when it became effective.
  *
  * @param status
  *   The status
  * @param asOf
  *   When the change became effective
  */
final case class StateChange[+Status <: PrettyPrintingFromCompanion](
    status: Status,
    asOf: TimeOfChange,
) extends PrettyPrintingFromCompanion {

  def timestamp: CantonTimestamp = asOf.timestamp

  override def prettyCompanion: PrettyPrintingCompanion[StateChange[PrettyPrintingFromCompanion]] =
    StateChange
}

object StateChange extends PrettyPrintingCompanion[StateChange[PrettyPrintingFromCompanion]] {
  def apply[Status <: PrettyPrintingFromCompanion](
      status: Status,
      timestamp: CantonTimestamp,
      repairCounterO: Option[RepairCounter],
  ): StateChange[Status] =
    StateChange[Status](status, TimeOfChange(timestamp, repairCounterO))

  implicit def stateChangeGetResult[A <: PrettyPrintingFromCompanion](implicit
      getResultStatus: GetResult[A]
  ): GetResult[StateChange[A]] =
    GetResult(r => StateChange(r.<<[A], r.<<[TimeOfChange]))

  override protected val pretty: Pretty[StateChange[PrettyPrintingFromCompanion]] = prettyOfClass(
    param("status", _.status),
    param("asOf", _.asOf),
  )
}
