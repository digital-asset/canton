// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pruning

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.store.db.DbSerializationException
import slick.jdbc.{GetResult, SetParameter}

sealed trait PruningPhase extends Product with Serializable with PrettyPrintingFromCompanion {
  def kind: String
  def index: Int

  override def prettyCompanion: PrettyPrintingCompanion[PruningPhase] = PruningPhase
}

object PruningPhase extends PrettyPrintingCompanion[PruningPhase] {

  override protected val pretty: Pretty[PruningPhase] = prettyOfParam(_.kind.unquoted)

  case object Started extends PruningPhase {
    override val kind: String = "started"
    override def index: Int = 0
  }
  case object Completed extends PruningPhase {
    override val kind: String = "completed"
    override def index: Int = 1
  }

  def tryFromDbPrimitive: String => PruningPhase = {
    case Started.kind => Started
    case Completed.kind => Completed
    case other => throw new DbSerializationException(s"Unknown pruning phase $other")
  }

  implicit val orderingPruningPhase: Ordering[PruningPhase] =
    Ordering.by[PruningPhase, Int](_.index)

  implicit val getResultPruningPhase: GetResult[PruningPhase] =
    GetResult(r => PruningPhase.tryFromDbPrimitive(r.nextString()))
  implicit val setParameterPruningPhase: SetParameter[PruningPhase] = (d, pp) => pp >> d.kind
}

final case class PruningStatus(
    phase: PruningPhase,
    timestamp: CantonTimestamp,
    lastSuccess: Option[CantonTimestamp],
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[PruningStatus] = PruningStatus
}

object PruningStatus extends PrettyPrintingCompanion[PruningStatus] {

  override protected val pretty: Pretty[PruningStatus] = prettyOfClass(
    param("phase", _.phase),
    param("timestamp", _.timestamp),
    param("lastSuccess", _.lastSuccess),
  )

  implicit val orderingPruningStatus: Ordering[PruningStatus] =
    Ordering.by[PruningStatus, (CantonTimestamp, PruningPhase)](status =>
      (status.timestamp, status.phase)
    )

  implicit val getResultPruningStatus: GetResult[PruningStatus] = GetResult(r =>
    PruningStatus(
      PruningPhase.getResultPruningPhase(r),
      GetResult[CantonTimestamp].apply(r),
      GetResult[Option[CantonTimestamp]].apply(r),
    )
  )
}
