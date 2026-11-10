// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}

final case class RollbackScope(inRollback: Boolean) extends PrettyPrintingFromCompanion {
  def prettyCompanion: PrettyPrintingCompanion[RollbackScope] = RollbackScope
}
object RollbackScope extends PrettyPrintingCompanion[RollbackScope] {
  override protected val pretty: Pretty[RollbackScope] =
    prettyOfClass(
      paramIfTrue("in rollback", _.inRollback)
    )
  val empty: RollbackScope = RollbackScope(inRollback = false)
}

final case class WithRollbackScope[T](rbScope: RollbackScope, unwrap: T)
