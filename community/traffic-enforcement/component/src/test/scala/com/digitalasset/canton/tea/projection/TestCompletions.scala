// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.daml.lf.data.Ref

/** Command and submission ids derive from the offset, so every completion has its own key. */
object TestCompletions {
  def record(
      offset: Long,
      actAs: Set[String],
      cost: Long,
      accepted: Boolean = true,
  ): CompletionRecord =
    CompletionRecord(
      ChangeId(
        Ref.UserId.assertFromString("user"),
        Ref.CommandId.assertFromString(s"command-$offset"),
        actAs.map(Ref.Party.assertFromString),
      ),
      submissionId = s"submission-$offset",
      paidTrafficCost = NonNegativeLong.tryCreate(cost),
      accepted = accepted,
    )

  def event(
      offset: Long,
      timestamp: CantonTimestamp,
      actAs: Set[String],
      cost: Long,
      accepted: Boolean = true,
  ): ProjectionEvent.Completion =
    ProjectionEvent.Completion(offset, timestamp, record(offset, actAs, cost, accepted))

  def checkpoint(
      offset: Long,
      timestamp: CantonTimestamp,
  ): ProjectionEvent.Checkpoint =
    ProjectionEvent.Checkpoint(offset, timestamp)
}
