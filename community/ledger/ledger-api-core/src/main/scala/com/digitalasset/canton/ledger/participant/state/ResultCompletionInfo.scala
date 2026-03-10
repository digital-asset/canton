// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Information about a completion for a submission and associated traffic cost.
  *
  * @param completionInfo
  *   see [[CompletionInfo]]
  * @param paidTrafficCost
  *   traffic cost paid by this node for the ordering of the associated event
  */
final case class ResultCompletionInfo(
    completionInfo: CompletionInfo,
    paidTrafficCost: NonNegativeLong,
) extends PrettyPrinting {

  override protected def pretty: Pretty[ResultCompletionInfo.this.type] = prettyOfClass(
    param("completionInfo", _.completionInfo),
    param("paidTrafficCost", _.paidTrafficCost),
  )
}

object ResultCompletionInfo {
  implicit val `ResultCompletionInfo to LoggingValue`: ToLoggingValue[ResultCompletionInfo] = {
    case ResultCompletionInfo(completionInfo, trafficCost) =>
      LoggingValue.Nested.fromEntries(
        "completionInfo" -> LoggingValue.from(completionInfo),
        "trafficCost" -> trafficCost.value,
      )
  }
}
