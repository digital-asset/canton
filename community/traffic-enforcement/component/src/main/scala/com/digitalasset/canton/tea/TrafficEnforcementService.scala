// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.tea.TrafficEnforcementService.TrafficEnforcementServiceError
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.TraceContext

/** Transport-agnostic Traffic Enforcement App (TEA) operations.
  */
trait TrafficEnforcementService extends FlagCloseable {

  /** Return the local accounts (and their balances) configured for the requested account ID. */
  def getAccount(request: GetAccountRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[TrafficEnforcementServiceError, GetAccountResponse]]

  /** Update the account state for the given account ID */
  def updateAccount(request: UpdateAccountRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[TrafficEnforcementServiceError, UpdateAccountResponse]]
}

object TrafficEnforcementService {
  sealed trait TrafficEnforcementServiceError
  final case class NotEnoughTraffic(account: String, balance: Long, cost: Long)
      extends TrafficEnforcementServiceError
}
