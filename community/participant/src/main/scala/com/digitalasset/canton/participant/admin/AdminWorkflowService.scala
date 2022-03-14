// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.v1.transaction.Transaction
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Admin service that connects to the ledger and process events
  */
trait AdminWorkflowService extends AutoCloseable {
  private[admin] def processTransaction(tx: Transaction)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}
