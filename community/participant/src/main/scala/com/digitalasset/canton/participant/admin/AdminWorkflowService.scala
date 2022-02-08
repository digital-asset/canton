// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.v1.transaction.Transaction
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Admin service that connects to the ledger and process events */
trait AdminWorkflowService extends AutoCloseable {
  private[admin] def processTransaction(tx: Transaction)(implicit traceContext: TraceContext): Unit
}

/** Admin service that supports processing transactions without blocking
  * TODO(soren): Convert ping service to returning a Future then remove this sync/async distinction
  */
trait AsyncAdminWorkflowService extends AdminWorkflowService with NamedLogging {

  protected def timeouts: ProcessingTimeout

  private[admin] def processTransactionAsync(tx: Transaction)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  override private[admin] def processTransaction(tx: Transaction)(implicit
      traceContext: TraceContext
  ): Unit =
    timeouts.unbounded.await(s"Processing transaction $tx")(processTransactionAsync(tx))
}
