// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.server.api.services.domain

import com.daml.logging.LoggingContext
import com.daml.tracing.TelemetryContext
import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest

import scala.concurrent.Future

trait CommandSubmissionService {
  def submit(
      request: SubmitRequest
  )(implicit telemetryContext: TelemetryContext, loggingContext: LoggingContext): Future[Unit]
}
