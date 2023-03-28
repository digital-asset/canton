// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.server.api.services.domain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.ledger.api.messages.command.completion.CompletionStreamRequest

import scala.concurrent.Future

trait CommandCompletionService {

  def getLedgerEnd(): Future[LedgerOffset.Absolute]

  def completionStreamSource(
      request: CompletionStreamRequest
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed]

}
