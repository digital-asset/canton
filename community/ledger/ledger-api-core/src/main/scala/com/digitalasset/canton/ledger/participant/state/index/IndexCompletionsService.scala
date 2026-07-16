// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamResponse,
  GetCompletionByHashResponse,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService]]
  */
trait IndexCompletionsService extends LedgerEndService {
  def getCompletions(
      begin: Option[Offset],
      userId: Option[Ref.UserId],
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Source[CompletionStreamResponse, NotUsed]

  /** Looks up the completion(s) for a given interactive-submission transaction hash.
    *
    * This is best-effort: submissions rejected before phase-3 reinterpretation may carry no hash on
    * their completion (rare topology-change races, e.g. acting parties reassigned between prepare
    * and submission, or a mediator disabled mid-submission), in which case they cannot be found
    * here even though a completion was emitted on the stream.
    */
  def getCompletionByHash(
      hash: ByteString,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetCompletionByHashResponse]
}
