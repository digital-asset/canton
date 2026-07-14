// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.completions

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamResponse,
  GetCompletionsRequest,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

class CompletionServiceClient(
    service: CommandCompletionServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    esf: ExecutionSequencerFactory
) {
  def getCompletionsSource(
      begin: Long,
      parties: Seq[String] = Seq.empty,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Source[CompletionStreamResponse, NotUsed] =
    ClientAdapter
      .serverStreaming(
        GetCompletionsRequest(
          beginExclusive = begin,
          parties = parties,
        ),
        LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken())).getCompletions,
      )
}
