// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}
import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.Update
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{TraceContext, Traced}

import scala.util.chaining.*

object AkkaSubmissionsBridge {
  def apply(loggerFactory: NamedLoggerFactory)(implicit
      materializer: Materializer
  ): ResourceOwner[
    (Sink[(Offset, Traced[Update]), NotUsed], Source[(Offset, Traced[Update]), NotUsed])
  ] =
    ResourceOwner.forValue(() => {
      val logger = loggerFactory.getTracedLogger(getClass)
      MergeHub
        // We can't instrument these buffers, therefore keep these to minimal sizes and
        // use a configurable instrumented buffer in the producer.
        .source[(Offset, Traced[Update])](perProducerBufferSize = 1)
        .toMat(BroadcastHub.sink(bufferSize = 1))(Keep.both)
        .run()
        .tap { _ =>
          logger.info("Instantiated Akka submissions bridge.")(TraceContext.empty)
        }
    })
}
