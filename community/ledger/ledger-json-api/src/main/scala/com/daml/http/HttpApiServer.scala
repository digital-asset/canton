// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.metrics.HttpApiMetrics
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import io.grpc.Channel
import scalaz.std.anyVal.*
import scalaz.std.option.*
import scalaz.syntax.show.*

import java.nio.file.Path
import scala.util.Success

object HttpApiServer {
  private[this] val logger = ContextualizedLogger.get(getClass)

  def apply(config: JsonApiConfig, channel: Channel)(implicit
      jsonApiMetrics: HttpApiMetrics,
      loggingContext: LoggingContext,
  ): ResourceOwner[Unit] =
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("http-json-ledger-api"))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      executionSequencerFactory <- ResourceOwner.forCloseable(() =>
        new AkkaExecutionSequencerPool("httpPool")(actorSystem)
      )
      serverBinding <- instanceUUIDLogCtx(implicit loggingContextOf =>
        new HttpService(config, channel)(
          actorSystem,
          materializer,
          executionSequencerFactory,
          loggingContextOf,
          jsonApiMetrics,
        )
      )
    } yield {
      logger.info(
        s"HTTP JSON API Server started with (address=${config.address: String}" +
          s", configured httpPort=${config.httpPort.getOrElse(0)}" +
          s", assigned httpPort=${serverBinding.localAddress.getPort}" +
          s", portFile=${config.portFile: Option[Path]}" +
          s", staticContentConfig=${config.staticContentConfig.shows}" +
          s", allowNonHttps=${config.allowNonHttps.shows}" +
          s", wsConfig=${config.wsConfig.shows}" +
          ")"
      )
    }
}
