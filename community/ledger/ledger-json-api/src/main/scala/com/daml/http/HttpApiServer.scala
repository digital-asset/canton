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

class HttpApiServer(
    config: JsonApiConfig,
    channel: Channel,
)(implicit jsonApiMetrics: HttpApiMetrics) {

  private[this] val logger = ContextualizedLogger.get(getClass)

  def owner(implicit lc: LoggingContext): ResourceOwner[Unit] = {
    implicit val asys: ActorSystem = ActorSystem("http-json-ledger-api")
    implicit val materializer: Materializer = Materializer(asys)
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("clientPool")(asys)

    for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits in a `for` comprehension.
      _ <- ResourceOwner.forTryCloseable(() => Success(aesf))
      _ <- ResourceOwner.forActorSystem(() => asys)
      _ <- ResourceOwner.forMaterializer(() => materializer)
      serverBinding <- instanceUUIDLogCtx(implicit loggingContextOf => new HttpService(config, channel))
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
}
