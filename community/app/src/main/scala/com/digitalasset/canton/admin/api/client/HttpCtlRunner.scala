// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import cats.data.EitherT
import com.digitalasset.canton.admin.api.client.commands.HttpAdminCommand
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.http.HttpClient
import com.digitalasset.canton.tracing.TraceContext

import java.net.URL
import scala.concurrent.{ExecutionContext, Future}

class HttpCtlRunner(val loggerFactory: NamedLoggerFactory) extends NamedLogging {

  /** Runs an HTTP command
    * @return Either a printable error as a String or a Unit indicating all was successful
    */
  def run[Req, Res, Result](
      instanceName: String,
      command: HttpAdminCommand[Req, Res, Result],
      baseUrl: URL,
      httpClient: HttpClient,
      timeouts: ProcessingTimeout,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, String, Result] = {

    val client = command.createService(baseUrl, httpClient, timeouts, loggerFactory)

    for {
      request <- EitherT.fromEither[Future](command.createRequest())
      response <- {
        logger.debug(s"Sending http request ${command.toString} to $instanceName.")
        command.submitRequest(client, request)
      }
      result <- EitherT.fromEither[Future](command.handleResponse(response))
    } yield result

  }
}
