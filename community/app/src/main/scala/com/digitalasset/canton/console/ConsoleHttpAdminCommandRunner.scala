// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.instances.future.*
import cats.syntax.either.*
import com.digitalasset.canton.admin.api.client.HttpCtlRunner
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  CustomClientTimeout,
  DefaultBoundedTimeout,
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
}
import com.digitalasset.canton.admin.api.client.commands.HttpAdminCommand
import com.digitalasset.canton.config.{DomainCcfConfig, NonNegativeDuration}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.http.HttpClient
import com.digitalasset.canton.sequencing.HttpSequencerConnection
import com.digitalasset.canton.tracing.Spanning
import io.opentelemetry.api.trace.Tracer

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, blocking}

/** Attempt to run an http admin-api command against whatever is pointed at in the config
  */
class ConsoleHttpAdminCommandRunner(consoleEnvironment: ConsoleEnvironment)
    extends NamedLogging
    with AutoCloseable
    with Spanning {

  private implicit val executionContext: ExecutionContextExecutor =
    consoleEnvironment.environment.executionContext
  override val loggerFactory: NamedLoggerFactory = consoleEnvironment.environment.loggerFactory
  implicit val tracer: Tracer = consoleEnvironment.tracer

  private val httpRunner = new HttpCtlRunner(loggerFactory)
  private val clients = TrieMap[String, HttpClient]()

  def runCommand[Result](
      instanceName: String,
      command: HttpAdminCommand[_, _, Result],
      sequencerConnection: HttpSequencerConnection,
      ccfConfig: DomainCcfConfig,
  ): ConsoleCommandResult[Result] =
    withNewTrace[ConsoleCommandResult[Result]](command.fullName) { implicit traceContext => span =>
      span.setAttribute("instance_name", instanceName)
      val commandTimeouts = consoleEnvironment.commandTimeouts

      val awaitTimeout = command.timeoutType match {
        case CustomClientTimeout(timeout) => timeout
        // If a custom timeout for a console command is set, it involves some non-gRPC timeout mechanism
        // -> we set the gRPC timeout to Inf, so gRPC never times out before the other timeout mechanism
        case ServerEnforcedTimeout => NonNegativeDuration(Duration.Inf)
        case DefaultBoundedTimeout => commandTimeouts.bounded
        case DefaultUnboundedTimeout => commandTimeouts.unbounded
      }
      logger.debug(
        s"Running on ${instanceName} command ${command} against ${sequencerConnection.urls}"
      )(traceContext)

      val timeouts = consoleEnvironment.environment.config.parameters.timeouts.processing

      val clientE = blocking(synchronized {
        clients.get(instanceName) match {
          case Some(client) =>
            Right(client)
          case None =>
            HttpClient.Insecure
              .create(
                sequencerConnection.certificate,
                ccfConfig.memberKeyStore,
                Some(ccfConfig.memberKeyName),
              )(timeouts, loggerFactory)
              .map { client =>
                clients.update(instanceName, client)
                client
              }
              .leftMap(err => s"Failed to create http client: $err")
        }
      })

      val apiResult =
        awaitTimeout.await(s"running command $command")((for {
          client <- clientE.toEitherT
          result <- httpRunner
            .run(instanceName, command, sequencerConnection.urls.write, client, timeouts)
        } yield result).value)

      // convert to a console command result
      apiResult.toResult
    }

  override def close(): Unit = {
    clients.values.foreach(_.close())
    clients.clear()
  }
}
