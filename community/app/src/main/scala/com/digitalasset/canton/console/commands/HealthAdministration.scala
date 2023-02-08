// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.CommandErrors.{CommandError, GenericCommandError}
import com.digitalasset.canton.console.ConsoleMacros.utils
import com.digitalasset.canton.console.commands.HealthAdministration.HealthDumpObserver
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CantonHealthAdministration,
  CommandErrors,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  Help,
  Helpful,
}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.admin.v0.HealthDumpChunk
import com.digitalasset.canton.health.admin.{data, v0}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ResourceUtil
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver

import java.io.FileOutputStream
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Await, Promise, TimeoutException}
import scala.util.{Failure, Success, Try}

object HealthAdministration {
  class HealthDumpObserver(healthDumpFile: File, requestComplete: Promise[String])
      extends StreamObserver[HealthDumpChunk] {
    private val os: FileOutputStream = healthDumpFile.newFileOutputStream(append = false)

    override def onNext(value: HealthDumpChunk): Unit = {
      Try(os.write(value.chunk.toByteArray)) match {
        case Failure(exception) =>
          ResourceUtil.closeAndAddSuppressed(Some(exception), os)
          throw exception
        case Success(_) => // all good
      }
    }
    override def onError(t: Throwable): Unit = {
      requestComplete.tryFailure(t).discard
      ResourceUtil.closeAndAddSuppressed(None, os)
    }
    override def onCompleted(): Unit = {
      requestComplete.trySuccess(healthDumpFile.pathAsString).discard
      ResourceUtil.closeAndAddSuppressed(None, os)
    }
  }
}

class HealthAdministration[S <: data.NodeStatus.Status](
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    deserialize: v0.NodeStatus.Status => ParsingResult[S],
) extends Helpful {
  private val initializedCache = new AtomicReference[Boolean](false)
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  import runner.*

  @Help.Summary("Get human (and machine) readable status info")
  def status: data.NodeStatus[S] = consoleEnvironment.run {
    CommandSuccessful(adminCommand(new StatusAdminCommands.GetStatus[S](deserialize)) match {
      case CommandSuccessful(success) => success
      case err: CommandError => data.NodeStatus.Failure(err.cause)
    })
  }

  @Help.Summary(
    "Creates a zip file containing diagnostic information about the canton process running this node"
  )
  def dump(
      outputFile: File = CantonHealthAdministration.defaultHealthDumpName,
      timeout: NonNegativeDuration = timeouts.ledgerCommand,
      chunkSize: Option[Int] = None,
  ): String = consoleEnvironment.run {
    val requestComplete = Promise[String]()
    val responseObserver = new HealthDumpObserver(outputFile, requestComplete)

    def call = consoleEnvironment.run {
      adminCommand(new StatusAdminCommands.GetHealthDump(responseObserver, chunkSize))
    }

    try {
      ResourceUtil.withResource(call) { _ =>
        CommandSuccessful(
          Await.result(requestComplete.future, timeout.duration)
        )
      }
    } catch {
      case sre: StatusRuntimeException =>
        GenericCommandError(GrpcError("Generating health dump file", "dump", sre).toString)
      case _: TimeoutException =>
        outputFile.delete(swallowIOExceptions = true)
        CommandErrors.ConsoleTimeout.Error(timeout.asJavaApproximation)
    }
  }

  private def runningCommand =
    adminCommand(
      StatusAdminCommands.IsRunning
    )
  private def initializedCommand =
    adminCommand(
      StatusAdminCommands.IsInitialized
    )

  def falseIfUnreachable(command: ConsoleCommandResult[Boolean]): Boolean =
    consoleEnvironment.run(CommandSuccessful(command match {
      case CommandSuccessful(result) => result
      case _: CommandError => false
    }))

  @Help.Summary("Check if the node is running")
  def running(): Boolean =
    // in case the node is not reachable, we assume it is not running
    falseIfUnreachable(runningCommand)

  @Help.Summary("Check if the node is running and is the active instance (mediator, participant)")
  def active: Boolean = status match {
    case NodeStatus.Success(status) => status.active
    case NodeStatus.NotInitialized(active) => active
    case _ => false
  }

  @Help.Summary("Returns true if node has been initialized.")
  def initialized(): Boolean = initializedCache.updateAndGet {
    case false =>
      // in case the node is not reachable, we cannot assume it is not initialized, because it could have been initialized in the past
      // and it's simply not running at the moment. so we'll allow the command to throw an error here
      consoleEnvironment.run(initializedCommand)
    case x => x
  }

  @Help.Summary("Wait for the node to be running")
  def wait_for_running(): Unit = waitFor(running())

  @Help.Summary("Wait for the node to be initialized")
  def wait_for_initialized(): Unit = {
    waitFor(initializedCache.updateAndGet {
      case false =>
        // in case the node is not reachable, we return false instead of throwing an error in order to keep retrying
        falseIfUnreachable(initializedCommand)
      case x => x
    })
  }

  private def waitFor(condition: => Boolean): Unit = {
    // all calls here are potentially unbounded. we do not know how long it takes
    // for a node to start or for a node to become initialised. so we use the unbounded
    // timeout
    utils.retry_until_true(timeout = consoleEnvironment.commandTimeouts.unbounded)(condition)
  }
}
