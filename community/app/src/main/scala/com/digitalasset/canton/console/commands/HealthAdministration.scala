// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.{HttpAdminCommand, StatusAdminCommands}
import com.digitalasset.canton.console.CommandErrors.CommandError
import com.digitalasset.canton.console.ConsoleMacros.utils
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  Help,
  Helpful,
}
import com.digitalasset.canton.health.admin.{data, v0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import java.util.concurrent.atomic.AtomicReference

class HealthAdministration[S <: data.NodeStatus.Status](
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    deserialize: v0.NodeStatus.Status => ParsingResult[S],
) extends Helpful {
  private val initializedCache = new AtomicReference[Boolean](false)

  import runner._

  @Help.Summary("Get human (and machine) readable status info")
  def status: data.NodeStatus[S] = consoleEnvironment.run {
    CommandSuccessful(adminCommand(new StatusAdminCommands.GetStatus[S](deserialize)) match {
      case CommandSuccessful(success) => success
      case err: CommandError => data.NodeStatus.Failure(err.cause)
    })
  }

  private def runningCommand =
    adminCommand(
      StatusAdminCommands.IsRunning,
      new HttpAdminCommand.Stub[Boolean]("running-stub", true),
    )
  private def initializedCommand =
    adminCommand(
      StatusAdminCommands.IsInitialized,
      new HttpAdminCommand.Stub[Boolean]("initialized-stub", true),
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
