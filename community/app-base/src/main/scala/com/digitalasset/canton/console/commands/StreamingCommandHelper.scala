// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.config
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.{CommandSuccessful, ConsoleCommandResult, ConsoleEnvironment}
import com.digitalasset.canton.networking.grpc.{BaseStreamObserver, GrpcError}
import com.digitalasset.canton.util.ResourceUtil
import io.grpc.StatusRuntimeException

import java.util.concurrent.TimeoutException
import scala.concurrent.Await

trait StreamingCommandHelper {
  protected def consoleEnvironment: ConsoleEnvironment
  protected def name: String

  def mkResult[In, Res](
      call: => AutoCloseable,
      requestDescription: String,
      observer: BaseStreamObserver[In, Res],
      timeout: config.NonNegativeDuration,
  ): Res =
    consoleEnvironment.run {
      try {
        ResourceUtil.withResource(call) { _ =>
          // Not doing noisyAwaitResult here, because we don't want to log warnings in case of a timeout.
          CommandSuccessful(
            Await.result(
              observer.result(),
              timeout.duration,
            )
          )
        }
      } catch {
        case sre: StatusRuntimeException =>
          GenericCommandError(GrpcError(requestDescription, name, sre).toString)
        case _: TimeoutException => ConsoleCommandResult.fromEither(observer.onTimeout(timeout))
      }
    }

}
