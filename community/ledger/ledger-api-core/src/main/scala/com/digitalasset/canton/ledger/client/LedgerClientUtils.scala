// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client

import com.digitalasset.base.error.utils.DecodedCantonError
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object LedgerClientUtils {

  /** Default retry rules which will retry on retryable known errors and if the ledger api is
    * unavailable or times out
    */
  def defaultRetryRules: Status => Option[FiniteDuration] = status =>
    defaultRetryRulesDecoded(DecodedCantonError.fromGrpcStatus(status), status.code)

  /** Default retry rules which will retry on retryable known errors and if the ledger api is
    * unavailable or times out
    */
  def defaultRetryRulesEx: StatusRuntimeException => Option[FiniteDuration] = status =>
    defaultRetryRulesDecoded(
      DecodedCantonError.fromStatusRuntimeException(status),
      status.getStatus.getCode.value,
    )

  private def defaultRetryRulesDecoded(
      decoded: Either[String, DecodedCantonError],
      code: Int,
  ): Option[FiniteDuration] =
    decoded.toOption.flatMap(_.retryIn).orElse {
      Option.when(code == Code.UNAVAILABLE.value || code == Code.DEADLINE_EXCEEDED.value)(1.second)
    }

  /** Convert codegen command to scala proto command */
  def javaCodegenToScalaProto(
      command: com.daml.ledger.javaapi.data.Command
  ): com.daml.ledger.api.v2.commands.Command =
    com.daml.ledger.api.v2.commands.Command.fromJavaProto(command.toProtoCommand)
}
