// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import com.digitalasset.canton.ledger.runner.common.CliConfig
import com.digitalasset.canton.ledger.runner.common.PureConfigReaderWriter.Secure.*
import com.digitalasset.canton.ledger.sandbox.BridgeConfig.DefaultMaximumDeduplicationDuration
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert
import scopt.OParser

import java.time.Duration
import scala.annotation.nowarn

final case class BridgeConfig(
    conflictCheckingEnabled: Boolean = true,
    submissionBufferSize: Int = 500,
    maxDeduplicationDuration: Duration = DefaultMaximumDeduplicationDuration,
    stageBufferSize: Int = 128,
)

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
object BridgeConfig {
  val DefaultMaximumDeduplicationDuration: Duration = Duration.ofMinutes(30L)
  val Default: BridgeConfig = BridgeConfig()

  implicit val Convert: ConfigConvert[BridgeConfig] = deriveConvert[BridgeConfig]

  val Parser: OParser[_, CliConfig[BridgeConfig]] = {
    val builder = OParser.builder[CliConfig[BridgeConfig]]
    OParser.sequence(
      builder
        .opt[Int]("bridge-submission-buffer-size")
        .text("Submission buffer size. Defaults to 500.")
        .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p))),
      builder
        .opt[Unit]("disable-conflict-checking")
        .hidden()
        .text("Disable ledger-side submission conflict checking.")
        .action((_, c) => c.copy(extra = c.extra.copy(conflictCheckingEnabled = false))),
      builder
        .opt[Boolean](name = "implicit-party-allocation")
        .optional()
        .text(
          "Deprecated parameter --  lf value translation cache doesn't exist anymore."
        )
        .action((_, config) => config)
        .text(
          "Deprecated parameter -- Implicit party creation isn't supported anymore."
        ),
      builder
        .opt[Int]("bridge-stage-buffer-size")
        .text(
          "Stage buffer size. This buffer is present between each conflict checking processing stage. Defaults to 128."
        )
        .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p))),
      builder.checkConfig(c =>
        Either.cond(
          c.maxDeduplicationDuration.forall(_.compareTo(Duration.ofHours(1L)) <= 0),
          (),
          "Maximum supported deduplication duration is one hour",
        )
      ),
    )
  }
}