// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import com.digitalasset.canton.ledger.runner.common.{
  CliConfig,
  Config,
  ConfigLoader,
  LegacyCliConfigConverter,
  PureConfigReaderWriter,
}
import com.typesafe.config.{Config as TypesafeConfig, ConfigFactory}
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import java.io.File
import scala.annotation.nowarn

final case class SandboxOnXConfig(
    ledger: Config = Config.Default,
    bridge: BridgeConfig = BridgeConfig.Default,
)
object SandboxOnXConfig {
  import PureConfigReaderWriter.Secure.*
  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  implicit val Convert: ConfigConvert[SandboxOnXConfig] = deriveConvert[SandboxOnXConfig]

  def loadFromConfig(
      configFiles: Seq[File] = Seq(),
      configMap: Map[String, String] = Map(),
      fallback: TypesafeConfig = ConfigFactory.load(),
  ): Either[String, SandboxOnXConfig] = {
    ConfigFactory.invalidateCaches()
    val typesafeConfig = ConfigLoader.toTypesafeConfig(configFiles, configMap, fallback)
    ConfigLoader.loadConfig[SandboxOnXConfig](typesafeConfig)
  }

  def fromLegacy(
      configAdaptor: BridgeConfigAdaptor,
      originalConfig: CliConfig[BridgeConfig],
  ): SandboxOnXConfig = {
    val Unsecure = new PureConfigReaderWriter(false)
    import Unsecure.*
    @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
    val Convert: ConfigConvert[SandboxOnXConfig] = deriveConvert[SandboxOnXConfig]
    val maxDeduplicationDuration = originalConfig.maxDeduplicationDuration.getOrElse(
      BridgeConfig.DefaultMaximumDeduplicationDuration
    )
    val sandboxOnXConfig = SandboxOnXConfig(
      ledger = LegacyCliConfigConverter.toConfig(configAdaptor, originalConfig),
      bridge = originalConfig.extra.copy(maxDeduplicationDuration = maxDeduplicationDuration),
    )
    // In order to support HOCON configuration via config files and key-value maps -
    // legacy config is rendered without redacting secrets and configuration is applied on top
    val fromConfig = loadFromConfig(
      configFiles = originalConfig.configFiles,
      configMap = originalConfig.configMap,
      fallback = ConfigFactory.parseString(ConfigRenderer.render(sandboxOnXConfig)(Convert)),
    )
    fromConfig.fold(
      msg => sys.error(s"Failed to parse config after applying config maps and config files: $msg"),
      identity,
    )
  }
}
