// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.typesafe.config.{Config as TypesafeConfig, ConfigFactory}
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigReader, ConfigSource, Derivation}

import java.io.File

trait ConfigLoader {

  private def toError(failures: ConfigReaderFailures): String = {
    s"Failed to load configuration: ${System.lineSeparator()}${failures.prettyPrint()}"
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null")) // interfacing with java
  private def configFromMap(configMap: Map[String, String]): TypesafeConfig = {
    import scala.jdk.CollectionConverters.*
    val map = configMap.map {
      case (key, value) if value.nonEmpty => key -> value
      case (key, _) => key -> null
    }.asJava
    ConfigFactory.parseMap(map)
  }

  def loadConfig[T](config: TypesafeConfig)(implicit
      reader: Derivation[ConfigReader[T]]
  ): Either[String, T] =
    ConfigSource.fromConfig(config).load[T].left.map(toError)

  def toTypesafeConfig(
      configFiles: Seq[File] = Seq(),
      configMap: Map[String, String] = Map(),
      fallback: TypesafeConfig = ConfigFactory.load(),
  ): TypesafeConfig = {
    val mergedConfig = configFiles
      .map(ConfigFactory.parseFile)
      .foldLeft(ConfigFactory.empty())((combined, config) => config.withFallback(combined))
      .withFallback(fallback)

    Seq(configFromMap(configMap)).foldLeft(mergedConfig)((combined, config) =>
      config.withFallback(combined)
    )
  }

}

object ConfigLoader extends ConfigLoader {}
