// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import java.nio.file.Path

sealed abstract class Mode

object Mode {

  /** Run the participant, accepts HOCON configuration */
  case object Run extends Mode

  /** Run the participant in legacy mode with accepted CLI arguments */
  case object RunLegacyCliConfig extends Mode

  /** Accepts legacy Cli parameters, but just prints configuration */
  case object ConvertConfig extends Mode

  final case class PrintDefaultConfig(outputFilePath: Option[Path]) extends Mode

  /** Dump index metadata and exit */
  final case class DumpIndexMetadata(jdbcUrls: Vector[String]) extends Mode

}