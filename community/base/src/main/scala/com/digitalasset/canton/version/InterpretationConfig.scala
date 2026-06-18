// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfInterpretationConfig

object InterpretationConfig {

  val V34 = LfInterpretationConfig.Legacy
  def Legacy: LfInterpretationConfig = V34
  val V35 = LfInterpretationConfig.Default
  def Default: LfInterpretationConfig = V35
  val VDev = LfInterpretationConfig.Dev
  def Dev: LfInterpretationConfig = VDev

  def forProtocolVersion(protocolVersion: ProtocolVersion): LfInterpretationConfig =
    protocolVersion match {
      case ProtocolVersion.v34 => V34
      case ProtocolVersion.v35 => V35
      case ProtocolVersion.v36 => VDev
      case ProtocolVersion.dev => VDev
      case other =>
        throw new IllegalArgumentException(s"Unsupported protocol version: $other")
    }

}
