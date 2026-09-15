// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfInterpretationConfig
import com.google.common.annotations.VisibleForTesting

object InterpretationConfig {

  val V34 = LfInterpretationConfig.V34
  val V35 = LfInterpretationConfig.V35
  val V36 = LfInterpretationConfig.V36
  val VDev = LfInterpretationConfig.Dev
  @VisibleForTesting
  def Legacy = LfInterpretationConfig.Legacy
  @VisibleForTesting
  def Default = LfInterpretationConfig.Default
  def Dev: LfInterpretationConfig = VDev

  def forProtocolVersion(protocolVersion: ProtocolVersion): LfInterpretationConfig =
    protocolVersion match {
      case ProtocolVersion.v34 => V34
      case ProtocolVersion.v35 => V35
      case ProtocolVersion.v36 => V36
      case ProtocolVersion.almostDev => VDev
      case ProtocolVersion.dev => VDev
      case other =>
        throw new IllegalArgumentException(s"Unsupported protocol version: $other")
    }

}
