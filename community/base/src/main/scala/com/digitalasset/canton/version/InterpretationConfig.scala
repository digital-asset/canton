// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfInterpretationConfig

object InterpretationConfig {

  def forProtocolVersion(protocolVersion: ProtocolVersion): LfInterpretationConfig =
    protocolVersion match {
      // Do not map to Dev versions, remove mapping
      case ProtocolVersion.v35 => LfInterpretationConfig.Stable
      case ProtocolVersion.v36 => LfInterpretationConfig.Stable
      case ProtocolVersion.v37 => LfInterpretationConfig.Stable
      case pv if pv.isDev || pv.isAlpha => LfInterpretationConfig.Dev
      case other => throw new IllegalArgumentException(s"Unsupported protocol version: $other")
    }

}
