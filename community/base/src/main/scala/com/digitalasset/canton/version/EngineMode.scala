// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfContractStateMode

object EngineMode {

  def forProtocolVersion(protocolVersion: ProtocolVersion): LfContractStateMode =
    protocolVersion match {
      case ProtocolVersion.v34 => LfContractStateMode.NoKey
      case ProtocolVersion.v35 => LfContractStateMode.NUCK
      case ProtocolVersion.dev => LfContractStateMode.devDefault
      case other => throw new IllegalArgumentException(s"Unsupported protocol version: $other")
    }

}
