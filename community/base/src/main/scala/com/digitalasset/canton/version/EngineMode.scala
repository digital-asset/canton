// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine as ContractStateMachine

object EngineMode {

  def forProtocolVersion(protocolVersion: ProtocolVersion): ContractStateMachine.Mode =
    protocolVersion match {
      case ProtocolVersion.v34 => ContractStateMachine.Mode.NoKey
      case ProtocolVersion.v35 => ContractStateMachine.Mode.NUCK
      case ProtocolVersion.dev => ContractStateMachine.Mode.devDefault
      case other => throw new IllegalArgumentException(s"Unsupported protocol version: $other")
    }

}
