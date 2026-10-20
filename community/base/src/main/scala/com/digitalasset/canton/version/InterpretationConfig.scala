// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfInterpretationConfig

object InterpretationConfig {

  /** Maps each protocol version to the LF interpretation config it uses.
    *
    * The interpretation config of a stable protocol version must never change, which is why
    * protocol versions map to constants of `LfInterpretationConfig` that are frozen per protocol
    * version. Do not map stable protocol versions to `LfInterpretationConfig.Dev`.
    *
    * If you add a new constant to `LfInterpretationConfig` or change an existing one, ask the Daml
    * language team to review your PR.
    */
  def forProtocolVersion(protocolVersion: ProtocolVersion): LfInterpretationConfig =
    protocolVersion match {
      case ProtocolVersion.v36 => LfInterpretationConfig.PV36
      case ProtocolVersion.v37 => LfInterpretationConfig.PV37
      case pv if pv.isDev || pv.isAlpha => LfInterpretationConfig.Dev
      case other => throw new IllegalArgumentException(s"Unsupported protocol version: $other")
    }

}
