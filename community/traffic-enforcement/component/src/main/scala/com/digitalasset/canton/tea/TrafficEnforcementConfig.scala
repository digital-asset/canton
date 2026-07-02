// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

/** Base configuration for the traffic enforcement app (TEA). Currently, only the internal,
  * in-process variant is supported, allowing for later an external TEA service to be configured as
  * well.
  */
sealed trait TrafficEnforcementConfig

object TrafficEnforcementConfig {
  final case class Internal() extends TrafficEnforcementConfig
}
