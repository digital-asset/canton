// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.version.ProtocolVersion

trait ProtocolConfig {
  // TODO (#11206) rename and check against non standard config
  def willCorruptYourSystemDevVersionSupport: Boolean
  def dontWarnOnDeprecatedPV: Boolean
  def initialProtocolVersion: ProtocolVersion
}
