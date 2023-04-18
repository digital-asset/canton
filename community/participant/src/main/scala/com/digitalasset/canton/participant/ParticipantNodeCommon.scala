// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrapCommon}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.time.HasUptime

trait ParticipantNodeBootstrapCommon {
  this: CantonNodeBootstrapCommon[_, _, _, _] =>

}

abstract class ParticipantNodeCommon extends CantonNode with NamedLogging with HasUptime
