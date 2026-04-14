// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencer

/** An environment plugin to use the programmable sequencer in an environment.
  *
  * The programmable sequencer allows you to control the sequencing of messages in a test, such that
  * you can test different inter-leavings of messages.
  */
class UseProgrammableSequencer(
    environmentId: String,
    override protected val loggerFactory: NamedLoggerFactory,
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ProgrammableSequencer.configOverride(environmentId, loggerFactory)(config)
}
