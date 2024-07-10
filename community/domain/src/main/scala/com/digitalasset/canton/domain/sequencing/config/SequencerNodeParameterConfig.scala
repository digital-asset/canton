// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  LocalNodeParametersConfig,
  ProtocolConfig,
  WatchdogConfig,
}

/** Various parameters for non-standard sequencer settings
  *
  * @param alphaVersionSupport if true, then dev version will be turned on, but we will brick this sequencer node if it is used for production.
  * @param dontWarnOnDeprecatedPV if true, then this sequencer will not emit a warning when configured to use protocol version 2.0.0.
  * @param maxConfirmationRequestsBurstFactor how forgiving the rate limit is in case of bursts (so rate limit starts after observing an initial burst of factor * max_rate commands)
  */
final case class SequencerNodeParameterConfig(
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val alphaVersionSupport: Boolean = true,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    maxConfirmationRequestsBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.5),
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val useUnifiedSequencer: Boolean = false,
    override val watchdog: Option[WatchdogConfig] = None,
) extends ProtocolConfig
    with LocalNodeParametersConfig
