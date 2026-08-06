// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt

/** Local limits for the sequencer public API.
  *
  * @param maxClientProtocolVersions
  *   maximum number of entries accepted in handshake `clientProtocolVersions`
  */
final case class SequencerLimits(
    maxClientProtocolVersions: PositiveInt = PositiveInt.tryCreate(1000)
)
