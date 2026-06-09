// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

object SequencerAggregatorTesting {

  def useNewAggregatorForTests: Boolean =
    sys.env.get(USE_NEW_SEQUENCER_AGGREGATOR).flatMap(_.toBooleanOption).getOrElse(false)

  private val USE_NEW_SEQUENCER_AGGREGATOR = "USE_NEW_SEQUENCER_AGGREGATOR"

}
