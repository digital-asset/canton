// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

sealed trait DigestProcessor extends BaseDigestProcessor

trait ReinitializingDigestProcessor extends DigestProcessor {
  def reinitializingTimepoint: Timepoint
}

trait RunningDigestProcessor extends DigestProcessor
