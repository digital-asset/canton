// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.digitalasset.canton.data.CantonTimestamp

import java.util.concurrent.atomic.AtomicReference

final class SingleStepIncreasingRecordTime() extends (() => CantonTimestamp) {
  private val recordTimeRef = new AtomicReference(CantonTimestamp.now())
  override def apply(): CantonTimestamp = recordTimeRef.updateAndGet(_.immediateSuccessor)
}
