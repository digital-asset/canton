// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

private[mediator] class MediatorStateInspection(state: MediatorState) {
  def finalizedResponseCount()(implicit traceContext: TraceContext): Future[Long] =
    state.finalizedResponseStore.count()
}
