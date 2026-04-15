// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.tracing.TraceContext

object NextGenInternalConsistencyChecker extends InternalConsistencyChecker {

  override def check(rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]])(implicit
      traceContext: TraceContext
  ): Either[ErrorWithInternalConsistencyCheck, Unit] = Right(())

}
