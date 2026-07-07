// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

private[store] trait AcsDigestJournal[K, V] extends AcsDigestStore.DigestJournal[K, V] {

  /** Deletes all digest entries whose offset is higher than `fromExclusive`.
    */
  def deleteAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Deletes all digest entries whose offset is lower than `toExclusive` and that satisfy one of
    * the following conditions:
    *
    *   - The entry has been replaced (see
    *     [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]])
    *     by an entry with a higher offset, but still lower than or equal `toExclusive`.
    *   - The entry's [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.digestO]]
    *     is [[scala.None$]].
    */
  def deleteUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}
