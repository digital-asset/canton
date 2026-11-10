// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.participant.admin.party.AcsReplicationTestInterceptor.*
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.protocol.party.SourceParticipantStore
import com.digitalasset.canton.tracing.TraceContext

/** ACS replication test interceptor interface
  *
  * Only for use in integration tests. In fact the test interceptor is only configurable via
  * integration tests and disabled via pureconfig.
  */
trait AcsReplicationTestInterceptor {

  /** Specifies whether the SP proceeds or waits depending on the contents of the store.
    * @return
    *   Proceed to execute as normal or Wait to do nothing.
    */
  def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
      traceContext: TraceContext
  ): ProceedOrWait = Proceed

  /** Specifies whether the TP proceeds or waits depending on ACS replication progress.
    * @return
    *   Proceed to execute as normal or Wait to do nothing.
    */
  def onTargetParticipantProgress(progress: AcsReplicationStatus.AcsReplicationProgress)(implicit
      traceContext: TraceContext
  ): ProceedOrWait = Proceed
}

object AcsReplicationTestInterceptor {
  sealed trait ProceedOrWait
  object Proceed extends ProceedOrWait
  object Wait extends ProceedOrWait
  final case class Fail(reason: String) extends ProceedOrWait

  /** In production, always proceed as any type of disruptive or stalling behavior is to be used in
    * integration tests only.
    */
  object AlwaysProceed extends AcsReplicationTestInterceptor
}
