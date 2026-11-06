// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.party

import com.digitalasset.canton.participant.admin.party.AcsReplicationTestInterceptor
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.protocol.party.SourceParticipantStore
import com.digitalasset.canton.tracing.TraceContext

class AcsReplicationTestInterceptorImpl extends AcsReplicationTestInterceptor {

  override def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
      traceContext: TraceContext
  ): AcsReplicationTestInterceptor.ProceedOrWait = AcsReplicationTestInterceptor.Proceed

  override def onTargetParticipantProgress(progress: AcsReplicationStatus.AcsReplicationProgress)(
      implicit traceContext: TraceContext
  ): AcsReplicationTestInterceptor.ProceedOrWait = AcsReplicationTestInterceptor.Proceed
}

object AcsReplicationTestInterceptorImpl {

  /** Create a test interceptor that proceeds iff the given condition is true (based on the contents
    * of the source participant store).
    */
  def sourceParticipantProceedsIf(
      canProceed: SourceParticipantStore => Boolean
  ): AcsReplicationTestInterceptorImpl =
    new AcsReplicationTestInterceptorImpl {
      override def onSourceParticipantProgress(store: SourceParticipantStore)(implicit
          traceContext: TraceContext
      ): AcsReplicationTestInterceptor.ProceedOrWait = proceedIf(canProceed(store))
    }

  /** Create a test interceptor that proceeds iff the given condition is true (based on the contents
    * of the target participant store).
    */
  def targetParticipantProceedsIf(
      canProceed: AcsReplicationStatus.AcsReplicationProgress => Boolean
  ): AcsReplicationTestInterceptorImpl =
    new AcsReplicationTestInterceptorImpl {
      override def onTargetParticipantProgress(
          progress: AcsReplicationStatus.AcsReplicationProgress
      )(implicit
          traceContext: TraceContext
      ): AcsReplicationTestInterceptor.ProceedOrWait = proceedIf(canProceed(progress))
    }

  /** Create a test interceptor that proceeds or waits by evaluating the given function (based on
    * the contents of the target participant store).
    */
  def targetParticipantEvaluates(
      evaluate: AcsReplicationStatus.AcsReplicationProgress => AcsReplicationTestInterceptor.ProceedOrWait
  ): AcsReplicationTestInterceptorImpl =
    new AcsReplicationTestInterceptorImpl {
      override def onTargetParticipantProgress(
          progress: AcsReplicationStatus.AcsReplicationProgress
      )(implicit
          traceContext: TraceContext
      ): AcsReplicationTestInterceptor.ProceedOrWait = evaluate(progress)
    }

  private def proceedIf(canProceed: Boolean): AcsReplicationTestInterceptor.ProceedOrWait =
    if (canProceed) AcsReplicationTestInterceptor.Proceed
    else AcsReplicationTestInterceptor.Wait
}
