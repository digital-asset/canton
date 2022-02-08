// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait DomainParameterStore {

  /** Sets new domain parameters. Calls with the same argument are idempotent.
    *
    * @return The future fails with an [[java.lang.IllegalArgumentException]] if different domain parameters have been stored before.
    */
  def setParameters(newParameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Returns the last set domain parameters, if any. */
  def lastParameters(implicit traceContext: TraceContext): Future[Option[StaticDomainParameters]]
}
