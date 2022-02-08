// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import cats.data.EitherT
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InMemoryDomainManagerNodeSequencerConfigStore(implicit executionContext: ExecutionContext)
    extends DomainManagerNodeSequencerConfigStore {
  private val currentConfiguration = new AtomicReference[Option[DomainNodeSequencerConfig]](None)

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[DomainNodeSequencerConfig]] =
    EitherT.pure(currentConfiguration.get())

  override def saveConfiguration(
      configuration: DomainNodeSequencerConfig
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    currentConfiguration.set(Some(configuration))
    EitherT.pure(())
  }
}
