// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

trait DomainManagerNodeSequencerConfigStore extends AutoCloseable {
  def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[DomainNodeSequencerConfig]]
  def saveConfiguration(configuration: DomainNodeSequencerConfig)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit]
}

object DomainManagerNodeSequencerConfigStore {
  def apply(
      storage: Storage,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): DomainManagerNodeSequencerConfigStore =
    storage match {
      case _: MemoryStorage => new InMemoryDomainManagerNodeSequencerConfigStore
      case storage: DbStorage =>
        new DbDomainManagerNodeSequencerConfigStore(
          storage,
          protocolVersion,
          timeouts,
          loggerFactory,
        )
    }
}
