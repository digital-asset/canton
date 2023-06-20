// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.logging.LoggingContextWithTrace.withNewLoggingContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.IndexMetadata
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

object DumpIndexMetadata {
  def dumpIndexMetadata(loggerFactory: NamedLoggerFactory)(
      jdbcUrl: String
  )(implicit
      executionContext: ExecutionContext,
      context: ResourceContext,
  ): Future[Unit] = {
    val logger = loggerFactory.getTracedLogger(getClass)
    implicit val traceContext: TraceContext = TraceContext.empty
    withNewLoggingContext() { implicit loggingContext =>
      val metadataFuture = IndexMetadata.read(jdbcUrl, loggerFactory).use { metadata =>
        logger.warn(s"ledger_id: ${metadata.ledgerId}")
        logger.warn(s"participant_id: ${metadata.participantId}")
        logger.warn(s"ledger_end: ${metadata.ledgerEnd}")
        logger.warn(s"version: ${metadata.participantIntegrationApiVersion}")
        Future.unit
      }
      metadataFuture.failed.foreach { exception =>
        logger.error("Error while retrieving the index metadata", exception)
      }
      metadataFuture
    }
  }

  def apply(
      jdbcUrls: Seq[String],
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[Unit] = {
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        Resource.sequenceIgnoringValues(
          jdbcUrls.map(dumpIndexMetadata(loggerFactory)).map(Resource.fromFuture)
        )
      }
    }
  }
}
