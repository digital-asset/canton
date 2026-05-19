// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.PruningOffsetCache.{Defined, Disabled, Undefined}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private sealed trait PruningOffsetCache
private object PruningOffsetCache {
  case object Disabled extends PruningOffsetCache
  sealed trait Enabled extends PruningOffsetCache
  case object Undefined extends Enabled
  final case class Defined(f: Future[Option[Offset]]) extends Enabled
}

trait PruningOffsetService {
  def pruningOffset(implicit traceContext: TraceContext): Future[Option[Offset]]
}

final class PruningOffsetServiceImpl(
    fetchFromDb: TraceContext => Future[Option[Offset]],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends PruningOffsetService
    with NamedLogging {

  private val state: AtomicReference[PruningOffsetCache] = new AtomicReference(Undefined)

  override def pruningOffset(implicit traceContext: TraceContext): Future[Option[Offset]] =
    state.updateAndGet {
      case Undefined =>
        logger.info("Pruning offset cache updated from Undefined to Defined")
        Defined(fetchFromDb(traceContext).thereafter {
          case Failure(ex) =>
            logger.info("Pruning offset fetch failed", ex)
            state.updateAndGet {
              case Defined(_) =>
                logger.info(
                  "Pruning offset cache reset from Defined to Undefined after fetch failure"
                )
                Undefined
              case other @ (Disabled | Undefined) =>
                logger.debug(
                  s"Pruning offset cache is $other during fetch failure, keeping $other"
                )
                other
            }.discard
          case Success(v) =>
            logger.debug(s"Pruning offset fetched and cached: $v")
        })
      case other => other
    } match {
      case Defined(f) =>
        logger.debug("Pruning offset served from cache")
        f
      case Disabled =>
        logger.debug("Pruning offset cache disabled, returning uncached result")
        fetchFromDb(traceContext)
      case Undefined =>
        throw new IllegalStateException(
          "Unreachable: updateAndGet should have transitioned Undefined to Defined"
        )
    }

  /** Disable caching. While disabled, reads go to the DB without being cached. */
  def disableCache()(implicit traceContext: TraceContext): Unit = {
    val previous = state.getAndSet(Disabled)
    logger.info(
      s"Pruning offset cache disabled (previous state: ${previous.getClass.getSimpleName})"
    )
  }

  /** Re-enable caching. The next read will fetch from DB and populate the cache. */
  def reEnableCache()(implicit traceContext: TraceContext): Unit = {
    val previous = state.getAndSet(Undefined)
    logger.info(
      s"Pruning offset cache re-enabled from ${previous.getClass.getSimpleName} to Undefined"
    )
  }
}
