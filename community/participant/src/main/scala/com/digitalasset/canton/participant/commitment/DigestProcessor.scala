// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.ledger.participant.state.InternalIndexService.AcsUpdateContainer
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfigOverrides
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{DelayUtil, LoggerUtil}
import com.digitalasset.daml.lf.data.Ref.Party
import io.grpc.StatusRuntimeException
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

sealed trait DigestProcessor extends BaseDigestProcessor

trait ReinitializingDigestProcessor extends DigestProcessor {
  def reinitializingTimepoint: Timepoint
}

trait RunningDigestProcessor extends DigestProcessor

object DigestProcessor {

  private def restartSource[A, Mat, Point](
      startingPoint: Point,
      retryable: (Int, Throwable) => Option[FiniteDuration],
  )(sourceFactory: Point => Source[A, Mat])(pointOf: A => Point)(implicit
      executionContext: ExecutionContext
  ): Source[A, NotUsed] = {

    def recursiveRecovery(point: Point, attempt: Int): Source[A, NotUsed] = {
      val retryAfter = new AtomicReference[Option[FiniteDuration]](None)
      sourceFactory(point)
        .mapMaterializedValue(_ => NotUsed)
        .recoverWith(
          Function.unlift { ex =>
            retryable(attempt, ex).map { delay =>
              retryAfter.set(Some(delay))
              Source.empty[A]
            }
          }
        )
        .foldConcatF(point)((_, next) => pointOf(next)) { last =>
          retryAfter.get match {
            case None => Future.successful(Source.empty)
            case Some(delay) =>
              DelayUtil.delay(delay).map((_: Unit) => recursiveRecovery(last, attempt + 1))
          }
        }
    }

    recursiveRecovery(startingPoint, 0)
  }

  /** Restarts the given source to be resilient to intermittent failures of the source. Skips over
    * all elements produced by the restarted source that have already been output before the
    * restart. This assumes that all the restarted sources deliver the elements in the same order,
    * and that each element's point (as determined by the `pointOf` function) is unique among all
    * delivered elements.
    */
  private def restartStableDistinctSource[A, Mat, Point](
      retryable: (Int, Throwable) => Option[FiniteDuration]
  )(
      sourceFromStart: () => Source[A, Mat]
  )(pointOf: A => Point)(implicit executionContext: ExecutionContext): Source[A, NotUsed] = {
    def sourceFactory(start: Option[Point]): Source[A, Mat] =
      start match {
        case None => sourceFromStart()
        case Some(previous) => sourceFromStart().dropWhile(pointOf(_) != previous).drop(1)
      }
    restartSource(Option.empty[Point], retryable)(sourceFactory)(elem => Some(pointOf(elem)))
  }

  private def retryRule(sourceName: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): (Int, Throwable) => Option[FiniteDuration] = (attempt, ex) =>
    ex match {
      case ex: StatusRuntimeException =>
        val retryIn = LedgerClientUtils.defaultRetryRulesEx(ex)
        retryIn.foreach { delay =>
          errorLoggingContext.info(
            s"Indexer source $sourceName has failed ${attempt + 1} times. Restarting the source after ${LoggerUtil
                .roundDurationForHumans(delay)}",
            ex,
          )
        }
        retryIn

      case _ => None
    }

  def acsUpdatesWithRetries(
      indexService: InternalIndexService,
      synchronizerId: SynchronizerId,
      startingOffset: Option[Offset],
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): Source[AcsUpdateContainer, NotUsed] = {
    implicit val traceContext: TraceContext = errorLoggingContext.traceContext
    restartSource(startingOffset, retryRule("acsUpdates"))(
      indexService.acsUpdates(synchronizerId, _)
    )(elem => Some(elem.offset))
  }

  def acsWithRetries(
      indexService: InternalIndexService,
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      stakeholders1: Set[LfPartyId],
      stakeholders2: Set[LfPartyId],
      configOverrides: ActiveContractsServiceStreamsConfigOverrides,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Source[InternalIndexService.ActiveContract, NotUsed] = {
    implicit val traceContext: TraceContext = errorLoggingContext.traceContext
    restartStableDistinctSource(
      retryRule(s"acs(at=$activeAt, stakeholders1=$stakeholders1, stakeholders2=$stakeholders2)")
    )(() =>
      indexService.acs(synchronizerId, activeAt, stakeholders1, stakeholders2, configOverrides)
    )(_.contractId)
  }

  def counterPartiesWithRetries(
      indexService: InternalIndexService,
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      party: Option[Party],
      configOverrides: ActiveContractsServiceStreamsConfigOverrides,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Source[LfPartyId, NotUsed] = {
    implicit val traceContext: TraceContext = errorLoggingContext.traceContext
    restartStableDistinctSource(retryRule(s"counterparties(at=$activeAt, party=$party)"))(() =>
      indexService.counterParties(synchronizerId, activeAt, party, configOverrides)
    )(Predef.identity)
  }
}
