// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.instances.future.catsStdInstancesForFuture
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, Threading}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{MonadUtil, OptionUtil}
import com.digitalasset.canton.{BaseTest, TestMetrics}
import org.scalatest.wordspec.AsyncWordSpecLike

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.Ordered.orderingToOrdered
import scala.concurrent.{ExecutionContext, Future}

trait PrunableByTimeTest {
  this: AsyncWordSpecLike & BaseTest & TestMetrics =>

  def prunableByTime[E](mkPrunable: ExecutionContext => PrunableByTime[E]): Unit = {

    val ts = CantonTimestamp.assertFromInstant(Instant.parse("2019-04-04T10:00:00.00Z"))
    val ts2 = ts.addMicros(1)
    val ts3 = ts2.addMicros(1)

    "pruning timestamps increase" in {
      val acs = mkPrunable(executionContext)
      for {
        status0 <- acs.pruningStatus
        _ <- acs.prune(ts)
        status1 <- acs.pruningStatus
        _ <- acs.prune(ts3)
        status2 <- acs.pruningStatus
        _ <- acs.prune(ts2)
        status3 <- acs.pruningStatus
      } yield {
        assert(status0 == None, "No pruning status initially")
        assert(
          status1.contains(PruningStatus(PruningPhase.Completed, ts)),
          s"Pruning status at $ts",
        )
        assert(
          status2.contains(PruningStatus(PruningPhase.Completed, ts3)),
          s"Pruniadvances to $ts3",
        )
        assert(
          status3.contains(PruningStatus(PruningPhase.Completed, ts3)),
          s"Pruning status remains at $ts3",
        )
      }
    }

    "pruning timestamps advance under concurrent pruning" in {
      val parallelEc =
        Threading.newExecutionContext("pruning-parallel-ec", logger, executorServiceMetrics)
      val prunable = mkPrunable(parallelEc)
      val iterations = 100

      def timestampForIter(iter: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(iter.toLong)
      def prune(iter: Int): Future[Unit] =
        valueOrFail(prunable.prune(timestampForIter(iter)))(s"pruning iteration $iter")

      val lastRead = new AtomicReference[Option[PruningStatus]](None)

      def read(): Future[Int] =
        valueOrFail(prunable.pruningStatus)("Failed to get the pruning status")
          .map { statusO =>
            val previousO = lastRead.getAndAccumulate(
              statusO,
              OptionUtil.mergeWith(_, _)(Ordering[PruningStatus].max),
            )
            assert(
              previousO.forall(previous => statusO.exists(previous <= _)),
              s"PrunableByTime pruning status decreased from $previousO to $statusO",
            )
            if (statusO.exists(_.phase == PruningPhase.Started)) 1 else 0
          }(parallelEc)

      val pruningsF = Future.traverse((1 to iterations).toList)(prune)(List, parallelEc)
      val readingsF = MonadUtil.sequentialTraverse(1 to iterations)(_ => read())(
        catsStdInstancesForFuture(parallelEc)
      )

      val testF = for {
        _ <- pruningsF
        readings <- readingsF
        statusEnd <- valueOrFail(prunable.pruningStatus)("Failed to get pruning status")
      } yield {
        logger.info(s"concurrent pruning test had ${readings.sum} intermediate readings")
        assert(
          statusEnd.contains(PruningStatus(PruningPhase.Completed, timestampForIter(iterations)))
        )
      }
      testF.thereafter { _ =>
        Lifecycle.close(
          ExecutorServiceExtensions(parallelEc)(logger, DefaultProcessingTimeouts.testing)
        )(logger)
      }
    }

  }

}
