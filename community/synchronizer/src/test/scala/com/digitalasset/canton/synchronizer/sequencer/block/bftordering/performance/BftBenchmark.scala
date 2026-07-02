// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance

import com.codahale.metrics.MetricRegistry
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBenchmark.{
  Separator,
  TxStatus,
  UuidLength,
  shutdownExecutorService,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBenchmarkConfig.TransactionSizeAndWeight
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftMetrics.{
  failedWriteMeters,
  pendingReads,
  readMeters,
  roundTripNanosHistogram,
  startedWriteMeters,
  successfulWriteMeters,
  writeNanosHistograms,
}
import com.digitalasset.nonempty.NonEmpty

import java.util.UUID
import java.util.concurrent.{
  Callable,
  ConcurrentHashMap,
  ExecutorService,
  Executors,
  Future as JFuture,
  ScheduledExecutorService,
  ScheduledFuture,
  TimeUnit,
}
import java.util.function.BiFunction

final class BftBenchmark(
    config: BftBenchmarkConfig,
    bftBindingFactory: BftBindingFactory,
    metrics: MetricRegistry,
) {

  private val log = ContextualizedLogger.get(getClass)
  implicit private val loggingContext: LoggingContext = LoggingContext.empty

  private val readNodeIndices =
    config.nodes.zipWithIndex
      .filter { case (node, _) => node.isInstanceOf[BftBenchmarkConfig.ReadNode[?]] }
      .map { case (_, index) => index }
      .toSet

  private val AdjustedTransactionSizesAndWeights: NonEmpty[Seq[TransactionSizeAndWeight]] =
    NonEmpty
      .from[Seq[TransactionSizeAndWeight]](config.transactionSizesAndWeights.map {
        case BftBenchmarkConfig.TransactionSizeAndWeight(size, weight) =>
          BftBenchmarkConfig.TransactionSizeAndWeight(
            NonNegativeInt
              .create(size.unwrap - UuidLength - Separator.length)
              .getOrElse(
                throw new IllegalArgumentException(
                  s"Transaction size must be at least ${UuidLength + Separator.length} bytes to accommodate the transaction ID and separator. Invalid size: ${size.unwrap}"
                )
              ),
            weight,
          )
      })
      .getOrElse(
        throw new IllegalArgumentException(
          "At least one transaction size and weight must be provided"
        )
      )

  log.info(s"Payloads will be $AdjustedTransactionSizesAndWeights")

  private val readNodes =
    config.nodes.zipWithIndex.flatMap {
      case (node: BftBenchmarkConfig.ReadNode[?], nodeIx) => Some(nodeIx -> node)
      case _ => None
    }

  private val writeNodes =
    config.nodes.zipWithIndex.flatMap {
      case (node: BftBenchmarkConfig.WriteNode[?], nodeIx) => Some(nodeIx -> node)
      case _ => None
    }

  private val bftBinding = bftBindingFactory.create(AdjustedTransactionSizesAndWeights)

  def run(): JFuture[Unit] = {
    val txsToBeRead = new ConcurrentHashMap[String, TxStatus]()

    startReads(txsToBeRead, metrics)

    val scheduler =
      Executors.newScheduledThreadPool(sys.runtime.availableProcessors() * config.nodes.size)

    val writeSchedules = startWrites(txsToBeRead, metrics)(scheduler)

    scheduleShutdown(scheduler, writeSchedules)
  }

  private def startReads(
      txsToBeRead: ConcurrentHashMap[String, TxStatus],
      metrics: MetricRegistry,
  ): Unit =
    readNodes.foreach { case (nodeIx, node) =>
      bftBinding.subscribeOnce(
        node,
        txIdFuture => {
          // Executes in the read task itself, avoiding delays and starvation.
          txIdFuture.thenAccept(processRead(nodeIx, txsToBeRead, metrics, _))
          ()
        },
      )
    }

  private def startWrites(
      txsToBeRead: ConcurrentHashMap[String, TxStatus],
      metrics: MetricRegistry,
  )(scheduler: ScheduledExecutorService): Seq[ScheduledFuture[?]] = {
    log.info(s"Starting scheduled writes every ${config.perNodeWritePeriod.toNanos} nanos")
    val testCatchup = config.testCatchup

    val delayForNodesThatAreDown =
      testCatchup.durationNodesAreDown.plus(testCatchup.durationNodeNeedToStartup).toNanos
    writeNodes.map { case (nodeIx, node) =>
      val initialDelay = if (testCatchup.nodesToStop.contains(nodeIx)) {
        delayForNodesThatAreDown
      } else {
        0L // No initial delay.
      }
      scheduler.scheduleAtFixedRate(
        writeRunnable(node, nodeIx, txsToBeRead, metrics),
        initialDelay,
        config.perNodeWritePeriod.toNanos, // Repeated write period.
        TimeUnit.NANOSECONDS,
      )
    }
  }

  private def scheduleShutdown(
      scheduler: ScheduledExecutorService,
      writeSchedules: Seq[ScheduledFuture[?]],
  ): ScheduledFuture[Unit] =
    scheduler.schedule(
      shutdownCallable(scheduler, writeSchedules),
      config.runDuration.toNanos,
      TimeUnit.NANOSECONDS,
    )

  private def processRead(
      nodeIndex: Int,
      txsToBeRead: ConcurrentHashMap[String, TxStatus],
      metrics: MetricRegistry,
      txId: String,
  ): Unit = {
    txsToBeRead.compute(
      txId,
      updateTxStatus(nodeIndex, metrics),
    )
    log.debug(
      s"In-progress transactions after processing read for $txId: $txsToBeRead"
    )
    val newInProgressTransactionsCount = txsToBeRead.size().toLong
    pendingReads = newInProgressTransactionsCount
    ()
  }

  private def updateTxStatus(
      nodeIndex: Int,
      metrics: MetricRegistry,
  ): BiFunction[String, TxStatus, TxStatus] = (txId: String, txStatus: TxStatus) => {
    val readNanos = System.nanoTime()

    Option(txStatus) match {
      case Some(TxStatus(writeNanos, awaitingNodeIndices)) =>
        readMeters(metrics, Seq(nodeIndex)).foreach(_.metric.mark())

        if (awaitingNodeIndices.sizeIs == 1) {
          roundTripNanosHistogram(metrics).metric.update(readNanos - writeNanos)
          log.debug(s"Transaction $txId is being received by the last read node")
          null // Free some memory.
        } else {
          val newTxStatus = txStatus.copy(awaitingNodeIndices = awaitingNodeIndices.excl(nodeIndex))
          log.debug(
            s"Transaction $txId still hasn't been received by nodes: ${txStatus.awaitingNodeIndices}"
          )
          log.trace(s"New transactions status for $txId: $newTxStatus")
          newTxStatus
        }

      case _ =>
        log.error(s"Transaction $txId read more than once per node")
        null
    }
  }

  private def writeRunnable(
      node: BftBenchmarkConfig.WriteNode[?],
      nodeIndex: Int,
      txsToBeRead: ConcurrentHashMap[String, TxStatus],
      metrics: MetricRegistry,
  ): Runnable = () => {
    log.trace(s"Starting scheduled write at ms ${System.nanoTime() / 1_000_000}")

    val txId = UUID.randomUUID().toString

    val startWriteNanos = System.nanoTime()

    txsToBeRead.put(txId, TxStatus(startWriteNanos, awaitingNodeIndices = readNodeIndices))
    startedWriteMeters(metrics, Seq(nodeIndex)).foreach(_.metric.mark())

    bftBinding
      .write(node, txId)
      .handle { (_, throwable) =>
        reportWriteMetrics(
          nodeIndex,
          txsToBeRead,
          txId,
          startWriteNanos,
          Option(throwable),
          metrics,
        )
      }
    ()
  }

  private def reportWriteMetrics(
      nodeIndex: Int,
      txsToBeRead: ConcurrentHashMap[String, TxStatus],
      txId: String,
      startWriteNanos: Long,
      throwable: Option[Throwable],
      metrics: MetricRegistry,
  ): Unit =
    throwable match {
      case Some(throwable) =>
        log.error(s"Write error for transaction $txId", throwable)
        txsToBeRead.remove(txId)
        failedWriteMeters(metrics, Seq(nodeIndex)).foreach(_.metric.mark())
      case _ =>
        val endWriteNanos = System.nanoTime()
        successfulWriteMeters(metrics, Seq(nodeIndex)).foreach(_.metric.mark())
        writeNanosHistograms(metrics, Seq(nodeIndex))
          .foreach(_.metric.update(endWriteNanos - startWriteNanos))
    }

  private def shutdownCallable(
      scheduler: ScheduledExecutorService,
      writeSchedules: Seq[ScheduledFuture[?]],
  ): Callable[Unit] = () => {

    log.info("Cancelling writes")
    writeSchedules.foreach(_.cancel(true))

    log.info("Shutting down the scheduler")
    shutdownExecutorService(scheduler)

    log.info("Closing the BFT binding")
    bftBinding.close()
  }
}

object BftBenchmark {

  private val Separator = "="

  private val UuidLength: Int = UUID.randomUUID().toString.length

  def shutdownExecutorService(executorService: ExecutorService): Unit = {
    executorService.shutdown()
    executorService.awaitTermination(10, TimeUnit.SECONDS)
    ()
  }

  private final case class TxStatus(
      writeStartNanos: Long,
      awaitingNodeIndices: Set[Int],
  )
}
