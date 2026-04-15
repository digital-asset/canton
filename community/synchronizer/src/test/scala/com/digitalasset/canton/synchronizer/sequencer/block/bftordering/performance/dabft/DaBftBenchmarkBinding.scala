// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.dabft

import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBenchmark.shutdownExecutorService
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBinding.TxConsumer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBinding,
  BftBindingFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.standalone.v1.StandaloneBftOrderingServiceGrpc.StandaloneBftOrderingServiceStub
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.standalone.v1.{
  ReadOrderedRequest,
  ReadOrderedResponse,
  SendRequest,
  StandaloneBftOrderingServiceGrpc,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex
import com.google.protobuf.ByteString
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.util.concurrent.*
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.language.existentials
import scala.util.chaining.*
import scala.util.{Failure, Success}

class DaBftBindingFactory(override val loggerFactory: NamedLoggerFactory)
    extends BftBindingFactory {
  override type T = DaBftBinding

  override def create(
      transactionSizesAndWeights: NonEmpty[Seq[BftBenchmarkConfig.TransactionSizeAndWeight]]
  ): DaBftBinding = {

    // We could generate enough random payloads in advance, but we may OOM, e.g. with 500k TPS:
    //
    //  1m run + 1m margin = 120s, Max total writes = 500k TPS * 120s = 60_000_000 < 2_147_483_647
    //  20KB * 60mil ~= 20GB * 60
    //
    // Alternatively, we could generate batches as we go but, since DABFT doesn't
    // compress, we just optimize and use pre-built payloads.

    val payloadsAndWeights =
      transactionSizesAndWeights.map {
        case BftBenchmarkConfig.TransactionSizeAndWeight(size, weight) =>
          ByteString.copyFromUtf8("0".repeat(size.unwrap)) -> weight
      }
    new DaBftBinding(payloadsAndWeights, loggerFactory)
  }
}

final class DaBftBinding(
    payloadsAndWeights: NonEmpty[Seq[(ByteString, PositiveInt)]],
    override val loggerFactory: NamedLoggerFactory,
) extends BftBinding
    with NamedLogging {

  implicit private val traceContext: TraceContext = TraceContext.empty

  private val MaxReadGrpcBytes = 32 * 1024 * 1024

  private val MaxRandom = payloadsAndWeights.map { case (_, weight) => weight.unwrap }.sum

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val navigablePayloadsWithSizesByCumulativeWeights = {
    val navigableMap = new java.util.TreeMap[Int, (ByteString, Int)]()
    var cumulativeWeight = 0
    payloadsAndWeights.foreach { case (payload, weight) =>
      cumulativeWeight += weight.unwrap
      navigableMap.put(cumulativeWeight, payload -> payload.size())
    }
    navigableMap
  }

  private val writeChannels =
    new ConcurrentHashMap[
      BftBenchmarkConfig.Node,
      (ManagedChannel, StandaloneBftOrderingServiceStub, Mutex),
    ]

  private val readChannels =
    new ConcurrentHashMap[BftBenchmarkConfig.ReadNode[
      ?
    ], (ManagedChannel, StreamObserver[ReadOrderedResponse])]

  private val scalaFutureExecutor =
    Threading.newExecutionContext(
      "da-bft-binding-scala-future",
      noTracingLogger,
      new ExecutorServiceMetrics(NoOpMetricsFactory),
    )
  private val scalaFutureExecutionContext =
    ExecutionContext.fromExecutor(scalaFutureExecutor)

  private val threadFactory =
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r)
        thread.setContextClassLoader(getClass.getClassLoader)
        thread.setUncaughtExceptionHandler((t: Thread, e: Throwable) =>
          logger.error(s"Uncaught exception in thread ${t.getName}", e)
        )
        thread
      }
    }
  private val readExecutor = Executors.newCachedThreadPool(threadFactory)
  private val writeExecutor = Executors.newCachedThreadPool(threadFactory)

  override def write(
      node: BftBenchmarkConfig.WriteNode[?],
      txId: String,
  ): CompletionStage[Unit] = {
    val result = new CompletableFuture[Unit]()
    val (stub, lock) = stubAndLockFor(node)

    logger.debug(s"Scheduling a write of txId $txId to node $node")

    writeExecutor.submit(new Runnable {
      override def run(): Unit = {

        val payloadSelector = ThreadLocalRandom.current().nextInt(1, MaxRandom + 1)

        logger.debug(s"Selecting payload for writing txId $txId to node $node")

        val (payload, payloadSize) =
          navigablePayloadsWithSizesByCumulativeWeights
            .ceilingEntry(payloadSelector)
            .getValue
        val request = SendRequest(txId, payload)

        logger.debug(s"Writing txId $txId to node $node (payload size $payloadSize bytes)")

        lock
          .exclusive {
            // The writer is not thread-safe
            stub.send(request)
          }
          .transform {
            case Success(response) =>
              response.rejectionReason.fold {
                logger.debug(s"Wrote txId $txId to node $node (payload size $payloadSize bytes)")
                result.complete(()).discard
              } { reason =>
                val exception =
                  new RuntimeException(
                    s"Request $txId (payload size $payloadSize bytes) was rejected by node $node: $reason"
                  )
                logger.error(
                  s"Rejected writing txId $txId to node $node (payload size $payloadSize bytes)",
                  exception,
                )
                result.completeExceptionally(exception).discard
              }
              Success(())
            case Failure(exception) =>
              logger.error(
                s"Error writing txId $txId to node $node (payload size $payloadSize bytes)",
                exception,
              )
              result.completeExceptionally(exception).discard
              Success(())
          }(scalaFutureExecutionContext)
          .discard
      }
    })

    result
  }

  override def subscribeOnce(node: BftBenchmarkConfig.ReadNode[?], txConsumer: TxConsumer): Unit = {
    readChannels
      .computeIfAbsent(
        node,
        _ => {
          val channelBuilder =
            node match {
              case BftBenchmarkConfig.InProcessReadWriteNode(_, _, readPort) =>
                InProcessChannelBuilder.forName(readPort)
              case BftBenchmarkConfig.NetworkedReadWriteNode(host, _, readPort) =>
                ManagedChannelBuilder.forTarget(s"$host:$readPort")
            }
          channelBuilder.usePlaintext().discard
          channelBuilder.maxInboundMessageSize(MaxReadGrpcBytes).discard
          val channel = channelBuilder.build()
          val stub = StandaloneBftOrderingServiceGrpc
            .stub(channel)
            .withMaxInboundMessageSize(MaxReadGrpcBytes)
            .withMaxOutboundMessageSize(MaxReadGrpcBytes)

          val reader: StreamObserver[ReadOrderedResponse] =
            new StreamObserver[ReadOrderedResponse] {
              override def onNext(response: ReadOrderedResponse): Unit = {
                logger.debug(s"Read batch of ${response.block.size} requests")
                response.block.foreach { o =>
                  readExecutor
                    .submit(new Runnable {
                      override def run(): Unit = {
                        val result = new CompletableFuture[String]()
                        try {
                          val txId = o.tag
                          result.complete(txId).discard
                          logger.debug(s"Read back UUID $txId")
                        } catch {
                          case t: Throwable =>
                            logger.error("Error while parsing request", t)
                        }
                        txConsumer(result)
                      }
                    })
                    .discard
                }
              }

              override def onError(t: Throwable): Unit = {
                logger.error("Error from server", t)
                complete()
              }

              override def onCompleted(): Unit =
                complete()

              def complete(): Unit = {
                logger.info(s"Write stream for node $node being completed")
                closeGrpcReadChannel(node)
                readChannels.remove(node).discard
              }
            }
          stub.readOrdered(ReadOrderedRequest(0), reader)
          channel -> reader
        },
      )
    ()
  }

  private def stubAndLockFor(
      writeNode: BftBenchmarkConfig.WriteNode[?]
  ): (StandaloneBftOrderingServiceStub, Mutex) =
    writeChannels
      .computeIfAbsent(
        writeNode,
        _ => {
          val channelBuilder =
            writeNode match {
              case BftBenchmarkConfig.InProcessWriteOnlyNode(_, writePort) =>
                InProcessChannelBuilder.forName(writePort)
              case BftBenchmarkConfig.InProcessReadWriteNode(_, writePort, _) =>
                InProcessChannelBuilder.forName(writePort)
              case BftBenchmarkConfig.NetworkedWriteOnlyNode(host, writePort) =>
                ManagedChannelBuilder.forTarget(s"$host:$writePort")
              case BftBenchmarkConfig.NetworkedReadWriteNode(host, writePort, _) =>
                ManagedChannelBuilder.forTarget(s"$host:$writePort")
            }
          channelBuilder.usePlaintext().discard
          val channel = channelBuilder.build()
          val stub =
            StandaloneBftOrderingServiceGrpc
              .stub(channel)
              .withMaxInboundMessageSize(MaxReadGrpcBytes)
              .withMaxOutboundMessageSize(MaxReadGrpcBytes)
          (channel, stub, Mutex())
        },
      )
      .pipe { case (_, stub, lock) => (stub, lock) }

  override def close(): Unit = {
    writeChannels.values().asScala.map { case (channel, _, _) => channel }.foreach(closeGrpcChannel)
    shutdownExecutorService(writeExecutor)

    closeGrpcStreamsAndChannels(readChannels)
    shutdownExecutorService(readExecutor)

    shutdownExecutorService(scalaFutureExecutor)
  }

  private def closeGrpcStreamsAndChannels[N <: BftBenchmarkConfig.Node, R](
      channels: ConcurrentHashMap[N, (ManagedChannel, StreamObserver[R])]
  ): Unit =
    channels.values().forEach { case (channel, observer) =>
      closeGrpcStreamAndChannel(channel, observer)
    }

  private def closeGrpcStreamAndChannel[R, N <: BftBenchmarkConfig.Node](
      channel: ManagedChannel,
      stream: StreamObserver[R],
  ): Unit = {
    stream.onCompleted()
    closeGrpcChannel(channel)
  }

  private def closeGrpcReadChannel[N <: BftBenchmarkConfig.Node](
      node: BftBenchmarkConfig.Node
  ): Unit =
    Option(readChannels.get(node)).map { case (channel, _) => channel }.foreach(closeGrpcChannel)

  private def closeGrpcChannel[N <: BftBenchmarkConfig.Node](channel: ManagedChannel): Unit =
    channel.shutdown().awaitTermination(20, TimeUnit.SECONDS).discard
}
