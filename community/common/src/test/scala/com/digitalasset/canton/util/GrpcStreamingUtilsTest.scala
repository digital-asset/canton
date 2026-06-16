// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, OutputStream}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{Future, Promise, blocking}
import scala.util.Success

final class GrpcStreamingUtilsTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  // we need to use the same value as in GrpcStreamingUtils since it's not configurable
  val defaultChunkSize = GrpcStreamingUtils.defaultChunkSize
  private def load(
      responseObserver: StreamObserver[String]
  ): StreamObserver[String] = new StreamObserver[String] {
    override def onNext(value: String): Unit = ()
    override def onError(t: Throwable): Unit = responseObserver.onError(t)

    override def onCompleted(): Unit = {
      responseObserver.onNext("response")
      responseObserver.onCompleted()
    }
  }
  def makeRequest(bytes: Array[Byte]): String = new String(bytes)

  /** Base test double for [[ServerCallStreamObserver]] with no-op implementations of the
    * flow-control and lifecycle callbacks. Tests override only what they care about (typically
    * `onNext` and the readiness behavior).
    */
  private class TestServerCallStreamObserver[T] extends ServerCallStreamObserver[T] {
    override def isReady: Boolean = true
    override def setOnReadyHandler(onReadyHandler: Runnable): Unit = ()
    override def disableAutoInboundFlowControl(): Unit = ()
    override def request(count: Int): Unit = ()
    override def setMessageCompression(enable: Boolean): Unit = ()
    override def setCompression(compression: String): Unit = ()
    override def isCancelled: Boolean = false
    override def setOnCancelHandler(onCancelHandler: Runnable): Unit = ()

    override def onNext(value: T): Unit = ()
    override def onError(t: Throwable): Unit = ()
    override def onCompleted(): Unit = ()
  }

  /** Test double whose readiness can be toggled to mimic gRPC flow control. It starts ready by
    * default. `setReady` flips readiness synchronously and runs the registered onReady handler,
    * while `goNotReadyThenReadyAfter` simulates the transport buffer filling up and then draining
    * asynchronously (firing the onReady handler from another thread).
    */
  private class ControllableReadyObserver[T](initiallyReady: Boolean = true)
      extends TestServerCallStreamObserver[T] {
    private val readyRef = new AtomicBoolean(initiallyReady)
    @volatile private var onReadyHandler: Runnable = () => ()

    override def isReady: Boolean = readyRef.get()
    override def setOnReadyHandler(handler: Runnable): Unit = onReadyHandler = handler

    def setReady(value: Boolean): Unit = {
      readyRef.set(value)
      onReadyHandler.run()
    }

    def goNotReadyThenReadyAfter(delayMillis: Long): Unit = {
      readyRef.set(false)
      val handler = onReadyHandler
      val _ = Future {
        blocking(Thread.sleep(delayMillis))
        readyRef.set(true)
        handler.run()
      }
    }
  }

  "streamToServer with InputStream" should {
    "stream all chunks to the server" in {
      val data = Array.tabulate[Byte](defaultChunkSize * 2 + 100)(_.toByte)
      val inputStream = new ByteArrayInputStream(data)
      val chunks = mutable.Buffer[Array[Byte]]()

      val result = GrpcStreamingUtils.streamToServer(
        load,
        bytes => { chunks += bytes.clone(); makeRequest(bytes) },
        inputStream,
      )

      result.futureValue
      chunks should have size 3
      chunks.last.length shouldBe 100
      chunks.flatten.toArray shouldBe data
    }

    "last chunk is equal to defaultChunkSize" in {
      val data = Array.tabulate[Byte](defaultChunkSize * 3)(_.toByte)
      val inputStream = new ByteArrayInputStream(data)
      val chunks = mutable.Buffer[Array[Byte]]()

      val result = GrpcStreamingUtils.streamToServer(
        load,
        bytes => { chunks += bytes.clone(); makeRequest(bytes) },
        inputStream,
      )

      result.futureValue
      chunks should have size 3
      chunks.last.length shouldBe defaultChunkSize
      chunks.flatten.toArray shouldBe data
    }

    "data is smaller than defaultChunkSize" in {
      val data = Array.tabulate[Byte](defaultChunkSize - 1)(_.toByte)
      val inputStream = new ByteArrayInputStream(data)
      val chunks = mutable.Buffer[Array[Byte]]()

      val result = GrpcStreamingUtils.streamToServer(
        load,
        bytes => { chunks += bytes.clone(); makeRequest(bytes) },
        inputStream,
      )

      result.futureValue
      chunks should have size 1
      chunks.last.length shouldBe defaultChunkSize - 1
      chunks.flatten.toArray shouldBe data
    }

    "not eagerly consume the entire inputStream before sending chunks" in {
      val totalChunks = 3
      val data = Array.tabulate[Byte](defaultChunkSize * totalChunks)(_.toByte)
      val bytesReadFromStream = new AtomicInteger(0)

      val inputStream = new ByteArrayInputStream(data) {
        override def read(b: Array[Byte], off: Int, len: Int): Int = {
          val n = super.read(b, off, len)
          if (n > 0) bytesReadFromStream.addAndGet(n)
          n
        }
      }

      val chunksReceived = mutable.Buffer[Int]()

      val result = GrpcStreamingUtils.streamToServer(
        load,
        bytes => {
          chunksReceived += bytesReadFromStream.get()
          makeRequest(bytes)
        },
        inputStream,
      )

      result.futureValue

      chunksReceived.size shouldBe totalChunks

      // At the time chunk i is sent, exactly (i+1) chunks have been read —
      // no look-ahead, no eager drain.
      chunksReceived.zipWithIndex.foreach { case (bytesReadAtSend, i) =>
        bytesReadAtSend shouldBe (i + 1) * defaultChunkSize
      }
    }
  }

  "streamGzippedChunksFromClient" should {
    "swallow gRPC inbound-stream termination to avoid double-closing the call" in {
      val onErrorCalled = new AtomicBoolean(false)

      val responseObserver = new StreamObserver[String] {
        override def onNext(value: String): Unit = ()
        override def onError(t: Throwable): Unit = onErrorCalled.set(true)
        override def onCompleted(): Unit = ()
      }

      val requestObserver =
        GrpcStreamingUtils.streamGzippedChunksFromClient[String, String, String, String](
          responseObserver = responseObserver,
          responseIfNoRequests = Success("default-response"),
          getGzippedBytes = _ => ByteString.EMPTY,
          parseMessage = _ => None, // unused InputStream parameter => None
        )(
          contextFromFirstRequest = req => Success(req)
        )(action =
          (
              _, // unused ctx
              _, // unused source parameters
          ) => FutureUnlessShutdown.pure("done")
        )

      // Simulate the gRPC framework terminating the inbound stream (e.g., node shutdown or client cancellation)
      requestObserver.onError(new RuntimeException("Simulated inbound stream cancellation"))

      // The responseObserver's onError MUST NOT be called. If it is, the gRPC library throws "IllegalStateException: call already closed".
      onErrorCalled.get() shouldBe false
    }
  }

  "streamToClient" should {

    val fromByteString: FromByteString[ByteString] = (chunk: ByteString) => chunk

    "deliver chunks incrementally (not all at once after producer finishes)" in {
      val chunkSize = 1024 * 64
      val totalChunks = 5

      val chunkDelivered = Array.fill(totalChunks)(new CountDownLatch(1))
      val proceedWithNext = Array.fill(totalChunks)(new CountDownLatch(1))
      val completed = new CountDownLatch(1)
      val chunksReceived = new AtomicInteger(0)
      val producerCompleted = new CountDownLatch(1)

      def awaitLatch(latch: CountDownLatch, clue: String): Unit =
        withClue(clue)(latch.await(10, TimeUnit.SECONDS) shouldBe true)

      def assertProducerStillRunning(chunkIdx: Int): Unit =
        withClue(s"Producer must not have completed when chunk $chunkIdx is delivered") {
          producerCompleted.getCount shouldBe 1L
        }

      val responseObserver = new TestServerCallStreamObserver[ByteString] {
        override def onNext(value: ByteString): Unit = {
          val idx = chunksReceived.getAndIncrement()
          if (idx < totalChunks) {
            chunkDelivered(idx).countDown()
            awaitLatch(proceedWithNext(idx), s"Chunk $idx should be allowed to proceed")
          }
        }
        override def onError(t: Throwable): Unit = completed.countDown()
        override def onCompleted(): Unit = completed.countDown()
      }

      def producer(os: OutputStream): Future[Unit] = Future {
        blocking {
          val data = new Array[Byte](chunkSize)
          for (i <- 0 until totalChunks) {
            java.util.Arrays.fill(data, i.toByte)
            os.write(data)
            os.flush()
            awaitLatch(chunkDelivered(i), s"Chunk $i should be delivered before producer continues")
          }
          producerCompleted.countDown()
        }
      }

      // Run in background because streamToClient blocks via Await.result
      val streamingF = Future {
        blocking {
          GrpcStreamingUtils.streamToClient(
            responseF = producer,
            responseObserver = responseObserver,
            fromByteString = fromByteString,
            chunkSizeO = Some(chunkSize),
          )
        }
      }

      // Verify first chunk is delivered while producer is still writing
      awaitLatch(chunkDelivered(0), "First chunk should be delivered")
      assertProducerStillRunning(0)
      proceedWithNext(0).countDown()

      // Verify remaining chunks are delivered incrementally
      for (i <- 1 until totalChunks) {
        awaitLatch(chunkDelivered(i), s"Chunk $i should be delivered")
        if (i < totalChunks - 1) assertProducerStillRunning(i)
        proceedWithNext(i).countDown()
      }

      completed.await(30, TimeUnit.SECONDS) shouldBe true
      chunksReceived.get() shouldBe totalChunks
      streamingF.futureValue
    }

    "wait until observer is ready before calling onNext" in {
      val chunkSize = 8
      val inputData = Array.tabulate[Byte](chunkSize)((i: Int) => i.toByte)
      val context = io.grpc.Context.current().withCancellation()

      val chunksReceived = new AtomicInteger(0)
      val calledWhileNotReady = new AtomicInteger(0)
      val onNextCalled = new CountDownLatch(1)

      val responseObserver = new ControllableReadyObserver[ByteString](initiallyReady = false) {
        override def onNext(value: ByteString): Unit = {
          if (!isReady) calledWhileNotReady.incrementAndGet()
          chunksReceived.incrementAndGet()
          onNextCalled.countDown()
        }
      }

      val streamingF =
        GrpcStreamingUtils.streamResponseChunks(context, responseObserver)(
          new ByteArrayInputStream(inputData),
          chunkSize,
          (chunk: ByteString) => chunk,
        )

      onNextCalled.await(5, TimeUnit.SECONDS) shouldBe false
      calledWhileNotReady.get() shouldBe 0
      chunksReceived.get() shouldBe 0

      responseObserver.setReady(true)

      onNextCalled.await(10, TimeUnit.SECONDS) shouldBe true
      chunksReceived.get() shouldBe 1
      calledWhileNotReady.get() shouldBe 0

      streamingF.futureValue
    }

    "complete the future when responseObserver.onNext throws an exception" in {
      val chunkSize = 8
      val inputData = Array.tabulate[Byte](chunkSize)((i: Int) => i.toByte)
      val context = io.grpc.Context.current().withCancellation()

      val responseObserver = new TestServerCallStreamObserver[ByteString] {
        override def onNext(value: ByteString): Unit =
          throw new RuntimeException("onNext exploded")
      }

      val resultF = GrpcStreamingUtils.streamResponseChunks(context, responseObserver)(
        new ByteArrayInputStream(inputData),
        chunkSize,
        (chunk: ByteString) => chunk,
      )

      // If the bug is present, this future never completes and the test times out
      resultF.failed.futureValue shouldBe a[RuntimeException]
    }

    "handle producer failure gracefully" in {
      val completed = new CountDownLatch(1)
      val error = new AtomicInteger(0)

      val responseObserver = new TestServerCallStreamObserver[ByteString] {
        override def onError(t: Throwable): Unit = {
          error.incrementAndGet()
          completed.countDown()
        }
        override def onCompleted(): Unit = completed.countDown()
      }

      GrpcStreamingUtils.streamToClient(
        responseF = { (_: OutputStream) =>
          Future.failed(new RuntimeException("producer exploded"))
        },
        responseObserver = responseObserver,
        fromByteString = fromByteString,
        chunkSizeO = Some(defaultChunkSize),
      )

      completed.await(10, TimeUnit.SECONDS) shouldBe true
      error.get() shouldBe 1
    }

    "complete the future when observer becomes not-ready right at EOF" in {
      // Reproduces a race where scso.isReady transitions to false right after sending a chunk
      // and the worker exits the loop without observing EOF. The completion must still be
      // signaled once the observer becomes ready again, even if iter.hasNext was never
      // queried at the point where the observer was ready.
      val chunkSize = 8
      val inputData = Array.tabulate[Byte](chunkSize)(_.toByte)
      val context = io.grpc.Context.current().withCancellation()

      val responseObserver = new ControllableReadyObserver[ByteString] {
        override def onNext(value: ByteString): Unit =
          // Simulate flow control: observer goes not-ready right after buffering a chunk,
          // then becomes ready again shortly after (as the network drains the buffer).
          goNotReadyThenReadyAfter(20)
      }

      val resultF = GrpcStreamingUtils.streamResponseChunks(context, responseObserver)(
        new ByteArrayInputStream(inputData),
        chunkSize,
        (chunk: ByteString) => chunk,
      )

      // If the bug is present, this future never completes and the test times out
      resultF.futureValue
    }

    "complete streamToClient end-to-end with a ServerCallStreamObserver under flow control" in {
      // Mirrors RemoteDumpIntegrationTest: a producer writes multiple chunks to a piped
      // output stream and a ServerCallStreamObserver simulates flow control by going
      // not-ready after each onNext and firing onReadyHandler shortly afterwards.
      // Regression test for a hang where the worker exited the send loop with the observer
      // not ready and EOF was never signaled.
      val chunkSize = 1024
      val totalChunks = 50

      val onCompletedLatch = new CountDownLatch(1)
      val received = new AtomicInteger(0)

      val responseObserver = new ControllableReadyObserver[ByteString] {
        override def onNext(value: ByteString): Unit = {
          received.incrementAndGet()
          // Half the time, simulate the buffer filling up: go not-ready and let it drain
          // asynchronously, firing onReadyHandler from another thread.
          if (received.get() % 2 == 0) goNotReadyThenReadyAfter(5)
        }

        override def onError(t: Throwable): Unit = onCompletedLatch.countDown()
        override def onCompleted(): Unit = onCompletedLatch.countDown()
      }

      def producer(os: OutputStream): Future[Unit] = Future {
        blocking {
          val data = new Array[Byte](chunkSize)
          for (i <- 0 until totalChunks) {
            java.util.Arrays.fill(data, i.toByte)
            os.write(data)
          }
        }
      }

      val streamingF = Future {
        blocking {
          GrpcStreamingUtils.streamToClient(
            responseF = producer,
            responseObserver = responseObserver,
            fromByteString = (chunk: ByteString) => chunk,
            chunkSizeO = Some(chunkSize),
          )
        }
      }

      onCompletedLatch.await(30, TimeUnit.SECONDS) shouldBe true
      received.get() shouldBe totalChunks
      streamingF.futureValue
    }

    "complete the future when the context is cancelled while the observer is not ready" in {
      val chunkSize = 8
      val inputData = Array.tabulate[Byte](chunkSize * 4)(_.toByte)
      val context = io.grpc.Context.current().withCancellation()

      val onNextCalls = new AtomicInteger(0)

      val workerParkedWhileNotReady = new CountDownLatch(1)

      val responseObserver = new TestServerCallStreamObserver[ByteString] {
        // The observer never becomes ready and never invokes the onReady/onCancel handlers
        override def isReady: Boolean = {
          workerParkedWhileNotReady.countDown()
          false
        }

        override def onNext(value: ByteString): Unit = {
          onNextCalls.incrementAndGet()
          ()
        }
      }

      val resultF = GrpcStreamingUtils.streamResponseChunks(context, responseObserver)(
        new ByteArrayInputStream(inputData),
        chunkSize,
        (chunk: ByteString) => chunk,
      )

      workerParkedWhileNotReady.await(10, TimeUnit.SECONDS) shouldBe true
      context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))

      resultF.futureValue
      onNextCalls.get() shouldBe 0
    }

    "not propagate InterruptedException when the serving thread is interrupted while awaiting" in {
      // Regression test: when the gRPC serving thread is interrupted (e.g. the client cancelled or
      // timed out) while finishStream is blocked in Await.result, the InterruptedException must be
      // handled internally and not escape. Otherwise it bubbles up to the gRPC request handler and
      // is logged as an "unexpected throwable"
      val responseObserver = new TestServerCallStreamObserver[ByteString]

      val producerStarted = new CountDownLatch(1)
      val producer: OutputStream => Future[Unit] = { _ =>
        producerStarted.countDown()
        Promise[Unit]().future
      }

      val thrown = new AtomicReference[Throwable](null)
      val finished = new CountDownLatch(1)
      val thread = new Thread(() => {
        try
          GrpcStreamingUtils.streamToClient(
            responseF = producer,
            responseObserver = responseObserver,
            fromByteString = fromByteString,
            chunkSizeO = Some(defaultChunkSize),
          )
        catch {
          case t: Throwable => thrown.set(t)
        } finally finished.countDown()
      })
      thread.start()

      producerStarted.await(10, TimeUnit.SECONDS) shouldBe true
      blocking(Thread.sleep(200))
      thread.interrupt()

      withClue("streamToClient must return after the serving thread is interrupted") {
        finished.await(10, TimeUnit.SECONDS) shouldBe true
      }
      // With the fix, the InterruptedException is handled internally and never propagates.
      Option(thrown.get()) shouldBe None
    }
  }
}
