// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import better.files.*
import better.files.File.newTemporaryFile
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.grpc.ByteStringStreamObserverWithContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.thereafterFutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.TryUtil.ForFailedOps
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  HasVersionedMessageCompanion,
  VersioningCompanion,
}
import com.google.protobuf.ByteString
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Context, Status}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Source, Source as PekkoSource}

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object GrpcStreamingUtils {

  private[util] final val defaultChunkSize: Int =
    1024 * 1024 * 2 // 2MB - This is half of the default max message size of gRPC

  def streamFromClient[Req, Resp, C](
      extractChunkBytes: Req => ByteString,
      extractContext: Req => C,
      processFullRequest: (ByteString, C) => Future[Resp],
      responseObserver: StreamObserver[Resp],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
  )(implicit ec: ExecutionContext): StreamObserver[Req] = {

    val observer =
      new ByteStringStreamObserverWithContext[Req, C](extractChunkBytes, extractContext) {
        override def onCompleted(): Unit = {
          super.onCompleted()
          val responseF = this.result.flatMap {
            case Some((byteString, context)) =>
              processFullRequest(byteString, context)
            case None =>
              Future.failed(new NoSuchElementException("No elements were received in stream"))
          }

          Try(Await.result(responseF, processingTimeout)) match {
            case Failure(exception) => responseObserver.onError(exception)
            case Success(response) =>
              responseObserver.onNext(response)
              responseObserver.onCompleted()
          }
        }
      }

    observer
  }

  /** Streams requests of gzipped bytes from the client in multiple stages, allowing for efficient
    * memory usage and safe early termination.
    *
    *   1. The first request can contain some context `RequestContext`, which is extracted using
    *      `contextFromFirstRequest` and passed along with the source.
    *   1. The gzipped bytes from each request are extracted using `getGzippedBytes`.
    *   1. Messages from the decompressed input stream are extracted via `parseMessage`.
    *   1. The source, which the caller uses for further processing, continually parses new messages
    *      from the decompressed input stream.
    *
    * @param responseObserver
    *   The response observer used to signal errors or completion to the client.
    * @param responseIfNoRequests
    *   The response to return if no requests were actually submitted.
    * @param getGzippedBytes
    *   The extractor method used to get a `ByteString` from the request `Req`.
    * @param parseMessage
    *   A method that returns `None` if there is no more input to read, `Some(Right(_))` for a
    *   successful parse, and `Some(Left(_))` if an error occurred during parsing. Any parsing
    *   errors are bubbled up, and processing is aborted.
    * @param contextFromFirstRequest
    *   The extractor method used to get the context from the first request.
    * @param action
    *   The main processing pipeline consuming the parsed messages. If the returned future fails
    *   (for example, due to a validation failure or an intentional abort), the stream terminates
    *   early and safely returns that exact application error to the client.
    */
  def streamGzippedChunksFromClient[Req, Resp, RequestContext, ParsedMessage](
      responseObserver: StreamObserver[Resp],
      responseIfNoRequests: Try[Resp],
      getGzippedBytes: Req => ByteString,
      parseMessage: InputStream => Option[ParsingResult[ParsedMessage]],
  )(
      contextFromFirstRequest: Req => Try[RequestContext]
  )(action: (RequestContext, Source[ParsedMessage, NotUsed]) => FutureUnlessShutdown[Resp])(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): StreamObserver[Req] = {
    // for extracting the context on the first request and creating the pekko source
    val isFirst = new AtomicBoolean(true)
    // this Piped*Stream setup connects the incoming requests containing the gzipped bytes
    // with the pekko parsing source
    val output = new PipedOutputStream()
    val input = new PipedInputStream(output)

    // when reporting the error, also close the output stream, since there will be no more data to be processed
    def reportError(err: Throwable): Unit = {
      responseObserver.onError(err)
      output.close()
    }

    // blocking write the request bytes to the output stream
    def writeRequestBytes(request: Req): Unit = Try {
      // gRPC backpressure works by "blocking" the onNext call.
      blocking(getGzippedBytes(request).writeTo(output))
    } forFailed {
      case err: java.io.IOException
          if err.getMessage != null && err.getMessage.contains("Pipe closed") =>
        // Expected race condition during an early stream abort.
        // If the downstream processing (`action`) fails with an application error (e.g., validation failure),
        // it closes the `input` stream (`PipedInputStream`). If the client concurrently sends another chunk,
        // writing to `output` (`PipedOutputStream`) throws this "Pipe closed" IOException.
        // We safely swallow this to prevent a double `onError` call on the gRPC observer (`responseObserver.onError`),
        // ensuring the client receives the actual application error instead of a broken pipe error.
        Try(output.close()).discard
      case err =>
        reportError(err)
    }

    new StreamObserver[Req] {
      override def onNext(value: Req): Unit =
        if (isFirst.getAndSet(false)) {
          // only extract the context from the first request
          contextFromFirstRequest(value) match {
            case Failure(exception) =>
              reportError(exception)

            case Success(context) =>
              // hold a lazy reference, because the constructor of GZIPInputStream immediately executes a blocking read
              // No zip-bomb risk: messages are parsed and ingested one at a time, so only a single
              // decompressed message is held in memory rather than the whole decompressed stream.
              lazy val gunzip = new GzipCompressorInputStream(input)
              // construct the source by repeatedly reading from the gunzip input stream until there's no more data.
              // we don't really have a "state", so we use Unit.
              val parsedMessagesSource = PekkoSource.unfold(())(_ =>
                blocking(parseMessage(gunzip))
                  .map {
                    case Left(err) =>
                      // throw the parsing error to cancel the source
                      throw ProtoDeserializationError.ProtoDeserializationFailure
                        .Wrap(err)
                        .asGrpcError
                    case Right(value) =>
                      () -> value
                  }
              )
              // the main processing pipeline is also only triggered after the first request
              val resultFUS = action(context, parsedMessagesSource)
              GrpcStreamingUtils.futureUnlessShutdownToStreamObserver(
                resultFUS.thereafter { _ =>
                  // after the main processing pipeline is finished for whatever reason, we can safely close the input
                  input.close()
                },
                responseObserver,
              )
              writeRequestBytes(value)
          }
        } else writeRequestBytes(value)

      override def onError(t: Throwable): Unit =
        // The gRPC framework invokes this requestObserver.onError exclusively when the inbound stream dies
        // (e.g., client cancellation, network drop, or server shutdown). An error on the request observer
        // implies that this handler's response observer is already closed, as the underlying HTTP/2
        // connection is tearing down.
        //
        // If we call `reportError(t)` here, it attempts to invoke `responseObserver.onError(t)`. This forces
        // an illegal second close attempt and throws "IllegalStateException: call already closed" within the
        // gRPC library code.
        //
        // Note: Application-level errors (e.g., validation failures) are handled safely via the
        // FutureUnlessShutdown pipeline. Here, our only responsibility is to close our local pipe
        // so that the downstream `GzipCompressorInputStream` gracefully halts.

        Try(output.close()).discard

      override def onCompleted(): Unit =
        if (isFirst.get()) {
          // If no request has been received (which means that the main processing pipeline has not been triggered),
          // complete the stream according to `responseIfNoRequests` and close the Piped*Streams.
          GrpcStreamingUtils.futureUnlessShutdownToStreamObserver(
            FutureUnlessShutdown.fromTry(responseIfNoRequests.thereafter { _ =>
              input.close()
              output.close()
            }),
            responseObserver,
          )
        } else {
          // if any requests were processed, simply close the output pipe. the final response will be triggered by the
          // processing pipeline
          output.close()
        }
    }
  }

  /** Stream the provided input stream to the server, reading lazily in chunks of
    * `defaultChunkSize`.
    *
    * @param load
    *   Loader (endpoint of the service)
    * @param requestBuilder
    *   Builds a request from an array of bytes
    * @param inputStream
    *   The input stream to be streamed. The caller is responsible for closing it after the returned
    *   future completes.
    */
  def streamToServer[Req, Resp](
      load: StreamObserver[Resp] => StreamObserver[Req],
      requestBuilder: Array[Byte] => Req,
      inputStream: InputStream,
  ): Future[Resp] = {
    val buffer = new Array[Byte](defaultChunkSize)
    def readNextChunk(): Option[Either[Throwable, Req]] = {
      val bytesRead = inputStream.read(buffer)
      if (bytesRead == -1) None
      else Some(Right(requestBuilder(buffer.slice(0, bytesRead))))
    }
    streamToServer(load, _ => readNextChunk())
  }

  /** Stream data to the server
    * @param load
    *   Loader (endpoint of the service)
    * @param readNextChunk
    *   Method to read the data to be streamed. Streaming happens as long as the method returns
    *   Some(Right()).
    *   - If Some(Left()) is encountered, the stream is terminated with an error.
    *   - When None is encountered, the stream is completed.
    */
  private def streamToServer[Req, Resp](
      load: StreamObserver[Resp] => StreamObserver[Req],
      readNextChunk: Unit => Option[Either[Throwable, Req]],
  ): Future[Resp] = {
    val requestComplete = Promise[Resp]()
    val ref = new AtomicReference[Option[Resp]](None)

    val responseObserver = new StreamObserver[Resp] {
      override def onNext(value: Resp): Unit =
        ref.set(Some(value))

      override def onError(t: Throwable): Unit = requestComplete.failure(t)

      override def onCompleted(): Unit =
        ref.get() match {
          case Some(response) => requestComplete.success(response)
          case None =>
            requestComplete.failure(
              io.grpc.Status.CANCELLED
                .withDescription("Server completed the request before providing a response")
                .asRuntimeException()
            )
        }
    }
    val requestObserver = load(responseObserver)

    @tailrec
    def read(): Unit =
      readNextChunk(()) match {
        case Some(Right(nextRequest)) =>
          blocking(requestObserver.onNext(nextRequest))
          read()

        case Some(Left(error)) =>
          requestObserver.onError(error)

        case None =>
          requestObserver.onCompleted()
      }

    read()
    requestComplete.future
  }

  def streamToClient[T](
      responseF: OutputStream => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): Unit = {
    val context = io.grpc.Context.current().withCancellation()
    val chunkSize = chunkSizeO.getOrElse(defaultChunkSize)
    val pipedInput = new PipedInputStream(chunkSize)
    // Introduced so that clients do not have to enclose calls in blocking
    val blockingPipedOutput = new PipedOutputStream(pipedInput) {
      override def write(b: Int): Unit = blocking(super.write(b))

      override def write(b: Array[Byte], off: Int, len: Int): Unit = blocking {
        super.write(b, off, len)
      }

      override def write(b: Array[Byte]): Unit = blocking(super.write(b))
    }

    def closeQuietly(c: java.io.Closeable): Unit = Try(c.close()).discard

    context.run { () =>
      // Close both piped streams on context cancellation so the producer unblocks promptly
      context.addListener(
        (_: Context) => {
          closeQuietly(pipedInput)
          closeQuietly(blockingPipedOutput)
        },
        (command: Runnable) => command.run(),
      )

      val consumerF =
        streamResponseChunks(context, responseObserver)(
          pipedInput,
          chunkSize,
          fromByteString,
        )

      val producerF = responseF(blockingPipedOutput).transform { result =>
        val closeResult = Try(blockingPipedOutput.close())
        result match {
          case Success(_) => closeResult
          case Failure(ex) =>
            closeResult.forFailed(ex.addSuppressed(_))
            result
        }
      }
      // use transformWith for a safe shutdown that intercepts both the consumer and producer lifecycles
      // instead of flatMap (which short-circuits on failure and abandons the consumer thread and thus)
      // can lead to race conditions between .onError and .onNext.
      val processingResult = producerF
        .transformWith {

          // happy path: If the producer successfully completes, chain directly to the consumer
          // future and wait for it to finish pushing the remaining chunks to the network.
          case Success(_) => consumerF

          // failure Path (e.g., Missing Onboarding Flag error in producer): Catch the error before it propagates
          // to the gRPC layer, ensuring we don't let the main thread panic.
          case Failure(ex) =>
            // Trigger the listener to close the pipes by cancelling the context.
            // This unblocks the consumer thread's read loop with a "Pipe Closed" IOException,
            // forcing it to cleanly stop and exit before the gRPC buffers are modified.
            context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))

            // force the combined future to wait until the consumer thread completely dies.
            // while preserving the original onboarding application error.
            consumerF.transform(_ => Failure(ex))
        }
        // Unconditionally close the input pipe when the processing
        // future completes. This prevents thread leaks and frees JVM resources.
        .thereafter(_ => closeQuietly(pipedInput))

      // Hand the future over to finishStream. Because processingResult strictly
      // waits for the consumer thread to die, finishStream's Await block will pause. It will only
      // call responseObserver.onError after the background thread is completely buried, preventing
      // concurrent access and exceptions from MessageFramer.
      finishStream(context, responseObserver)(processingResult, processingTimeout)
    }
  }

  def streamToClientFromFile[T](
      responseF: File => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): Unit = {
    val file = newTemporaryFile()

    val context = io.grpc.Context
      .current()
      .withCancellation()

    context.run { () =>
      val processingResult = responseF(file).flatMap { _ =>
        val chunkSize = chunkSizeO.getOrElse(defaultChunkSize)
        streamResponseChunks(context, responseObserver)(
          file.newInputStream.buffered(chunkSize),
          chunkSize,
          fromByteString,
        )
      }
      finishStream(context, responseObserver)(processingResult, processingTimeout)
    }
  }

  /** Deserializes versioned message instances from a given stream.
    *
    * IMPORTANT: Expects data in the input stream that has been serialized with
    * [[com.digitalasset.canton.version.HasProtocolVersionedWrapper#writeDelimitedTo]]! Otherwise,
    * you'll get weird deserialization behaviour without errors, or you'll observe misaligned
    * message fields and message truncation errors result from having used
    * [[scalapb.GeneratedMessage#writeDelimitedTo]] directly.
    *
    * @return
    *   either an error, or a list of versioned message instances
    */
  def parseDelimitedFromTrusted[ValueClass <: HasRepresentativeProtocolVersion](
      stream: InputStream,
      objectType: VersioningCompanion[ValueClass],
  ): Either[String, List[ValueClass]] =
    parseDelimitedFromTrustedInternal(stream, objectType.parseDelimitedFromTrusted)

  /** Deserializes versioned message instances from a given stream.
    *
    * IMPORTANT: Expects data in the input stream that has been serialized with
    * [[com.digitalasset.canton.version.HasVersionedWrapper#writeDelimitedTo]]! Otherwise, you'll
    * get weird deserialization behaviour without errors, or you'll observe misaligned message
    * fields and message truncation errors result from having used
    * [[scalapb.GeneratedMessage#writeDelimitedTo]] directly.
    *
    * @return
    *   either an error, or a list of versioned message instances
    */
  def parseDelimitedFromTrusted[ValueClass](
      stream: InputStream,
      objectType: HasVersionedMessageCompanion[ValueClass],
  ): Either[String, List[ValueClass]] =
    parseDelimitedFromTrustedInternal(stream, objectType.parseDelimitedFromTrusted)

  private def parseDelimitedFromTrustedInternal[ValueClass](
      stream: InputStream,
      parser: InputStream => Option[ParsingResult[ValueClass]],
  ): Either[String, List[ValueClass]] = {
    // Assume we can load all parsed messages into memory
    @tailrec
    def read(acc: List[ValueClass]): Either[String, List[ValueClass]] =
      parser(stream) match {
        case Some(parsed) =>
          parsed match {
            case Left(parseError) =>
              Left(parseError.message)
            case Right(value) =>
              // Prepend for efficiency!
              read(value :: acc)
          }
        case None =>
          Right(acc.reverse)
      }
    read(Nil)
  }

  private[util] def streamResponseChunks[T](
      context: Context.CancellableContext,
      responseObserver: StreamObserver[T],
  )(
      inputStream: InputStream,
      chunkSize: Int,
      fromByteString: FromByteString[T],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[Unit] =
    withServerCallStreamObserverF(responseObserver) { scso =>
      val iter = Iterator
        .continually(inputStream.readNBytes(chunkSize))
        // Before pushing new chunks to the stream, keep checking that the context has not been cancelled
        // This avoids the server reading the entire dump file for nothing if the client has already cancelled
        .takeWhile(_.nonEmpty && !context.isCancelled)

      val isWorkerRunning = new AtomicBoolean(false)
      val allBytesWrittenPromise = Promise[Unit]()

      def runWorkerInBackground(): Unit = FutureUtil.doNotAwait(
        Future {
          // Returns true if the stream was exhausted (EOF reached), false if the loop stopped
          // because the observer is no longer ready
          Try {
            @tailrec
            def sendChunks(): Boolean =
              if (!scso.isReady) {
                false
              } else if (iter.hasNext) {
                val byteArray = iter.next()
                val chunk: ByteString = ByteString.copyFrom(byteArray)
                responseObserver.onNext(fromByteString.toT(chunk))
                sendChunks()
              } else {
                true
              }
            sendChunks()
          } match {
            case Failure(ex) =>
              isWorkerRunning.set(false)
              allBytesWrittenPromise.tryFailure(ex).discard
            case Success(exhausted) =>
              if (exhausted) {
                allBytesWrittenPromise.trySuccess(()).discard
              }
              isWorkerRunning.set(false)
              if (!exhausted) {
                if (context.isCancelled) {
                  allBytesWrittenPromise.trySuccess(()).discard
                } else if (scso.isReady) {
                  // this check is important in case the `onReadyHandler` gets triggered before setting `isWorkerRunning` to `false` above. Not triggering here would mean that the stream will stall.
                  triggerWorker()
                } else {
                  // Fix for misbehaving observer - that never triggers onReady/onCancel
                  DelayUtil
                    .delay(100.millis)
                    .foreach(_ => triggerWorker().discard)
                }
              }
          }
        },
        "Cannot start streaming response to client",
      )

      def triggerWorker(): Future[Unit] = Future {
        // Do not start a new worker once streaming is finished or the client has gone away,
        // to avoid spinning up workers (and rescheduling them) for an already-completed stream.
        if (context.isCancelled) {
          allBytesWrittenPromise.trySuccess(()).discard
        } else if (
          !allBytesWrittenPromise.isCompleted && isWorkerRunning
            .compareAndSet(false, true)
        )
          runWorkerInBackground()
      }

      scso.setOnReadyHandler(() => triggerWorker().discard)
      scso.setOnCancelHandler { () =>
        Try(inputStream.close()).discard
        if (!isWorkerRunning.get()) {
          allBytesWrittenPromise.trySuccess(()).discard
        }
      }

      triggerWorker().discard

      allBytesWrittenPromise.future.thereafter(_ => Try(inputStream.close()).discard)
    }

  private def finishStream[T](
      context: Context.CancellableContext,
      responseObserver: StreamObserver[T],
  )(f: Future[Unit], timeout: Duration): Unit = {
    def cancelContext(): Unit = {
      context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
      ()
    }

    try {
      Await.result(f, timeout)
      if (!context.isCancelled) responseObserver.onCompleted()
      else cancelContext()
    } catch {
      case _: InterruptedException =>
        // The serving thread was interrupted (e.g. node shutdown or call cancellation) while blocked
        // in `Await.result`. Cancel the context to terminate the gRPC call exactly once (CANCELLED),
        // and only restore the thread's interrupt status. We must NOT rethrow: letting the
        // InterruptedException escape into the gRPC request handler makes it close the call a second
        // time, writing to the already-terminated HTTP/2 stream, which Netty logs as
        // "Stream closed before write could take place".
        cancelContext()
        Thread.currentThread().interrupt()
      case NonFatal(exception) =>
        responseObserver.onError(exception)
        cancelContext()
    }
  }

  def futureUnlessShutdownToStreamObserver[A](
      fus: FutureUnlessShutdown[A],
      observer: StreamObserver[A],
  )(implicit ec: ExecutionContext, elc: ErrorLoggingContext): Unit = fus.unwrap.onComplete {
    case Failure(exception) => observer.onError(exception)
    case Success(Outcome(value)) =>
      observer.onNext(value)
      observer.onCompleted()
    case Success(AbortedDueToShutdown) =>
      observer.onError(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
  }

  private def withServerCallStreamObserverG[R, A](
      observer: StreamObserver[R]
  )(ifNotSupported: => A)(
      handler: ServerCallStreamObserver[R] => A
  )(implicit errorLoggingContext: ErrorLoggingContext): A =
    observer match {
      case serverCallStreamObserver: ServerCallStreamObserver[R] =>
        handler(serverCallStreamObserver)
      case other =>
        val statusException =
          Status.INTERNAL
            .withDescription(s"Unknown stream observer request")
            .asException()
        errorLoggingContext.warn(
          s"${statusException.getMessage} StreamObserver:(${other.getClass})",
          statusException,
        )
        observer.onError(statusException)
        ifNotSupported
    }

  /** Ensure the observer is a ServerCallStreamObserver, running `handler` if so. Otherwise reports
    * an INTERNAL error to the observer. See `withServerCallStreamObserverG`.
    *
    * @param observer
    *   underlying observer
    * @param handler
    *   handler requiring a ServerCallStreamObserver
    */
  def withServerCallStreamObserver[R](
      observer: StreamObserver[R]
  )(handler: ServerCallStreamObserver[R] => Unit)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Unit =
    withServerCallStreamObserverG(observer)(())(handler)

  /** Ensure the observer is a ServerCallStreamObserver, running `handler` if so. Otherwise reports
    * an INTERNAL error to the observer and returns a completed future. See
    * `withServerCallStreamObserverG`.
    *
    * @param observer
    *   underlying observer
    * @param handler
    *   handler requiring a ServerCallStreamObserver
    */
  def withServerCallStreamObserverF[R](
      observer: StreamObserver[R]
  )(handler: ServerCallStreamObserver[R] => Future[Unit])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[Unit] =
    withServerCallStreamObserverG(observer)(Future.unit)(handler)
}

// Define a type class for converting ByteString to the generic type T
trait FromByteString[T] {
  def toT(chunk: ByteString): T
}
