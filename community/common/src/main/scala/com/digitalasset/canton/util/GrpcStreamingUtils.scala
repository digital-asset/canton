// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import better.files.*
import better.files.File.newTemporaryFile
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.grpc.ByteStringStreamObserverWithContext
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  HasVersionedMessageCompanion,
  VersioningCompanion,
}
import com.google.protobuf.ByteString
import io.grpc.Context
import io.grpc.stub.StreamObserver

import java.io.{
  BufferedInputStream,
  ByteArrayInputStream,
  ByteArrayOutputStream,
  InputStream,
  OutputStream,
}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

object GrpcStreamingUtils {
  private final val defaultChunkSize: Int =
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
          val responseF = this.result.flatMap { case (byteString, context) =>
            processFullRequest(byteString, context)
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

  def streamToServer[Req, Resp](
      load: StreamObserver[Resp] => StreamObserver[Req],
      requestBuilder: Array[Byte] => Req,
      byteString: ByteString,
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

    byteString.toByteArray
      .grouped(defaultChunkSize)
      .foreach { bytes =>
        blocking {
          requestObserver.onNext(requestBuilder(bytes))
        }
      }
    requestObserver.onCompleted()
    requestComplete.future
  }

  def streamToClient[T](
      responseF: OutputStream => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext): Unit = {
    val context = io.grpc.Context
      .current()
      .withCancellation()

    val outputStream = new ByteArrayOutputStream()
    context.run { () =>
      val processingResult = responseF(outputStream).map { _ =>
        val chunkSize = chunkSizeO.getOrElse(defaultChunkSize)
        val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
        streamResponseChunks(context, responseObserver)(
          new BufferedInputStream(inputStream),
          chunkSize,
          fromByteString,
        )
      }
      finishStream(context, responseObserver)(processingResult, processingTimeout)
    }
  }

  def streamToClientFromFile[T](
      responseF: File => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext): Unit = {
    val file = newTemporaryFile()

    val context = io.grpc.Context
      .current()
      .withCancellation()

    context.run { () =>
      val processingResult = responseF(file).map { _ =>
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
    *   either an error, or a list of versioned message instances in reverse order as appeared in
    *   the given stream
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
    *   either an error, or a list of versioned message instances in reverse order as appeared in
    *   the given stream
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
          Right(acc)
      }
    read(Nil)
  }

  private def streamResponseChunks[T](
      context: Context.CancellableContext,
      responseObserver: StreamObserver[T],
  )(
      inputStream: InputStream,
      chunkSize: Int,
      fromByteString: FromByteString[T],
  ): Unit =
    inputStream.autoClosed { s =>
      Iterator
        .continually(s.readNBytes(chunkSize))
        // Before pushing new chunks to the stream, keep checking that the context has not been cancelled
        // This avoids the server reading the entire dump file for nothing if the client has already cancelled
        .takeWhile(_.nonEmpty && !context.isCancelled)
        .foreach { byteArray =>
          val chunk: ByteString = ByteString.copyFrom(byteArray)
          responseObserver.onNext(fromByteString.toT(chunk))
        }
    }

  private def finishStream[T](
      context: Context.CancellableContext,
      responseObserver: StreamObserver[T],
  )(f: Future[Unit], timeout: Duration): Unit =
    Try(Await.result(f, timeout)) match {
      case Failure(exception) =>
        responseObserver.onError(exception)
        context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
        ()
      case Success(_) =>
        if (!context.isCancelled) responseObserver.onCompleted()
        else {
          context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
          ()
        }
    }
}

// Define a type class for converting ByteString to the generic type T
trait FromByteString[T] {
  def toT(chunk: ByteString): T
}
