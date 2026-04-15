// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.grpc

import better.files.File
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.TryUtil.*
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/** A StreamObserver that writes incoming elements to a file, using a provided converter to convert
  * the elements to bytes.
  */
class OutputFileStreamObserver[T](
    outputFile: File,
    converter: T => ByteString,
) extends StreamObserver[T] {
  private val os = outputFile.newFileOutputStream(append = false)
  private val requestComplete: Promise[Unit] = Promise[Unit]()

  def result: Future[Unit] = requestComplete.future
  override def onNext(value: T): Unit =
    Try(os.write(converter(value).toByteArray)) match {
      case Failure(exception) =>
        Try(os.close()).forFailed { suppressed =>
          // Avoid an IllegalArgumentException if it's the same exception,
          if (!(suppressed eq exception)) exception.addSuppressed(suppressed)
        }
        throw exception
      case Success(_) => // all good
    }

  override def onError(t: Throwable): Unit = {
    os.close()
    requestComplete.tryFailure(t).discard
  }

  override def onCompleted(): Unit = {
    os.close()
    requestComplete.trySuccess(()).discard
  }
}
