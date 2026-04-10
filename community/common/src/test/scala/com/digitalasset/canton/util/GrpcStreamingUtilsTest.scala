// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import io.grpc.stub.StreamObserver
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

final class GrpcStreamingUtilsTest extends AnyWordSpec with BaseTest {
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
}
