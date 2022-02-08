// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.io.EOFException
import java.util.zip.{GZIPInputStream, ZipException}

import cats.Order
import com.digitalasset.canton.serialization.DeserializationError
import com.google.protobuf.ByteString

import scala.annotation.tailrec

object ByteStringUtil {

  /** Lexicographic ordering on [[com.google.protobuf.ByteString]]s */
  val orderByteString: Order[ByteString] = new Order[ByteString] {
    override def compare(x: ByteString, y: ByteString): Int = {
      val iterX = x.iterator()
      val iterY = y.iterator()

      @tailrec def go(): Int = {
        if (iterX.hasNext) {
          if (iterY.hasNext) {
            val cmp = iterX.next().compareTo(iterY.next())
            if (cmp == 0) go() else cmp
          } else 1
        } else if (iterY.hasNext) -1
        else 0
      }

      go()
    }
  }

  def compressGzip(bytes: ByteString): ByteString = {
    ByteString.copyFrom(ByteArrayUtil.compressGzip(bytes))
  }

  def decompressGzip(bytes: ByteString): Either[DeserializationError, ByteString] = {
    try {
      val gunzipper = new GZIPInputStream(bytes.newInput())
      val decompressed = ByteString.readFrom(gunzipper)
      gunzipper.close()
      Right(decompressed)
    } catch {
      // all exceptions that were observed when testing these methods (see also `GzipCompressionTests`)
      case ex: ZipException => Left(DeserializationError(ex.getMessage, bytes))
      case _: EOFException =>
        Left(DeserializationError("Compressed byte input ended too early", bytes))
    }
  }
}

object ByteStringImplicits {
  implicit val orderByteString: Order[ByteString] = ByteStringUtil.orderByteString
}
