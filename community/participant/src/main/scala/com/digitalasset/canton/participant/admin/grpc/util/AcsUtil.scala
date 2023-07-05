// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc.util

import better.files.File
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.participant.admin.SyncStateInspection.SerializableContractWithDomainId
import com.digitalasset.canton.util.{ByteStringUtil, ResourceUtil}
import com.google.protobuf.ByteString

import java.io.ByteArrayInputStream
import scala.io.Source

object AcsUtil {
  def loadFromByteString(
      bytes: ByteString,
      gzip: Boolean,
  ): Either[String, LazyList[SerializableContractWithDomainId]] = {
    for {
      decompressedBytes <-
        if (gzip)
          ByteStringUtil
            .decompressGzip(bytes, None)
            .leftMap(err => s"Failed to decompress bytes: $err")
        else Right(bytes)
      contracts <- ResourceUtil.withResource(
        Source.fromInputStream(new ByteArrayInputStream(decompressedBytes.toByteArray))
      ) { inputSource =>
        loadFromSource(inputSource)
      }
    } yield contracts
  }

  def loadFromFile(fileInput: File): Iterator[SerializableContractWithDomainId] = {
    val decompressedInput = if (fileInput.toJava.getName.endsWith(".gz")) {
      fileInput.newGzipInputStream(8192)
    } else {
      fileInput.newFileInputStream
    }
    ResourceUtil.withResource(decompressedInput) { fileInput =>
      ResourceUtil.withResource(Source.fromInputStream(fileInput)) { inputSource =>
        loadFromSource(inputSource) match {
          case Left(error) => throw new Exception(error)
          case Right(value) => value.iterator
        }
      }
    }
  }

  private def loadFromSource(
      source: Source
  ): Either[String, LazyList[SerializableContractWithDomainId]] = {
    // build source iterator (we can't load everything into memory)
    LazyList
      .from(
        source
          .getLines()
          .zipWithIndex
      )
      .traverse { case (line, lineNumber) =>
        SerializableContractWithDomainId.decode(line, lineNumber)
      }
  }

}
