// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client

import java.io.{File, FileInputStream, InputStream}
import scala.util.control.NonFatal

class FileLoader[T](mapper: InputStream => T) {
  def load(path: String): Either[String, T] =
    for {
      file <- loadFile(path)
      content <- readFile(file)
    } yield content

  private def readFile(file: File): Either[String, T] = {
    val reader = new FileInputStream(file)

    try {
      val result = mapper(reader)
      Right(result)
    } catch {
      case NonFatal(ex) =>
        Left(ex.getMessage)
    } finally {
      reader.close()
    }
  }

  private def loadFile(path: String): Either[String, File] = {
    val file = new File(path)
    Either.cond(file.canRead, file, s"Cannot read $path")
  }
}
