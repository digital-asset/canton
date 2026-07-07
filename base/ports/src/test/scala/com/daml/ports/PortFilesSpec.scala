// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import com.daml.ports.PortFiles.FileAlreadyExists
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}
import java.util.UUID

class PortFilesSpec extends AnyFreeSpec with Matchers with Inside {

  "Can create a port file with a unique file name" in {
    val path = uniquePath()
    PortFiles.write(path, Port(1024)) shouldBe Right(())
    path.toFile.exists() shouldBe true
  }

  "Cannot create a port file with a nonunique file name" in {
    val path = uniquePath()
    PortFiles.write(path, Port(1024)) shouldBe Right(())
    PortFiles.write(path, Port(1024)) shouldBe Left(FileAlreadyExists(path))
  }

  private def uniquePath(): Path = {
    val fileName = s"${this.getClass.getSimpleName}-${UUID.randomUUID().toString}.dummy"
    Paths.get(fileName)
  }
}
