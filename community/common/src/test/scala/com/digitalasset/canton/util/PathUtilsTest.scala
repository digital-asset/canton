// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Paths

class PathUtilsTest extends AnyWordSpec with BaseTest {
  "PathUtils" should {
    "extract filename without extension" in {
      PathUtils.getFilenameWithoutExtension(Paths.get("/dir1/dir2/file.txt")) shouldBe "file"
      PathUtils.getFilenameWithoutExtension(Paths.get("dir1/dir2/file.txt")) shouldBe "file"
      PathUtils.getFilenameWithoutExtension(Paths.get("./dir1/dir2/file.txt")) shouldBe "file"
      PathUtils.getFilenameWithoutExtension(Paths.get("")) shouldBe ""
      PathUtils.getFilenameWithoutExtension(Paths.get("./d.ir.1/di.r.2/file.txt")) shouldBe "file"
      PathUtils.getFilenameWithoutExtension(
        Paths.get("/d.ir.1/di.r.2/file.a.b.c.txt")
      ) shouldBe "file.a.b.c"
    }
  }
}
