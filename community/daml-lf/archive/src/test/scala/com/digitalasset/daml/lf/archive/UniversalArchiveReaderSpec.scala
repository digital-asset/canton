// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class UniversalArchiveReaderSpec extends AnyFlatSpec with Matchers with TryValues {

  private val darFile = new File(getClass.getClassLoader.getResource("DarReaderTest.dar").getFile)

  private val dalfFile = new File(getClass.getClassLoader.getResource("DarReaderTest.dalf").getFile)

  behavior of UniversalArchiveReader.toString

  it should "parse a DAR file" in {
    UniversalArchiveReader.readFile(darFile) shouldBe a[Right[?, ?]]
  }

  it should "parse a DALF file" in {
    UniversalArchiveReader.readFile(dalfFile) shouldBe a[Right[?, ?]]
  }

  it should "parse a DAR file and return language version" in {
    UniversalArchiveReader.readFile(darFile) shouldBe a[Right[?, ?]]
  }

  it should "parse a DALF file and return language version" in {
    UniversalArchiveReader.readFile(dalfFile) shouldBe a[Right[?, ?]]
  }

  it should "reject a zip bomb with the proper error" in {
    UniversalArchiveReader
      .readFile(darFile, entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

}
