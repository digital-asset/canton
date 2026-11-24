// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

class DecodeCacheSpec extends AnyWordSpec with Matchers {
  private lazy val archive: DamlLf.Archive = {
    val darFile = new File(getClass.getClassLoader.getResource("DarReaderTest.dar").toURI)
    DarParser.assertReadArchiveFromFile(darFile).main
  }
  private def countingCache() = {
    val calls = new AtomicInteger(0)
    val underlying: (
        (com.digitalasset.daml.lf.data.Ref.PackageId, DamlLf.Archive, Boolean)
    ) => Either[
      Error,
      (com.digitalasset.daml.lf.data.Ref.PackageId, com.digitalasset.daml.lf.language.Ast.Package),
    ] = input => {
      calls.incrementAndGet()
      Decode.uncachedDecodeValidatedArchive(input)
    }
    calls -> Decode.cachedDecodeValidatedArchive(underlying)
  }
  "cachedDecodeValidatedArchive" should {
    "decode the same archive only once and return the same result as uncached decoding" in {
      val Right(hash) = Reader.validateArchive(archive, schemaMode = false)
      val (calls, cached) = countingCache()
      val first = cached((hash, archive, false))
      val second = cached((hash, archive, false))
      calls.get shouldBe 1
      first shouldBe a[Right[?, ?]]
      second shouldBe first
      first
        .map(_._2) shouldBe Decode.uncachedDecodeValidatedArchive((hash, archive, false)).map(_._2)
    }
    "keep separate entries for schema and non-schema modes" in {
      val Right(hash) = Reader.validateArchive(archive, schemaMode = false)
      val (calls, cached) = countingCache()
      cached((hash, archive, false))
      cached((hash, archive, true))
      cached((hash, archive, false))
      cached((hash, archive, true))
      calls.get shouldBe 2
    }
    "not cache errors" in {
      val Right(hash) = Reader.validateArchive(archive, schemaMode = false)
      val broken =
        archive.toBuilder.setPayload(com.google.protobuf.ByteString.copyFromUtf8("garbage")).build()
      val (calls, cached) = countingCache()
      cached((hash, broken, false)) shouldBe a[Left[?, ?]]
      cached((hash, broken, false)) shouldBe a[Left[?, ?]]
      calls.get shouldBe 2
    }
  }
}
