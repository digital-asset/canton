// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsStringInterning
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (StringInterning)"

  private val dtos = Vector(
    DbDto.StringInterningDto(2, "a"),
    DbDto.StringInterningDto(3, "b"),
    DbDto.StringInterningDto(4, "c"),
    DbDto.StringInterningDto(5, "d"),
  )

  private val expectedFullList = List(
    2 -> "a",
    3 -> "b",
    4 -> "c",
    5 -> "d",
  )

  it should "store and load string-interning entries" in {
    val interningIdsBeforeBegin = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 5)
    )
    executeSql(ingest(dtos, _))
    val interningIdsFull = executeSql(backend.stringInterning.loadStringInterningEntries(0, 5))
    val interningIdsOverFetch = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 10)
    )
    val interningIdsEmpty = executeSql(
      backend.stringInterning.loadStringInterningEntries(5, 10)
    )
    val interningIdsSubset = executeSql(
      backend.stringInterning.loadStringInterningEntries(3, 10)
    )
    interningIdsBeforeBegin shouldBe Nil
    interningIdsFull shouldBe expectedFullList
    interningIdsOverFetch shouldBe expectedFullList
    interningIdsEmpty shouldBe Nil
    interningIdsSubset shouldBe expectedFullList.drop(2)
  }

  it should "load string-interning entries by external string range" in {
    def loadRange(fromExclusive: String, toInclusive: String): Vector[(Int, String)] =
      executeSql(
        backend.stringInterning.loadStringInterningEntriesRange(fromExclusive, toInclusive)
      )

    loadRange("", "z") shouldBe empty
    executeSql(ingest(dtos, _))

    loadRange("", "z") shouldBe expectedFullList

    // lower bound is exclusive
    loadRange("a", "z") shouldBe expectedFullList.drop(1)

    // upper bound is inclusive
    loadRange("", "c") shouldBe expectedFullList.take(3)

    loadRange("a", "c") shouldBe expectedFullList.slice(1, 3)

    loadRange("b", "b") shouldBe empty

    loadRange("d", "a") shouldBe empty

    loadRange("d", "z") shouldBe empty

    // result is ordered by external string, not by internal id
    executeSql(ingest(Vector(DbDto.StringInterningDto(1, "e")), _))
    loadRange("", "z") shouldBe expectedFullList :+ (1 -> "e")
  }

  it should "find the last external string of a page within a prefix" in {
    val prefixedDtos = Vector(
      DbDto.StringInterningDto(2, "p|c"),
      DbDto.StringInterningDto(3, "t|a"),
      DbDto.StringInterningDto(4, "p|a"),
      DbDto.StringInterningDto(5, "p|d"),
      DbDto.StringInterningDto(6, "t|b"),
      DbDto.StringInterningDto(7, "p|b"),
    )

    def lastInRange(prefix: String, fromExclusive: String, limit: Int): Option[String] =
      executeSql(
        backend.stringInterning.lastExternalStringInRange(
          prefix = prefix,
          fromExclusive = fromExclusive,
          limit = limit,
        )
      )

    lastInRange(prefix = "p|", fromExclusive = "", limit = 10) shouldBe None
    executeSql(ingest(prefixedDtos, _))

    // paging through the prefix
    lastInRange(prefix = "p|", fromExclusive = "", limit = 2) shouldBe Some("p|b")
    lastInRange(prefix = "p|", fromExclusive = "p|b", limit = 2) shouldBe Some("p|d")
    lastInRange(prefix = "p|", fromExclusive = "p|d", limit = 2) shouldBe None
    // fromExclusive is exclusive
    lastInRange(prefix = "p|", fromExclusive = "p|a", limit = 1) shouldBe Some("p|b")
    // fromExclusive between existing entries
    lastInRange(prefix = "p|", fromExclusive = "p|aa", limit = 1) shouldBe Some("p|b")
    // fewer remaining entries than the limit
    lastInRange(prefix = "p|", fromExclusive = "p|c", limit = 10) shouldBe Some("p|d")
    // entries of other prefixes are not included, even if the limit allows it
    lastInRange(prefix = "p|", fromExclusive = "", limit = 100) shouldBe Some("p|d")
    lastInRange(prefix = "t|", fromExclusive = "", limit = 100) shouldBe Some("t|b")
    // unknown prefix
    lastInRange(prefix = "u|", fromExclusive = "", limit = 10) shouldBe None
  }

  it should "batch lookup internal ids by external strings" in {
    def lookup(rawStrings: Iterable[String]): Vector[(Int, String)] =
      executeSql(backend.stringInterning.batchLookupInternalIds(rawStrings))

    lookup(Seq("a", "b")) shouldBe empty
    executeSql(ingest(dtos, _))

    // empty input
    lookup(Seq.empty) shouldBe empty
    // all entries
    lookup(Seq("a", "b", "c", "d")) shouldBe expectedFullList
    // result is ordered by external string, regardless of the input order
    lookup(Seq("d", "a", "c")) shouldBe expectedFullList.filter { case (_, s) =>
      Set("a", "c", "d")(s)
    }
    // unknown strings are skipped
    lookup(Seq("b", "x")) shouldBe expectedFullList.slice(1, 2)
    lookup(Seq("x", "y")) shouldBe empty
    // duplicates in the input yield a single entry
    lookup(Seq("b", "b")) shouldBe expectedFullList.slice(1, 2)
  }

  it should "batch lookup external strings by internal ids" in {
    def lookup(ids: Iterable[Int]): Vector[(Int, String)] =
      executeSql(backend.stringInterning.batchLookupExternalStrings(ids))

    lookup(Seq(2, 3)) shouldBe empty
    executeSql(ingest(dtos, _))

    // empty input
    lookup(Seq.empty) shouldBe empty
    // all entries
    lookup(Seq(2, 3, 4, 5)) shouldBe expectedFullList
    // result is ordered by internal id, regardless of the input order
    lookup(Seq(5, 2, 4)) shouldBe expectedFullList.filter { case (id, _) =>
      Set(2, 4, 5)(id)
    }
    // unknown ids are skipped
    lookup(Seq(3, 42)) shouldBe expectedFullList.slice(1, 2)
    lookup(Seq(1, 42)) shouldBe empty
    // duplicates in the input yield a single entry
    lookup(Seq(3, 3)) shouldBe expectedFullList.slice(1, 2)
  }
}
