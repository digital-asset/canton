// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterInput,
  IdFilterPaginationInput,
  PaginationFromTo,
  PaginationInput,
  PaginationLastOnlyInput,
}
import com.digitalasset.canton.platform.store.dao.events.IdPageSizing
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import scala.concurrent.Future

class PaginatingAsyncStreamSpec
    extends AsyncFlatSpec
    with Matchers
    with BaseTest
    with PekkoBeforeAndAfterAll {
  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  behavior of "streamIdsFromSeekPaginationWithoutIdFilter"

  it should "stream in forward order with increasing page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      50L,
      ids,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe ids
      queries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 50), 1),
        PaginationInput(PaginationFromTo.ascending(1, 50), 4),
        PaginationInput(PaginationFromTo.ascending(5, 50), 16),
        PaginationInput(PaginationFromTo.ascending(21, 50), 20),
        PaginationInput(PaginationFromTo.ascending(41, 50), 20),
        PaginationInput(PaginationFromTo.ascending(50, 50), 20),
      )
    }
  }

  it should "stream IDs in forward order with constant page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 20, maxPageSize = 20),
      0L,
      50L,
      ids,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe ids
      queries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 50), 20),
        PaginationInput(PaginationFromTo.ascending(20, 50), 20),
        PaginationInput(PaginationFromTo.ascending(40, 50), 20),
        PaginationInput(PaginationFromTo.ascending(50, 50), 20),
      )
    }
  }

  it should "stream empty range in forward order" in {
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      0L,
      Vector.empty,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe Vector.empty
      queries shouldBe Vector(PaginationInput(PaginationFromTo.ascending(0, 0), 1))
    }
  }

  it should "stream single element in forward order" in {
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 2, maxPageSize = 10),
      0L,
      1L,
      Vector(1L),
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe Vector(1L)
      queries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 1), 2),
        PaginationInput(PaginationFromTo.ascending(1, 1), 8),
      )
    }
  }

  it should "stream forward order with id set being subset requested range" in {
    val ids = (5L to 15L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 10),
      0L,
      30L,
      ids,
      descendingOrder = false,
    ).map { case (result, _) =>
      result shouldBe ids
    }
  }

  it should "stream forward order with id set wider than requested range" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 5),
      5L,
      20L,
      ids,
      descendingOrder = false,
    )
      .map { case (result, _) =>
        result shouldBe (6 to 20)
      }
  }

  it should "stream IDs in backward order when range matches id set" in {
    val ids = (1L to 100L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      100L,
      ids,
      descendingOrder = true,
    )
      .map { case (result, _) =>
        result shouldBe ids.reverse
      }
  }

  it should "stream IDs in backward order when range is wider than id set" in {
    val ids = (10L to 50L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      100L,
      ids,
      descendingOrder = true,
    )
      .map { case (result, _) =>
        result shouldBe ids.reverse
      }
  }

  it should "stream empty range in backward order" in {
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      0L,
      (1L to 10L).toVector,
      descendingOrder = true,
    ).map { case (result, queries) =>
      result shouldBe Vector.empty
      queries shouldBe Vector(PaginationInput(PaginationFromTo.descending(0, 0), 1))
    }
  }

  it should "stream empty set in backward order" in {
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      1L,
      100L,
      Vector.empty,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe Vector.empty
    }
  }

  it should "stream descending order with id set being subset requested range" in {
    val ids = (5L to 15L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 10),
      0L,
      30L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe ids.reverse
    }
  }

  it should "stream descending order with id set wider than requested range" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 5),
      5L,
      20L,
      ids,
      descendingOrder = true,
    )
      .map { case (result, _) =>
        result shouldBe (6 to 20).reverse
      }
  }

  private def runStreamWithoutIdFilter(
      idPageSizing: IdPageSizing,
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
      ids: Vector[Long],
      descendingOrder: Boolean,
  ): Future[(Vector[Long], Vector[PaginationInput])] = {
    val queries = Vector.newBuilder[PaginationInput]
    paginatingAsyncStream
      .streamIdsFromSeekPaginationWithoutIdFilter(
        idStreamName = "test-stream",
        idPageSizing = idPageSizing,
        idPageBufferSize = 1,
        initialFromIdExclusive = initialFromIdExclusive,
        initialEndInclusive = initialEndInclusive,
        descendingOrder = descendingOrder,
      )(_ => {
        case input: PaginationInput if descendingOrder != input.paginationFromTo.descending =>
          throw new IllegalArgumentException(
            s"Got PaginationInput request with different descending setting (${input.paginationFromTo.descending}) then the test's ($descendingOrder"
          )
        case input: PaginationInput if !descendingOrder =>
          queries.addOne(input)
          ids
            .filter(id =>
              id > input.paginationFromTo.fromExclusive && id <= input.paginationFromTo.toInclusive
            )
            .take(input.limit)
        case input: PaginationInput =>
          queries.addOne(input)
          ids
            .filter(id =>
              id < input.paginationFromTo.fromExclusive && id >= input.paginationFromTo.toInclusive
            ) // In backward query end is exclusive!
            .reverse
            .take(input.limit)
      })(f => Future.successful(f(mock[Connection])))
      .runWith(Sink.seq[Long])
      .map(result => (result.toVector, queries.result()))
  }

  it should "stream descending order with length smaller than min page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      IdPageSizing(minPageSize = 4, maxPageSize = 5),
      2L,
      4L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe Vector(4, 3)
    }
  }

  behavior of "streamIdsFromSeekPaginationWithIdFilter"

  it should "stream in forward order with increasing page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      50L,
      ids,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe ids
      val lastOnlyQueries = queries.collect { case q: PaginationLastOnlyInput =>
        q
      }
      lastOnlyQueries shouldBe Vector(
        PaginationLastOnlyInput(PaginationFromTo.ascending(0, 50), 1),
        PaginationLastOnlyInput(PaginationFromTo.ascending(1, 50), 4),
        PaginationLastOnlyInput(PaginationFromTo.ascending(5, 50), 16),
        PaginationLastOnlyInput(PaginationFromTo.ascending(21, 50), 20),
        PaginationLastOnlyInput(PaginationFromTo.ascending(41, 50), 20),
        PaginationLastOnlyInput(PaginationFromTo.ascending(50, 50), 20),
      )
    }
  }

  it should "stream IDs in forward order with constant page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 20, maxPageSize = 20),
      0L,
      50L,
      ids,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe ids
      val lastOnlyQueries = queries.collect { case q: PaginationLastOnlyInput =>
        q
      }
      lastOnlyQueries shouldBe Vector(
        PaginationLastOnlyInput(PaginationFromTo.ascending(0, 50), 20),
        PaginationLastOnlyInput(PaginationFromTo.ascending(20, 50), 20),
        PaginationLastOnlyInput(PaginationFromTo.ascending(40, 50), 20),
        PaginationLastOnlyInput(PaginationFromTo.ascending(50, 50), 20),
      )
    }
  }

  it should "stream empty range in forward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      0L,
      Vector.empty,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe Vector.empty
      queries shouldBe Vector(PaginationLastOnlyInput(PaginationFromTo.ascending(0, 0), 1))
    }
  }

  it should "stream single element in forward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 2, maxPageSize = 10),
      0L,
      1L,
      Vector(1L),
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe Vector(1L)
      val lastOnlyQueries = queries.collect { case q: PaginationLastOnlyInput =>
        q
      }
      lastOnlyQueries shouldBe Vector(
        PaginationLastOnlyInput(PaginationFromTo.ascending(0, 1), 2),
        PaginationLastOnlyInput(PaginationFromTo.ascending(1, 1), 8),
      )
    }
  }

  it should "stream forward order with id set being subset requested range" in {
    val ids = (5L to 15L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 10),
      0L,
      30L,
      ids,
      descendingOrder = false,
    ).map { case (result, _) =>
      result shouldBe ids
    }
  }

  it should "stream forward order with id set wider than requested range" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 5),
      5L,
      20L,
      ids,
      descendingOrder = false,
    ).map { case (result, _) =>
      result shouldBe (6 to 20)
    }
  }

  it should "stream IDs in backward order when range matches id set" in {
    val ids = (1L to 100L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      100L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe ids.reverse
    }
  }

  it should "stream IDs in backward order when range is wider than id set" in {
    val ids = (10L to 50L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      100L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe ids.reverse
    }
  }

  it should "stream empty range in backward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      0L,
      (1L to 10L).toVector,
      descendingOrder = true,
    ).map { case (result, queries) =>
      result shouldBe Vector.empty
      queries shouldBe Vector(PaginationLastOnlyInput(PaginationFromTo.descending(0, 0), 1))
    }
  }

  it should "stream empty set in backward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      1L,
      100L,
      Vector.empty,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe Vector.empty
    }
  }

  it should "stream descending order with id set being subset requested range" in {
    val ids = (5L to 15L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 10),
      0L,
      30L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe ids.reverse
    }
  }

  it should "stream descending order with id set wider than requested range" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 3, maxPageSize = 5),
      5L,
      20L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe (6 to 20).reverse
    }
  }

  it should "stream descending order with length smaller than min page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 4, maxPageSize = 5),
      2L,
      4L,
      ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe Vector(4, 3)
    }
  }

  private def runStreamWithIdFilter(
      idPageSizing: IdPageSizing,
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
      ids: Vector[Long],
      descendingOrder: Boolean,
      idFilterQueryParallelism: Int = 1,
  ): Future[(Vector[Long], Vector[IdFilterPaginationInput])] = {
    val queries = Vector.newBuilder[IdFilterPaginationInput]
    paginatingAsyncStream
      .streamIdsFromSeekPaginationWithIdFilter(
        idStreamName = "test-stream",
        idPageSizing = idPageSizing,
        idPageBufferSize = 1,
        initialFromIdExclusive = initialFromIdExclusive,
        initialEndInclusive = initialEndInclusive,
        descendingOrder = descendingOrder,
      )(_ => {
        case input: PaginationLastOnlyInput if !descendingOrder =>
          queries.addOne(input)
          ids
            .filter(id =>
              id > input.paginationFromTo.fromExclusive && id <= input.paginationFromTo.toInclusive
            )
            .take(input.limit)
            .lastOption
            .toList
            .toVector
        case input: PaginationLastOnlyInput if descendingOrder =>
          queries.addOne(input)
          ids
            .filter(id =>
              id < input.paginationFromTo.fromExclusive && id >= input.paginationFromTo.toInclusive
            )
            .reverse
            .view
            .take(input.limit)
            .lastOption
            .toList
            .toVector
        case input: IdFilterInput =>
          queries.addOne(input)
          val filtered = ids.filter(id =>
            if (input.paginationFromTo.descending)
              id < input.paginationFromTo.fromExclusive && id >= input.paginationFromTo.toInclusive
            else
              id > input.paginationFromTo.fromExclusive && id <= input.paginationFromTo.toInclusive
          )
          if (input.paginationFromTo.descending) filtered.reverse else filtered
        case _: PaginationLastOnlyInput =>
          throw new NotImplementedError(
            "Descending test should not receive PaginationLastOnlyInput"
          )
        case _: PaginationInput =>
          throw new IllegalArgumentException(
            "PaginationInput should not happen in this test"
          )
      })(
        executeLastIdQuery = f => Future.successful(f(mock[Connection])),
        idFilterQueryParallelism = idFilterQueryParallelism,
        executeIdFilterQuery = f => Future.successful(f(mock[Connection])),
      )(TraceContext.empty)
      .runWith(Sink.seq[Long])
      .map(result => (result.toVector, queries.result()))
  }

}
