// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterPageQuery,
  IdPage,
  IdPageBounds,
  IdPageQuery,
  PaginationFromTo,
  PaginationInput,
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
      )(new IdPageQuery {
        override def fetchPage(
            connection: Connection
        )(input: PaginationInput): PaginatingAsyncStream.IdPage = {
          if (descendingOrder != input.fromTo.descending) {
            throw new IllegalArgumentException(
              s"Got PaginationInput request with different descending setting (${input.fromTo.descending}) then the test's ($descendingOrder"
            )
          }
          queries.addOne(input)
          val resultIdsPlusOne = if (descendingOrder) {
            ids
              .filter(id =>
                id < input.fromTo.fromExclusive && id >= input.fromTo.toInclusive
              ) // In backward query end is exclusive!
              .reverse
              .take(input.limit + 1)
          } else {
            ids
              .filter(id => id > input.fromTo.fromExclusive && id <= input.fromTo.toInclusive)
              .take(input.limit + 1)
          }
          IdPage(
            ids = resultIdsPlusOne.take(input.limit),
            lastPage = resultIdsPlusOne.sizeIs < input.limit + 1,
          )
        }
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 50), 1),
        PaginationInput(PaginationFromTo.ascending(1, 50), 4),
        PaginationInput(PaginationFromTo.ascending(5, 50), 16),
        PaginationInput(PaginationFromTo.ascending(21, 50), 20),
        PaginationInput(PaginationFromTo.ascending(41, 50), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(0, 1),
        PaginationFromTo.ascending(1, 5),
        PaginationFromTo.ascending(5, 21),
        PaginationFromTo.ascending(21, 41),
        PaginationFromTo.ascending(41, 50),
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 50), 20),
        PaginationInput(PaginationFromTo.ascending(20, 50), 20),
        PaginationInput(PaginationFromTo.ascending(40, 50), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(0, 20),
        PaginationFromTo.ascending(20, 40),
        PaginationFromTo.ascending(40, 50),
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe Vector.empty
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 0), 1)
      )
      pageQueries shouldBe Vector()
    }
  }

  it should "stream single element in forward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 2, maxPageSize = 10),
      0L,
      1L,
      Vector(1L),
      descendingOrder = false,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe Vector(1L)
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 1), 2)
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(0, 1)
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(0, 30L), 3),
        PaginationInput(PaginationFromTo.ascending(7, 30L), 10),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(0, 7),
        PaginationFromTo.ascending(7, 30),
      )
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe (6 to 20)
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(5L, 20L), 3),
        PaginationInput(PaginationFromTo.ascending(8L, 20L), 5),
        PaginationInput(PaginationFromTo.ascending(13L, 20L), 5),
        PaginationInput(PaginationFromTo.ascending(18L, 20L), 5),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(5L, 8L),
        PaginationFromTo.ascending(8L, 13L),
        PaginationFromTo.ascending(13L, 18L),
        PaginationFromTo.ascending(18L, 20L),
      )
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids.reverse
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.descending(0L, 100L), 1),
        PaginationInput(PaginationFromTo.descending(0L, 99L), 4),
        PaginationInput(PaginationFromTo.descending(0L, 95L), 16),
        PaginationInput(PaginationFromTo.descending(0L, 79L), 20),
        PaginationInput(PaginationFromTo.descending(0L, 59L), 20),
        PaginationInput(PaginationFromTo.descending(0L, 39L), 20),
        PaginationInput(PaginationFromTo.descending(0L, 19L), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.descending(99L, 100L),
        PaginationFromTo.descending(95L, 99L),
        PaginationFromTo.descending(79L, 95L),
        PaginationFromTo.descending(59L, 79L),
        PaginationFromTo.descending(39L, 59L),
        PaginationFromTo.descending(19L, 39L),
        PaginationFromTo.descending(0L, 19L),
      )
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
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids.reverse
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.descending(0L, 100L), 1),
        PaginationInput(PaginationFromTo.descending(0L, 49L), 4),
        PaginationInput(PaginationFromTo.descending(0L, 45L), 16),
        PaginationInput(PaginationFromTo.descending(0L, 29L), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.descending(49L, 100L),
        PaginationFromTo.descending(45L, 49L),
        PaginationFromTo.descending(29L, 45L),
        PaginationFromTo.descending(0L, 29L),
      )
    }
  }

  it should "stream empty range in backward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      0L,
      0L,
      (1L to 10L).toVector,
      descendingOrder = true,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe Vector.empty
      boundQueries shouldBe Vector(PaginationInput(PaginationFromTo.descending(0, 0), 1))
      pageQueries shouldBe Vector.empty
    }
  }

  it should "stream empty set in backward order" in {
    runStreamWithIdFilter(
      IdPageSizing(minPageSize = 1, maxPageSize = 20),
      1L,
      100L,
      Vector.empty,
      descendingOrder = true,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe Vector.empty
      boundQueries shouldBe Vector(PaginationInput(PaginationFromTo.descending(1, 100), 1))
      pageQueries shouldBe Vector.empty
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
    ).map { case (result, boundQueries, pageQueries) =>
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
    ).map { case (result, boundQueries, pageQueries) =>
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
    ).map { case (result, boundQueries, pageQueries) =>
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
  ): Future[(Vector[Long], Vector[PaginationInput], Vector[PaginationFromTo])] = {
    val boundQueries = Vector.newBuilder[PaginationInput]
    val pageQueries = Vector.newBuilder[PaginationFromTo]
    paginatingAsyncStream
      .streamIdsFromSeekPaginationWithIdFilter(
        idStreamName = "test-stream",
        idPageSizing = idPageSizing,
        idPageBufferSize = 1,
        initialFromIdExclusive = initialFromIdExclusive,
        initialEndInclusive = initialEndInclusive,
        descendingOrder = descendingOrder,
      )(new IdFilterPageQuery {
        override def fetchPageBounds(
            connection: Connection
        )(input: PaginationInput): Option[PaginatingAsyncStream.IdPageBounds] = {
          boundQueries.addOne(input)
          if (descendingOrder) {
            val unfilteredIds = ids
              .filter(id => id < input.fromTo.fromExclusive && id >= input.fromTo.toInclusive)
              .reverse
              .take(input.limit + 1)
            val lastPage = unfilteredIds.sizeIs < input.limit + 1
            unfilteredIds.lastOption.map(last =>
              IdPageBounds(
                fromTo =
                  if (lastPage) input.fromTo
                  else
                    input.fromTo.copy(
                      toInclusive = last + 1
                    ),
                lastPage = lastPage,
              )
            )
          } else {
            val unfilteredIds = ids
              .filter(id => id > input.fromTo.fromExclusive && id <= input.fromTo.toInclusive)
              .take(input.limit + 1)
            val lastPage = unfilteredIds.sizeIs < input.limit + 1
            unfilteredIds.lastOption.map(last =>
              IdPageBounds(
                fromTo =
                  if (lastPage) input.fromTo
                  else
                    input.fromTo.copy(
                      toInclusive = last - 1
                    ),
                lastPage = lastPage,
              )
            )
          }
        }

        override def fetchPage(connection: Connection)(fromTo: PaginationFromTo): Vector[Long] = {
          pageQueries.addOne(fromTo)
          val filtered = ids.filter(id =>
            if (fromTo.descending)
              id < fromTo.fromExclusive && id >= fromTo.toInclusive
            else
              id > fromTo.fromExclusive && id <= fromTo.toInclusive
          )
          if (fromTo.descending) filtered.reverse else filtered
        }
      })(
        executeFetchBounds = f => Future.successful(f(mock[Connection])),
        idFilterQueryParallelism = idFilterQueryParallelism,
        executeFetchPage = f => Future.successful(f(mock[Connection])),
      )(TraceContext.empty)
      .runWith(Sink.seq[Long])
      .map(result => (result.toVector, boundQueries.result(), pageQueries.result()))
  }

}
