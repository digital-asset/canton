// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.EventSeqIdRange
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
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 50L,
      ids = ids,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe ids
      queries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(1, 50)), 1),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(2, 50)), 4),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(6, 50)), 16),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(22, 50)), 20),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(42, 50)), 20),
      )
    }
  }

  it should "stream IDs in forward order with constant page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 20, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 50L,
      ids = ids,
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe ids
      queries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(1, 50)), 20),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(21, 50)), 20),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(41, 50)), 20),
      )
    }
  }

  it should "stream single element in forward order" in {
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 2, maxPageSize = 10),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 1L,
      ids = Vector(1L),
      descendingOrder = false,
    ).map { case (result, queries) =>
      result shouldBe Vector(1L)
      queries shouldBe Vector(
        PaginationInput(fromTo = PaginationFromTo.ascending(EventSeqIdRange(1, 1)), limit = 2)
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
      idPageSizing = IdPageSizing(minPageSize = 3, maxPageSize = 5),
      initialFromIdInclusive = 6L,
      initialEndInclusive = 20L,
      ids = ids,
      descendingOrder = false,
    )
      .map { case (result, _) =>
        result shouldBe (6 to 20)
      }
  }

  it should "stream IDs in backward order when range matches id set" in {
    val ids = (1L to 100L).toVector
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 100L,
      ids = ids,
      descendingOrder = true,
    )
      .map { case (result, _) =>
        result shouldBe ids.reverse
      }
  }

  it should "stream IDs in backward order when range is wider than id set" in {
    val ids = (10L to 50L).toVector
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 100L,
      ids = ids,
      descendingOrder = true,
    )
      .map { case (result, _) =>
        result shouldBe ids.reverse
      }
  }

  it should "stream empty set in backward order" in {
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 100L,
      ids = Vector.empty,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe Vector.empty
    }
  }

  it should "stream descending order with id set being subset requested range" in {
    val ids = (5L to 15L).toVector
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 3, maxPageSize = 10),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 30L,
      ids = ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe ids.reverse
    }
  }

  it should "stream descending order with id set wider than requested range" in {
    val ids = (1L to 50L).toVector
    runStreamWithoutIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 3, maxPageSize = 5),
      initialFromIdInclusive = 6L,
      initialEndInclusive = 20L,
      ids = ids,
      descendingOrder = true,
    )
      .map { case (result, _) =>
        result shouldBe (6 to 20).reverse
      }
  }

  private def runStreamWithoutIdFilter(
      idPageSizing: IdPageSizing,
      initialFromIdInclusive: Long,
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
        initialEventSeqIdRange = EventSeqIdRange(initialFromIdInclusive, initialEndInclusive),
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
                id <= input.fromTo.eventSeqIdRange.startInclusive && id >= input.fromTo.eventSeqIdRange.endInclusive
              )
              .reverse
              .take(input.limit + 1)
          } else {
            ids
              .filter(id =>
                id >= input.fromTo.eventSeqIdRange.startInclusive && id <= input.fromTo.eventSeqIdRange.endInclusive
              )
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
      idPageSizing = IdPageSizing(minPageSize = 4, maxPageSize = 5),
      initialFromIdInclusive = 3L,
      initialEndInclusive = 4L,
      ids = ids,
      descendingOrder = true,
    ).map { case (result, _) =>
      result shouldBe Vector(4, 3)
    }
  }

  behavior of "streamIdsFromSeekPaginationWithIdFilter"

  it should "stream in forward order with increasing page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 50L,
      ids = ids,
      descendingOrder = false,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(1, 50)), 1),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(2, 50)), 4),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(6, 50)), 16),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(22, 50)), 20),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(42, 50)), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(EventSeqIdRange(1, 1)),
        PaginationFromTo.ascending(EventSeqIdRange(2, 5)),
        PaginationFromTo.ascending(EventSeqIdRange(6, 21)),
        PaginationFromTo.ascending(EventSeqIdRange(22, 41)),
        PaginationFromTo.ascending(EventSeqIdRange(42, 50)),
      )
    }
  }

  it should "stream IDs in forward order with constant page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 20, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 50L,
      ids = ids,
      descendingOrder = false,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(1, 50)), 20),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(21, 50)), 20),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(41, 50)), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(EventSeqIdRange(1, 20)),
        PaginationFromTo.ascending(EventSeqIdRange(21, 40)),
        PaginationFromTo.ascending(EventSeqIdRange(41, 50)),
      )
    }
  }

  it should "stream single element in forward order" in {
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 2, maxPageSize = 10),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 1L,
      ids = Vector(1L),
      descendingOrder = false,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe Vector(1L)
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(1, 1)), 2)
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(EventSeqIdRange(1, 1))
      )
    }
  }

  it should "stream forward order with id set being subset requested range" in {
    val ids = (5L to 15L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 3, maxPageSize = 10),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 30L,
      ids = ids,
      descendingOrder = false,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(1, 30L)), 3),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(8, 30L)), 10),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(EventSeqIdRange(1, 7)),
        PaginationFromTo.ascending(EventSeqIdRange(8, 30)),
      )
    }
  }

  it should "stream forward order with id set wider than requested range" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 3, maxPageSize = 5),
      initialFromIdInclusive = 6L,
      initialEndInclusive = 20L,
      ids = ids,
      descendingOrder = false,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe (6 to 20)
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(6L, 20L)), 3),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(9L, 20L)), 5),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(14L, 20L)), 5),
        PaginationInput(PaginationFromTo.ascending(EventSeqIdRange(19L, 20L)), 5),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.ascending(EventSeqIdRange(6L, 8L)),
        PaginationFromTo.ascending(EventSeqIdRange(9L, 13L)),
        PaginationFromTo.ascending(EventSeqIdRange(14L, 18L)),
        PaginationFromTo.ascending(EventSeqIdRange(19L, 20L)),
      )
    }
  }

  it should "stream IDs in backward order when range matches id set" in {
    val ids = (1L to 100L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 100L,
      ids = ids,
      descendingOrder = true,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids.reverse
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 100L)), 1),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 99L)), 4),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 95L)), 16),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 79L)), 20),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 59L)), 20),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 39L)), 20),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 19L)), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.descending(EventSeqIdRange(100L, 100L)),
        PaginationFromTo.descending(EventSeqIdRange(96L, 99L)),
        PaginationFromTo.descending(EventSeqIdRange(80L, 95L)),
        PaginationFromTo.descending(EventSeqIdRange(60L, 79L)),
        PaginationFromTo.descending(EventSeqIdRange(40L, 59L)),
        PaginationFromTo.descending(EventSeqIdRange(20L, 39L)),
        PaginationFromTo.descending(EventSeqIdRange(1L, 19L)),
      )
    }
  }

  it should "stream IDs in backward order when range is wider than id set" in {
    val ids = (10L to 50L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 1, maxPageSize = 20),
      initialFromIdInclusive = 1L,
      initialEndInclusive = 100L,
      ids = ids,
      descendingOrder = true,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe ids.reverse
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 100L)), 1),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 49L)), 4),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 45L)), 16),
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1L, 29L)), 20),
      )
      pageQueries shouldBe Vector(
        PaginationFromTo.descending(EventSeqIdRange(50L, 100L)),
        PaginationFromTo.descending(EventSeqIdRange(46L, 49L)),
        PaginationFromTo.descending(EventSeqIdRange(30L, 45L)),
        PaginationFromTo.descending(EventSeqIdRange(1L, 29L)),
      )
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
      boundQueries shouldBe Vector(
        PaginationInput(PaginationFromTo.descending(EventSeqIdRange(1, 100)), 1)
      )
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
      idPageSizing = IdPageSizing(minPageSize = 3, maxPageSize = 5),
      initialFromIdInclusive = 6L,
      initialEndInclusive = 20L,
      ids = ids,
      descendingOrder = true,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe (6 to 20).reverse
    }
  }

  it should "stream descending order with length smaller than min page size" in {
    val ids = (1L to 50L).toVector
    runStreamWithIdFilter(
      idPageSizing = IdPageSizing(minPageSize = 4, maxPageSize = 5),
      initialFromIdInclusive = 3L,
      initialEndInclusive = 4L,
      ids = ids,
      descendingOrder = true,
    ).map { case (result, boundQueries, pageQueries) =>
      result shouldBe Vector(4, 3)
    }
  }

  private def runStreamWithIdFilter(
      idPageSizing: IdPageSizing,
      initialFromIdInclusive: Long,
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
        initialEventSeqIdRange = EventSeqIdRange(initialFromIdInclusive, initialEndInclusive),
        descendingOrder = descendingOrder,
      )(new IdFilterPageQuery {
        override def fetchPageBounds(
            connection: Connection
        )(input: PaginationInput): Option[PaginatingAsyncStream.IdPageBounds] = {
          boundQueries.addOne(input)
          if (descendingOrder) {
            val unfilteredIds = ids
              .filter(id =>
                id <= input.fromTo.eventSeqIdRange.startInclusive && id >= input.fromTo.eventSeqIdRange.endInclusive
              )
              .reverse
              .take(input.limit + 1)
            val lastPage = unfilteredIds.sizeIs < input.limit + 1
            unfilteredIds.lastOption.map(last =>
              IdPageBounds(
                fromTo =
                  if (lastPage) input.fromTo
                  else
                    input.fromTo.withEndInclusive(last + 1),
                lastPage = lastPage,
              )
            )
          } else {
            val unfilteredIds = ids
              .filter(id =>
                id >= input.fromTo.eventSeqIdRange.startInclusive && id <= input.fromTo.eventSeqIdRange.endInclusive
              )
              .take(input.limit + 1)
            val lastPage = unfilteredIds.sizeIs < input.limit + 1
            unfilteredIds.lastOption.map(last =>
              IdPageBounds(
                fromTo =
                  if (lastPage) input.fromTo
                  else
                    input.fromTo.withEndInclusive(last - 1),
                lastPage = lastPage,
              )
            )
          }
        }

        override def fetchPage(connection: Connection)(fromTo: PaginationFromTo): Vector[Long] = {
          pageQueries.addOne(fromTo)
          val filtered = ids.filter(id =>
            if (fromTo.descending)
              id <= fromTo.eventSeqIdRange.startInclusive && id >= fromTo.eventSeqIdRange.endInclusive
            else
              id >= fromTo.eventSeqIdRange.startInclusive && id <= fromTo.eventSeqIdRange.endInclusive
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
