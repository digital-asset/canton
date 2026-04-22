// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReaderSpec.*
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, HasExecutorServiceGeneric}
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.scalatest.Assertion
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.chaining.*

class BufferedStreamsReaderSpec
    extends AnyWordSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with TestFixtures
    with HasExecutionContext {

  "stream (static)" when {
    "buffer filter" should {
      "return filtered elements (Inclusive slice)" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBuffer,
          startInclusive = offset2,
          endInclusive = offset3,
          bufferSliceFilter = noFilterBufferSlice(_).filterNot(
            _.updateId == TestUpdateId("tx-3").toHexString
          ),
          descendingOrder = false,
        )
        streamElements should contain theSameElementsInOrderAs Seq(
          offset2 -> TestUpdateId("tx-2").toHexString
        )
      }
    }

    "request within buffer range inclusive" should {
      "fetch from buffer" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBuffer,
          startInclusive = offset2,
          endInclusive = offset3,
          descendingOrder = false,
        )
        streamElements should contain theSameElementsInOrderAs Seq(
          offset2 -> TestUpdateId("tx-2").toHexString,
          offset3 -> TestUpdateId("tx-3").toHexString,
        )
      }
    }

    "request within buffer range inclusive (multiple chunks)" should {
      "correctly fetch from buffer" in new StaticTestScope {
        run(
          transactionsBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          startInclusive = offset2,
          endInclusive = offset3,
          descendingOrder = false,
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset2 -> TestUpdateId("tx-2").toHexString,
          offset3 -> TestUpdateId("tx-3").toHexString,
        )
      }
    }

    "request before buffer start" should {
      "fetch from buffer and storage" in new StaticTestScope {
        val filterMock = new Object

        val anotherResponseForOffset1 = "(1) Response fetched from storage"
        val anotherResponseForOffset2 = "(2) Response fetched from storage"

        val fetchFromPersistence = buildFetchFromPersistence(
          expectedStartInclusive = offset0,
          expectedEndInclusive = offset2,
          expectedDescendingOrder = false,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(
            Seq(offset1 -> anotherResponseForOffset1, offset2 -> anotherResponseForOffset2)
          ),
        )

        run(
          transactionsBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          startInclusive = offset0,
          endInclusive = offset3,
          descendingOrder = false,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
          bufferSliceFilter = noFilterBufferSlice,
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset1 -> anotherResponseForOffset1,
          offset2 -> anotherResponseForOffset2,
          offset3 -> TestUpdateId("tx-3").toHexString,
        )
      }

      "fetch from buffer and storage chunked with buffer filter" in new StaticTestScope {
        val filterMock = new Object

        val anotherResponseForOffset1 = "(1) Response fetched from storage"

        val fetchFromPersistence = buildFetchFromPersistence(
          expectedStartInclusive = offset0,
          expectedEndInclusive = offset1,
          expectedDescendingOrder = false,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(Seq(offset1 -> anotherResponseForOffset1)),
        )

        run(
          startInclusive = offset0,
          endInclusive = offset3,
          descendingOrder = false,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
          bufferSliceFilter = noFilterBufferSlice(_).filterNot(
            _.updateId == TestUpdateId("tx-3").toHexString
          ),
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset1 -> anotherResponseForOffset1,
          offset2 -> TestUpdateId("tx-2").toHexString,
        )
      }
    }

    "request before buffer bounds" should {
      "fetch only from storage" in new StaticTestScope {
        val filterMock = new Object

        val fetchedElements = Vector(
          offset1 -> "Some API response from persistence",
          offset2 -> "Another API response from persistence",
        )

        val fetchFromPersistence = buildFetchFromPersistence(
          expectedStartInclusive = offset1,
          expectedEndInclusive = offset2,
          expectedDescendingOrder = false,
          expectedFilter = `filterMock`,
          thenReturnStream = Source(fetchedElements),
        )

        run(
          transactionsBuffer = smallInMemoryFanoutBuffer,
          startInclusive = offset1,
          endInclusive = offset2,
          descendingOrder = false,
          fetchFromPersistence = fetchFromPersistence,
          persistenceFetchArgs = filterMock,
        )

        streamElements should contain theSameElementsInOrderAs fetchedElements
      }
    }
  }

  "stream (dynamic)" when {
    "catching up from buffer begin (exclusive)" should {
      "return the correct ranges" in new DynamicTestScope() {
        runF(
          for {
            // Prepopulate stores
            _ <- updateStores(maxBufferSize)
            // Stream from the beginning and assert
            _ <- stream(1, maxBufferSize, descendingOrder = false)
          } yield succeed
        )
      }
    }

    "catching up from buffer (inclusive)" should {
      "return the correct ranges" in new DynamicTestScope() {
        runF(
          for {
            // Prepopulate stores
            _ <- updateStores(maxBufferSize)
            // Stream from the middle and assert
            _ <- stream(maxBufferSize / 2 + 1, maxBufferSize, descendingOrder = false)
          } yield succeed
        )
      }
    }

    def testConsumerFallingBehind(
        bufferSize: Int,
        bufferChunkSize: Int,
        consumerSubscriptionFrom: Int,
        updateAgainWithCount: Int,
    ) = new DynamicTestScope(maxBufferSize = bufferSize, maxBufferChunkSize = bufferChunkSize) {
      runF(
        for {
          // Prepopulate stores
          _ <- updateStores(count = bufferSize)
          // Start stream subscription
          (assertFirst1000, unblockConsumer) = streamWithHandle(
            startInclusiveIdx = consumerSubscriptionFrom,
            endInclusiveIdx = bufferSize,
            descendingOrder = false,
          )
          // Feed the buffer and effectively force the consumer to fall behind
          _ <- updateStores(count = updateAgainWithCount)
          _ = unblockConsumer()
          _ <- assertFirst1000
        } yield succeed
      )
    }

    val bufferSize = 100
    val bufferChunkSize = 10

    "falling completely behind" should {
      "return the correct ranges when starting from the beginning" in {
        testConsumerFallingBehind(
          bufferSize = bufferSize,
          bufferChunkSize = bufferChunkSize,
          consumerSubscriptionFrom = 1,
          updateAgainWithCount = bufferSize,
        )
      }

      "return the correct ranges when starting from an offset originally in the buffer at subscription time" in {
        testConsumerFallingBehind(
          bufferSize = bufferSize,
          bufferChunkSize = bufferChunkSize,
          consumerSubscriptionFrom = bufferSize / 2 + 1,
          updateAgainWithCount = bufferSize,
        )
      }
    }
  }

  "stream (reverse)" when {
    "requested part is contained within buffer " should {
      "not request form persistence" in new StaticTestScope {
        run(
          startInclusive = offset1,
          endInclusive = offset3,
          descendingOrder = true,
        )
        streamElements should contain theSameElementsInOrderAs Seq(
          offset3 -> TestUpdateId("tx-3").toHexString,
          offset2 -> TestUpdateId("tx-2").toHexString,
          offset1 -> TestUpdateId("tx-1").toHexString,
        )
      }
    }

    "requested range is disjoint with buffer" should {
      "request whole range from persistence" in new StaticTestScope {
        val filterMock = new Object
        val fetchFromPersistenceMock = buildFetchFromPersistence(
          expectedStartInclusive = offset0.decrement.value,
          expectedEndInclusive = offset0,
          expectedDescendingOrder = true,
          expectedFilter = filterMock,
          thenReturnStream = Source(
            Seq(
              offset0.decrement.value -> TestUpdateId("tx").toHexString
            )
          ),
        )
        run(
          startInclusive = offset0.decrement.value,
          endInclusive = offset0,
          descendingOrder = true,
          fetchFromPersistence = fetchFromPersistenceMock,
          persistenceFetchArgs = filterMock,
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset0.decrement.value -> TestUpdateId("tx").toHexString
        )
      }
    }

    "requested range is partially contained in buffer" should {
      "request missing part from persistence" in new StaticTestScope {
        val filterMock = new Object
        val fetchFromPersistenceMock = buildFetchFromPersistence(
          expectedStartInclusive = offset0,
          expectedEndInclusive = offset0,
          expectedDescendingOrder = true,
          expectedFilter = filterMock,
          thenReturnStream = Source(Seq(offset0 -> TestUpdateId("tx-0").toHexString)),
        )

        run(
          transactionsBuffer = inMemoryFanoutBuffer,
          startInclusive = offset0,
          endInclusive = offset3,
          descendingOrder = true,
          fetchFromPersistence = fetchFromPersistenceMock,
          persistenceFetchArgs = filterMock,
        )

        streamElements should contain theSameElementsInOrderAs Seq(
          offset3 -> TestUpdateId("tx-3").toHexString,
          offset2 -> TestUpdateId("tx-2").toHexString,
          offset1 -> TestUpdateId("tx-1").toHexString,
          offset0 -> TestUpdateId("tx-0").toHexString,
        )
      }
    }

    val bufferSize = 5
    val bufferChunkSize = 2
    "requested range is initially contained in buffer, after first fetch buffer moves forward" should {
      "request missing part from persistence" in new DynamicTestScope(
        maxBufferSize = bufferSize,
        maxBufferChunkSize = bufferChunkSize,
      ) {
        runF(
          for {
            // Prepopulate stores
            _ <- updateStores(count = bufferSize)
            // Start stream subscription
            (assertResult, unblockConsumer) = streamWithHandle(
              startInclusiveIdx = 1,
              endInclusiveIdx = bufferSize,
              descendingOrder = true,
            )
            // Feed the buffer and effectively make the next chunk in reverse outside of it
            _ <- updateStores(count = bufferSize)
            _ = unblockConsumer()
            _ <- assertResult
          } yield succeed
        )
      }
    }
  }
}

object BufferedStreamsReaderSpec {

  trait TestFixtures
      extends Matchers
      with ScalaFutures
      with BaseTest
      with HasExecutorServiceGeneric { self: PekkoBeforeAndAfterAll =>

    implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

    implicit val ec: ExecutionContext = executorService

    val metrics = LedgerApiServerMetrics.ForTesting
    val Seq(offset0, offset1, offset2, offset3) =
      (0 to 3) map { idx => offset(idx.toLong) }: @nowarn("msg=match may not be exhaustive")
    val offsetUpdates: Seq[TransactionLogUpdate.TransactionAccepted] =
      (1L to 3L).map(transaction)
    val offsetUpdatesAfterBeginning: Seq[TransactionLogUpdate.TransactionAccepted] =
      (5L to 8L).map(transaction)

    val noFilterBufferSlice
        : TransactionLogUpdate => Option[TransactionLogUpdate.TransactionAccepted] =
      tracedUpdate =>
        tracedUpdate match {
          case update: TransactionLogUpdate.TransactionAccepted => Some(update)
          case _ => None
        }

    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 3,
      metrics = metrics,
      maxBufferedChunkSize = 3,
      loggerFactory = loggerFactory,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(inMemoryFanoutBuffer.push))

    val inMemoryFanoutBufferWithSmallChunkSize: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 3,
      metrics = metrics,
      maxBufferedChunkSize = 1,
      loggerFactory = loggerFactory,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(inMemoryFanoutBuffer.push))

    val smallInMemoryFanoutBuffer: InMemoryFanoutBuffer = new InMemoryFanoutBuffer(
      maxBufferSize = 1,
      metrics = metrics,
      maxBufferedChunkSize = 1,
      loggerFactory = loggerFactory,
    ).tap(inMemoryFanoutBuffer => offsetUpdates.foreach(inMemoryFanoutBuffer.push))

    trait StaticTestScope {

      val streamElements: ArrayBuffer[(Offset, String)] =
        ArrayBuffer.empty[(Offset, String)]

      private val failingPersistenceFetch = new FetchFromPersistence[Object, String] {
        override def apply(
            startInclusive: Offset,
            endInclusive: Offset,
            descendingOrder: Boolean,
            filter: Object,
            skipPruningChecks: Boolean,
        )(implicit
            loggingContext: LoggingContextWithTrace
        ): Source[(Offset, String), NotUsed] = fail(
          s"Unexpected call to fetch from persistence startInclusive=$startInclusive, endInclusive=$endInclusive, descendingOrder=$descendingOrder"
        )
      }

      def run(
          startInclusive: Offset,
          endInclusive: Offset,
          descendingOrder: Boolean,
          transactionsBuffer: InMemoryFanoutBuffer = inMemoryFanoutBufferWithSmallChunkSize,
          fetchFromPersistence: FetchFromPersistence[Object, String] = failingPersistenceFetch,
          persistenceFetchArgs: Object = new Object,
          bufferSliceFilter: TransactionLogUpdate => Option[
            TransactionLogUpdate.TransactionAccepted
          ] = noFilterBufferSlice,
      ): Done =
        new BufferedStreamsReader[Object, String](
          inMemoryFanoutBuffer = transactionsBuffer,
          fetchFromPersistence = fetchFromPersistence,
          bufferedStreamEventsProcessingParallelism = 2,
          metrics = metrics,
          streamName = "some_tx_stream",
          loggerFactory,
        )(executorService)
          .stream[TransactionLogUpdate.TransactionAccepted](
            startInclusive = startInclusive,
            endInclusive = endInclusive,
            persistenceFetchArgs = persistenceFetchArgs,
            bufferFilter = bufferSliceFilter,
            toApiResponse = tx => Future.successful(tx.updateId),
            descendingOrder = descendingOrder,
            skipPruningChecks = false,
          )
          .runWith(Sink.foreach(streamElements.addOne))
          .futureValue

      def buildFetchFromPersistence(
          expectedStartInclusive: Offset,
          expectedEndInclusive: Offset,
          expectedDescendingOrder: Boolean,
          expectedFilter: Object,
          thenReturnStream: Source[(Offset, String), NotUsed],
      ): FetchFromPersistence[Object, String] =
        new FetchFromPersistence[Object, String] {
          override def apply(
              startInclusive: Offset,
              endInclusive: Offset,
              descendingOrder: Boolean,
              filter: Object,
              skipPruningChecks: Boolean,
          )(implicit
              loggingContext: LoggingContextWithTrace
          ): Source[(Offset, String), NotUsed] =
            (startInclusive, endInclusive, filter, descendingOrder, skipPruningChecks) match {
              case (
                    `expectedStartInclusive`,
                    `expectedEndInclusive`,
                    `expectedFilter`,
                    `expectedDescendingOrder`,
                    false,
                  ) =>
                thenReturnStream
              case unexpected =>
                fail(s"Unexpected fetch transactions subscription start: $unexpected")
            }
        }
    }

    class DynamicTestScope(val maxBufferSize: Int = 100, val maxBufferChunkSize: Int = 10) {
      @volatile private var persistenceStore =
        Vector.empty[(Offset, TransactionLogUpdate.TransactionAccepted)]
      @volatile private var ledgerEndIndex = 0L
      private val inMemoryFanoutBuffer =
        new InMemoryFanoutBuffer(maxBufferSize, metrics, maxBufferChunkSize, loggerFactory)

      private val fetchFromPersistence = new FetchFromPersistence[Object, String] {
        override def apply(
            startInclusive: Offset,
            endInclusive: Offset,
            descendingOrder: Boolean,
            filter: Object,
            skipPruningChecks: Boolean,
        )(implicit
            loggingContext: LoggingContextWithTrace
        ): Source[(Offset, String), NotUsed] =
          if (startInclusive > endInclusive) fail("startExclusive after endInclusive")
          else if (endInclusive > offset(ledgerEndIndex))
            fail("endInclusive after ledgerEnd")
          else if (!descendingOrder)
            persistenceStore
              .dropWhile(_._1 < startInclusive)
              .takeWhile(_._1 <= endInclusive)
              .map { case (o, tx) => o -> tx.updateId }
              .pipe(Source(_))
          else
            persistenceStore
              .dropWhile(_._1 < startInclusive)
              .takeWhile(_._1 <= endInclusive)
              .reverse
              .map { case (o, tx) => o -> tx.updateId }
              .pipe(Source(_))
      }

      private val streamReader = new BufferedStreamsReader[Object, String](
        inMemoryFanoutBuffer = inMemoryFanoutBuffer,
        fetchFromPersistence = fetchFromPersistence,
        bufferedStreamEventsProcessingParallelism = 2,
        metrics = metrics,
        streamName = "some_tx_stream",
        loggerFactory,
      )

      def updateStores(count: Int): Future[Done] = {
        val (done, handle) = {
          val blockingPromise = Promise[Unit]()
          val unblockHandle: () => Unit = () => blockingPromise.success(())

          val done = Source
            .fromIterator(() => (ledgerEndIndex + 1L to count + ledgerEndIndex).iterator)
            .async
            .mapAsync(1) { idx =>
              blockingPromise.future.map(_ => idx)
            }
            .async
            .runForeach(updateFixtures)

          done -> unblockHandle
        }
        handle()
        done
      }

      def stream(
          startInclusiveIdx: Int,
          endInclusiveIdx: Int,
          descendingOrder: Boolean,
      ): Future[Assertion] = {
        val (done, handle) = streamWithHandle(startInclusiveIdx, endInclusiveIdx, descendingOrder)
        handle()
        done
      }

      def streamWithHandle(
          startInclusiveIdx: Int,
          endInclusiveIdx: Int,
          descendingOrder: Boolean,
      ): (Future[Assertion], () => Unit) = {
        val blockingPromise = Promise[Unit]()
        val unblockHandle: () => Unit = () => blockingPromise.success(())

        val assertReadStream = streamReader
          .stream[TransactionLogUpdate.TransactionAccepted](
            startInclusive = offset(startInclusiveIdx.toLong),
            endInclusive = offset(endInclusiveIdx.toLong),
            persistenceFetchArgs = new Object, // Not used
            bufferFilter = noFilterBufferSlice, // Do not filter
            toApiResponse = tx => Future.successful(tx.updateId),
            descendingOrder = descendingOrder,
            skipPruningChecks = false,
          )
          .async
          .mapAsync(1) { idx =>
            blockingPromise.future.map(_ => idx)
          }
          .async
          .runWith(Sink.seq)
          .map { result =>
            withClue(s"[$startInclusiveIdx, $endInclusiveIdx]") {
              result.size shouldBe endInclusiveIdx - startInclusiveIdx + 1
            }
            val expectedElements = {
              val elements = (startInclusiveIdx.toLong to endInclusiveIdx.toLong) map { idx =>
                offset(idx) -> TestUpdateId(s"tx-$idx").toHexString
              }
              if (descendingOrder)
                elements.reverse
              else
                elements
            }
            result should contain theSameElementsInOrderAs expectedElements
          }

        assertReadStream -> unblockHandle
      }

      def runF(f: => Future[Assertion]): Assertion =
        f.futureValue

      private def updateFixtures(idx: Long): Unit = {
        val offsetAt = offset(idx)
        val tx = transaction(idx)
        persistenceStore = persistenceStore.appended(offsetAt -> tx)
        inMemoryFanoutBuffer.push(tx)
        ledgerEndIndex = idx
      }
    }
  }

  private val someSynchronizerId = SynchronizerId.tryFromString("some::synchronizer id")

  private def transaction(i: Long) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = TestUpdateId(s"tx-$i").toHexString,
      commandId = "",
      workflowId = "",
      effectiveAt = Timestamp.Epoch,
      offset = offset(i),
      events = Vector(null),
      completionStreamResponseO = None,
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      recordTime = Timestamp.Epoch,
      externalTransactionHash = None,
    )(TraceContext.empty)

  private def offset(idx: Long): Offset = {
    val base = 1000000000L
    Offset.tryFromLong(base + idx)
  }
}
