// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.ReassignmentInfo
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.BufferSlice.LastBufferChunkSuffix
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.{
  BufferSlice,
  UnorderedException,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.data.{Ref, Time}
import org.scalatest.Succeeded
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.util.concurrent.Executors
import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.{View, immutable}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

class InMemoryFanoutBufferSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with BaseTest {
  private val offsetIdx = Vector(2, 4, 6, 8, 10)
  private val firstOffset = offset(1L)
  private val offsets = offsetIdx.map(i => offset(i.toLong))
  private val someSynchronizerId = SynchronizerId.tryFromString("some::synchronizer id")

  private val IdentityFilter: TransactionLogUpdate => Option[TransactionLogUpdate] =
    tracedUpdate => Some(tracedUpdate)

  inside(offsets) { case Seq(offset1, offset2, offset3, offset4, offset5) =>
    val txAccepted1 = txAccepted(1L, offset1)
    val reassignmentAccepted2 = reassignmentAccepted(2L, offset2)
    val txAccepted3 = txAccepted(3L, offset3)
    val topologyTxAccepted4 = topologyTxAccepted(4L, offset4)
    val bufferValues = Seq(txAccepted1, reassignmentAccepted2, txAccepted3, topologyTxAccepted4)
    val txAccepted5 = txAccepted(5L, offset5)
    val bufferElements = offsets.zip(bufferValues)
    val LastOffset = offset4

    inside(bufferElements) { case Seq(entry1, entry2, entry3, entry4) =>
      "push" when {
        "max buffer size reached" should {
          "drop oldest" in withBuffer(3) { buffer =>
            // Assert data structure sizes
            buffer._bufferLog.size shouldBe 3
            buffer._lookupMap.size shouldBe 3

            buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset2,
              slice = Vector(entry3, entry4),
            )

            // Assert that all the entries are visible by lookup
            verifyLookupPresent(buffer, reassignmentAccepted2, txAccepted3, topologyTxAccepted4)

            buffer.push(txAccepted5)
            // Assert data structure sizes respect their limits after pushing a new element
            buffer._bufferLog.size shouldBe 3
            buffer._lookupMap.size shouldBe 3

            buffer.slice(firstOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset3,
              slice = Vector(entry4, offset5 -> txAccepted5),
            )

            // Assert that the new entry is visible by lookup
            verifyLookupPresent(buffer, txAccepted5)
            // Assert oldest entry is evicted
            verifyLookupAbsent(buffer, reassignmentAccepted2)
          }
        }

        "element with smaller offset added" should {
          "throw" in withBuffer(3) { buffer =>
            intercept[UnorderedException[Int]] {
              buffer.push(txAccepted1)
            }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset1"
          }
        }

        "element with equal offset added" should {
          "throw" in withBuffer(3) { buffer =>
            intercept[UnorderedException[Int]] {
              buffer.push(topologyTxAccepted4)
            }.getMessage shouldBe s"Elements appended to the buffer should have strictly increasing offsets: $offset4 vs $offset4"
          }
        }

        "maxBufferSize is 0" should {
          "not enqueue the update" in withBuffer(0) { buffer =>
            buffer.push(txAccepted5)
            buffer.slice(firstOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset5,
              slice = Vector.empty,
            )
            buffer._bufferLog shouldBe empty
          }
        }

        "maxBufferSize is -1" should {
          "not enqueue the update" in withBuffer(-1) { buffer =>
            buffer.push(txAccepted5)
            buffer.slice(firstOffset, offset5, IdentityFilter) shouldBe LastBufferChunkSuffix(
              bufferedStartExclusive = offset5,
              slice = Vector.empty,
            )
            buffer._bufferLog shouldBe empty
          }
        }

        s"does not update the lookupMap with ${TransactionLogUpdate.TransactionRejected.getClass.getSimpleName}" in withBuffer(
          4
        ) { buffer =>
          // Assert that all the entries are visible by lookup
          verifyLookupPresent(
            buffer,
            txAccepted1,
            reassignmentAccepted2,
            txAccepted3,
            topologyTxAccepted4,
          )

          // Enqueue a rejected transaction
          buffer.push(txRejected(5L, offset5))

          // Assert the last element is evicted on full buffer
          verifyLookupAbsent(buffer, txAccepted1)

          // Assert that the buffer does not include the rejected transaction
          buffer._lookupMap should contain theSameElementsAs Map(
            reassignmentAccepted2.updateId -> reassignmentAccepted2,
            txAccepted3.updateId -> txAccepted3,
            topologyTxAccepted4.updateId -> topologyTxAccepted4,
          )
        }
      }

      "slice" when {
        "filters" in withBuffer() { buffer =>
          buffer.slice(offset2, offset4, Some(_).filterNot(_ == entry3._2)) shouldBe BufferSlice
            .Inclusive(
              Vector(entry2, entry4)
            )
        }

        "called with startInclusive gteq than the buffer start" should {
          "return an Inclusive slice" in withBuffer() { buffer =>
            buffer.slice(offset1, succ(offset3), IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry1, entry2, entry3)
            )
            buffer.slice(offset2, succ(offset3), IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3)
            )
            buffer.slice(offset2, offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3, entry4)
            )
            buffer.slice(succ(offset1), offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3, entry4)
            )
          }

          "return an Inclusive chunk result if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.slice(offset2, offset4, IdentityFilter) shouldBe BufferSlice.Inclusive(
              Vector(entry2, entry3)
            )
          }
        }

        "called with endInclusive lteq startInclusive" should {
          "return an empty Inclusive slice if startInclusive is greater than buffer start and endInclusive" in withBuffer() {
            buffer =>
              buffer.slice(offset2, offset1, IdentityFilter) shouldBe BufferSlice.Inclusive(
                Vector.empty
              )
          }
          "return an Inclusive slice if startInclusive is greater than buffer start and equal to endInclusive" in withBuffer() {
            buffer =>
              buffer.slice(offset2, offset2, IdentityFilter) shouldBe BufferSlice.Inclusive(
                Vector(entry2)
              )
          }
          "return an empty LastBufferChunkSuffix slice if startExclusive is before buffer start" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.slice(offset1, offset1, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset1,
              Vector.empty,
            )
            buffer.slice(offset2, offset1, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset1,
              Vector.empty,
            )
          }
        }
        "called with startInclusive before the buffer start" should {
          "return a LastBufferChunkSuffix slice" in withBuffer() { buffer =>
            buffer.slice(
              firstOffset,
              offset3,
              IdentityFilter,
            ) shouldBe LastBufferChunkSuffix(
              offset1,
              Vector(entry2, entry3),
            )
            buffer.slice(
              firstOffset,
              succ(offset3),
              IdentityFilter,
            ) shouldBe LastBufferChunkSuffix(
              offset1,
              Vector(entry2, entry3),
            )
          }

          "return the last filtered chunk as LastBufferChunkSuffix slice if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.slice(
              firstOffset,
              offset4,
              IdentityFilter,
            ) shouldBe LastBufferChunkSuffix(
              offset2,
              Vector(entry3, entry4),
            )
          }
        }

        "called after push from a different thread" should {
          "always see the most recent updates" in withBuffer(
            1000,
            Vector.empty,
            maxFetchSize = 1000,
          ) { buffer =>
            (0 until 1000).foreach { idx =>
              val updateOffset = offset(idx.toLong)
              buffer.push(txAccepted(idx.toLong, updateOffset))
            } // fill buffer to max size

            val pushExecutor, sliceExecutor =
              ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

            (0 until 1000).foreach { idx =>
              val expected = ((idx + 901) to (1000 + idx)).map { idx =>
                val updateOffset = offset(idx.toLong)
                updateOffset -> txAccepted(idx.toLong, updateOffset)
              }

              implicit val ec: ExecutionContextExecutorService = pushExecutor

              Await.result(
                // Simulate different thread accesses for push/slice
                awaitable = {
                  val lastInsertedIdx = (1000 + idx).toLong
                  val updateOffset = offset(lastInsertedIdx)
                  for {
                    _ <- Future(
                      buffer.push(txAccepted(lastInsertedIdx, updateOffset))
                    )(
                      pushExecutor
                    )
                    _ <- Future(
                      buffer.slice(
                        offset((901 + idx).toLong),
                        offset(lastInsertedIdx),
                        IdentityFilter,
                      )
                    )(
                      sliceExecutor
                    )
                      .map(_.slice should contain theSameElementsInOrderAs expected)(sliceExecutor)
                  } yield Succeeded
                },
                atMost = 1.seconds,
              )
            }
            Succeeded
          }
        }
      }

      "prune" when {
        "element found" should {
          "prune inclusive" in withBuffer() { buffer =>
            verifyLookupPresent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
              topologyTxAccepted4,
            )

            buffer.prune(offset3)

            buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset4,
              bufferElements.drop(4),
            )

            verifyLookupAbsent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
            )
            verifyLookupPresent(buffer, topologyTxAccepted4)
          }
        }

        "element not present" should {
          "prune inclusive" in withBuffer() { buffer =>
            verifyLookupPresent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
              topologyTxAccepted4,
            )

            buffer.prune(offset(6))
            buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset4,
              bufferElements.drop(4),
            )
            verifyLookupAbsent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
            )
            verifyLookupPresent(buffer, topologyTxAccepted4)
          }
        }

        "element before series" should {
          "not prune" in withBuffer() { buffer =>
            verifyLookupPresent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
              topologyTxAccepted4,
            )

            buffer.prune(offset(1))
            buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset1,
              bufferElements.drop(1),
            )

            verifyLookupPresent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
              topologyTxAccepted4,
            )
          }
        }

        "element after series" should {
          "prune all" in withBuffer() { buffer =>
            verifyLookupPresent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
              topologyTxAccepted4,
            )

            buffer.prune(offset5)
            buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
              LastOffset,
              Vector.empty,
            )

            verifyLookupAbsent(
              buffer,
              txAccepted1,
              reassignmentAccepted2,
              txAccepted3,
              topologyTxAccepted4,
            )
          }
        }

        "one element in buffer" should {
          "prune all" in withBuffer(
            1,
            Vector(offset(1) -> reassignmentAccepted2.copy(offset = offset(1))),
          ) { buffer =>
            verifyLookupPresent(
              buffer,
              reassignmentAccepted2.copy(offset = offset(1)),
            )

            buffer.prune(offset(1))
            buffer.slice(firstOffset, offset(1), IdentityFilter) shouldBe LastBufferChunkSuffix(
              offset(1),
              Vector.empty,
            )

            verifyLookupAbsent(buffer, reassignmentAccepted2)
          }
        }
      }

      "flush" should {
        "remove all entries from the buffer" in withBuffer(3) { buffer =>
          verifyLookupPresent(
            buffer,
            reassignmentAccepted2,
            txAccepted3,
            topologyTxAccepted4,
          )

          buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
            bufferedStartExclusive = offset2,
            slice = Vector(entry3, entry4),
          )

          buffer.flush()

          buffer._bufferLog shouldBe empty
          buffer._lookupMap shouldBe empty
          buffer.slice(firstOffset, LastOffset, IdentityFilter) shouldBe LastBufferChunkSuffix(
            bufferedStartExclusive = LastOffset,
            slice = Vector.empty,
          )
          verifyLookupAbsent(
            buffer,
            reassignmentAccepted2,
            txAccepted3,
            topologyTxAccepted4,
          )
        }
      }

      "indexAfter" should {
        "yield the index gt the searched entry" in {
          InMemoryFanoutBuffer.indexAfter(InsertionPoint(3)) shouldBe 3
          InMemoryFanoutBuffer.indexAfter(Found(3)) shouldBe 4
        }
      }

      "filterAndChunkSlice" should {
        "return an Inclusive result with filter" in {
          val input = Vector(entry1, entry2, entry3, entry4).view

          InMemoryFanoutBuffer.filterAndChunkSlice[TransactionLogUpdate](
            sliceView = input,
            filter = tracedUpdate => Option(tracedUpdate).filterNot(_ == entry2._2),
            maxChunkSize = 3,
          ) shouldBe Vector(entry1, entry3, entry4)

          InMemoryFanoutBuffer.filterAndChunkSlice[TransactionLogUpdate](
            sliceView = View.empty,
            filter = tu => Some(tu),
            maxChunkSize = 3,
          ) shouldBe Vector.empty
        }
      }

      "lastFilteredChunk" should {
        val input = Vector(entry1, entry2, entry3, entry4)

        "return a LastBufferChunkSuffix with the last maxChunkSize-sized chunk from the slice with filter" in {
          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Option(tu).filterNot(_ == entry2._2),
            maxChunkSize = 1,
          ) shouldBe LastBufferChunkSuffix(entry3._1, Vector(entry4))

          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Option(tu).filterNot(_ == entry2._2),
            maxChunkSize = 2,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry3, entry4))

          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Option(tu).filterNot(_ == entry2._2),
            maxChunkSize = 3,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry3, entry4))

          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = tu => Some(tu), // No filter
            maxChunkSize = 4,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector(entry2, entry3, entry4))
        }

        "use the slice head as bufferedStartExclusive when filter yields an empty result slice" in {
          InMemoryFanoutBuffer.lastFilteredChunk[TransactionLogUpdate](
            bufferSlice = input,
            filter = _ => None,
            maxChunkSize = 2,
          ) shouldBe LastBufferChunkSuffix(entry1._1, Vector.empty)
        }
      }
    }

    def withBuffer(
        maxBufferSize: Int = 5,
        elems: immutable.Vector[(Offset, TransactionLogUpdate)] = bufferElements,
        maxFetchSize: Int = 10,
    )(test: InMemoryFanoutBuffer => Assertion): Assertion = {
      val buffer = new InMemoryFanoutBuffer(
        maxBufferSize,
        LedgerApiServerMetrics.ForTesting,
        maxBufferedChunkSize = maxFetchSize,
        loggerFactory = loggerFactory,
      )
      elems.foreach { case (_, event) => buffer.push(event) }
      test(buffer)
    }
  }

  private def offset(idx: Long): Offset = {
    val base = 1000000000L
    Offset.tryFromLong(base + idx)
  }

  private def succ(offset: Offset): Offset = offset.increment

  private def txAccepted(idx: Long, offset: Offset) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = s"tx-$idx",
      workflowId = s"workflow-$idx",
      effectiveAt = Time.Timestamp.Epoch,
      offset = offset,
      events = Vector.empty,
      completionStreamResponse = None,
      commandId = "",
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      recordTime = Time.Timestamp.Epoch,
      externalTransactionHash = None,
    )

  private def txRejected(idx: Long, offset: Offset) =
    TransactionLogUpdate.TransactionRejected(
      offset = offset,
      completionStreamResponse = CompletionStreamResponse.defaultInstance.withCompletion(
        Completion.defaultInstance.copy(
          actAs = Seq(s"submitter-$idx")
        )
      ),
    )

  private def reassignmentAccepted(idx: Long, offset: Offset) =
    TransactionLogUpdate.ReassignmentAccepted(
      updateId = s"reassignment-$idx",
      workflowId = s"workflow-$idx",
      offset = offset,
      completionStreamResponse = None,
      commandId = "",
      recordTime = Time.Timestamp.Epoch,
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = ReassignmentTag.Source(someSynchronizerId),
        targetSynchronizer = ReassignmentTag.Target(someSynchronizerId),
        submitter = None,
        reassignmentId = ReassignmentId.tryCreate("0001"),
        isReassigningParticipant = false,
      ),
      reassignment = null,
    )

  private def topologyTxAccepted(idx: Long, offset: Offset) =
    TransactionLogUpdate.TopologyTransactionEffective(
      updateId = s"topology-tx-$idx",
      offset = offset,
      effectiveTime = Time.Timestamp.Epoch,
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      events = Vector.empty,
    )

  private def verifyLookupPresent(
      buffer: InMemoryFanoutBuffer,
      txs: TransactionLogUpdate*
  ): Assertion =
    txs.foldLeft(succeed) {
      case (Succeeded, tx) =>
        buffer.lookup(
          LookupKey.UpdateId(Ref.TransactionId.assertFromString(getUpdateId(tx)))
        ) shouldBe Some(tx)
        buffer.lookup(LookupKey.Offset(tx.offset)) shouldBe Some(tx)
      case (failed, _) => failed
    }

  private def verifyLookupAbsent(
      buffer: InMemoryFanoutBuffer,
      txs: TransactionLogUpdate*
  ): Assertion =
    txs.foldLeft(succeed) {
      case (Succeeded, tx) =>
        buffer.lookup(
          LookupKey.UpdateId(Ref.TransactionId.assertFromString(getUpdateId(tx)))
        ) shouldBe None
        buffer.lookup(LookupKey.Offset(tx.offset)) shouldBe None
      case (failed, _) => failed
    }

  private def getUpdateId(tx: TransactionLogUpdate): String = tx match {
    case txAccepted: TransactionLogUpdate.TransactionAccepted => txAccepted.updateId
    case _: TransactionLogUpdate.TransactionRejected =>
      throw new RuntimeException("did not expect a TransactionRejected")
    case reassignment: TransactionLogUpdate.ReassignmentAccepted => reassignment.updateId
    case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
      topologyTransaction.updateId
  }

}
