// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.ReassignmentInfo
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.OffsetGen.offset
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.{
  BackwardSlice,
  ContinueFromImfo,
  ContinueFromPersistence,
  NoContinue,
  SliceWithContinuationOffset,
  UnorderedException,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.{ReassignmentId, TestUpdateId, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.data.Time
import com.google.protobuf.ByteString
import org.scalatest.Succeeded
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.util.concurrent.Executors
import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.immutable
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
            buffer.sliceForward(
              firstOffset,
              LastOffset,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = offset2.decrement,
              slice = Vector(entry2, entry3, entry4),
            )

            // Assert that all the entries are visible by lookup
            verifyLookupPresent(buffer, reassignmentAccepted2, txAccepted3, topologyTxAccepted4)

            buffer.push(txAccepted5)
            // Assert data structure sizes respect their limits after pushing a new element
            buffer._bufferLog.size shouldBe 3
            buffer._lookupMap.size shouldBe 3

            buffer.sliceForward(
              firstOffset,
              offset5,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = offset3.decrement,
              slice = Vector(entry3, entry4, offset5 -> txAccepted5),
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
            buffer.sliceForward(
              firstOffset,
              offset5,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = Some(offset5),
              slice = Vector.empty,
            )
            buffer._bufferLog shouldBe empty
          }
        }

        "maxBufferSize is -1" should {
          "not enqueue the update" in withBuffer(-1) { buffer =>
            buffer.push(txAccepted5)
            buffer.sliceForward(
              firstOffset,
              offset5,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = Some(offset5),
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
          buffer.sliceForward(
            offset2,
            offset4,
            Some(_).filterNot(_ == entry3._2),
          ) shouldBe SliceWithContinuationOffset(
            checkPersistenceToIncl = None,
            slice = Vector(entry2, entry4),
          )
        }

        "called with startInclusive gteq than the buffer start" should {
          "return a slice without checkPersistenceToIncl" in withBuffer() { buffer =>
            buffer.sliceForward(
              offset1,
              succ(offset3),
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = None,
              slice = Vector(entry1, entry2, entry3),
            )
            buffer.sliceForward(
              offset2,
              succ(offset3),
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = None,
              slice = Vector(entry2, entry3),
            )
            buffer.sliceForward(
              offset2,
              offset4,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = None,
              slice = Vector(entry2, entry3, entry4),
            )
            buffer.sliceForward(
              succ(offset1),
              offset4,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = None,
              slice = Vector(entry2, entry3, entry4),
            )
          }

          "return a chunk without checkPersistenceToIncl if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.sliceForward(
              offset2,
              offset4,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = None,
              slice = Vector(entry2, entry3),
            )
          }
        }

        "called with endInclusive lteq startInclusive" should {
          "return an empty slice without checkPersistenceToIncl if startInclusive is greater than buffer start and endInclusive" in withBuffer() {
            buffer =>
              buffer.sliceForward(
                offset2,
                offset1,
                IdentityFilter,
              ) shouldBe SliceWithContinuationOffset(
                checkPersistenceToIncl = None,
                slice = Vector.empty,
              )
          }
          "return a slice without checkPersistenceToIncl if startInclusive is greater than buffer start and equal to endInclusive" in withBuffer() {
            buffer =>
              buffer.sliceForward(
                offset2,
                offset2,
                IdentityFilter,
              ) shouldBe SliceWithContinuationOffset(
                checkPersistenceToIncl = None,
                slice = Vector(entry2),
              )
          }
          "return an empty slice with checkPersistenceToIncl if startExclusive is before buffer start" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceForward(
              offset1,
              offset1,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = Some(offset1),
              slice = Vector.empty,
            )
          }
          "return an empty slice without continuation if begin>end and begin is before buffer start" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceForward(
              offset2,
              offset1,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = None,
              slice = Vector.empty,
            )
          }
        }
        "called with startInclusive before the buffer start" should {
          "return a SliceWithContinuationOffset with checkPersistenceToIncl" in withBuffer() {
            buffer =>
              buffer.sliceForward(
                firstOffset,
                offset3,
                IdentityFilter,
              ) shouldBe SliceWithContinuationOffset(
                checkPersistenceToIncl = offset1.decrement,
                slice = Vector(entry1, entry2, entry3),
              )
              buffer.sliceForward(
                firstOffset,
                succ(offset3),
                IdentityFilter,
              ) shouldBe SliceWithContinuationOffset(
                checkPersistenceToIncl = offset1.decrement,
                slice = Vector(entry1, entry2, entry3),
              )
          }

          "return a slice with checkPersistenceToIncl if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.sliceForward(
              firstOffset,
              offset4,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = offset1.decrement,
              slice = Vector(entry1, entry2),
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
                      buffer.sliceForward(
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

      "sliceBackwards" when {
        "called with startInclusive gteq than the buffer start" should {
          "return an Backward slice with NoContinue" in withBuffer() { buffer =>
            buffer.sliceBackwards(
              offset1,
              succ(offset3),
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector(entry3, entry2, entry1),
              NoContinue,
            )
            buffer.sliceBackwards(
              offset2,
              succ(offset3),
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector(entry3, entry2),
              NoContinue,
            )
            buffer.sliceBackwards(offset2, offset4, IdentityFilter) shouldBe BackwardSlice(
              Vector(entry4, entry3, entry2),
              NoContinue,
            )
            buffer.sliceBackwards(
              succ(offset1),
              offset4,
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector(entry4, entry3, entry2),
              NoContinue,
            )
          }

          "return a result with ContinueFromImfo if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.sliceBackwards(offset2, offset4, IdentityFilter) shouldBe BackwardSlice(
              Vector(entry4, entry3),
              ContinueFromImfo(entry2._1),
            )
          }
        }

        "called with endInclusive lteq startInclusive" should {
          "return an empty slice with NoContinue if startInclusive is greater than buffer start and endInclusive" in withBuffer() {
            buffer =>
              buffer.sliceBackwards(offset2, offset1, IdentityFilter) shouldBe BackwardSlice(
                Vector.empty,
                NoContinue,
              )
          }
          "return an slice with NoContinue if startInclusive is greater than buffer start and equal to endInclusive" in withBuffer() {
            buffer =>
              buffer.sliceBackwards(offset2, offset2, IdentityFilter) shouldBe BackwardSlice(
                Vector(entry2),
                NoContinue,
              )
          }
          "return an empty slice with ContinueFromPersistence if startExclusive is before buffer start" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceBackwards(offset1, offset1, IdentityFilter) shouldBe BackwardSlice(
              Vector.empty,
              ContinueFromPersistence(offset1),
            )
          }

          "return an empty slice with NoContinue if startExclusive is before buffer start and start < end" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceBackwards(
              offset1,
              offset1.decrement.value,
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector.empty,
              NoContinue,
            )
          }
        }
        "called with startInclusive before the buffer start" should {
          "return a slie with ContinueFromPersistence" in withBuffer() { buffer =>
            buffer.sliceBackwards(
              firstOffset,
              offset3,
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector(entry3, entry2, entry1),
              ContinueFromPersistence(entry1._1.decrement.value),
            )
            buffer.sliceBackwards(
              firstOffset,
              succ(offset3),
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector(entry3, entry2, entry1),
              ContinueFromPersistence(entry1._1.decrement.value),
            )
          }

          "return slice with Imfo continuation if resulting slice is bigger than maxFetchSize" in withBuffer(
            maxFetchSize = 2
          ) { buffer =>
            buffer.sliceBackwards(
              firstOffset,
              offset4,
              IdentityFilter,
            ) shouldBe BackwardSlice(
              Vector(entry4, entry3),
              ContinueFromImfo(entry2._1),
            )
          }
        }
      }

      "sliceForwardWitLimit" when {
        "range start is gte buffer start" should {
          "return a slice of size of limit if the buffer contains more matching elemens in range" in withBuffer() {
            buffer =>
              buffer.sliceForwardWithLimit(
                offset2,
                offset4,
                IdentityFilter,
                2,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry2, entry3),
                checkPersistenceToIncl = None,
              )
          }

          "return a slice shorter than limit if there is fewer matching elments in bufer" in withBuffer() {
            buffer =>
              buffer.sliceForwardWithLimit(
                offset2,
                offset4,
                IdentityFilter,
                100,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry2, entry3, entry4),
                checkPersistenceToIncl = None,
              )

              buffer.sliceForwardWithLimit(
                offset2,
                offset4,
                tlu => Option.when(tlu.offset == entry2._1)(tlu),
                2,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry2),
                checkPersistenceToIncl = None,
              )
          }

          "return an empty slice if start > end" in withBuffer() { buffer =>
            buffer.sliceForwardWithLimit(
              offset4,
              offset2,
              IdentityFilter,
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector.empty,
              checkPersistenceToIncl = None,
            )
          }

          "return all elements from buffer if the boundary is exactly at buffer end" in withBuffer() {
            buffer =>
              buffer.sliceForwardWithLimit(
                offset1,
                offset4,
                IdentityFilter,
                100,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry1, entry2, entry3, entry4),
                checkPersistenceToIncl = None,
              )
          }
        }

        "range start is before buffer start" should {
          "return a slice with checkPersistenceToIncl if the buffer contains enough elemens in range" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceForwardWithLimit(
              offset2,
              offset4,
              IdentityFilter,
              2,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector(entry3, entry4),
              checkPersistenceToIncl = offset3.decrement,
            )
          }

          "return an empty slice without checkPersistenceToIncl if start > end" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceForwardWithLimit(
              offset2,
              offset1,
              IdentityFilter,
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector.empty,
              checkPersistenceToIncl = None,
            )
          }

          "return a slice with checkPersistenceToIncl if the buffer does not contain enough elemens in range" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceForwardWithLimit(
              offset1,
              offset4,
              IdentityFilter,
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector(entry3, entry4),
              checkPersistenceToIncl = offset3.decrement,
            )
          }

          "return an empty slice with checkPersistenceToIncl if filter does not match any elements in buffer" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceForwardWithLimit(
              offset1,
              offset4,
              tlu => Option.when(tlu.offset == entry2._1)(tlu),
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector.empty,
              checkPersistenceToIncl = offset3.decrement,
            )
          }
        }
      }

      "sliceBackwardWithLimit" when {
        "range start is >= buffer start" should {
          "return a slice of size of limit if the buffer contains more matching elemens in range" in withBuffer() {
            buffer =>
              buffer.sliceBackwardsWithLimit(
                offset2,
                offset4,
                IdentityFilter,
                2,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry4, entry3),
                checkPersistenceToIncl = None,
              )
          }

          "return a slice shorter than limit if there is fewer matching elments in bufer" in withBuffer() {
            buffer =>
              buffer.sliceBackwardsWithLimit(
                offset2,
                offset4,
                IdentityFilter,
                100,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry4, entry3, entry2),
                checkPersistenceToIncl = None,
              )

              buffer.sliceBackwardsWithLimit(
                offset2,
                offset4,
                tlu => Option.when(tlu.offset == entry2._1)(tlu),
                2,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry2),
                checkPersistenceToIncl = None,
              )
          }

          "return an empty slice if start > end" in withBuffer() { buffer =>
            buffer.sliceBackwardsWithLimit(
              offset4,
              offset2,
              IdentityFilter,
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector.empty,
              checkPersistenceToIncl = None,
            )
          }

          "return all elements from buffer if the boundary is exactly at buffer end" in withBuffer() {
            buffer =>
              buffer.sliceBackwardsWithLimit(
                offset1,
                offset4,
                IdentityFilter,
                100,
              ) shouldBe SliceWithContinuationOffset(
                slice = Vector(entry4, entry3, entry2, entry1),
                checkPersistenceToIncl = None,
              )
          }
        }

        "range start is before buffer start" should {
          "return a slice without checkPersistenceToIncl if the buffer contains enough elemens in range" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceBackwardsWithLimit(
              offset2,
              offset4,
              IdentityFilter,
              2,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector(entry4, entry3),
              checkPersistenceToIncl = None,
            )
          }

          "return a slice with checkPersistenceToIncl if the buffer does not contain enough elemens in range" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceBackwardsWithLimit(
              offset1,
              offset4,
              IdentityFilter,
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector(entry4, entry3),
              checkPersistenceToIncl = offset3.decrement,
            )
          }

          "return a slice with checkPersistenceToIncl if the buffer does not contain enough matching elemens in range" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceBackwardsWithLimit(
              offset1,
              offset4,
              tlu => Option.when(tlu.offset == entry3._1)(tlu),
              2,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector(entry3),
              checkPersistenceToIncl = offset3.decrement,
            )
          }

          "return an empty slice with checkPersistenceToIncl if end range is before buffer start" in withBuffer(
            maxBufferSize = 2
          ) { buffer =>
            buffer.sliceBackwardsWithLimit(
              offset1,
              offset2,
              IdentityFilter,
              100,
            ) shouldBe SliceWithContinuationOffset(
              slice = Vector.empty,
              checkPersistenceToIncl = Some(offset2),
            )
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

            buffer.sliceForward(
              firstOffset,
              LastOffset,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = offset4.decrement,
              slice = bufferElements.drop(3),
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
            buffer.sliceForward(
              firstOffset,
              LastOffset,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = offset4.decrement,
              slice = bufferElements.drop(3),
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
            buffer.sliceForward(
              firstOffset,
              LastOffset,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = offset1.decrement,
              slice = bufferElements,
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
            buffer.sliceForward(
              firstOffset,
              LastOffset,
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = Some(LastOffset),
              slice = Vector.empty,
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
            buffer.sliceForward(
              firstOffset,
              offset(1),
              IdentityFilter,
            ) shouldBe SliceWithContinuationOffset(
              checkPersistenceToIncl = Some(offset(1)),
              slice = Vector.empty,
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

          buffer.sliceForward(
            firstOffset,
            LastOffset,
            IdentityFilter,
          ) shouldBe SliceWithContinuationOffset(
            checkPersistenceToIncl = offset2.decrement,
            slice = Vector(entry2, entry3, entry4),
          )

          buffer.flush()

          buffer._bufferLog shouldBe empty
          buffer._lookupMap shouldBe empty
          buffer.sliceForward(
            firstOffset,
            LastOffset,
            IdentityFilter,
          ) shouldBe SliceWithContinuationOffset(
            checkPersistenceToIncl = Some(LastOffset),
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

      "hash-based lookup" should {
        "find a transaction by its hash" in {
          val hash = mkHash("hash-1")
          val txWithHash = txAccepted(10L, offset(20L), Some(hash))
          withBuffer(10, Vector(offset(20L) -> txWithHash)) { buffer =>
            buffer.lookup(LookupKey.ByHash(hash.unwrap)) shouldBe Some(txWithHash)
          }
        }

        "return None for a hash that is not in the buffer" in {
          val txWithHash = txAccepted(10L, offset(20L), Some(mkHash("hash-2")))
          withBuffer(10, Vector(offset(20L) -> txWithHash)) { buffer =>
            buffer.lookup(
              LookupKey.ByHash(mkHash("nonexistent").unwrap)
            ) shouldBe None
          }
        }

        "evict hash from hash map when entry is dropped" in {
          val hash = mkHash("hash-3")
          val txWithHash = txAccepted(10L, offset(20L), Some(hash))
          withBuffer(
            2,
            Vector(
              offset(20L) -> txWithHash,
              offset(21L) -> txAccepted(11L, offset(21L)),
              offset(22L) -> txAccepted(12L, offset(22L)),
            ),
          ) { buffer =>
            buffer.lookup(LookupKey.ByHash(hash.unwrap)) shouldBe None
          }
        }

        "clear hash map on flush" in {
          val hash = mkHash("hash-4")
          val txWithHash = txAccepted(10L, offset(20L), Some(hash))
          withBuffer(10, Vector(offset(20L) -> txWithHash)) { buffer =>
            buffer.flush()
            buffer.lookup(LookupKey.ByHash(hash.unwrap)) shouldBe None
            buffer._acceptedByHash shouldBe empty
          }
        }

        "not add entries without hash to hash map" in {
          withBuffer(10, Vector(offset(20L) -> txAccepted(10L, offset(20L)))) { buffer =>
            buffer._acceptedByHash shouldBe empty
          }
        }
      }

      "rejection indexing" should {
        "index a rejection with hash in _rejectionsByHash" in {
          val hash = mkHash("rej-hash-1")
          val rejected = txRejected(5L, offset(20L), Some(hash))
          withBuffer(10, Vector(offset(20L) -> rejected)) { buffer =>
            buffer._rejectionsByHash(hash.unwrap) shouldBe Vector(rejected)
          }
        }

        "not index a rejection without hash" in {
          val rejected = txRejected(5L, offset(20L))
          withBuffer(10, Vector(offset(20L) -> rejected)) { buffer =>
            buffer._rejectionsByHash shouldBe empty
          }
        }

        "store multiple rejections for the same hash newest-first" in {
          val hash = mkHash("rej-hash-2")
          val rejected1 = txRejected(5L, offset(20L), Some(hash))
          val rejected2 = txRejected(6L, offset(21L), Some(hash))
          val rejected3 = txRejected(7L, offset(22L), Some(hash))
          withBuffer(
            10,
            Vector(
              offset(20L) -> rejected1,
              offset(21L) -> rejected2,
              offset(22L) -> rejected3,
            ),
          ) { buffer =>
            buffer._rejectionsByHash(hash.unwrap) shouldBe Vector(
              rejected3,
              rejected2,
              rejected1,
            )
          }
        }

        "evict oldest rejection from vector on drop" in {
          val hash = mkHash("rej-hash-3")
          val rejected1 = txRejected(5L, offset(20L), Some(hash))
          val rejected2 = txRejected(6L, offset(21L), Some(hash))
          withBuffer(
            2,
            Vector(
              offset(20L) -> rejected1,
              offset(21L) -> rejected2,
              offset(22L) -> txAccepted(7L, offset(22L)),
            ),
          ) { buffer =>
            // rejected1 should have been evicted (buffer size 2; 3 entries pushed)
            buffer._rejectionsByHash(hash.unwrap) shouldBe Vector(rejected2)
          }
        }

        "remove hash key when last rejection is evicted" in {
          val hash = mkHash("rej-hash-4")
          val rejected1 = txRejected(5L, offset(20L), Some(hash))
          withBuffer(
            2,
            Vector(
              offset(20L) -> rejected1,
              offset(21L) -> txAccepted(6L, offset(21L)),
              offset(22L) -> txAccepted(7L, offset(22L)),
            ),
          ) { buffer =>
            buffer._rejectionsByHash should not contain key(hash.unwrap)
          }
        }

        "lookupCompletionsByHash returns both accepted and rejections" in {
          val hash = mkHash("rej-hash-5")
          val accepted = txAccepted(5L, offset(20L), Some(hash))
          val rejected = txRejected(6L, offset(21L), Some(hash))
          withBuffer(
            10,
            Vector(
              offset(20L) -> accepted,
              offset(21L) -> rejected,
            ),
          ) { buffer =>
            val (acc, rejs) = buffer.lookupCompletionsByHash(hash.unwrap)
            acc shouldBe Some(accepted)
            rejs shouldBe Vector(rejected)
          }
        }

        "lookupCompletionsByHash returns empty for unknown hash" in {
          withBuffer(
            10,
            Vector(offset(20L) -> txAccepted(5L, offset(20L), Some(mkHash("known")))),
          ) { buffer =>
            val (acc, rejs) = buffer.lookupCompletionsByHash(mkHash("unknown").unwrap)
            acc shouldBe None
            rejs shouldBe empty
          }
        }

        "flush clears _rejectionsByHash" in {
          val hash = mkHash("rej-hash-6")
          val rejected = txRejected(5L, offset(20L), Some(hash))
          withBuffer(10, Vector(offset(20L) -> rejected)) { buffer =>
            buffer.flush()
            buffer._rejectionsByHash shouldBe empty
          }
        }
      }

      "indexAfter" should {
        "yield the index gt the searched entry" in {
          InMemoryFanoutBuffer.indexAfter(InsertionPoint(3)) shouldBe 3
          InMemoryFanoutBuffer.indexAfter(Found(3)) shouldBe 4
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

  private def succ(offset: Offset): Offset = offset.increment

  private def mkHash(input: String): Hash =
    Hash.digest(
      HashPurpose.PreparedSubmission,
      ByteString.copyFromUtf8(input),
      HashAlgorithm.Sha256,
    )

  private def txAccepted(idx: Long, offset: Offset, hash: Option[Hash] = None) =
    TransactionLogUpdate.TransactionAccepted(
      updateId = TestUpdateId(s"tx-$idx").toHexString,
      workflowId = s"workflow-$idx",
      effectiveAt = Time.Timestamp.Epoch,
      offset = offset,
      events = Vector.empty,
      completionStreamResponseO = None,
      commandId = "",
      synchronizerId = someSynchronizerId.toProtoPrimitive,
      recordTime = Time.Timestamp.Epoch,
      transactionHash = hash,
    )

  private def txRejected(idx: Long, offset: Offset, hash: Option[Hash] = None) =
    TransactionLogUpdate.TransactionRejected(
      offset = offset,
      completionStreamResponse = CompletionStreamResponse.defaultInstance.withCompletion(
        Completion.defaultInstance.copy(
          actAs = Seq(s"submitter-$idx"),
          transactionHash = hash.map(_.unwrap),
        )
      ),
    )

  private def reassignmentAccepted(idx: Long, offset: Offset) =
    TransactionLogUpdate.ReassignmentAccepted(
      updateId = TestUpdateId(s"reassignment-$idx").toHexString,
      workflowId = s"workflow-$idx",
      offset = offset,
      completionStreamResponseO = None,
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
      synchronizerId = someSynchronizerId.toProtoPrimitive,
    )

  private def topologyTxAccepted(idx: Long, offset: Offset) =
    TransactionLogUpdate.TopologyTransactionEffective(
      updateId = TestUpdateId(s"topology-tx-$idx").toHexString,
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
          LookupKey.ByUpdateId(getUpdateId(tx))
        ) shouldBe Some(tx)
        buffer.lookup(LookupKey.ByOffset(tx.offset)) shouldBe Some(tx)
      case (failed, _) => failed
    }

  private def verifyLookupAbsent(
      buffer: InMemoryFanoutBuffer,
      txs: TransactionLogUpdate*
  ): Assertion =
    txs.foldLeft(succeed) {
      case (Succeeded, tx) =>
        buffer.lookup(
          LookupKey.ByUpdateId(getUpdateId(tx))
        ) shouldBe None
        buffer.lookup(LookupKey.ByOffset(tx.offset)) shouldBe None
      case (failed, _) => failed
    }

  private def getUpdateId(tx: TransactionLogUpdate): UpdateId = {
    val updateStr = tx match {
      case txAccepted: TransactionLogUpdate.TransactionAccepted => txAccepted.updateId
      case _: TransactionLogUpdate.TransactionRejected =>
        throw new RuntimeException("did not expect a TransactionRejected")
      case reassignment: TransactionLogUpdate.ReassignmentAccepted => reassignment.updateId
      case topologyTransaction: TransactionLogUpdate.TopologyTransactionEffective =>
        topologyTransaction.updateId
      case _: TransactionLogUpdate.ReceivedAcsCommitment =>
        throw new RuntimeException("did not expect a ReceivedAcsCommitment")
    }
    UpdateId.fromLedgerString(updateStr).valueOrFail("invalid update id")
  }

}
