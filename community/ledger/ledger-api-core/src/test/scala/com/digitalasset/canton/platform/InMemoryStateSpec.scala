// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.AchsLastPointers
import com.digitalasset.canton.platform.store.backend.{LedgerEnd, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.cache.{
  AchsStateCache,
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
  OffsetCheckpointCache,
}
import com.digitalasset.canton.platform.store.interning.{
  StringInterningView,
  UpdatingStringInterningView,
}
import com.digitalasset.daml.lf.data.Ref
import org.mockito.{InOrder, Mockito, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class InMemoryStateSpec extends AsyncFlatSpec with MockitoSugar with Matchers with TestEssentials {
  private val className = classOf[InMemoryState].getSimpleName

  s"$className.initialized" should "return false if not initialized" in withTestFixture {
    case (inMemoryState, _, _, _, _, _, _, _, _, _, _) =>
      inMemoryState.initialized shouldBe false
  }

  s"$className.initializeTo" should "initialize the state" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          achsStateCache,
          contractStateCaches,
          inMemoryFanoutBuffer,
          stringInterningView,
          dispatcherState,
          updateStringInterningView,
          transactionSubmissionTracker,
          reassignmentSubmissionTracker,
          inOrder,
        ) =>
      val initOffset = Offset.tryFromLong(12345678L)
      val initEventSequentialId = 1337L
      val initStringInterningId = 17
      val initPublicationTime = CantonTimestamp.now()

      val initLedgerEnd = LedgerEnd(
        initOffset,
        initEventSequentialId,
        initStringInterningId,
        initPublicationTime,
        Map.empty,
      ) // Fake map
      val initAchsState = ParameterStorageBackend.AchsState(
        validAt = 0,
        AchsLastPointers(lastRemoved = 0, lastPopulated = 0),
      )

      when(updateStringInterningView(stringInterningView, initLedgerEnd))
        .thenReturn(Future.unit)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
      when(dispatcherState.isRunning).thenReturn(true)
      when(mutableLedgerEndCache.apply()).thenReturn(None)
      when(dispatcherState.getDispatcher).thenReturn(
        Dispatcher(
          name = "",
          firstIndex = Offset.firstOffset,
          headAtInitialization = None,
        )
      )

      for {
        // INITIALIZED THE STATE
        _ <- inMemoryState.initializeTo(
          Some(initLedgerEnd),
          initAchsState,
        )

        _ = {
          // ASSERT STATE INITIALIZED

          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(contractStateCaches).reset(Some(initLedgerEnd))
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder
            .verify(mutableLedgerEndCache)
            .set(Some(initLedgerEnd))
          inOrder.verify(achsStateCache).set(initAchsState)
          inOrder.verify(transactionSubmissionTracker).close()
          inOrder.verify(reassignmentSubmissionTracker).close()
          inOrder
            .verify(dispatcherState)
            .startDispatcher(Some(initLedgerEnd.lastOffset))

          inMemoryState.cachesUpdatedUpto.get() shouldBe Some(initOffset)
          inMemoryState.initialized shouldBe true
        }

        reInitOffset = Offset.tryFromLong(12345678L)
        reInitEventSequentialId = 9999L
        reInitStringInterningId = 50
        reInitPublicationTime = CantonTimestamp.now()
        reInitLedgerEnd = LedgerEnd(
          reInitOffset,
          reInitEventSequentialId,
          reInitStringInterningId,
          reInitPublicationTime,
          Map.empty,
        )

        // RESET MOCKS
        _ = {
          reset(
            mutableLedgerEndCache,
            achsStateCache,
            contractStateCaches,
            inMemoryFanoutBuffer,
            updateStringInterningView,
          )
          when(achsStateCache.get()).thenReturn(initAchsState)
          when(updateStringInterningView(stringInterningView, reInitLedgerEnd))
            .thenReturn(
              Future.unit
            )

          when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
          when(mutableLedgerEndCache.apply()).thenReturn(Some(initLedgerEnd))
          when(dispatcherState.getDispatcher).thenReturn(
            Dispatcher(
              name = "",
              firstIndex = Offset.firstOffset,
              headAtInitialization = Some(initOffset),
            )
          )
        }

        // RE-INITIALIZE THE STATE
        _ <- inMemoryState.initializeTo(
          Some(reInitLedgerEnd),
          initAchsState,
        )

        // ASSERT STATE RE-INITIALIZED
        _ = {
          inOrder.verify(dispatcherState).stopDispatcher()

          when(dispatcherState.isRunning).thenReturn(false)
          inMemoryState.initialized shouldBe false
          inOrder.verify(contractStateCaches).reset(Some(reInitLedgerEnd))
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder
            .verify(mutableLedgerEndCache)
            .set(Some(reInitLedgerEnd))
          inOrder.verify(achsStateCache).set(initAchsState)
          inOrder.verify(dispatcherState).startDispatcher(Some(reInitOffset))

          inMemoryState.cachesUpdatedUpto.get() shouldBe Some(reInitOffset)

          when(dispatcherState.isRunning).thenReturn(true)
          inMemoryState.initialized shouldBe true
        }

        // RE-INITIALIZE THE SAME STATE
        _ = when(mutableLedgerEndCache.apply()).thenReturn(Some(reInitLedgerEnd))
        _ <- inMemoryState.initializeTo(
          Some(reInitLedgerEnd),
          initAchsState,
        )

        // ASSERT STATE RE-INITIALIZED
        _ = {
          verify(dispatcherState, times(2)).stopDispatcher()
          verify(dispatcherState, times(2)).startDispatcher(Some(reInitOffset))
          verify(contractStateCaches, times(1)).reset(Some(reInitLedgerEnd))
          inMemoryState.initialized shouldBe true
          inMemoryState.cachesUpdatedUpto.get() shouldBe Some(reInitOffset)
        }
      } yield succeed
  }

  s"$className.initializeTo" should "reset the state when dispatcher is not running" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          achsStateCache,
          contractStateCaches,
          inMemoryFanoutBuffer,
          _,
          dispatcherState,
          _,
          transactionSubmissionTracker,
          reassignmentSubmissionTracker,
          inOrder,
        ) =>
      val initOffset = Offset.tryFromLong(42L)
      val initLedgerEnd = LedgerEnd(
        lastOffset = initOffset,
        lastEventSeqId = 7L,
        lastStringInterningId = 3,
        lastPublicationTime = CantonTimestamp.now(),
        synchronizerIndices = Map.empty,
      )
      val achsState = ParameterStorageBackend.AchsState(
        validAt = 1,
        lastPointers = AchsLastPointers(lastRemoved = 2, lastPopulated = 3),
      )

      when(dispatcherState.isRunning).thenReturn(false)
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)

      for {
        _ <- inMemoryState.initializeTo(Some(initLedgerEnd), achsState)
        _ = {
          inOrder.verify(dispatcherState).stopDispatcher()
          inOrder.verify(contractStateCaches).reset(Some(initLedgerEnd))
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(Some(initLedgerEnd))
          inOrder.verify(achsStateCache).set(achsState)
          inOrder.verify(transactionSubmissionTracker).close()
          inOrder.verify(reassignmentSubmissionTracker).close()
          inOrder.verify(dispatcherState).startDispatcher(Some(initOffset))

          inMemoryState.cachesUpdatedUpto.get() shouldBe Some(initOffset)
          inMemoryState.initialized shouldBe false
        }
      } yield succeed
  }

  // since cachesUpdatedUpto can be None to signify invalid caches, we need to ensure that we reset memory state when initializing to None
  "InMemoryState.initializeTo(None)" should "should reset the in-memory state" in withTestFixture {
    case (
          inMemoryState,
          mutableLedgerEndCache,
          achsStateCache,
          contractStateCaches,
          inMemoryFanoutBuffer,
          _,
          dispatcherState,
          _,
          _,
          _,
          inOrder,
        ) =>
      when(dispatcherState.stopDispatcher()).thenReturn(Future.unit)
      when(dispatcherState.isRunning).thenReturn(true)
      when(mutableLedgerEndCache.apply()).thenReturn(None)
      when(dispatcherState.getDispatcher).thenReturn(
        Dispatcher(
          name = "",
          firstIndex = Offset.firstOffset,
          headAtInitialization = None,
        )
      )

      inMemoryState.ledgerEndCache() shouldBe None
      dispatcherState.getDispatcher.getHead() shouldBe None
      inMemoryState.cachesUpdatedUpto.get() shouldBe None
      val achsState = ParameterStorageBackend.AchsState(
        validAt = 0,
        AchsLastPointers(lastRemoved = 0, lastPopulated = 0),
      )

      for {
        _ <- inMemoryState.initializeTo(
          None,
          achsState,
        )

        _ = {
          verify(dispatcherState).stopDispatcher()
          verify(dispatcherState).startDispatcher(None)
          inOrder.verify(contractStateCaches).reset(None)
          inOrder.verify(inMemoryFanoutBuffer).flush()
          inOrder.verify(mutableLedgerEndCache).set(None)
          inOrder.verify(achsStateCache).set(achsState)
          inMemoryState.cachesUpdatedUpto.get() shouldBe None
        }
      } yield succeed
  }

  private def withTestFixture(
      test: (
          InMemoryState,
          MutableLedgerEndCache,
          AchsStateCache,
          ContractStateCaches,
          InMemoryFanoutBuffer,
          StringInterningView,
          DispatcherState,
          (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
          SubmissionTracker,
          SubmissionTracker,
          InOrder,
      ) => Future[Assertion]
  ): Future[Assertion] = {
    val mutableLedgerEndCache = mock[MutableLedgerEndCache]
    val achsStateCache = mock[AchsStateCache]
    when(achsStateCache.get()).thenReturn(
      ParameterStorageBackend.AchsState(
        validAt = 0,
        AchsLastPointers(lastRemoved = 0, lastPopulated = 0),
      )
    )
    val contractStateCaches = mock[ContractStateCaches]
    val offsetCheckpointCache = mock[OffsetCheckpointCache]
    val inMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView = mock[StringInterningView]
    val dispatcherState = mock[DispatcherState]
    val updateStringInterningView =
      mock[(UpdatingStringInterningView, LedgerEnd) => Future[Unit]]
    val transactionSubmissionTracker = mock[SubmissionTracker]
    val reassignmentSubmissionTracker = mock[SubmissionTracker]
    val partyAllocationTracker = mock[PartyAllocation.Tracker]
    val commandProgressTracker = CommandProgressTracker.NoOp

    // Mocks should be called in the asserted order
    val inOrderMockCalls = Mockito.inOrder(
      mutableLedgerEndCache,
      achsStateCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
      transactionSubmissionTracker,
      reassignmentSubmissionTracker,
    )

    val inMemoryState = new InMemoryState(
      participantId = Ref.ParticipantId.assertFromString("participant1"),
      ledgerEndCache = mutableLedgerEndCache,
      achsStateCache = achsStateCache,
      contractStateCaches = contractStateCaches,
      offsetCheckpointCache = offsetCheckpointCache,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      transactionSubmissionTracker = transactionSubmissionTracker,
      reassignmentSubmissionTracker = reassignmentSubmissionTracker,
      partyAllocationTracker = partyAllocationTracker,
      commandProgressTracker = commandProgressTracker,
      loggerFactory = loggerFactory,
    )

    test(
      inMemoryState,
      mutableLedgerEndCache,
      achsStateCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      updateStringInterningView,
      transactionSubmissionTracker,
      reassignmentSubmissionTracker,
      inOrderMockCalls,
    )
  }
}
