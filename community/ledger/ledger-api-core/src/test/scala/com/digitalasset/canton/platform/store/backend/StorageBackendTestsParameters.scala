// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.SqlStringInterpolation
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{RepairIndex, SynchronizerIndex}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{ACHSState, LedgerEnd}
import com.digitalasset.canton.{HasExecutionContext, RepairCounter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsParameters
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend Parameters"

  import StorageBackendTestValues.*

  it should "store and retrieve ledger end and synchronizer indexes correctly" in {
    val someOffset = offset(1)
    val someSequencerTime = CantonTimestamp.now().plusSeconds(10)
    val someSynchronizerIndex = SynchronizerIndex.forRepairUpdate(
      RepairIndex(
        timestamp = someSequencerTime,
        counter = RepairCounter(20),
      )
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.ledgerEnd) shouldBe LedgerEnd.beforeBegin
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    ) shouldBe None
    val someSynchronizerIdInterned =
      backend.stringInterningSupport.synchronizerId.internalize(
        StorageBackendTestValues.someSynchronizerId
      )
    executeSql(connection =>
      ingest(
        Vector(
          DbDto.StringInterningDto(
            someSynchronizerIdInterned,
            "d|" + StorageBackendTestValues.someSynchronizerId.toProtoPrimitive,
          )
        ),
        connection,
      )
    )
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId2)
    ) shouldBe None
    val someSynchronizerIdInterned2 =
      backend.stringInterningSupport.synchronizerId.internalize(
        StorageBackendTestValues.someSynchronizerId2
      )
    executeSql(connection =>
      ingest(
        Vector(
          DbDto.StringInterningDto(
            someSynchronizerIdInterned2,
            "d|" + StorageBackendTestValues.someSynchronizerId2.toProtoPrimitive,
          )
        ),
        connection,
      )
    )

    // updating ledger end and inserting one synchronizer index
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = someOffset,
          lastEventSeqId = 1,
          lastStringInterningId = 1,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
        ),
        lastSynchronizerIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndex
        ),
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = someOffset,
        lastEventSeqId = 1,
        lastStringInterningId = 1,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
      )
    )
    val resultSynchronizerIndex = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultSynchronizerIndex.value.repairIndex shouldBe someSynchronizerIndex.repairIndex
    resultSynchronizerIndex.value.sequencerIndex shouldBe someSynchronizerIndex.sequencerIndex
    resultSynchronizerIndex.value.recordTime shouldBe someSynchronizerIndex.recordTime

    // updating ledger end and inserting two synchronizer index (one is updating just the request index part, the other is inserting just a sequencer index)
    val someSynchronizerIndexSecond = SynchronizerIndex.forRepairUpdate(
      RepairIndex(
        timestamp = someSequencerTime.plusSeconds(10),
        counter = RepairCounter.Genesis,
      )
    )
    val someSynchronizerIndex2 = SynchronizerIndex.forSequencedUpdate(
      sequencerTimestamp = someSequencerTime.plusSeconds(5)
    )
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = offset(100),
          lastEventSeqId = 100,
          lastStringInterningId = 100,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(100),
        ),
        lastSynchronizerIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexSecond,
          StorageBackendTestValues.someSynchronizerId2 -> someSynchronizerIndex2,
        ),
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = offset(100),
        lastEventSeqId = 100,
        lastStringInterningId = 100,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(100),
      )
    )
    val resultSynchronizerIndexSecond = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultSynchronizerIndexSecond.value.repairIndex shouldBe someSynchronizerIndexSecond.repairIndex
    resultSynchronizerIndexSecond.value.sequencerIndex shouldBe someSynchronizerIndex.sequencerIndex
    resultSynchronizerIndexSecond.value.recordTime shouldBe someSynchronizerIndexSecond.recordTime
    val resultSynchronizerIndexSecond2 = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId2)
    )
    resultSynchronizerIndexSecond2.value.repairIndex shouldBe None
    resultSynchronizerIndexSecond2.value.sequencerIndex shouldBe someSynchronizerIndex2.sequencerIndex
    resultSynchronizerIndexSecond2.value.recordTime shouldBe someSynchronizerIndex2.recordTime

    // updating ledger end and inserting one synchronizer index only overriding the record time
    val someSynchronizerIndexThird =
      SynchronizerIndex.forFloatingUpdate(someSequencerTime.plusSeconds(20))
    executeSql(
      backend.parameter.updateLedgerEnd(
        ledgerEnd = LedgerEnd(
          lastOffset = offset(200),
          lastEventSeqId = 200,
          lastStringInterningId = 200,
          lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(200),
        ),
        lastSynchronizerIndex = Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexThird
        ),
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = offset(200),
        lastEventSeqId = 200,
        lastStringInterningId = 200,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(200),
      )
    )
    val resultSynchronizerIndexThird = executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    )
    resultSynchronizerIndexThird.value.repairIndex shouldBe someSynchronizerIndexSecond.repairIndex
    resultSynchronizerIndexThird.value.sequencerIndex shouldBe someSynchronizerIndex.sequencerIndex
    resultSynchronizerIndexThird.value.recordTime shouldBe someSequencerTime.plusSeconds(20)

    // resetting and disabling interning
    backend.stringInterningSupport.reset()
    backend.stringInterningSupport.setAutoIntern(false)

    // ensuring that auto-interning indeed does not work
    backend.stringInterningSupport.synchronizerId.tryInternalize(
      StorageBackendTestValues.someSynchronizerId
    ) shouldBe None
    backend.stringInterningSupport.synchronizerId.tryInternalize(
      StorageBackendTestValues.someSynchronizerId2
    ) shouldBe None

    // ensuring the same results
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    ) shouldBe resultSynchronizerIndexThird
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId2)
    ) shouldBe resultSynchronizerIndexSecond2

    // and if string interning table is wiped
    executeSql { c =>
      SQL"delete from lapi_string_interning".executeUpdate()(c) shouldBe 2
    }

    // cleanSynchronizerIndex returns empty
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId)
    ) shouldBe None
    executeSql(
      backend.parameter.cleanSynchronizerIndex(StorageBackendTestValues.someSynchronizerId2)
    ) shouldBe None
  }

  it should "store and retrieve post processing end correctly" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.postProcessingEnd) shouldBe None
    executeSql(backend.parameter.updatePostProcessingEnd(Some(offset(10))))
    executeSql(backend.parameter.postProcessingEnd) shouldBe Some(offset(10))
    executeSql(backend.parameter.updatePostProcessingEnd(Some(offset(20))))
    executeSql(backend.parameter.postProcessingEnd) shouldBe Some(offset(20))
    executeSql(backend.parameter.updatePostProcessingEnd(None))
    executeSql(backend.parameter.postProcessingEnd) shouldBe None
  }

  it should "fetch and update ACHSState correctly" in {
    executeSql(backend.parameter.fetchACHSState) shouldBe None

    val achsState0 = ACHSState(
      validAt = 1000L,
      lastRemoved = 123L,
      lastPopulated = 10L,
    )
    // check insertion to empty state
    executeSql(backend.parameter.insertACHSState(achsState0))
    executeSql(backend.parameter.fetchACHSState) shouldBe Some(achsState0)

    // check updates of validAt
    executeSql(backend.parameter.updateACHSValidAt(validAt = 2000L))
    val achsState1 = achsState0.copy(validAt = 2000L)
    executeSql(backend.parameter.fetchACHSState) shouldBe Some(achsState1)

    // check updates of lastRemoved and lastPopulated
    executeSql(backend.parameter.updateACHSLastPointers(lastRemoved = 200L, lastPopulated = 20L))
    val achsState2 = achsState1.copy(lastRemoved = 200L, lastPopulated = 20L)
    executeSql(backend.parameter.fetchACHSState) shouldBe Some(achsState2)

    // clear the state
    executeSql(backend.parameter.clearACHSState)
    executeSql(backend.parameter.fetchACHSState) shouldBe None

    // updating a non-existing state with validAt fails
    an[IllegalStateException] should be thrownBy executeSql(
      backend.parameter.updateACHSValidAt(validAt = 3000L)
    )

    // updating a non-existing state with lastRemoved and lastPopulated fails
    an[IllegalStateException] should be thrownBy executeSql(
      backend.parameter.updateACHSLastPointers(lastRemoved = 300L, lastPopulated = 30L)
    )
  }

}
