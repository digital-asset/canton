// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{RepairIndex, SynchronizerIndex}
import com.digitalasset.canton.platform.store.backend.DbDto.StringInterningDto
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsLastPointers,
  AchsState,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{HasExecutionContext, RepairCounter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

import scala.concurrent.duration.DurationInt

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
          synchronizerIndices = Map(
            StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndex
          ),
        )
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = someOffset,
        lastEventSeqId = 1,
        lastStringInterningId = 1,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(10),
        Map(StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndex),
      )
    )

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
          synchronizerIndices = Map(
            StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexSecond,
            StorageBackendTestValues.someSynchronizerId2 -> someSynchronizerIndex2,
          ),
        )
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = offset(100),
        lastEventSeqId = 100,
        lastStringInterningId = 100,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(100),
        Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexSecond,
          StorageBackendTestValues.someSynchronizerId2 -> someSynchronizerIndex2,
        ),
      )
    )

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
          synchronizerIndices = Map(
            StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndexThird
          ),
        )
      )
    )
    executeSql(backend.parameter.ledgerEnd) shouldBe Some(
      LedgerEnd(
        lastOffset = offset(200),
        lastEventSeqId = 200,
        lastStringInterningId = 200,
        lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(200),
        Map(
          StorageBackendTestValues.someSynchronizerId -> someSynchronizerIndex
            .max(someSynchronizerIndexSecond)
            .max(someSynchronizerIndexThird),
          StorageBackendTestValues.someSynchronizerId2 -> someSynchronizerIndex2,
        ),
      )
    )
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

  it should "fetch and update AchsState correctly" in {
    executeSql(backend.parameter.fetchAchsState) shouldBe None

    val achsState0 = AchsState(
      validAt = 1000L,
      lastPointers = AchsLastPointers(
        lastRemoved = 123L,
        lastPopulated = 10L,
      ),
    )
    // check insertion to empty state
    executeSql(backend.parameter.insertAchsState(achsState0))
    executeSql(backend.parameter.fetchAchsState) shouldBe Some(achsState0)

    // check updates of validAt
    executeSql(backend.parameter.updateAchsValidAt(validAt = 2000L))
    val achsState1 = achsState0.copy(validAt = 2000L)
    executeSql(backend.parameter.fetchAchsState) shouldBe Some(achsState1)

    // check updates of lastRemoved and lastPopulated
    executeSql(
      backend.parameter.updateAchsLastPointers(
        AchsLastPointers(lastRemoved = 200L, lastPopulated = 20L)
      )
    )
    val achsState2 =
      achsState1.copy(lastPointers = AchsLastPointers(lastRemoved = 200L, lastPopulated = 20L))
    executeSql(backend.parameter.fetchAchsState) shouldBe Some(achsState2)

    // clear the state
    executeSql { c =>
      c.setAutoCommit(false)
      backend.parameter.clearAchsStateAndData()(c, implicitly)
      c.setAutoCommit(true)
    }
    executeSql(backend.parameter.fetchAchsState) shouldBe None

    // updating a non-existing state with validAt fails
    an[IllegalStateException] should be thrownBy executeSql(
      backend.parameter.updateAchsValidAt(validAt = 3000L)
    )

    // updating a non-existing state with lastRemoved and lastPopulated fails
    an[IllegalStateException] should be thrownBy executeSql(
      backend.parameter.updateAchsLastPointers(
        AchsLastPointers(lastRemoved = 300L, lastPopulated = 30L)
      )
    )
  }

  it should "fail if there is no corresponding string interning entry for persited clean synchronizer index" in {
    executeSql(
      backend.parameter
        .initializeParameters(StorageBackendTestValues.someIdentityParams, loggerFactory)
    )
    val ledgerEnd = LedgerEnd(
      lastOffset = offset(10),
      lastEventSeqId = 10,
      lastStringInterningId = 100,
      lastPublicationTime = CantonTimestamp.Epoch.add(1.second),
      synchronizerIndices = Map(
        SynchronizerId.tryFromString("u::unknownSynchronizerId") -> SynchronizerIndex(
          None,
          None,
          CantonTimestamp.Epoch,
        )
      ),
    )

    executeSql(
      backend.parameter.updateLedgerEnd(ledgerEnd)
    )

    val exception = intercept[IllegalStateException](executeSql(backend.parameter.ledgerEnd))
    exception
      .getMessage() should fullyMatch regex ("""String interning entry for internalized synchornizer id=\d+ missing""")
  }

  it should "fail if  corresponding string interning entry for persited clean synchronizer index has party prefix" in {
    executeSql(
      backend.parameter
        .initializeParameters(StorageBackendTestValues.someIdentityParams, loggerFactory)
    )
    val ledgerEnd = LedgerEnd(
      lastOffset = offset(10),
      lastEventSeqId = 10,
      lastStringInterningId = 100,
      lastPublicationTime = CantonTimestamp.Epoch.add(1.second),
      synchronizerIndices = Map(
        SynchronizerId.tryFromString("u::unknownSynchronizerId") -> SynchronizerIndex(
          None,
          None,
          CantonTimestamp.Epoch,
        )
      ),
    )

    executeSql(
      backend.parameter.updateLedgerEnd(ledgerEnd)
    )

    val dto = StringInterningDto(
      backend.stringInterningSupport.synchronizerId
        .internalize(SynchronizerId.tryFromString("u::unknownSynchronizerId")),
      "p|x::participant1",
    )
    executeSql(ingest(Vector(dto), _))

    val exception = intercept[IllegalStateException](executeSql(backend.parameter.ledgerEnd))
    exception.getMessage() should equal(
      "Externalized string p|x::participant1 does not have the expected synchronizer id prefix d|"
    )
  }

}
