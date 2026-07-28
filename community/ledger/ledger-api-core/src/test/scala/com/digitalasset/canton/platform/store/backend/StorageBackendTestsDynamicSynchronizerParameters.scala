// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawDynamicSynchronizerParameters
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.{
  EventSeqIdRange,
  Ids,
}
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsDynamicSynchronizerParameters
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend (dynamic synchronizer parameters)"

  import StorageBackendTestValues.*

  private val synchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizerId2 = SynchronizerId.tryFromString("x::synchronizer2")

  private val singleDto = Vector(
    dtoGenericTopologyEvent(offset(1), 1L, synchronizerId = synchronizerId1)
  )

  private val multipleDtos = Vector(
    dtoGenericTopologyEvent(offset(1), 1L, synchronizerId = synchronizerId1),
    dtoGenericTopologyEvent(offset(2), 2L, synchronizerId = synchronizerId2),
    dtoGenericTopologyEvent(offset(3), 3L, synchronizerId = synchronizerId1),
    dtoGenericTopologyEvent(offset(4), 4L, synchronizerId = synchronizerId1),
  )

  private def toRaw(dbDto: DbDto.GenericTopologyEvent): RawDynamicSynchronizerParameters =
    RawDynamicSynchronizerParameters(
      offset = Offset.tryFromLong(dbDto.event_offset),
      eventSequentialId = dbDto.event_sequential_id,
      updateId = UpdateId
        .fromProtoPrimitive(ByteString.copyFrom(dbDto.update_id))
        .fold(err => throw new IllegalArgumentException(err.message), identity)
        .toHexString,
      synchronizerId = dbDto.synchronizer_id.toProtoPrimitive,
      recordTime = Timestamp.assertFromLong(dbDto.record_time),
      payload = dbDto.payload,
      traceContext = dbDto.trace_context,
    )

  private def sanitize: RawDynamicSynchronizerParameters => RawDynamicSynchronizerParameters =
    _.copy(traceContext = Array.emptyByteArray, payload = Array.emptyByteArray)

  private def fetchEventIds(
      idRange: EventSeqIdRange = EventSeqIdRange(1L, 10L)
  ): Vector[Long] =
    executeSql(
      backend.event.fetchDynamicSynchronizerParametersEventIds(idRange)
    )

  it should "return the event ids for a single event" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(singleDto, _))
    executeSql(updateLedgerEnd(offset(1), ledgerEndSequentialId = 1L))

    fetchEventIds() should contain theSameElementsAs Vector(1L)
  }

  it should "respond with payloads for a single event" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(singleDto, _))
    executeSql(updateLedgerEnd(offset(1), ledgerEndSequentialId = 1L))

    val payloads = executeSql(
      backend.event.dynamicSynchronizerParametersBatch(Ids(Vector(1L)))
    )

    payloads should not be empty
    payloads.map(sanitize) should contain theSameElementsAs singleDto.map(toRaw).map(sanitize)

    val payloadsRange = executeSql(
      backend.event.dynamicSynchronizerParametersBatch(EventSeqIdRange(1L, 1L))
    )
    payloadsRange.map(sanitize) shouldBe payloads.map(sanitize)
  }

  it should "respond with payloads for multiple events across synchronizers" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 4L))

    val payloads = executeSql(
      backend.event
        .dynamicSynchronizerParametersBatch(Ids(Vector(1L, 2L, 3L, 4L)))
    )

    payloads.map(_.eventSequentialId) should contain theSameElementsInOrderAs Vector(1L, 2L, 3L, 4L)
    payloads.map(sanitize) should contain theSameElementsAs multipleDtos.map(toRaw).map(sanitize)

    val payloadsRange = executeSql(
      backend.event
        .dynamicSynchronizerParametersBatch(
          EventSeqIdRange(startInclusive = 1L, endInclusive = 4L)
        )
    )
    payloadsRange.map(sanitize) shouldBe payloads.map(sanitize)
  }

  it should "deliver events ordered by event sequential id" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 4L))

    val payloads = executeSql(
      backend.event.dynamicSynchronizerParametersBatch(EventSeqIdRange(1L, 4L))
    )

    payloads.map(_.eventSequentialId) shouldBe sorted
  }

  it should "respond with no events for a range with no matching ids" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 4L))

    val payloads = executeSql(
      backend.event.dynamicSynchronizerParametersBatch(Ids(Vector(5L, 6L)))
    )

    payloads shouldBe empty
  }
}
