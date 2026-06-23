// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawAcsCommitment
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.{
  IdRange,
  Ids,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsAcsCommitments
    extends Matchers
    with Inside
    with OptionValues
    with StorageBackendSpec
    with HasExecutionContext { this: AnyFlatSpec =>

  behavior of "StorageBackend (acs commitments)"

  import StorageBackendTestValues.*

  private val synchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizerId2 = SynchronizerId.tryFromString("x::synchronizer2")
  private val unknownSynchronizerId = SynchronizerId.tryFromString("x::unknown")

  private val singleDto = Vector(
    dtoAcsCommitment(offset(1), 1L, synchronizerId = synchronizerId1)
  )

  private val multipleDtos = Vector(
    dtoAcsCommitment(offset(1), 1L, synchronizerId = synchronizerId1),
    dtoAcsCommitment(offset(2), 2L, synchronizerId = synchronizerId2),
    dtoAcsCommitment(offset(3), 3L, synchronizerId = synchronizerId1),
    dtoAcsCommitment(offset(4), 4L, synchronizerId = synchronizerId1),
    dtoAcsCommitment(offset(4), 5L, synchronizerId = synchronizerId2),
  )

  private def toRaw(dbDto: DbDto.AcsCommitment): RawAcsCommitment =
    RawAcsCommitment(
      offset = Offset.tryFromLong(dbDto.event_offset),
      eventSequentialId = dbDto.event_sequential_id,
      synchronizerId = dbDto.synchronizer_id.toProtoPrimitive,
      recordTime = Timestamp.assertFromLong(dbDto.record_time),
      payload = dbDto.payload,
      traceContext = dbDto.trace_context,
    )

  private def sanitize: RawAcsCommitment => RawAcsCommitment =
    _.copy(traceContext = Array.emptyByteArray, payload = Array.emptyByteArray)

  it should "respond with payloads for a single acs commitment" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(singleDto, _))
    executeSql(updateLedgerEnd(offset(1), ledgerEndSequentialId = 1L))

    val payloads = executeSql(
      backend.event.fetchAcsCommitments(Ids(Vector(1L)), synchronizerId1, descendingOrder = false)
    )

    payloads should not be empty
    payloads.map(sanitize) should contain theSameElementsAs singleDto.map(toRaw).map(sanitize)

    val payloadsRange = executeSql(
      backend.event.fetchAcsCommitments(IdRange(1L, 1L), synchronizerId1, descendingOrder = false)
    )
    payloadsRange.map(sanitize) shouldBe payloads.map(sanitize)
  }

  it should "respond with payloads for multiple acs commitments of the requested synchronizer" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 5L))

    val payloads = executeSql(
      backend.event.fetchAcsCommitments(
        Ids(Vector(1L, 2L, 3L, 4L, 5L)),
        synchronizerId1,
        descendingOrder = false,
      )
    )

    payloads.map(_.eventSequentialId) should contain theSameElementsInOrderAs Vector(1L, 3L, 4L)
    payloads.map(sanitize) should contain theSameElementsAs
      multipleDtos.filter(_.synchronizer_id == synchronizerId1).map(toRaw).map(sanitize)

    val payloadsRange = executeSql(
      backend.event
        .fetchAcsCommitments(
          IdRange(fromInclusive = 1L, toInclusive = 5L),
          synchronizerId1,
          descendingOrder = false,
        )
    )
    payloadsRange.map(sanitize) shouldBe payloads.map(sanitize)

    val payloadsSingle = executeSql(
      backend.event
        .fetchAcsCommitments(
          IdRange(fromInclusive = 1L, toInclusive = 1L),
          synchronizerId1,
          descendingOrder = false,
        )
    )
    payloadsSingle.map(_.eventSequentialId) shouldBe Vector(1L)

    val payloadsRange2 = executeSql(
      backend.event
        .fetchAcsCommitments(
          IdRange(fromInclusive = 1L, toInclusive = 5L),
          synchronizerId2,
          descendingOrder = false,
        )
    )
    payloadsRange2.map(sanitize) should contain theSameElementsAs
      multipleDtos.filter(_.synchronizer_id == synchronizerId2).map(toRaw).map(sanitize)
  }

  it should "deliver acs commitments ordered by event sequential id" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 5L))

    val payloads = executeSql(
      backend.event.fetchAcsCommitments(IdRange(1L, 5L), synchronizerId1, descendingOrder = false)
    )

    payloads.map(_.eventSequentialId) shouldBe sorted
  }

  it should "deliver acs commitments in descending event sequential id order when requested" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 5L))

    val payloads = executeSql(
      backend.event.fetchAcsCommitments(IdRange(1L, 5L), synchronizerId1, descendingOrder = true)
    )

    payloads.map(_.eventSequentialId) shouldBe Vector(4L, 3L, 1L)
  }

  it should "respond with no commitments for a different synchronizer" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 5L))

    // synchronizerId2 only has the commitment with seq id 2 and 5
    val payloads = executeSql(
      backend.event
        .fetchAcsCommitments(Ids(Vector(1L, 3L, 4L)), synchronizerId2, descendingOrder = false)
    )

    payloads shouldBe empty

    val payloadsRange = executeSql(
      backend.event
        .fetchAcsCommitments(
          IdRange(fromInclusive = 3L, toInclusive = 4L),
          synchronizerId2,
          descendingOrder = false,
        )
    )

    payloadsRange shouldBe empty
  }

  it should "respond with no commitments for a synchronizer unknown to the participant" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(multipleDtos, _))
    executeSql(updateLedgerEnd(offset(4), ledgerEndSequentialId = 4L))

    // the unknown synchronizer was never ingested, hence it has no interned id
    val payloads = executeSql(
      backend.event
        .fetchAcsCommitments(
          IdRange(fromInclusive = 1L, toInclusive = 5L),
          unknownSynchronizerId,
          descendingOrder = false,
        )
    )

    payloads shouldBe empty
  }
}
