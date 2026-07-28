// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.SqlParser.long
import anorm.SqlStringInterpolation
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.EventSeqIdRange
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  PaginationFromTo,
  PaginationInput,
}
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside}

private[backend] trait StorageBackendTestsInitializeIngestion
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (initializeIngestion)"
  import StorageBackendTestValues.*

  private val signatory = Ref.Party.assertFromString("signatory")
  private val participant = api.ParticipantId(Ref.ParticipantId.assertFromString("someParticipant"))

  private val payload1 =
    ByteString.copyFromUtf8("dynamic-synchronizer-parameters-payload-1")
  private val payload2 =
    ByteString.copyFromUtf8("dynamic-synchronizer-parameters-payload-2")
  val dtos = Vector(
    // 1: party allocation
    dtoPartyEntry(offset(1), someParty)
  )
  it should "delete overspill entries - parties" in {
    fixture(
      dtos1 = dtos,
      lastOffset1 = 2L,
      lastEventSeqId1 = 0L,
      dtos2 = Vector(
        // 3: party allocation
        dtoPartyEntry(offset(3), someParty2)
      ),
      lastOffset2 = 3L,
      lastEventSeqId2 = 0L,
      checkContentsBefore = () => {
        val parties = executeSql(backend.party.knownParties(None, None, 10))
        parties should have length 1
      },
      checkContentsAfter = () => {
        val parties = executeSql(backend.party.knownParties(None, None, 10))
        parties should have length 1
      },
    )
  }

  it should "delete overspill entries written before first ledger end update - parties" in {
    fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
      dtos = dtos,
      lastOffset = 3,
      lastEventSeqId = 0L,
      checkContentsAfter = () => {
        val parties2 = executeSql(backend.party.knownParties(None, None, 10))
        parties2 shouldBe empty
      },
    )
  }

  val dtos1 = Vector(
    // 1: transaction with a create node
    dtosCreate(
      1L,
      event_sequential_id = 1,
      internal_contract_id = 101,
      additional_witnesses = Set(someParty),
    )(stakeholders = Set(signatory, someParty)),
    Seq(
      dtoTransactionMeta(
        offset(1),
        event_sequential_id_first = 1L,
        event_sequential_id_last = 1L,
      ),
      dtoCompletion(offset(41)),
    ),
    // 2: transaction with exercise node
    dtosWitnessedExercised(
      2L,
      event_sequential_id = 2,
      consuming = false,
      internal_contract_id = Some(101),
      additional_witnesses = Set(someParty),
    ),
    dtosConsumingExercise(
      2L,
      event_sequential_id = 3,
      internal_contract_id = Some(102),
      stakeholders = Set(someParty),
      additional_witnesses = Set(someParty),
    ),
    Seq(
      dtoTransactionMeta(
        offset(2),
        event_sequential_id_first = 2L,
        event_sequential_id_last = 4L,
      ),
      dtoCompletion(offset(2)),
    ),
    // 3: assign
    dtosAssign(
      3L,
      event_sequential_id = 4,
      internal_contract_id = 103,
    )(stakeholders = Set(someParty)),
    // 4: unassign
    dtosUnassign(
      4L,
      event_sequential_id = 5,
      internal_contract_id = Some(103),
      stakeholders = Set(someParty),
    ),
    // 5: topology transactions
    Seq(
      dtoPartyToParticipant(
        offset(5),
        eventSequentialId = 6,
        party = someParty,
        participant = participant,
      ),
      dtoPartyToParticipant(
        offset(5),
        eventSequentialId = 7,
        party = someParty2,
        participant = participant,
      ),
    ),
    // 6: acs commitment (persisted, within the first ledger end)
    Seq(dtoAcsCommitment(offset(5), eventSequentialId = 8L)),
    // 7: dynamic synchronizer parameters (persisted, within the first ledger end)
    Seq(
      dtoGenericTopologyEvent(
        offset(5),
        eventSequentialId = 9L,
        payload = payload1,
      )
    ),
  ).flatten

  it should "delete overspill entries - events, transaction meta, completions" in {
    val dtos2 = Vector(
      // 8: transaction with create node
      dtosCreate(
        6L,
        event_sequential_id = 10L,
        internal_contract_id = 201,
        additional_witnesses = Set(someParty),
      )(stakeholders = Set(signatory, someParty)),
      Seq(
        dtoTransactionMeta(
          offset(6),
          event_sequential_id_first = 10L,
          event_sequential_id_last = 10L,
        ),
        dtoCompletion(offset(6)),
      ),
      // 9: transaction with exercise node
      dtosWitnessedExercised(
        7L,
        event_sequential_id = 11L,
        consuming = false,
        internal_contract_id = Some(201),
        additional_witnesses = Set(someParty),
      ),
      dtosConsumingExercise(
        7L,
        event_sequential_id = 12L,
        internal_contract_id = Some(202),
        stakeholders = Set(someParty),
        additional_witnesses = Set(someParty),
      ),
      Seq(
        dtoTransactionMeta(
          offset(7),
          event_sequential_id_first = 11L,
          event_sequential_id_last = 12L,
        ),
        dtoCompletion(offset(7)),
      ),
      // 10: assign
      dtosAssign(8L, event_sequential_id = 13, internal_contract_id = 203)(stakeholders =
        Set(someParty)
      ),
      // 11: unassign
      dtosUnassign(
        9L,
        event_sequential_id = 14,
        internal_contract_id = Some(203),
        stakeholders = Set(someParty),
      ),
      // 12: topology transactions
      Seq(
        dtoPartyToParticipant(
          offset(10),
          eventSequentialId = 15,
          party = someParty,
          participant = participant,
        ),
        dtoPartyToParticipant(
          offset(10),
          eventSequentialId = 16,
          party = someParty3,
          participant = participant,
        ),
      ),
      // 13: acs commitment
      Seq(dtoAcsCommitment(offset(12), eventSequentialId = 17L)),
      // 14: dynamic synchronizer parameters
      Seq(
        dtoGenericTopologyEvent(
          offset(12),
          eventSequentialId = 18L,
          payload = payload2,
        )
      ),
    ).flatten
    val allDtos = dtos1 ++ dtos2
    fixture(
      dtos1 = dtos1,
      lastOffset1 = 5L,
      lastEventSeqId1 = 9L,
      dtos2 = dtos2,
      lastOffset2 = 12L,
      lastEventSeqId2 = 18L,
      checkContentsBefore = () => {
        val activateEventSeqIds =
          executeSql(
            backend.event.fetchEventPayloadsAcsDelta(
              EventPayloadSourceForUpdatesAcsDelta.Activate
            )(EventSeqIdRange(1L, 100L), Some(Set.empty), None)
          ).map(_.eventSeqId)
        val deactivateEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsAcsDelta(
            EventPayloadSourceForUpdatesAcsDelta.Deactivate
          )(EventSeqIdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val witnessEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
          )(EventSeqIdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val topologyPartyEvents =
          executeSql(
            backend.event.topologyPartyEventBatch(EventSeqIdRange(1L, 100L))
          ).map(_.partyId)
        activateEventSeqIds shouldBe List(1, 4, 10, 13)
        deactivateEventSeqIds shouldBe List(3, 5, 12, 14)
        witnessEventSeqIds shouldBe List(2, 11)
        topologyPartyEvents shouldBe List(
          someParty,
          someParty2,
          someParty,
          someParty3,
        ) // not constrained by ledger end
        acsCommitmentSeqIds() shouldBe List(8L, 17L)
        dynamicSynchronizerParametersSeqIds() shouldBe List(9L, 18L)
        dynamicSynchronizerParametersPayloads() shouldBe List(
          payload1,
          payload2,
        ) // not constrained by ledger end
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe Set((1, 1), (2, 4))
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe fetchIdsFromTransactionMetaOffsets(allDtos.collect {
          case meta: DbDto.TransactionMeta =>
            meta.event_offset
        })
        fetchIdsCreateStakeholder() shouldBe List(
          1L,
          10L,
        ) // since ledger-end does not limit the range query
        fetchIdsCreateNonStakeholder() shouldBe List(1L, 10L)
        fetchIdsConsumingStakeholder() shouldBe List(3L, 12L)
        fetchIdsConsumingNonStakeholder() shouldBe List(3L, 12L)
        fetchIdsNonConsuming() shouldBe List(2L, 11L)
        fetchIdsAssignStakeholder() shouldBe List(4L, 13L)
        fetchTopologyParty() shouldBe List(6, 15)
      },
      checkContentsAfter = () => {
        val activateEventSeqIds =
          executeSql(
            backend.event.fetchEventPayloadsAcsDelta(
              EventPayloadSourceForUpdatesAcsDelta.Activate
            )(EventSeqIdRange(1L, 100L), Some(Set.empty), None)
          ).map(_.eventSeqId)
        val deactivateEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsAcsDelta(
            EventPayloadSourceForUpdatesAcsDelta.Deactivate
          )(EventSeqIdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val witnessEventSeqIds = executeSql(
          backend.event.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
          )(EventSeqIdRange(1L, 100L), Some(Set.empty), None)
        ).map(_.eventSeqId)
        val topologyPartyEvents =
          executeSql(
            backend.event.topologyPartyEventBatch(EventSeqIdRange(1L, 100L))
          ).map(_.partyId)
        activateEventSeqIds shouldBe List(1, 4)
        deactivateEventSeqIds shouldBe List(3, 5)
        witnessEventSeqIds shouldBe List(2)
        topologyPartyEvents shouldBe List(
          someParty,
          someParty2,
        ) // not constrained by ledger end
        acsCommitmentSeqIds() shouldBe List(8L)
        dynamicSynchronizerParametersSeqIds() shouldBe List(9L)
        dynamicSynchronizerParametersPayloads() shouldBe List(
          payload1
        )
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe Set((1, 1), (2, 4))
        fetchIdsFromTransactionMetaUpdateIds(allDtos.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe fetchIdsFromTransactionMetaOffsets(allDtos.collect {
          case meta: DbDto.TransactionMeta =>
            meta.event_offset
        })
        fetchIdsCreateStakeholder() shouldBe List(1L)
        fetchIdsCreateNonStakeholder() shouldBe List(1L)
        fetchIdsConsumingStakeholder() shouldBe List(3L)
        fetchIdsConsumingNonStakeholder() shouldBe List(3L)
        fetchIdsNonConsuming() shouldBe List(2L)
        fetchIdsAssignStakeholder() shouldBe List(4L)
        fetchTopologyParty() shouldBe List(6)
      },
    )
  }

  it should "delete overspill entries written before first ledger end update - events, transaction meta, completions" in {
    fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
      dtos = dtos1,
      lastOffset = 5,
      lastEventSeqId = 9L,
      checkContentsAfter = () => {
        val contractsCreated =
          executeSql(
            backend.contract
              .activeContracts(List(101, 201), 1000)
          )
        val contractsAssigned =
          executeSql(
            backend.contract
              .activeContracts(List(103, 203), 1000)
          )
        val topologyPartyEvents =
          executeSql(
            backend.event.topologyPartyEventBatch(EventSeqIdRange(1L, 100L))
          ).map(_.partyId)
        contractsCreated should not contain hashCid("#101")
        contractsAssigned should not contain hashCid("#103")
        contractsAssigned should not contain hashCid("#203")
        topologyPartyEvents shouldBe empty
        fetchIdsFromTransactionMetaUpdateIds(dtos1.collect { case meta: DbDto.TransactionMeta =>
          meta.update_id
        }) shouldBe empty
        fetchIdsFromTransactionMetaOffsets(dtos1.collect { case meta: DbDto.TransactionMeta =>
          meta.event_offset
        }) shouldBe empty
        fetchIdsCreateStakeholder() shouldBe empty
        fetchIdsCreateNonStakeholder() shouldBe empty
        fetchIdsConsumingStakeholder() shouldBe empty
        fetchIdsConsumingNonStakeholder() shouldBe empty
        fetchIdsNonConsuming() shouldBe empty
        fetchIdsAssignStakeholder() shouldBe empty
        fetchTopologyParty() shouldBe empty
      },
    )
  }

  private def fetchIdsNonConsuming(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .variousWitnessIds(
          witnessO = Some(someParty),
          templateIdO = None,
        )
        .filteredForEventTypes(Set(PersistentEventType.NonConsumingExercise))
        .fetchPage(_)(
          PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000))
        )
    )

  private def fetchIdsConsumingNonStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .deactivateWitnessesIds(
          witnessO = Some(someParty),
          templateIdO = None,
        )
        .filteredForEventTypes(Set(PersistentEventType.ConsumingExercise))
        .fetchPage(_)(
          PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000))
        )
    )

  private def fetchIdsConsumingStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .deactivateStakeholderIds(
          witnessO = Some(someParty),
          templateIdO = None,
        )
        .filteredForEventTypes(Set(PersistentEventType.ConsumingExercise))
        .fetchPage(_)(
          PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000))
        )
    )

  private def fetchIdsCreateNonStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .activateWitnessesIds(
          witnessO = Some(someParty),
          templateIdO = None,
        )
        .filteredForEventTypes(Set(PersistentEventType.Create))
        .fetchPage(_)(
          PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000))
        )
    )

  private def fetchIdsCreateStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .activateStakeholderIds(
          witnessO = Some(someParty),
          templateIdO = None,
        )
        .filteredForEventTypes(Set(PersistentEventType.Create))
        .fetchPage(_)(
          PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000))
        )
    )

  private def fetchIdsAssignStakeholder(): Vector[Long] =
    executeSql(
      backend.event.updateStreamingQueries
        .activateStakeholderIds(
          witnessO = Some(someParty),
          templateIdO = None,
        )
        .filteredForEventTypes(Set(PersistentEventType.Assign))
        .fetchPage(_)(
          PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000))
        )
    )

  private def fetchTopologyParty(): Vector[Long] =
    executeSql(
      backend.event
        .fetchTopologyPartyEventIds(
          party = Some(someParty)
        )
        .fetchPage(_)(
          PaginationInput(
            fromTo =
              PaginationFromTo.ascending(EventSeqIdRange(startInclusive = 1, endInclusive = 1000)),
            limit = 1000,
          )
        )
        .ids
    )

  private def acsCommitmentSeqIds(): Vector[Long] =
    executeSql(
      backend.event.fetchAcsCommitments(
        EventSeqIdRange(1L, 100L),
        someSynchronizerId,
        descendingOrder = false,
      )
    ).map(_.eventSequentialId)

  private def dynamicSynchronizerParametersSeqIds(): Vector[Long] =
    executeSql(
      backend.event.dynamicSynchronizerParametersBatch(
        EventSeqIdRange(1L, 100L)
      )
    ).map(_.eventSequentialId)

  private def dynamicSynchronizerParametersPayloads(): Vector[ByteString] =
    executeSql(
      backend.event.dynamicSynchronizerParametersBatch(
        EventSeqIdRange(1L, 100L)
      )
    ).map(raw => ByteString.copyFrom(raw.payload))

  private def fetchIdsFromTransactionMetaUpdateIds(
      updateIds: Seq[Array[Byte]]
  ): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.updatePointwiseQueries
    updateIds
      .map(UpdateId.tryFromByteArray)
      .map { updateId =>
        executeSql(
          txPointwiseQueries.fetchIdsFromUpdateMeta(
            lookupKey = LookupKey.ByUpdateId(updateId)
          )
        )
          .map(eventSeqIdRange => (eventSeqIdRange.startInclusive, eventSeqIdRange.endInclusive))
      }
      .flatMap(_.toList)
      .toSet
  }

  private def fetchIdsFromTransactionMetaOffsets(offsets: Seq[Long]): Set[(Long, Long)] = {
    val txPointwiseQueries = backend.event.updatePointwiseQueries
    offsets
      .map(Offset.tryFromLong)
      .map { offset =>
        executeSql(
          txPointwiseQueries.fetchIdsFromUpdateMeta(
            lookupKey = LookupKey.ByOffset(offset)
          )
        )
          .map(eventSeqIdRange => (eventSeqIdRange.startInclusive, eventSeqIdRange.endInclusive))
      }
      .flatMap(_.toList)
      .toSet
  }

  private def fixture(
      dtos1: Vector[DbDto],
      lastOffset1: Long,
      lastEventSeqId1: Long,
      dtos2: Vector[DbDto],
      lastOffset2: Long,
      lastEventSeqId2: Long,
      checkContentsBefore: () => Assertion,
      checkContentsAfter: () => Assertion,
  ): Assertion = {
    val loggerFactory = SuppressingLogger(getClass)
    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Start the indexer (a no-op in this case)
    val end1 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end1))
    // Fully insert first batch of updates
    executeSql(ingest(dtos1, _))
    executeSql(updateLedgerEnd(ledgerEnd(lastOffset1, lastEventSeqId1)))
    // Partially insert second batch of updates (indexer crashes before updating ledger end)
    executeSql(ingest(dtos2, _))
    // Check the contents
    checkContentsBefore()
    // Restart the indexer - should delete data from the partial insert above
    val end2 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end2))
    // Move the ledger end so that any non-deleted data would become visible
    executeSql(updateLedgerEnd(ledgerEnd(lastOffset2 + 1, lastEventSeqId2 + 1)))
    // Check the contents
    checkContentsAfter()
  }

  private def fixtureOverspillEntriesPriorToFirstLedgerEndUpdate(
      dtos: Vector[DbDto],
      lastOffset: Long,
      lastEventSeqId: Long,
      checkContentsAfter: () => Assertion,
  ): Assertion = {
    val loggerFactory = SuppressingLogger(getClass)
    // Initialize
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Start the indexer (a no-op in this case)
    val end1 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end1))
    // Insert first batch of updates, but crash before writing the first ledger end
    executeSql(ingest(dtos, _))
    // Restart the indexer - should delete data from the partial insert above
    val end2 = executeSql(backend.parameter.ledgerEnd)
    executeSql(backend.ingestion.deletePartiallyIngestedData(end2))
    // Move the ledger end so that any non-deleted data would become visible
    executeSql(updateLedgerEnd(ledgerEnd(lastOffset + 1, lastEventSeqId + 1)))
    checkContentsAfter()
  }

  behavior of "addContractPruningCandidatesAfter"

  it should "populate candidates correctly during initialization" in {
    // baseline: there should be no candidates
    contractCandidates() shouldBe Vector.empty

    val ledgerEnd = 1000L

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(
      ingest(
        Vector(
          // before ledgerEnd1
          dtosCreate(
            event_offset = 100L,
            event_sequential_id = 100L,
            internal_contract_id = 50,
          )(),
          dtosWitnessedCreate(
            event_offset = 1000L,
            event_sequential_id = 1000L,
            internal_contract_id = 51,
          )(),
          // between  ledgerEnd1 and ledgerEnd2

          // check activated
          // will be added + lower bound check
          dtosCreate(
            event_offset = 1001L,
            event_sequential_id = 1001L,
            internal_contract_id = 100,
          )(),
          // will be added second
          dtosCreate(
            event_offset = 1002L,
            event_sequential_id = 1002L,
            internal_contract_id = 101,
          )(),
          // won't be added: has before activate
          dtosCreate(
            event_offset = 1003L,
            event_sequential_id = 1003L,
            internal_contract_id = 50,
          )(),
          // won't be added: has before witnessed
          dtosCreate(
            event_offset = 1004L,
            event_sequential_id = 1004L,
            internal_contract_id = 51,
          )(),
          // won't be added: already there
          dtosCreate(
            event_offset = 1005L,
            event_sequential_id = 1005L,
            internal_contract_id = 1,
          )(),
          // won't be added: duplicate activate
          dtosCreate(
            event_offset = 1006L,
            event_sequential_id = 1006L,
            internal_contract_id = 101,
          )(),
          // won't be added: duplicate witnessed
          dtosCreate(
            event_offset = 1007L,
            event_sequential_id = 1007L,
            internal_contract_id = 104,
          )(),

          // check witnessed
          // will be added
          dtosWitnessedCreate(
            event_offset = 1008L,
            event_sequential_id = 1008L,
            internal_contract_id = 103,
          )(),
          // will be added second
          dtosWitnessedCreate(
            event_offset = 1009L,
            event_sequential_id = 1009L,
            internal_contract_id = 104,
          )(),
          // won't be added: has before activate
          dtosWitnessedCreate(
            event_offset = 1010L,
            event_sequential_id = 1010L,
            internal_contract_id = 50,
          )(),
          // won't be added: has before witnessed
          dtosWitnessedCreate(
            event_offset = 1011L,
            event_sequential_id = 1011L,
            internal_contract_id = 51,
          )(),
          // won't be added: already there
          dtosWitnessedCreate(
            event_offset = 1012L,
            event_sequential_id = 1012L,
            internal_contract_id = 2,
          )(),
          // won't be added: duplicate activate
          dtosWitnessedCreate(
            event_offset = 1013L,
            event_sequential_id = 1013L,
            internal_contract_id = 101,
          )(),
          // won't be added: duplicate witnessed
          dtosWitnessedCreate(
            event_offset = 1014L,
            event_sequential_id = 1014L,
            internal_contract_id = 104,
          )(),

          // won't be there, no deactivation is selected
          dtosConsumingExercise(
            event_offset = 1015L,
            event_sequential_id = 1015L,
            internal_contract_id = Some(3),
          ),
        ).flatten,
        _,
      )
    )

    manuallyAddContractCandidates(Vector(1, 2))
    contractCandidates() shouldBe Vector(1, 2)

    executeSql { connection =>
      // non-auto commit is enforced by the PG locking mechanism used inside
      connection.setAutoCommit(false)
      backend.event.addContractPruningCandidatesAfter(ledgerEnd, testDbLockMeta)(
        connection,
        implicitly,
      )
      connection.commit()
    }
    contractCandidates() shouldBe Vector(1, 2, 100, 101, 103, 104)
  }

  private def contractCandidates(): Vector[Long] =
    executeSql(
      SQL"""
            select internal_contract_id
            from lapi_pruning_contract_candidate
            order by internal_contract_id
      """.asVectorOf(long("internal_contract_id"))(_)
    )

  private def manuallyAddContractCandidates(internalContractIds: Vector[Long]): Unit =
    executeSql(
      SQL"""
            insert into lapi_pruning_contract_candidate(internal_contract_id)
            values #${internalContractIds.map(id => s"($id)").mkString(", ")}
      """.executeUpdate()(_)
    ) shouldBe internalContractIds.size

}
