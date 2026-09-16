// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.platform.v1.acs_continuation.AcsContinuationTokenPayload
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.messages.state.{
  AcsContinuationPointerActiveContracts,
  AcsRangeInfo,
}
import com.digitalasset.canton.ledger.api.{EventFormat, UpdateFormat}
import com.digitalasset.canton.ledger.error.IndexErrors
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.{
  AcsChangeUpdate,
  UpdatesResponse,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.config.ActiveContractsServiceStreamsConfigOverrides
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec

class InternalIndexServiceSpec
    extends AnyFlatSpec
    with PekkoBeforeAndAfterAll
    with HasExecutionContext
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest {

  val party1: LfPartyId = LfPartyId.assertFromString("party1")
  val party2: LfPartyId = LfPartyId.assertFromString("party2")
  val party3: LfPartyId = LfPartyId.assertFromString("party3")

  def continuationTokenBytes(eventSeqId: Long): ByteString =
    AcsContinuationTokenPayload(
      pointer = Some(AcsContinuationPointerActiveContracts(eventSeqId).toPayload)
    ).toByteString

  def activeContractsResponses(seqIds: Long*): Source[GetActiveContractsResponse, NotUsed] =
    Source(
      seqIds.map(id =>
        GetActiveContractsResponse.defaultInstance
          .withWorkflowId(id.toString)
          .withStreamContinuationToken(continuationTokenBytes(id))
      )
    )

  behavior of "activeContracts"

  it should "convert continuation token successfully back and forth" in {
    AcsRangeInfo
      .assertFromContinuationTokenBytes(
        Some(
          continuationTokenBytes(11)
        )
      )
      .continuationPointer shouldBe Some(AcsContinuationPointerActiveContracts(11))
  }

  it should "work correctly in the happy path" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.getActiveContracts(
        eventFormat = any[EventFormat],
        activeAt = eqTo(Some(Offset.tryFromLong(10))),
        rangeInfo = eqTo(AcsRangeInfo.empty),
        configOverrides = any[Option[ActiveContractsServiceStreamsConfigOverrides]],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContractsResponses(1, 2, 3, 4, 5)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).activeContracts(
      partyIds = Set(party1, party2),
      validAt = Some(Offset.tryFromLong(10)),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.workflowId) shouldBe Seq(1, 2, 3, 4, 5).map(_.toString)
  }

  it should "retry a retryable error" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.getActiveContracts(
        eventFormat = any[EventFormat],
        activeAt = eqTo(Some(Offset.tryFromLong(10))),
        rangeInfo = eqTo(AcsRangeInfo.empty),
        configOverrides = any[Option[ActiveContractsServiceStreamsConfigOverrides]],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContractsResponses(1, 2, 3, 4, 5)
        .map(response =>
          if (response.workflowId == "3")
            throw IndexErrors.DatabaseErrors.SqlTransientError.Reject(new Exception).asGrpcError
          else response
        )
    )
    when(
      mockIndexService.getActiveContracts(
        eventFormat = any[EventFormat],
        activeAt = eqTo(Some(Offset.tryFromLong(10))),
        rangeInfo = eqTo(
          AcsRangeInfo.empty.copy(continuationPointer =
            Some(AcsContinuationPointerActiveContracts(2))
          )
        ),
        configOverrides = any[Option[ActiveContractsServiceStreamsConfigOverrides]],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContractsResponses(3, 4, 5)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    )
      .activeContracts(
        partyIds = Set(party1, party2),
        validAt = Some(Offset.tryFromLong(10)),
      )
      .toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.workflowId) shouldBe Seq(1, 2, 3, 4, 5).map(_.toString)
  }

  it should "not retry on a non-retryable error" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.getActiveContracts(
        eventFormat = any[EventFormat],
        activeAt = eqTo(Some(Offset.tryFromLong(10))),
        rangeInfo = eqTo(AcsRangeInfo.empty),
        configOverrides = any[Option[ActiveContractsServiceStreamsConfigOverrides]],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContractsResponses(1, 2, 3, 4, 5)
        .map(response =>
          if (response.workflowId == "3")
            throw IndexErrors.DatabaseErrors.SqlNonTransientError.Reject(new Exception).asGrpcError
          else response
        )
    )
    loggerFactory.assertLogs(
      new InternalIndexServiceImpl(
        indexService = mockIndexService,
        loggerFactory = loggerFactory,
      ).activeContracts(
        partyIds = Set(party1, party2),
        validAt = Some(Offset.tryFromLong(10)),
      ).toMat(Sink.seq)(Keep.right)
        .run()
        .failed
        .futureValue
        .getMessage should include("INTERNAL: An error occurred."),
      _.errorMessage should include("INDEX_DB_SQL_NON_TRANSIENT_ERROR"),
      _.warningMessage should include(
        "Internal stream [Internal Active Contracts Stream] completed with a failure"
      ),
    )
  }

  behavior of "topologyTransactions"

  def topologyTransactions(ids: Int*): Source[UpdatesResponse.ProtoUpdates, NotUsed] =
    Source(
      ids.map(id =>
        UpdatesResponse.ProtoUpdates(
          response = Some(
            GetUpdatesResponse.defaultInstance.withTopologyTransaction(
              TopologyTransaction.defaultInstance.withOffset(id.toLong)
            )
          ),
          synchronizerParametersResponse = None,
        )
      )
    )

  it should "work correctly in the happy path" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.updates(
        begin = eqTo(Some(Offset.tryFromLong(10))),
        endAt = eqTo(None),
        updateFormat = any[UpdateFormat],
        descendingOrder = any[Boolean],
        skipPruningChecks = any[Boolean],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      topologyTransactions(15, 16, 17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).topologyTransactions(
      partyId = party1,
      fromExclusive = Offset.tryFromLong(10),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.offset) shouldBe Seq(15, 16, 17, 18, 19)
  }

  it should "recover for retryable errors" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.updates(
        begin = eqTo(Some(Offset.tryFromLong(10))),
        endAt = eqTo(None),
        updateFormat = any[UpdateFormat],
        descendingOrder = any[Boolean],
        skipPruningChecks = any[Boolean],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      topologyTransactions(15, 16, 17, 18, 19).map(t =>
        if (t.response.value.getTopologyTransaction.offset == 17)
          throw IndexErrors.DatabaseErrors.SqlTransientError.Reject(new Exception).asGrpcError
        else t
      )
    )
    when(
      mockIndexService.updates(
        begin = eqTo(Some(Offset.tryFromLong(16))),
        endAt = eqTo(None),
        updateFormat = any[UpdateFormat],
        descendingOrder = any[Boolean],
        skipPruningChecks = any[Boolean],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      topologyTransactions(17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).topologyTransactions(
      partyId = party1,
      fromExclusive = Offset.tryFromLong(10),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.offset) shouldBe Seq(15, 16, 17, 18, 19)
  }

  behavior of "acsUpdates"

  val synchronizerId: SynchronizerId = SynchronizerId.tryFromString("x::synchronizerId1")

  def acsUpdates(ids: Int*): Source[UpdatesResponse.AcsChange, NotUsed] =
    Source(ids).map(id =>
      UpdatesResponse.AcsChange(
        AcsChangeUpdate(
          acsChange = AcsChange(Map.empty, Map.empty),
          offset = Offset.tryFromLong(id.toLong),
          recordTime = CantonTimestamp.assertFromLong(100000 + id.toLong),
          traceContext = implicitly,
        )
      )
    )

  it should "work correctly in the happy path" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.updates(
        begin = eqTo(Some(Offset.tryFromLong(10))),
        endAt = eqTo(None),
        updateFormat = any[UpdateFormat],
        descendingOrder = any[Boolean],
        skipPruningChecks = any[Boolean],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      acsUpdates(15, 16, 17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).acsUpdates(
      synchronizerId = synchronizerId,
      fromExclusive = Some(Offset.tryFromLong(10)),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.offset.unwrap.toInt) shouldBe Seq(15, 16, 17, 18, 19)
  }

  it should "recover for retryable errors" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.updates(
        begin = eqTo(Some(Offset.tryFromLong(10))),
        endAt = eqTo(None),
        updateFormat = any[UpdateFormat],
        descendingOrder = any[Boolean],
        skipPruningChecks = any[Boolean],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      acsUpdates(15, 16, 17, 18, 19).map(t =>
        if (t.change.offset.unwrap == 17)
          throw IndexErrors.DatabaseErrors.SqlTransientError.Reject(new Exception).asGrpcError
        else t
      )
    )
    when(
      mockIndexService.updates(
        begin = eqTo(Some(Offset.tryFromLong(16))),
        endAt = eqTo(None),
        updateFormat = any[UpdateFormat],
        descendingOrder = any[Boolean],
        skipPruningChecks = any[Boolean],
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      acsUpdates(17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).acsUpdates(
      synchronizerId = synchronizerId,
      fromExclusive = Some(Offset.tryFromLong(10)),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.offset.unwrap.toInt) shouldBe Seq(15, 16, 17, 18, 19)
  }

  behavior of "acs"

  def activeContracts(seqIds: Long*): Source[InternalIndexService.ActiveContract, NotUsed] =
    Source(
      seqIds.map(id =>
        InternalIndexService.ActiveContract(
          contractId = ContractId.V1(Hash.hashPrivateKey(id.toString)),
          stakeholders = Set(party1, party2) ++ (
            if (id == 19) Set(party3)
            else Set.empty
          ),
          reassignmentCounter = ReassignmentCounter(id),
          continuationToken = continuationTokenBytes(id),
        )
      )
    )

  it should "work correctly in the happy path" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.acs(
        synchronizerId = eqTo(synchronizerId),
        activeAt = eqTo(Offset.tryFromLong(10)),
        stakeholders1 = eqTo(Set(party1, party2)),
        stakeholders2 = eqTo(Set.empty),
        configOverrides = eqTo(ActiveContractsServiceStreamsConfigOverrides(10, 10)),
        continuationToken = eqTo(None),
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContracts(15, 16, 17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).acs(
      synchronizerId = synchronizerId,
      activeAt = Offset.tryFromLong(10),
      stakeholders1 = Set(party1, party2),
      stakeholders2 = Set.empty,
      configOverrides = ActiveContractsServiceStreamsConfigOverrides(10, 10),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.reassignmentCounter.unwrap.toInt) shouldBe Seq(15, 16, 17, 18, 19)
  }

  it should "recover for retryable errors" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.acs(
        synchronizerId = eqTo(synchronizerId),
        activeAt = eqTo(Offset.tryFromLong(10)),
        stakeholders1 = eqTo(Set(party1, party2)),
        stakeholders2 = eqTo(Set.empty),
        configOverrides = eqTo(ActiveContractsServiceStreamsConfigOverrides(10, 10)),
        continuationToken = eqTo(None),
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContracts(15, 16, 17, 18, 19).map(t =>
        if (t.reassignmentCounter.unwrap.toInt == 17)
          throw IndexErrors.DatabaseErrors.SqlTransientError.Reject(new Exception).asGrpcError
        else t
      )
    )
    when(
      mockIndexService.acs(
        synchronizerId = eqTo(synchronizerId),
        activeAt = eqTo(Offset.tryFromLong(10)),
        stakeholders1 = eqTo(Set(party1, party2)),
        stakeholders2 = eqTo(Set.empty),
        configOverrides = eqTo(ActiveContractsServiceStreamsConfigOverrides(10, 10)),
        continuationToken = eqTo(Some(continuationTokenBytes(16))),
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContracts(17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    ).acs(
      synchronizerId = synchronizerId,
      activeAt = Offset.tryFromLong(10),
      stakeholders1 = Set(party1, party2),
      stakeholders2 = Set.empty,
      configOverrides = ActiveContractsServiceStreamsConfigOverrides(10, 10),
    ).toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .map(_.reassignmentCounter.unwrap.toInt) shouldBe Seq(15, 16, 17, 18, 19)
  }

  behavior of "counterParties"

  it should "work correctly in the happy path" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.acs(
        synchronizerId = eqTo(synchronizerId),
        activeAt = eqTo(Offset.tryFromLong(10)),
        stakeholders1 = eqTo(Set.empty),
        stakeholders2 = eqTo(Set.empty),
        configOverrides = eqTo(ActiveContractsServiceStreamsConfigOverrides(10, 10)),
        continuationToken = eqTo(None),
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContracts(15, 16, 17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    )
      .counterParties(
        synchronizerId = synchronizerId,
        activeAt = Offset.tryFromLong(10),
        party = None,
        configOverrides = ActiveContractsServiceStreamsConfigOverrides(10, 10),
      )
      .toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .toSet shouldBe Set(party1, party2, party3)
  }

  it should "recover for retryable errors" in {
    val mockIndexService = mock[IndexService]
    when(
      mockIndexService.acs(
        synchronizerId = eqTo(synchronizerId),
        activeAt = eqTo(Offset.tryFromLong(10)),
        stakeholders1 = eqTo(Set.empty),
        stakeholders2 = eqTo(Set.empty),
        configOverrides = eqTo(ActiveContractsServiceStreamsConfigOverrides(10, 10)),
        continuationToken = eqTo(None),
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContracts(15, 16, 17, 18, 19).map(t =>
        if (t.reassignmentCounter.unwrap.toInt == 17)
          throw IndexErrors.DatabaseErrors.SqlTransientError.Reject(new Exception).asGrpcError
        else t
      )
    )
    when(
      mockIndexService.acs(
        synchronizerId = eqTo(synchronizerId),
        activeAt = eqTo(Offset.tryFromLong(10)),
        stakeholders1 = eqTo(Set.empty),
        stakeholders2 = eqTo(Set.empty),
        configOverrides = eqTo(ActiveContractsServiceStreamsConfigOverrides(10, 10)),
        continuationToken = eqTo(Some(continuationTokenBytes(16))),
      )(any[LoggingContextWithTrace])
    ).thenReturn(
      activeContracts(17, 18, 19)
    )
    new InternalIndexServiceImpl(
      indexService = mockIndexService,
      loggerFactory = loggerFactory,
    )
      .counterParties(
        synchronizerId = synchronizerId,
        activeAt = Offset.tryFromLong(10),
        party = None,
        configOverrides = ActiveContractsServiceStreamsConfigOverrides(10, 10),
      )
      .toMat(Sink.seq)(Keep.right)
      .run()
      .futureValue
      .toSet shouldBe Set(party1, party2, party3)
  }
}
