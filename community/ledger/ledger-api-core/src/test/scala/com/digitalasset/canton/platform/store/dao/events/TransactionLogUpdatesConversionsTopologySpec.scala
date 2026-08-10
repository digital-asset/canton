// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.{ParticipantAuthorizationFormat, TopologyFormat}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.Added
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.Observation
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.GenericTopologyEvent.SynchronizerParametersState
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.InternalUpdateFormat
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class TransactionLogUpdatesConversionsTopologySpec
    extends AsyncFlatSpec
    with Matchers
    with BaseTest
    with HasExecutionContext
    with MockitoSugar {

  import TransactionLogUpdatesConversionsTopologySpec.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

  // The topology conversion path never touches LfValueTranslation, so a null is sufficient here.
  private val noLfValueTranslation: LfValueTranslation = null

  behavior of "TransactionLogUpdatesConversions.filter (topology)"

  it should "keep party events when the authorization format matches and drop synchronizer params if not requested" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        )
      )
      .apply(topologyTx(withParty = true, withSynchronizerParams = true))

    result shouldBe Some(
      topologyTx(withParty = true, withSynchronizerParams = false)
    )
  }

  it should "keep synchronizer params only when requested with a matching synchronizer" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = None,
              synchronizerParametersFormat = true,
              synchronizerId = Some(synchronizerId1),
            )
          )
        )
      )
      .apply(topologyTx(withParty = false, withSynchronizerParams = true))

    result shouldBe Some(
      topologyTx(withParty = false, withSynchronizerParams = true)
    )
  }

  it should "drop synchronizer params when not requested" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = None,
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        )
      )
      .apply(topologyTx(withParty = false, withSynchronizerParams = true))

    result shouldBe None
  }

  it should "drop synchronizer params when the requested synchronizer does not match" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = None,
              synchronizerParametersFormat = true,
              synchronizerId = Some(synchronizerId2),
            )
          )
        )
      )
      .apply(topologyTx(withParty = false, withSynchronizerParams = true))

    result shouldBe None
  }

  it should "keep both party events and synchronizer params when both requested and matching" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = true,
              synchronizerId = Some(synchronizerId1),
            )
          )
        )
      )
      .apply(topologyTx(withParty = true, withSynchronizerParams = true))

    result shouldBe Some(
      topologyTx(withParty = true, withSynchronizerParams = true)
    )
  }

  it should "drop the update when neither party events nor synchronizer params survive" in {
    val result = TransactionLogUpdatesConversions
      .filter(
        updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = true,
              synchronizerId = Some(synchronizerId1),
            )
          )
        )
      )
      .apply(topologyTx(withParty = false, withSynchronizerParams = false))

    result shouldBe None
  }

  it should "drop the update when topology events are not requested at all" in {
    val result = TransactionLogUpdatesConversions
      .filter(updateFormatFor(topologyFormat = None))
      .apply(topologyTx(withParty = true, withSynchronizerParams = true))

    result shouldBe None
  }

  behavior of "TransactionLogUpdatesConversions.toUpdateResponse (topology)"

  it should "emit Update.Empty with the synchronizer parameters response for a params-only update" in {
    TransactionLogUpdatesConversions
      .toUpdateResponse(
        updateFormatFor(topologyFormat = None),
        noLfValueTranslation,
      )
      .apply(topologyTx(withParty = false, withSynchronizerParams = true))
      .map {
        case UpdateResponse.ProtoUpdate(response, synchronizerParametersResponse) =>
          response shouldBe empty
          synchronizerParametersResponse should not be empty
        case other => fail(s"Unexpected response: $other")
      }
  }

  it should "emit a TopologyTransaction when party events are present" in {
    TransactionLogUpdatesConversions
      .toUpdateResponse(
        updateFormatFor(topologyFormat = None),
        noLfValueTranslation,
      )
      .apply(topologyTx(withParty = true, withSynchronizerParams = false))
      .map {
        case UpdateResponse.ProtoUpdate(Some(response), synchronizerParametersResponse) =>
          response.update.topologyTransaction should not be empty
          synchronizerParametersResponse shouldBe empty
        case other => fail(s"Unexpected response: $other")
      }
  }

  behavior of "TransactionLogUpdatesConversions.toGetUpdateResponse (topology)"

  it should "not surface an empty topology transaction in the pointwise endpoint" in {
    TransactionLogUpdatesConversions
      .toGetUpdateResponse(
        transactionLogUpdate = topologyTx(withParty = false, withSynchronizerParams = true),
        internalUpdateFormat = updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        ),
        lfValueTranslation = noLfValueTranslation,
      )
      .map(_ shouldBe None)
  }

  it should "surface a non-empty topology transaction in the pointwise endpoint" in {
    TransactionLogUpdatesConversions
      .toGetUpdateResponse(
        transactionLogUpdate = topologyTx(withParty = true, withSynchronizerParams = false),
        internalUpdateFormat = updateFormatFor(
          topologyFormat = Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        ),
        lfValueTranslation = noLfValueTranslation,
      )
      .map { responseO =>
        responseO.map(_.update.topologyTransaction) should not be empty
      }
  }

}

object TransactionLogUpdatesConversionsTopologySpec {

  private val synchronizerId1: SynchronizerId = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizerId2: SynchronizerId = SynchronizerId.tryFromString("x::synchronizer2")

  private val party: Ref.Party = Ref.Party.assertFromString("someparty")
  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("participant1")

  private def topologyTx(
      withParty: Boolean,
      withSynchronizerParams: Boolean,
  ): TransactionLogUpdate.TopologyTransactionEffective = {
    implicit val traceContext: TraceContext = TraceContext.empty
    TransactionLogUpdate.TopologyTransactionEffective(
      updateId = TestUpdateId("some-topology-update").toHexString,
      offset = Offset.tryFromLong(15L),
      effectiveTime = Time.Timestamp.Epoch,
      synchronizerId = synchronizerId1.toProtoPrimitive,
      events =
        if (withParty)
          Vector(
            TransactionLogUpdate.PartyToParticipantAuthorization(
              party = party,
              participant = participantId,
              authorizationEvent = Added(Observation),
            )
          )
        else Vector.empty,
      synchronizerParametersState = Option.when(withSynchronizerParams)(
        SynchronizerParametersState(ByteString.copyFromUtf8("params"))
      ),
    )
  }

  private def updateFormatFor(
      topologyFormat: Option[TopologyFormat]
  ): InternalUpdateFormat =
    InternalUpdateFormat(
      includeTransactions = None,
      includeReassignments = None,
      includeTopologyEvents = topologyFormat,
      includeAcsCommitments = None,
    )
}
