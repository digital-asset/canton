// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.platform.InternalUpdateFormat
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransactionLogUpdatesConversionsAcsCommitmentSpec
    extends AnyFlatSpec
    with Matchers
    with BaseTest
    with HasExecutionContext {

  import TransactionLogUpdatesConversionsAcsCommitmentSpec.*

  behavior of "TransactionLogUpdatesConversions.filter (acs commitments)"

  it should "keep the commitment when the format requests the matching synchronizer" in {
    val result = TransactionLogUpdatesConversions
      .filter(updateFormatFor(Some(synchronizerId1)))
      .apply(commitment(synchronizerId1))

    result shouldBe Some(commitment(synchronizerId1))
  }

  it should "drop the commitment when the format requests a different synchronizer" in {
    val result = TransactionLogUpdatesConversions
      .filter(updateFormatFor(Some(synchronizerId2)))
      .apply(commitment(synchronizerId1))

    result shouldBe None
  }

  it should "drop the commitment when the format does not request acs commitments" in {
    val result = TransactionLogUpdatesConversions
      .filter(updateFormatFor(None))
      .apply(commitment(synchronizerId1))

    result shouldBe None
  }

}

object TransactionLogUpdatesConversionsAcsCommitmentSpec {

  private val synchronizerId1: SynchronizerId = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizerId2: SynchronizerId = SynchronizerId.tryFromString("x::synchronizer2")

  private def commitment(
      synchronizerId: SynchronizerId
  ): TransactionLogUpdate.ReceivedAcsCommitment = {
    implicit val traceContext: TraceContext = TraceContext.empty
    TransactionLogUpdate.ReceivedAcsCommitment(
      offset = Offset.tryFromLong(15L),
      update = Update.ReceivedAcsCommitment(
        synchronizerId = synchronizerId,
        recordTime = CantonTimestamp.ofEpochMicro(12345678L),
        payload = ByteString.copyFromUtf8("some-acs-commitment-payload"),
        updateId = TestUpdateId("some-update-id"),
      ),
    )
  }

  private def updateFormatFor(
      includeAcsCommitments: Option[SynchronizerId]
  ): InternalUpdateFormat =
    InternalUpdateFormat(
      includeTransactions = None,
      includeReassignments = None,
      includeTopologyEvents = None,
      includeAcsCommitments = includeAcsCommitments,
    )
}
