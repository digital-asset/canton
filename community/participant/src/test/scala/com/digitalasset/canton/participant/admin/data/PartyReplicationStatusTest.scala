// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiPartyReplicationStatus
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus as InternalStatus
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class PartyReplicationStatusTest extends AnyWordSpec with BaseTest {

  private val dummyParams = InternalStatus.ReplicationParams(
    requestId = Hash.digest(
      HashPurpose.OnlinePartyReplicationId,
      ByteString.copyFromUtf8("dummy-request-id"),
      HashAlgorithm.Sha256,
    ),
    partyId = PartyId.tryFromProtoPrimitive("alice::1220abcd"),
    synchronizerId = SynchronizerId.tryFromString("da::1220abcd"),
    sourceParticipantId = ParticipantId.tryFromProtoPrimitive("PAR::source::1220abcd"),
    targetParticipantId = ParticipantId.tryFromProtoPrimitive("PAR::target::1220abcd"),
    serial = PositiveInt.one,
    participantPermission = ParticipantPermission.Submission,
  )

  private def createInternalStatus(hasCompleted: Boolean, errorO: Option[String]): InternalStatus =
    InternalStatus(
      params = dummyParams,
      pv = testedProtocolVersion,
      agreementO = None,
      authorizationO = None,
      replicationO = None,
      indexingO = None,
      hasCompleted = hasCompleted,
      errorO = errorO.map(InternalStatus.PartyReplicationFailed.apply),
    )

  "PartyReplicationStatus mapping to LAPI Proto" should {

    "map to STATE_IN_PROGRESS when not completed and no error is present" in {
      val internal = createInternalStatus(hasCompleted = false, errorO = None)
      val lapiStatus = PartyReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.state shouldBe LapiPartyReplicationStatus.State.STATE_IN_PROGRESS
      lapiStatus.error shouldBe empty
    }

    "map to STATE_COMPLETED when completed and no error is present" in {
      val internal = createInternalStatus(hasCompleted = true, errorO = None)
      val lapiStatus = PartyReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.state shouldBe LapiPartyReplicationStatus.State.STATE_COMPLETED
      lapiStatus.error shouldBe empty
    }

    "map to STATE_FAILED when an error is present" in {
      val internal = createInternalStatus(
        hasCompleted = false,
        errorO = Some("Network timeout during ACS import"),
      )
      val lapiStatus = PartyReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.state shouldBe LapiPartyReplicationStatus.State.STATE_FAILED
      lapiStatus.error.value.message shouldBe "Network timeout during ACS import"
    }

    "map to STATE_FAILED even if hasCompleted is true, if an error is present" in {
      val internal =
        createInternalStatus(hasCompleted = true, errorO = Some("Terminal failure during cleanup"))
      val lapiStatus = PartyReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.state shouldBe LapiPartyReplicationStatus.State.STATE_FAILED
      lapiStatus.error.value.message shouldBe "Terminal failure during cleanup"
    }
  }
}
