// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiAcsReplicationStatus
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus as InternalStatus
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class AcsReplicationStatusTest extends AnyWordSpec with BaseTest {

  private val dummyParams = InternalStatus.AcsReplicationParameters(
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

  private def createInternalStatus(
      hasCompleted: Boolean,
      errorO: Option[InternalStatus.AcsReplicationError],
  ): InternalStatus =
    InternalStatus(
      params = dummyParams,
      pv = testedProtocolVersion,
      authorizationO = None,
      replicationO = None,
      agreementStatus = InternalStatus.AgreementStatus.Proposed,
      hasCompleted = hasCompleted,
      errorO = errorO,
    )

  "AcsReplicationStatus mapping to LAPI Proto" should {

    "map to STATE_IN_PROGRESS when not completed and no error is present" in {
      val internal = createInternalStatus(hasCompleted = false, errorO = None)
      val lapiStatus = AcsReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.current shouldBe LapiAcsReplicationStatus.State.STATE_IN_PROGRESS
      lapiStatus.error shouldBe empty
    }

    "map to STATE_COMPLETED when completed and no error is present" in {
      val internal = createInternalStatus(hasCompleted = true, errorO = None)
      val lapiStatus = AcsReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.current shouldBe LapiAcsReplicationStatus.State.STATE_COMPLETED
      lapiStatus.error shouldBe empty
    }

    "map to STATE_FAILED when an error is present" in {
      val internal = createInternalStatus(
        hasCompleted = false,
        errorO = Some(InternalStatus.AcsReplicationFailed("Terminal failure")),
      )
      val lapiStatus = AcsReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.current shouldBe LapiAcsReplicationStatus.State.STATE_FAILED
      lapiStatus.error.value.message shouldBe "Terminal failure"
    }

    "map to STATE_IN_PROGRESS when a disconnected error is present" in {
      val internal = createInternalStatus(
        hasCompleted = false,
        errorO = Some(InternalStatus.Disconnected("Sequencer disconnected")),
      )
      val lapiStatus = AcsReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.current shouldBe LapiAcsReplicationStatus.State.STATE_IN_PROGRESS
      lapiStatus.error shouldBe empty
    }

    "map to STATE_FAILED even if hasCompleted is true, if an error is present" in {
      val internal =
        createInternalStatus(
          hasCompleted = true,
          Some(InternalStatus.AcsReplicationFailed("Terminal failure during cleanup")),
        )
      val lapiStatus = AcsReplicationStatus.fromInternal(internal).toLapiProto

      lapiStatus.current shouldBe LapiAcsReplicationStatus.State.STATE_FAILED
      lapiStatus.error.value.message shouldBe "Terminal failure during cleanup"
    }
  }
}
