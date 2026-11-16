// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationParameters
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationParameters.AcsReplicationDestination
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationProcessor
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.topology.{DefaultTestIdentities, PartyId}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, RepairCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayOutputStream

/** Verifies that the AcsReplicationStateManager only accepts progress from the current
  * AcsReplicationProcessor and rejects progress from superseded processors, for example after a
  * restart of the ACS replication.
  */
class AcsReplicationStateManagerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val requestId = TestHash.digest(0)

  private def outputStreamDestination: AcsReplicationDestination.OutputStream =
    AcsReplicationDestination.OutputStream(new ByteArrayOutputStream())(abort = () => ())

  private def mkStateManager(): AcsReplicationStateManager = {
    val stateManager = new AcsReplicationStateManager(
      DefaultTestIdentities.participant2,
      new MemoryStorage(loggerFactory, timeouts),
      futureSupervisor,
      exitOnFatalFailures = false,
      loggerFactory,
      timeouts,
    )
    stateManager
      .add(
        AcsReplicationStatus(
          AcsReplicationParameters(
            requestId,
            PartyId.tryFromProtoPrimitive("alice::default"),
            DefaultTestIdentities.synchronizerId,
            DefaultTestIdentities.participant1,
            DefaultTestIdentities.participant2,
            CantonTimestamp.ofEpochSecond(10),
            outputStreamDestination,
          ),
          testedProtocolVersion,
        )
      )
      .futureValueUS
      .value
    stateManager
  }

  private def progressFromProcessor(
      processor: AcsReplicationProcessor
  ): AcsReplicationStatus.AcsReplicationProgress =
    AcsReplicationStatus.EphemeralSequencerChannelProgress(
      NonNegativeLong.tryCreate(5L),
      RepairCounter.Genesis,
      acsHashO = None,
      fullyProcessedAcs = false,
      Some(processor),
    )

  private def processedContractCount(stateManager: AcsReplicationStateManager): Option[Long] =
    stateManager.get(requestId).flatMap(_.replicationO).map(_.processedContractCount.unwrap)

  "AcsReplicationStateManager" should {
    "accept progress of the current processor" in {
      val stateManager = mkStateManager()
      val processor = mock[AcsReplicationProcessor]
      stateManager.update_(requestId, _.setProcessor(processor)).futureValueUS.value

      stateManager
        .updateAcsReplicationProgress(requestId, progressFromProcessor(processor))
        .futureValueUS
        .value

      processedContractCount(stateManager) shouldBe Some(5L)
      stateManager.close()
    }

    "reject progress of a processor that has been replaced" in {
      val stateManager = mkStateManager()
      val oldProcessor = mock[AcsReplicationProcessor]
      val newProcessor = mock[AcsReplicationProcessor]
      stateManager.update_(requestId, _.setProcessor(oldProcessor)).futureValueUS.value
      stateManager.update_(requestId, _.setProcessor(newProcessor)).futureValueUS.value

      stateManager
        .updateAcsReplicationProgress(requestId, progressFromProcessor(oldProcessor))
        .futureValueUS
        .left
        .value should include("superseded ACS replication processor")

      processedContractCount(stateManager) shouldBe Some(0L)
      stateManager.get(requestId).flatMap(_.replicationO).flatMap(_.processorO) shouldBe Some(
        newProcessor
      )
      stateManager.close()
    }

    "reject progress of a processor of a restarted replication" in {
      val stateManager = mkStateManager()
      val oldProcessor = mock[AcsReplicationProcessor]
      stateManager.update_(requestId, _.setProcessor(oldProcessor)).futureValueUS.value
      stateManager.resetProgress(requestId, outputStreamDestination).futureValueUS.value

      stateManager
        .updateAcsReplicationProgress(requestId, progressFromProcessor(oldProcessor))
        .futureValueUS
        .left
        .value should include("superseded ACS replication processor")

      stateManager.get(requestId).value.replicationO shouldBe None
      stateManager.close()
    }
  }
}
