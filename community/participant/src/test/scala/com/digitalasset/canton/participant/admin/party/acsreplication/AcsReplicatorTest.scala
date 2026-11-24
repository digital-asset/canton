// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AcsReplicationParameters.AcsReplicationDestination
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.AgreementStatus
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicator.AcsReplicationArguments
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.config.AlphaAcsReplicationConfig
import com.digitalasset.canton.participant.protocol.party.AcsReplicationContractHandlerFactory
import com.digitalasset.canton.participant.store.{
  StoredSynchronizerConnectionConfig,
  SyncPersistentState,
  SynchronizerConnectionConfigStore,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerConnectionConfig,
  SynchronizerHandle,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  KnownPhysicalSynchronizerId,
  ParticipantId,
  TestingTopology,
}
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasActorSystem,
  HasExecutionContext,
  NeedsNewLfContractIds,
  SequencerAlias,
  SynchronizerAlias,
}
import com.digitalasset.nonempty.NonEmpty
import org.slf4j.event.Level

import java.io.OutputStream

/** Verifies the behavior of [[AcsReplicator]].
  *
  * Current verifications
  *
  *   - ACS replication `asOf` (`timestamp`) parameter should not be higher than the latest topology
  *     snapshot timestamp: validated when the replication is initiated and during proposal and
  *     agreement processing
  */
class AcsReplicatorTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with HasActorSystem
    with NeedsNewLfContractIds {

  private val sourceParticipant = DefaultTestIdentities.participant1
  private val targetParticipant = DefaultTestIdentities.participant2
  private val party = DefaultTestIdentities.party1
  private val psid = DefaultTestIdentities.physicalSynchronizerId
  private val synchronizerId = DefaultTestIdentities.synchronizerId
  private val sequencerId = DefaultTestIdentities.sequencerId

  private val topologyKnownUntil = CantonTimestamp.Epoch.plusSeconds(60)
  private val timestampWithinKnownTopology = topologyKnownUntil
  private val timestampBeyondKnownTopology = topologyKnownUntil.immediateSuccessor

  private val topologyErrorMessage = "topology is known until"

  private lazy val identityFactory = TestingTopology()
    .withSimpleParticipants(sourceParticipant, targetParticipant)
    .withReversedTopology(
      Map(
        sourceParticipant -> Map(party.toLf -> ParticipantPermission.Submission),
        targetParticipant -> Map(party.toLf -> ParticipantPermission.Submission),
      )
    )
    .build(loggerFactory)

  private lazy val synchronizerConnectionConfig = SynchronizerConnectionConfig(
    SynchronizerAlias.tryCreate("da"),
    SequencerConnections.single(
      GrpcSequencerConnection(
        NonEmpty(Set, Endpoint("host1", Port.tryCreate(500))),
        transportSecurity = false,
        None,
        SequencerAlias.tryCreate("sequencer1"),
        sequencerId = None,
      )
    ),
    manualConnect = false,
    Some(psid),
    42,
    None,
    None,
    SynchronizerTimeTrackerConfig(),
  )

  /** Creates an [[AcsReplicator]] for the `localParticipant` connected to a synchronizer whose
    * topology is known until `topologyKnownUntil`, runs `test` and closes the replicator.
    */
  private def withAcsReplicator[A](localParticipant: ParticipantId)(
      test: AcsReplicator => A
  ): A = {
    val syncService = mock[CantonSyncService]
    val connectedSynchronizer = mock[ConnectedSynchronizer]
    val synchronizerHandle = mock[SynchronizerHandle]
    val syncPersistentState = mock[SyncPersistentState]
    val topologyClient = mock[SynchronizerTopologyClientWithInit]
    val configStore = mock[SynchronizerConnectionConfigStore]

    when(syncService.isActive).thenReturn(() => true)
    when(syncService.readyConnectedSynchronizerById(synchronizerId))
      .thenReturn(Some(connectedSynchronizer))
    when(syncService.synchronizerConnectionConfigStore).thenReturn(configStore)
    when(configStore.getActive(synchronizerId)).thenReturn(
      Right(
        StoredSynchronizerConnectionConfig(
          synchronizerConnectionConfig,
          SynchronizerConnectionConfigStore.Active,
          KnownPhysicalSynchronizerId(psid),
          None,
        )
      )
    )

    when(connectedSynchronizer.psid).thenReturn(psid)
    when(connectedSynchronizer.synchronizerCrypto).thenReturn(
      identityFactory.forOwnerAndSynchronizer(
        localParticipant,
        psid,
        availableUpToInclusive = topologyKnownUntil,
      )
    )
    when(connectedSynchronizer.synchronizerHandle).thenReturn(synchronizerHandle)
    // Without a channel client, ACS replication proposals cannot pass sequencer selection.
    when(connectedSynchronizer.sequencerChannelClientO).thenReturn(None)

    when(synchronizerHandle.syncPersistentState).thenReturn(syncPersistentState)
    when(synchronizerHandle.topologyClient).thenReturn(topologyClient)
    when(topologyClient.headSnapshot(anyTraceContext))
      .thenReturn(identityFactory.topologySnapshot(synchronizerId))

    when(syncPersistentState.topologyStore)
      .thenReturn(identityFactory.topologyStore(synchronizerId))
    when(syncPersistentState.psid).thenReturn(psid)

    val clock = new SimClock(loggerFactory = loggerFactory)
    val storage = new MemoryStorage(loggerFactory, timeouts)
    val replicator = new AcsReplicator(
      localParticipant,
      syncService,
      mock[InternalIndexService],
      clock,
      AlphaAcsReplicationConfig(),
      storage,
      mock[AcsReplicationContractHandlerFactory],
      FutureSupervisor.Noop,
      exitOnFatalFailures = false,
      timeouts,
      loggerFactory,
    )
    replicator.initializeDamlAdminWorkflow(mock[AcsReplicationAdminWorkflow])

    try test(replicator)
    finally {
      replicator.close()
      storage.close()
      clock.close()
    }
  }

  "replicateAcsAsync" should {
    def replicateArgs(timestamp: CantonTimestamp) = AcsReplicationArguments(
      TestHash.digest("replicate-acs"),
      party,
      synchronizerId,
      sourceParticipant,
      timestamp,
      AcsReplicationDestination.OutputStream(OutputStream.nullOutputStream())(() => ()),
    )

    "reject an ACS replication with a timestamp higher than latest topology snapshot timestamp" in
      withAcsReplicator(targetParticipant) { replicator =>
        val args = replicateArgs(timestampBeyondKnownTopology)

        val error = replicator.replicateAcsAsync(args).value.futureValueUS.left.value

        error should include(topologyErrorMessage)
        replicator.getAcsReplicationStatus(args.requestId) shouldBe None
      }

    "accept an ACS replication with a timestamp less or equal to latest topology snapshot timestamp" in
      withAcsReplicator(targetParticipant) { replicator =>
        val args = replicateArgs(timestampWithinKnownTopology)

        val status = replicator.replicateAcsAsync(args).value.futureValueUS.value

        status.params.requestId shouldBe args.requestId
        status.agreementStatus shouldBe AgreementStatus.NotProposed
        status.errorO shouldBe None
        replicator.getAcsReplicationStatus(args.requestId) should not be empty
      }
  }

  "processAcsReplicationProposalAtSourceParticipant" should {
    def proposalParams(timestamp: CantonTimestamp): AcsReplicationProposalParams =
      AcsReplicationProposalParams
        .fromDaml(
          newLfContractId().coid,
          new M.acsreplication.AcsReplicationProposal(
            TestHash.digest("proposal").toHexString,
            party.toProtoPrimitive,
            sourceParticipant.adminParty.toProtoPrimitive,
            targetParticipant.adminParty.toProtoPrimitive,
            java.util.List.of(sequencerId.uid.toProtoPrimitive),
            timestamp.toInstant,
            "test-operation",
          ),
          synchronizerId.toProtoPrimitive,
        )
        .value

    def processProposal(
        replicator: AcsReplicator,
        proposal: AcsReplicationProposalParams,
    ): (Either[String, Unit], Option[Either[String, AcsReplicationAgreementParams]]) = {
      var response: Option[Either[String, AcsReplicationAgreementParams]] = None
      val result = replicator
        .processAcsReplicationProposalAtSourceParticipant(
          Right(proposal),
          agreementOrError => response = Some(agreementOrError),
        )
        .futureValueUS
      (result, response)
    }

    def assertProposalRejectedAndRecorded(
        replicator: AcsReplicator,
        proposal: AcsReplicationProposalParams,
        response: Option[Either[String, AcsReplicationAgreementParams]],
    ) = {
      clue("the proposal is rejected")(response.value.left.value should not be empty)
      val status = replicator.getAcsReplicationStatus(proposal.requestId).value
      status.agreementStatus shouldBe AgreementStatus.Proposed(proposal.proposalContractId)
      status.errorO should not be empty
    }

    "reject a proposal with a timestamp higher than latest topology snapshot timestamp" in
      withAcsReplicator(sourceParticipant) { replicator =>
        val proposal = proposalParams(timestampBeyondKnownTopology)

        val (result, response) = processProposal(replicator, proposal)

        result.left.value should include(topologyErrorMessage)
        assertProposalRejectedAndRecorded(replicator, proposal, response)
      }

    "pass topology verification for a proposal with a timestamp less or equal to latest topology snapshot timestamp" in
      withAcsReplicator(sourceParticipant) { replicator =>
        val proposal = proposalParams(timestampWithinKnownTopology)

        val (result, response) = processProposal(replicator, proposal)

        // The proposal fails later at sequencer selection because no channel client is configured,
        // which proves that the topology verification has passed.
        val error = result.left.value
        error should not include topologyErrorMessage
        error shouldBe "Channel client not configured"
        assertProposalRejectedAndRecorded(replicator, proposal, response)
      }
  }

  "processAcsReplicationAgreement" when {
    def agreementParams(timestamp: CantonTimestamp) = AcsReplicationAgreementParams(
      TestHash.digest("agreement"),
      party,
      synchronizerId,
      sourceParticipant,
      targetParticipant,
      sequencerId,
      timestamp,
    )

    def processUntrackedAgreement(
        replicator: AcsReplicator,
        agreementCid: LfContractId,
        agreedAt: CantonTimestamp,
        params: AcsReplicationAgreementParams,
    ): Unit = {
      replicator.getAcsReplicationStatus(params.requestId) shouldBe None
      replicator.processAcsReplicationAgreement(
        agreementCid,
        agreedAt,
        mightNotRememberProposal = true,
      )(params)
    }

    "the agreement is not tracked at the source participant" should {
      "reject an agreement with a timestamp higher than latest topology snapshot timestamp" in
        withAcsReplicator(sourceParticipant) { replicator =>
          val params = agreementParams(timestampBeyondKnownTopology)

          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              processUntrackedAgreement(
                replicator,
                newLfContractId(),
                CantonTimestamp.Epoch,
                params,
              )
              eventually()(loggerFactory.numberOfRecordedEntries should be > 0)
            },
            logEntries =>
              forExactly(1, logEntries)(
                _.warningMessage should (include(
                  s"Unable to process party replication ${params.requestId} agreement"
                ) and include(topologyErrorMessage))
              ),
          )
          replicator.getAcsReplicationStatus(params.requestId) shouldBe None
        }

      "accept an agreement with a timestamp less or equal to latest topology snapshot timestamp" in
        withAcsReplicator(sourceParticipant) { replicator =>
          val params = agreementParams(timestampWithinKnownTopology)
          val agreementCid = newLfContractId()
          val agreedAt = CantonTimestamp.Epoch.plusSeconds(1)

          processUntrackedAgreement(replicator, agreementCid, agreedAt, params)

          eventually() {
            val status = replicator.getAcsReplicationStatus(params.requestId).value
            status.agreementStatus shouldBe AgreementStatus.Exists(
              agreementCid,
              agreedAt,
              sequencerId,
            )
            status.errorO shouldBe None
          }
        }
    }
  }
}
