// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiPartyReplicationStatus
import com.daml.ledger.api.v2.event.Event.Event.{Created, Exercised}
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.state_service.ParticipantPermission as LapiParticipantPermission
import com.daml.ledger.api.v2.topology_transaction.*
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.value.Value.Sum.Party
import com.daml.ledger.api.v2.value.{RecordField, Value}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalInstanceReference,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.bftsequencer.AwaitsBftSequencerAuthenticationDisseminationQuorum
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationAdminWorkflow
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.config.AlphaOnlinePartyReplicationConfig
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.CircuitBreakerConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.{BftSequencer, External}
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Objective: Test the negotiation of party replication via the AcsReplication.daml workflow.
  *
  * Setup:
  *   - 4 participants: the first two host the party to replicate and the third one doesn't for
  *     negative testing. The fourth participant hosts the party but shares no channel-capable
  *     sequencer with the other participants.
  *   - 6 sequencers: sequencer2 and sequencer3 are connected to both participants 1 and 2 (see
  *     connectivityMap), and only sequencer1, sequencer2, sequencer4, sequencer5 and sequencer 6
  *     support channels (see selectivelyEnableSequencerChannels).
  */
sealed trait OnlinePartyReplicationNegotiationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AwaitsBftSequencerAuthenticationDisseminationQuorum {

  private def connectivityMap(implicit env: TestConsoleEnvironment) = {
    // Connect participants 1 and 2 to different sequencers to test negotiating sequencer
    import env.*
    Map(
      participant1 -> Seq(sequencer1, sequencer2, sequencer3),
      participant2 -> Seq(sequencer2, sequencer3, sequencer4),
      participant3 -> Seq(sequencer1, sequencer2, sequencer3),
      participant4 -> Seq(sequencer3, sequencer5, sequencer6),
    )
  }

  private def selectivelyEnableSequencerChannels(
      sequencer: String,
      config: SequencerNodeConfig,
  ): SequencerNodeConfig =
    // Enable channels on all sequencer except sequencer3 to test
    // that a usable sequencer is chosen.
    config
      .focus(_.parameters.unsafeSequencerChannelSupport)
      .replace(sequencer != "sequencer3")

  /** Flake prevention: Disables sequencer circuit breakers to prevent spurious test failures. In
    * resource-constrained CI environments, temporary CPU/memory throttling can trip the circuit
    * breaker, resulting in false-positive sync rejection errors rather than standard timeouts.
    */
  private def disableCircuitBreakers(config: SequencerNodeConfig): SequencerNodeConfig = {
    val disabledCb = CircuitBreakerConfig(enabled = false)

    config.focus(_.sequencer).modify {
      case bft: BftSequencer => bft.focus(_.block.circuitBreaker).replace(disabledCb)
      case ext: External => ext.focus(_.block.circuitBreaker).replace(disabledCb)
      case other => other
    }
  }

  registerPlugin(new UseBftSequencer(loggerFactory))

  private val aliceName = "Alice"

  private var alice: PartyId = _

  private val channelServiceNotImplementedWarning =
    "GrpcServiceUnavailable: UNIMPLEMENTED/Method not found: .*SequencerChannelService/Ping"

  // Generate a unique replication request ID to prevent collisions and rejections across happy-path tests.
  private def validOnPRId(salt: String): String = Hash
    .build(HashPurpose.OnlinePartyReplicationId, HashAlgorithm.Sha256)
    .addString(salt)
    .finish()
    .toHexString

  private val dummyTimestamp = CantonTimestamp.now().minusSeconds(60)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 4,
        numSequencers = 6,
        numMediators = 1,
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.alphaOnlinePartyReplicationSupport)
            .replace(Some(AlphaOnlinePartyReplicationConfig(unsafeSequencerChannelSupport = true)))
        ),
        ConfigTransforms.updateAllSequencerConfigs(selectivelyEnableSequencerChannels),
        // Flake prevention: strip circuit breakers to prevent flaky sync rejections on slow CI runners.
        ConfigTransforms.updateAllSequencerConfigs((_, config) => disableCircuitBreakers(config)),
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](mediator1),
            sequencers =
              Seq(sequencer1, sequencer2, sequencer3, sequencer4, sequencer5, sequencer6),
            mediators = Seq(mediator1),
            synchronizerThreshold = PositiveInt.one,
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        // Flake prevention: avoid `RequestRefused` warnings in the log by ensuring BFT sequencers
        //  have fully established their P2P network before we start allocating parties.
        waitUntilAllBftSequencersAuthenticateDisseminationQuorum()

        connectivityMap.foreach { case (participant, sequencers) =>
          val sequencerConnections = SequencerConnections.tryMany(
            sequencers
              .map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
            // A threshold of 3 ensures that each participant connects to all the three sequencers in the connectivity map
            // and stays connected.
            // TODO(#19911) Make this properly configurable
            sequencerTrustThreshold = PositiveInt.three,
            sequencerLivenessMargin = NonNegativeInt.zero,
            submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
            sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
            subscriptionLivenessLimits = SubscriptionLivenessLimits.default,
          )
          participant.synchronizers.connect_by_config(
            SynchronizerConnectionConfig(
              synchronizerAlias = daName,
              sequencerConnections = sequencerConnections,
              manualConnect = false,
              synchronizerId = None,
              priority = 0,
              initialRetryDelay = None,
              maxRetryDelay = None,
              SynchronizerTimeTrackerConfig(),
            ),
            validation = SequencerConnectionValidation.Active,
            synchronize = Some(env.commandTimeouts.bounded),
          )
        }

        alice = participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2, participant4),
        )
      }

  "Obtain agreement to establish to replicate a party" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      val serial = PositiveInt.two

      val partyOwners = Seq[LocalInstanceReference](sourceParticipant)
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .propose_delta(
            party = alice,
            adds = Seq((targetParticipant, ParticipantPermission.Confirmation)),
            store = daId,
            serial = Some(serial),
            requiresPartyToBeOnboarded = true,
          )
      )
      eventually() {
        partyOwners.foreach(
          _.topology.party_to_participant_mappings
            .list(daId, filterParty = alice.filterString, proposals = true)
            .flatMap(_.item.participants.map(_.participantId)) shouldBe Seq(
            sourceParticipant.id,
            targetParticipant.id,
          )
        )
      }

      // to be used later to query ledger transactions for verification
      val replicationStartTpOffset = targetParticipant.ledger_api.state.end()
      val replicationStartSpOffset = sourceParticipant.ledger_api.state.end()

      val addPartyRequestId =
        clue("Initiate add party async")(
          targetParticipant.parties.add_party_async(
            party = alice,
            synchronizerId = daId,
            sourceParticipant = sourceParticipant,
            serial = serial,
            participantPermission = ParticipantPermission.Confirmation,
          )
        )

      def partyToReplicate(create: CreatedEvent) = create.createArguments
        .getOrElse(fail("missing arguments record"))
        .fields
        .collect { case RecordField("partyId", Some(Value(Party(partyId)))) =>
          partyId
        }
        .loneElement

      // Wait for the sequencer channel agreement to be in place
      Seq(sourceParticipant, targetParticipant).foreach { participant =>
        clue(s"Checking participant ${participant.name}: ") {
          eventually() {
            val txs = participant.ledger_api.updates.transactions(
              Set(sourceParticipant.adminParty),
              completeAfter = 10,
              timeout = config.NonNegativeDuration.ofSeconds(3),
              transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
            )

            // The TP asks the SP to replicate Alice via proposal
            val createProposal = txs.flatMap { case TransactionWrapper(tx: Transaction) =>
              tx.events.collect {
                case Event(Created(event))
                    if event.templateId.contains(
                      AcsReplicationAdminWorkflow.proposalTemplate
                    ) =>
                  event
              }
            }.loneElement
            val party = partyToReplicate(createProposal)
            createProposal.signatories shouldBe Seq(
              targetParticipant.adminParty.toProtoPrimitive
            )
            party shouldBe alice.toProtoPrimitive

            // The SP accepts the party replication proposal
            val accept = txs
              .collect { case TransactionWrapper(tx: Transaction) =>
                tx.events.collect {
                  case Event(Exercised(event))
                      if event.templateId.contains(
                        AcsReplicationAdminWorkflow.proposalTemplate
                      ) =>
                    event
                }
              }
              .flatten
              .loneElement
            accept.choice shouldBe M.acsreplication.AcsReplicationProposal.CHOICE_Accept.name
            accept.consuming shouldBe true
            accept.contractId shouldBe createProposal.contractId
            accept.actingParties shouldBe Seq(
              sourceParticipant.adminParty.toProtoPrimitive
            )
            accept.offset
          }
        }
      }

      // Wait until both participants observe that both participants are allowed to host the party.
      eventually()(Seq(sourceParticipant, targetParticipant).foreach { participant =>
        val p2p = participant.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.filterString)
          .loneElement
          .item

        p2p.participants.map(_.participantId) should contain theSameElementsAs Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
      })

      // Wait until both SP and TP report that party replication has completed.
      // Clearing the onboarding flag takes up to max-decision-timeout (initial value of 60s),
      // so wait at least 1 minute.
      eventually(timeUntilSuccess = 2.minutes) {
        val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
        val spStatus = sourceParticipant.parties.get_add_party_status(addPartyRequestId)

        logger.info(s"TP status: $tpStatus")
        logger.info(s"SP status: $spStatus")
        assert(tpStatus.hasCompleted, "Target participant must complete")
        assert(spStatus.hasCompleted, "Source participant must complete")

        val acsReplicationTpStatus =
          targetParticipant.parties.get_acs_replication_status(addPartyRequestId)
        val acsReplicationSpStatus =
          sourceParticipant.parties.get_acs_replication_status(addPartyRequestId)
        assert(
          acsReplicationTpStatus.hasCompleted,
          "Target participant must complete ACS replication",
        )
        assert(
          acsReplicationSpStatus.hasCompleted,
          "Source participant must complete ACS replication",
        )
        acsReplicationSpStatus.ptpTopologyTransaction.value shouldEqual
          acsReplicationTpStatus.ptpTopologyTransaction.value
        acsReplicationTpStatus.replicationProgress.value.fullyProcessedAcs shouldBe true
        acsReplicationTpStatus.replicationProgress.value.processedContractCount shouldBe NonNegativeLong.zero

        val tpLapiStatus = targetParticipant.ledger_api.parties.get_add_party_status(
          alice,
          daId,
          targetParticipant.id,
        )
        val spLapiStatus = sourceParticipant.ledger_api.parties.get_add_party_status(
          alice,
          daId,
          targetParticipant.id,
        )
        assert(
          tpLapiStatus.current == LapiPartyReplicationStatus.State.STATE_COMPLETED,
          "LAPI TP must be completed",
        )
        assert(
          spLapiStatus.current == LapiPartyReplicationStatus.State.STATE_COMPLETED,
          "LAPI SP must be completed",
        )
      }

      // Ensure both SP and TP first publish the Onboarding topology event followed by Added.
      // Read from spOffsetAgreement/tpOffsetAgreement to skip over the test bootstrap topology events.
      val expectedLapiOnboardingEvent = TopologyEvent.Event.ParticipantAuthorizationOnboarding(
        ParticipantAuthorizationOnboarding(
          partyId = alice.toProtoPrimitive,
          participantId = targetParticipant.id.uid.toProtoPrimitive,
          LapiParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
        )
      )
      val expectedLapiAddedEvent = TopologyEvent.Event.ParticipantAuthorizationAdded(
        ParticipantAuthorizationAdded(
          partyId = alice.toProtoPrimitive,
          participantId = targetParticipant.id.uid.toProtoPrimitive,
          LapiParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
        )
      )
      def readLapiTopologyTransactions(
          participant: ParticipantReference,
          offset: Long,
      ): Seq[TopologyEvent.Event] =
        participant.ledger_api.updates
          .topology_transactions(PositiveInt.two, beginOffsetExclusive = offset)
          .flatMap(_.topologyTransaction.events.map(_.event))

      val spLapiTopologyEvents =
        readLapiTopologyTransactions(sourceParticipant, replicationStartSpOffset)

      spLapiTopologyEvents should have size 2
      spLapiTopologyEvents.head shouldBe expectedLapiOnboardingEvent
      spLapiTopologyEvents.tail.head shouldBe expectedLapiAddedEvent

      val tpLapiTopologyEvents =
        readLapiTopologyTransactions(targetParticipant, replicationStartTpOffset)

      tpLapiTopologyEvents should have size 2
      tpLapiTopologyEvents.head shouldBe expectedLapiOnboardingEvent
      tpLapiTopologyEvents.tail.head shouldBe expectedLapiAddedEvent
  }

  "Prevent malformed party replication proposals" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val participantWithParty = participant1
      val participantWithParty2 = participant2
      val participantWithoutParty = participant3

      def testProposalError(
          log: String,
          errorRegex: String,
          targetParticipant: LocalParticipantReference = participantWithoutParty,
          sourceParticipant: LocalParticipantReference = participantWithParty2,
          synchronizerId: SynchronizerId = daId,
          serial: PositiveInt = PositiveInt.one,
      ): Unit =
        clue(log)(
          loggerFactory.assertThrowsAndLogsUnorderedOptional[CommandFailure](
            targetParticipant.parties.add_party_async(
              party = alice,
              synchronizerId = synchronizerId,
              sourceParticipant = sourceParticipant.id,
              serial = serial,
              participantPermission = ParticipantPermission.Observation,
            ),
            LogEntryOptionality.Required -> (_.errorMessage should include regex errorRegex),
            LogEntryOptionality.Optional -> (_.warningMessage should include regex channelServiceNotImplementedWarning),
          )
        )

      testProposalError(
        "source-participant-does-not-host-party",
        "Party .* is not hosted by source participant",
        sourceParticipant = participantWithoutParty,
        targetParticipant = participantWithParty,
      )

      testProposalError(
        "target-participant-already-hosts-party",
        "Party .* is already hosted by target participant",
        targetParticipant = participantWithParty,
      )

      testProposalError(
        "matching-participants",
        "Source and target participants .* cannot match",
        sourceParticipant = participantWithParty,
        targetParticipant = participantWithParty,
      )

      testProposalError(
        "bad-synchronizer id",
        "Unknown synchronizer bad::synchronizer",
        synchronizerId = SynchronizerId.tryFromString("bad::synchronizer"),
      )

      testProposalError(
        "unexpected-topology-serial",
        "Specified serial .* does not match the expected serial",
        serial = PositiveInt.tryCreate(1000),
      )
  }

  "Prevent malformed party replication agreements" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val participantWithParty = participant1
      val participantWithParty2 = participant2
      val participantWithoutParty = participant3
      val synchronizerId = participantWithParty.synchronizers.list_connected().head.synchronizerId

      import scala.concurrent.duration.*

      def enableFreshParty(label: String): PartyId =
        participant1.parties.enable(
          s"party-$label",
          synchronizeParticipants = Seq(participant2, participant4),
        )

      /** Creates an ACS replication proposal contract directly via the Ledger API of the target
        * participant, bypassing the Daml admin workflow, so that malformed proposals reach the
        * participants' proposal validation instead of being rejected upon `add_party_async`.
        */
      def createProposalDirectly(
          requestId: String,
          partyId: String,
          sourceParticipant: LocalParticipantReference,
          targetParticipant: LocalParticipantReference,
          sequencerUids: Seq[String],
          timestamp: CantonTimestamp,
      ): Unit = {
        val proposal = new M.acsreplication.AcsReplicationProposal(
          requestId,
          partyId,
          sourceParticipant.adminParty.toProtoPrimitive,
          targetParticipant.adminParty.toProtoPrimitive,
          sequencerUids.asJava,
          timestamp.toInstant,
          M.acsreplication.ParticipantPermission.CONFIRMATION,
          "online-party-replication",
        )
        targetParticipant.ledger_api.commands
          .submit(
            actAs = Seq(targetParticipant.adminParty),
            commands = proposal.create.commands.asScala.toSeq
              .map(LedgerClientUtils.javaCodegenToScalaProto),
            synchronizerId = Some(daId),
          )
          .discard
      }

      // Test the party replication proposal parser by directly creating malformed contracts
      // rather than the daml workflow so that the errors are not already caught upon proposal
      // submission.
      def testParsingError(
          log: String,
          warningRegex: String,
          sourceParticipant: LocalParticipantReference = participantWithParty,
          targetParticipant: LocalParticipantReference = participantWithoutParty,
          sequencerStringUids: Seq[String] = Seq(sequencer2.id.uid.toProtoPrimitive),
          partyIdString: Option[String] = None,
          timestamp: CantonTimestamp = dummyTimestamp,
          partyReplicationIdS: Option[String] = None,
      ): Unit =
        clue(log) {
          val requestId = partyReplicationIdS.getOrElse(validOnPRId(log))
          val partyId = partyIdString.getOrElse(enableFreshParty(log).toProtoPrimitive)
          loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            createProposalDirectly(
              requestId,
              partyId,
              sourceParticipant,
              targetParticipant,
              sequencerStringUids,
              timestamp,
            ),
            // Both participants should log warnings for parsing errors.
            forExactly(2, _)(_.warningMessage should include regex warningRegex),
            timeUntilSuccess = 2.seconds,
          )
          assertNoActiveContracts()
        }

      // Test the party replication agreement response by directly creating malformed contracts
      // rather than the daml workflow so that the errors are not already caught upon proposal
      // submission.
      def testAgreementError(
          log: String,
          warningRegex: String,
          sourceParticipant: LocalParticipantReference = participantWithParty,
          targetParticipant: LocalParticipantReference = participantWithoutParty,
          sequencerStringUids: Seq[String] = Seq(sequencer2.id.uid.toProtoPrimitive),
          timestamp: CantonTimestamp = dummyTimestamp,
          partyReplicationIdS: Option[String] = None,
      ): Unit =
        clue(log) {
          val requestId = partyReplicationIdS.getOrElse(validOnPRId(log))
          val partyId = enableFreshParty(log).toProtoPrimitive
          loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            createProposalDirectly(
              requestId,
              partyId,
              sourceParticipant,
              targetParticipant,
              sequencerStringUids,
              timestamp,
            ),
            logEntries => {
              // The proposal contract was created directly, bypassing the Daml workflow,
              // so we expect the target participant to log a warning.
              forExactly(1, logEntries)(
                _.warningMessage should include regex "Received an ACS replication proposal created outside the target participant"
              )
              forExactly(1, logEntries)(_.warningMessage should include regex warningRegex)
            },
            timeUntilSuccess = 2.seconds,
          )
          assertNoActiveContracts()
          eventually(10.seconds) {
            val adminApiStatus = sourceParticipant.parties.get_add_party_status(requestId)
            adminApiStatus.errorO
              .getOrElse(
                fail(
                  s"Expected SP status to be failed (error must be present) (${sourceParticipant.toProtoPrimitive})"
                )
              )
              .message should include regex warningRegex withClue s"errorO message must match the regex $warningRegex (${sourceParticipant.toProtoPrimitive})"
            adminApiStatus.hasCompleted shouldBe true
            // The contract was not created via the Daml workflow, so the target participant
            // cannot track its state, and get_add_party_status always fails for an unknown requestId.
            if (targetParticipant != sourceParticipant) {
              loggerFactory.suppressErrors {
                assert(
                  Try(targetParticipant.parties.get_add_party_status(requestId)).isFailure,
                  s"get_add_party_status(...) should fail on target participant",
                )
              }
            }
          }
        }

      def testProposalExceptionOnEmptySequencerUids(log: String) =
        clue(log) {
          val partyId = enableFreshParty(log).toProtoPrimitive
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            createProposalDirectly(
              validOnPRId(log),
              partyId,
              participantWithParty,
              participantWithParty2,
              Seq.empty,
              dummyTimestamp,
            ),
            logEntry => {
              logEntry.errorMessage should (include(
                "Interpretation error: Error: User failure:"
              ) and include(
                "Template precondition violated: AcsReplicationProposal"
              ))
              logEntry.shouldBeCantonErrorCode(
                CommandExecutionErrors.Interpreter.UnhandledException
              )
            },
          )
          assertNoActiveContracts()
        }

      def assertNoActiveContracts(): Unit =
        // Check that all proposals were either archived (rejected) or never created.
        Seq(participantWithParty, participantWithParty2, participantWithoutParty).foreach {
          participant =>
            clue(s"participant ${participant.name}: ") {
              eventually() {
                val proposals = participant.ledger_api.state.acs
                  .of_all(filterTemplates =
                    Seq(
                      TemplateId.fromIdentifier(
                        AcsReplicationAdminWorkflow.proposalTemplatePkgName
                      ),
                      TemplateId.fromIdentifier(
                        AcsReplicationAdminWorkflow.agreementTemplatePkgName
                      ),
                    )
                  )
                proposals shouldBe Seq.empty
              }
            }
        }

      def findLatestPtpTopologyTimestamp(participant: LocalParticipantReference): CantonTimestamp =
        CantonTimestamp
          .fromInstant(
            participant.topology.party_to_participant_mappings
              .list(synchronizerId, filterParty = aliceName)
              .head
              .context
              .validFrom
          )
          .value

      testParsingError(
        "bad-party-id-format",
        "Invalid partyId .*Invalid unique identifier .* with missing namespace",
        partyIdString = Some("bad-party-id"),
      )

      testParsingError(
        "bad-acs-replication-id",
        "Invalid ACS replication id: .*Failed to parse hex string: bad-party-replication-id",
        partyReplicationIdS = Some("bad-party-replication-id"),
      )

      testParsingError(
        "empty-acs-replication-id",
        "Empty ACS replication id",
        partyReplicationIdS = Some(""),
      )

      testParsingError(
        "bad-sequencer-uid-format",
        "Invalid unique identifier .* with missing namespace",
        sequencerStringUids = Seq("bad-sequencer-uid"),
      )

      testAgreementError(
        "bad-sequencer-uid",
        "None of the proposed sequencers are active on synchronizer",
        sequencerStringUids = Seq("wrong::sequencer"),
      )

      testProposalExceptionOnEmptySequencerUids("empty-sequencer-uids")

      testAgreementError(
        "source-participant-does-not-host-party",
        "PartyToParticipant mapping for party .* doesn't exist for either source or target participant",
        sourceParticipant = participantWithoutParty,
        targetParticipant = participantWithParty,
        timestamp = findLatestPtpTopologyTimestamp(participantWithParty),
      )

      testAgreementError(
        "matching-participants",
        "Source and target participants .* cannot match",
        sourceParticipant = participantWithParty,
        targetParticipant = participantWithParty,
      )
  }

  "Handle replication proposal rejections" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*
    // The only sequencer shared by both (sequencer3) does not support channels.
    val (sourceParticipant, targetParticipant) = (participant1, participant4)

    val serial = targetParticipant.topology.party_to_participant_mappings
      .list(daId, filterParty = alice.filterString)
      .head
      .context
      .serial
      .tryAdd(1)

    val partyOwners = Seq[LocalInstanceReference](sourceParticipant)
    partyOwners.foreach(
      _.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((targetParticipant, ParticipantPermission.Confirmation)),
          store = daId,
          serial = Some(serial),
          requiresPartyToBeOnboarded = true,
        )
    )
    eventually() {
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.filterString, proposals = true)
          .flatMap(_.item.participants.map(_.participantId)) should contain allElementsOf Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
      )
    }

    // to be used later to query ledger transactions for verification
    val replicationStartTpOffset = targetParticipant.ledger_api.state.end()
    val replicationStartSpOffset = sourceParticipant.ledger_api.state.end()

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      {
        val addPartyRequestId =
          clue("Initiate add party async")(
            targetParticipant.parties.add_party_async(
              party = alice,
              synchronizerId = daId,
              sourceParticipant = sourceParticipant,
              serial = serial,
              participantPermission = ParticipantPermission.Confirmation,
            )
          )

        def partyToReplicate(create: CreatedEvent) = create.createArguments
          .getOrElse(fail("missing arguments record"))
          .fields
          .collect { case RecordField("partyId", Some(Value(Party(partyId)))) =>
            partyId
          }
          .loneElement

        // Wait for rejection due to missing sequencer channel support.
        Seq(
          (sourceParticipant, replicationStartSpOffset),
          (targetParticipant, replicationStartTpOffset),
        ).foreach { case (participant, offset) =>
          clue(s"Checking participant ${participant.name}: ") {
            eventually(timeUntilSuccess = 1.minute) {
              val txs = participant.ledger_api.updates.transactions(
                Set(participant.adminParty),
                completeAfter = 10,
                timeout = config.NonNegativeDuration.ofSeconds(3),
                transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
                beginOffsetExclusive = offset,
              )

              // The TP asks the SP to replicate Alice via proposal
              val createProposal = txs.flatMap { case TransactionWrapper(tx: Transaction) =>
                tx.events.collect {
                  case Event(Created(event))
                      if event.templateId.contains(
                        AcsReplicationAdminWorkflow.proposalTemplate
                      ) =>
                    event
                }
              }.loneElement
              val party = partyToReplicate(createProposal)
              createProposal.signatories shouldBe Seq(
                targetParticipant.adminParty.toProtoPrimitive
              )
              party shouldBe alice.toProtoPrimitive

              // The SP rejects the party replication proposal
              val reject = txs.flatMap { case TransactionWrapper(tx: Transaction) =>
                tx.events.collect {
                  case Event(Exercised(event))
                      if event.templateId.contains(
                        AcsReplicationAdminWorkflow.proposalTemplate
                      ) =>
                    event
                }
              }.loneElement
              reject.choice shouldBe M.acsreplication.AcsReplicationProposal.CHOICE_Reject.name
              reject.consuming shouldBe true
              reject.contractId shouldBe createProposal.contractId
              reject.actingParties shouldBe Seq(
                sourceParticipant.adminParty.toProtoPrimitive
              )
            }
          }
        }

        // Assert replication has failed
        eventually(timeUntilSuccess = 30.seconds) {
          val spStatus = sourceParticipant.parties.get_add_party_status(addPartyRequestId)
          spStatus.errorO
            .getOrElse(fail("Expected SP status to be failed (error must be present)"))
            .message should include regex "No sequencers .* support channels"
          spStatus.hasCompleted shouldBe true

          val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
          tpStatus.errorO
            .getOrElse(fail("Expected TP status to be failed (error must be present)"))
            .message should include regex "No sequencers .* support channels"
          tpStatus.hasCompleted shouldBe true

          val spLapiStatus = sourceParticipant.ledger_api.parties.get_add_party_status(
            alice,
            daId,
            targetParticipant.id,
          )
          assert(
            spLapiStatus.current == LapiPartyReplicationStatus.State.STATE_FAILED,
            "LAPI SP status must be failed",
          )

          val tpLapiStatus = targetParticipant.ledger_api.parties.get_add_party_status(
            alice,
            daId,
            targetParticipant.id,
          )
          assert(
            tpLapiStatus.current == LapiPartyReplicationStatus.State.STATE_FAILED,
            "LAPI TP status must be failed",
          )
        }
      },
      // At least one participant should log a warning about missing sequencer channel support.
      forExactly(2, _)(
        _.warningMessage should include regex "No sequencers .* support channels"
      ),
    )
  }
}

// class OnlinePartyReplicationNegotiationTestH2 extends OnlinePartyReplicationNegotiationTest {
//   registerPlugin(new UseH2(loggerFactory))
// }

class OnlinePartyReplicationNegotiationTestPostgres extends OnlinePartyReplicationNegotiationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
