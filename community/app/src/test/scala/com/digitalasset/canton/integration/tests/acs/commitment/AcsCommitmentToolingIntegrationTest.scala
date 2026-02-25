// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.acs.commitment

import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeProportion}
import com.digitalasset.canton.config.{CommitmentSendDelay, DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.acs.commitment.util.{
  CommitmentTestUtil,
  IntervalDuration,
  IouContractsAndCommitment,
}
import com.digitalasset.canton.integration.util.AcsInspection.assertInAcsSync
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.grpc.ParticipantInspectionServiceError
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  ReceivedCmtState,
  SentCmtState,
}
import com.digitalasset.canton.participant.pruning.{
  ContractArchived,
  ContractAssigned,
  ContractCreated,
  ContractUnassigned,
  SortedReconciliationIntervalsHelpers,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.protocol.messages.AcsCommitment
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import java.time.Duration as JDuration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Promise}
import scala.jdk.CollectionConverters.*

trait AcsCommitmentToolingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers
    with CommitmentTestUtil
    with HasProgrammableSequencer {

  private val iouContract = new AtomicReference[Iou.Contract]
  private val interval: JDuration = JDuration.ofSeconds(5)
  private implicit val intervalDuration: IntervalDuration = IntervalDuration(interval)
  private val minObservationDuration1 = NonNegativeFiniteDuration.tryOfHours(1)
  // Participant2 has a longer minObservationDuration than participant1
  private val minObservationDuration2 = minObservationDuration1 * NonNegativeInt.tryCreate(2)

  private val alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]] =
    new AtomicReference[Seq[Iou.Contract]](Seq.empty)

  private lazy val maxCommandDeduplicationDuration = java.time.Duration.ofHours(1)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfHours(1)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfHours(1)
  private val pruningTimeout =
    Ordering[JDuration].max(
      interval
        .plus(confirmationResponseTimeout.duration)
        .plus(mediatorReactionTimeout.duration),
      maxCommandDeduplicationDuration,
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxCommandDeduplicationDuration),
        ConfigTransforms.updateTargetTimestampForwardTolerance(24.hours),
      )
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay).replace(
          Some(
            CommitmentSendDelay(
              minCommitmentSendDelay = Some(NonNegativeProportion.zero),
              maxCommitmentSendDelay = Some(NonNegativeProportion.zero),
            )
          )
        )
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronisation.await_idle()
        sequencer2.topology.synchronisation.await_idle()
        initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
          initializedSynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                initializedSynchronizer.synchronizerId,
                _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
              )
          )
        }

        def connect(
            participant: ParticipantReference,
            minObservationDuration: NonNegativeFiniteDuration,
        ): Unit =
          participant.synchronizers.connect_by_config(
            SynchronizerConnectionConfig(
              synchronizerAlias = daName,
              sequencerConnections = sequencer1,
              timeTracker = SynchronizerTimeTrackerConfig(
                minObservationDuration = minObservationDuration.toConfig
              ),
            )
          )

        connect(participant1, minObservationDuration1)
        connect(participant2, minObservationDuration2)
        connect(participant3, minObservationDuration2)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.foreach { p =>
          p.dars.upload(CantonExamplesPath, synchronizerId = daId)
          p.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        }
        passTopologyRegistrationTimeout(env)
      }

  "Commitment retrieval inspection" should {
    "participant can retrieve commitment it previously sent / received with correct matching states" in {
      implicit env =>
        import env.*

        logger.debug(
          s"Delay the first commitment from P2, so that we can make various assertions on P1's sent and received commitment matching state"
        )
        val sequencer = getProgrammableSequencer(sequencer1.name)
        val delayP2FirstCmt = Promise[Unit]()
        val countCommitments = new AtomicInteger()
        sequencer.setPolicy("Delay first ACS commitments from P2") {
          implicit traceContext => submissionRequest =>
            if (
              ProgrammableSequencerPolicies.isAcsCommitment(
                submissionRequest
              ) && submissionRequest.sender == participant2.id.member
            ) {
              if (countCommitments.getAndIncrement() == 0) {
                logger.debug(
                  s"Withholding first commitment from participant2 ${submissionRequest.batch.envelopes
                      .map(_.toOpenEnvelope(participant1.crypto.pureCrypto, testedProtocolVersion))}"
                )
                SendDecision.HoldBack(delayP2FirstCmt.future)
              } else SendDecision.Process
            } else SendDecision.Process
        }

        logger.debug(s"P1 sends two commitments to P2")
        val IouContractsAndCommitment(cids1da, commitmentPeriod1da, commitment1da) =
          deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)
        val IouContractsAndCommitment(_, commitmentPeriod2da, commitment2da) =
          deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

        logger.debug(
          "Check that P1 can retrieve the two commitments it computed and sent. Eventually P1 receives the" +
            s"second commitment from P2 and it matches its own."
        )

        val synchronizerId = daId
        val startTimestamp = commitmentPeriod1da.fromExclusive.forgetRefinement
        val endTimestamp = commitmentPeriod2da.toInclusive.forgetRefinement
        // TODO(#27011): Move to test where we demonstrate the commitmentState filter
        // user-manual-entry-begin: InspectSentCommitments
        import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
          TimeRange,
          SynchronizerTimeRange,
        }
        participant1.commitments.lookup_sent_acs_commitments(
          synchronizerTimeRanges = Seq(
            SynchronizerTimeRange(
              synchronizerId,
              Some(TimeRange(startTimestamp, endTimestamp)),
            )
          ),
          counterParticipants = Seq.empty,
          commitmentState = Seq(SentCmtState.Mismatch),
          verboseMode = true,
        )

        // user-manual-entry-end: InspectSentCommitments
        val p1SentCommitments = eventually() {
          val p1SentCommitments = participant1.commitments.lookup_sent_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                daId,
                Some(
                  TimeRange(
                    startTimestamp,
                    endTimestamp,
                  )
                ),
              )
            ),
            counterParticipants = Seq.empty,
            commitmentState = Seq.empty,
            verboseMode = true,
          )

          logger.debug(
            "P1 sent commitments only for synchronizer da, so the result size should be 1"
          )
          p1SentCommitments.size shouldBe 1
          val p1DaSentCommitments = p1SentCommitments.get(daId).value
          logger.debug("P1 sent two commitments for synchronizer da")
          p1DaSentCommitments.size shouldBe 2
          val secondP1DaSentCommitment = p1DaSentCommitments(1)

          logger.debug("Check that the second commitment matches the received one")
          secondP1DaSentCommitment.sentCommitment.value shouldBe commitment2da
          secondP1DaSentCommitment.state shouldBe SentCmtState.Match
          secondP1DaSentCommitment.destCounterParticipant shouldBe participant2.id

          p1SentCommitments
        }

        logger.debug(s"P1 never sent commitments for acme")
        always() {
          val p1AcmeSentCommitments = p1SentCommitments.get(acmeId)
          p1AcmeSentCommitments shouldBe None
        }

        logger.debug(
          s"Before we release P2's first commitment, P1's first commitment is never matched"
        )
        always() {
          val p1DaSentCommitment1 = p1SentCommitments.get(daId).value.headOption.value
          p1DaSentCommitment1.sentCommitment.value shouldBe commitment1da
          p1DaSentCommitment1.state shouldBe SentCmtState.NotCompared
          p1DaSentCommitment1.destCounterParticipant shouldBe participant2.id
        }

        logger.debug(
          "Check that P1 can retrieve the commitments it received or is waiting to receive. Eventually P1 receives the" +
            s"second commitment from P2 and it matches its own, but the first commitment remains outstanding"
        )
        // TODO(#27011): Move to test where we demonstrate the commitmentState filter
        // user-manual-entry-begin: InspectReceivedCommitments
        import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
          TimeRange,
          SynchronizerTimeRange,
        }

        participant1.commitments.lookup_received_acs_commitments(
          synchronizerTimeRanges = Seq(
            SynchronizerTimeRange(
              synchronizerId,
              Some(TimeRange(startTimestamp, endTimestamp)),
            )
          ),
          counterParticipants = Seq.empty,
          commitmentState = Seq(ReceivedCmtState.Buffered),
          verboseMode = true,
        )
        // user-manual-entry-end: InspectReceivedCommitments
        val p1Received = eventually() {
          val p1Received = participant1.commitments.lookup_received_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                synchronizerId,
                Some(
                  TimeRange(
                    startTimestamp,
                    endTimestamp,
                  )
                ),
              )
            ),
            counterParticipants = Seq.empty,
            commitmentState = Seq.empty,
            verboseMode = true,
          )

          logger.debug(
            "P1 received (and is waiting to receive) commitments only for synchronizer da, so the result size should be 1"
          )
          p1Received.size shouldBe 1
          val p1DaReceivedCmts = p1Received.get(daId).value
          logger.debug(
            "P1 received one commitment for synchronizer da, and is expecting to receive another one for synchronizer da"
          )
          p1DaReceivedCmts.size shouldBe 2
          val secondP1DaReceivedCommitment = p1DaReceivedCmts(1)

          logger.debug("Check that the second received commitment matches the sent one")
          secondP1DaReceivedCommitment.receivedCommitment.value shouldBe commitment2da
          secondP1DaReceivedCommitment.localCommitment.value shouldBe commitment2da
          secondP1DaReceivedCommitment.state shouldBe ReceivedCmtState.Match
          secondP1DaReceivedCommitment.originCounterParticipant shouldBe participant2.id

          p1Received
        }

        logger.debug(s"P1 never expects receiving commitments for acme")
        always() {
          val p1AcmeReceivedCommitments = p1Received.get(acmeId)
          p1AcmeReceivedCommitments shouldBe None
        }

        logger.debug(
          s"Before we release P2's first commitment, P1's first commitment to be received is outstanding"
        )
        always() {
          val p1DaReceivedCommitments = p1Received.get(daId).value
          // Check that the first commitment to be received is outstanding
          val firstP1DaReceivedCommitment = p1DaReceivedCommitments.head
          firstP1DaReceivedCommitment.receivedCommitment shouldBe None
          firstP1DaReceivedCommitment.localCommitment.value shouldBe commitment1da
          firstP1DaReceivedCommitment.state shouldBe ReceivedCmtState.Outstanding
          firstP1DaReceivedCommitment.originCounterParticipant shouldBe participant2.id
        }

        logger.debug(
          s"We release P2's first commitment. Eventually, P1's receives it and it should match its first commitment."
        )
        delayP2FirstCmt.trySuccess(())
        eventually() {
          val p1SentCommitments = participant1.commitments.lookup_sent_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                daId,
                Some(
                  TimeRange(
                    startTimestamp,
                    endTimestamp,
                  )
                ),
              )
            ),
            counterParticipants = Seq.empty,
            commitmentState = Seq.empty,
            verboseMode = true,
          )

          p1SentCommitments.size shouldBe 1
          val p1DaSentCommitments = p1SentCommitments.get(daId).value
          p1DaSentCommitments.size shouldBe 2
          val firstP1DaSentCommitment = p1DaSentCommitments.headOption.value

          firstP1DaSentCommitment.sentCommitment.value shouldBe commitment1da
          firstP1DaSentCommitment.state shouldBe SentCmtState.Match
          firstP1DaSentCommitment.destCounterParticipant shouldBe participant2.id
        }

        eventually() {
          val p1ReceivedCommitments = participant1.commitments.lookup_received_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                daId,
                Some(
                  TimeRange(
                    startTimestamp,
                    endTimestamp,
                  )
                ),
              )
            ),
            counterParticipants = Seq.empty,
            commitmentState = Seq.empty,
            verboseMode = true,
          )

          p1ReceivedCommitments.size shouldBe 1
          val p1ReceivedDaCommitments = p1ReceivedCommitments.get(daId).value
          p1ReceivedDaCommitments.size shouldBe 2
          val firstP1DaReceivedCommitment = p1ReceivedDaCommitments.headOption.value

          firstP1DaReceivedCommitment.receivedCommitment.value shouldBe commitment1da
          firstP1DaReceivedCommitment.localCommitment.value shouldBe commitment1da
          firstP1DaReceivedCommitment.state shouldBe ReceivedCmtState.Match
          firstP1DaReceivedCommitment.originCounterParticipant shouldBe participant2.id
        }

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            logger.debug(
              s"P2 sends a commitment that does not match P1's first commitment. We do that by purging one contract from P2's ACS."
            )
            participant2.synchronizers.disconnect_all()
            eventually() {
              participant2.synchronizers.list_connected() shouldBe empty
            }
            participant2.repair.purge(daName, Seq(cids1da.headOption.value.id.toLf))
            participant2.synchronizers.reconnect_all()
            eventually() {
              participant2.synchronizers
                .list_connected()
                .map(_.physicalSynchronizerId) should contain(daId)
            }

            logger.debug(
              "Now have P1 and P2 exchange commitments again, so that P1 can see the mismatch."
            )
            val IouContractsAndCommitment(_, commitmentPeriod3da, commitment3da) =
              deployThreeContractsAndCheck(
                daId,
                alreadyDeployedContracts,
                participant1,
                participant2,
              )
            val p1SentCommitments = eventually() {
              val p1LookupSentCommitments = participant1.commitments.lookup_sent_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    daId,
                    Some(
                      TimeRange(
                        startTimestamp,
                        commitmentPeriod3da.toInclusive.forgetRefinement,
                      )
                    ),
                  )
                ),
                counterParticipants = Seq.empty,
                commitmentState = Seq.empty,
                verboseMode = true,
              )

              p1LookupSentCommitments.size shouldBe 1
              val p1DaSentCommitments = p1LookupSentCommitments.get(daId).value
              logger.debug("In total P1 sent three commitments for synchronizer da")
              p1DaSentCommitments.size shouldBe 3
              val thirdP1DaSentCommitment = p1DaSentCommitments(2)

              thirdP1DaSentCommitment.sentCommitment.value shouldBe commitment3da
              thirdP1DaSentCommitment.state shouldBe SentCmtState.Mismatch
              thirdP1DaSentCommitment.destCounterParticipant shouldBe participant2.id

              p1LookupSentCommitments
            }

            logger.debug(s"The last commitment sent to P2 is never a match")
            always() {
              val lastP1SentDaCommitment = p1SentCommitments.get(daId).value(2)
              lastP1SentDaCommitment.state should not be SentCmtState.Match
            }

            val p1ReceivedCommitments = eventually() {
              val p1LookupReceivedCommitments =
                participant1.commitments.lookup_received_acs_commitments(
                  synchronizerTimeRanges = Seq(
                    SynchronizerTimeRange(
                      daId,
                      Some(
                        TimeRange(
                          startTimestamp,
                          commitmentPeriod3da.toInclusive.forgetRefinement,
                        )
                      ),
                    )
                  ),
                  counterParticipants = Seq.empty,
                  commitmentState = Seq.empty,
                  verboseMode = true,
                )

              p1LookupReceivedCommitments.size shouldBe 1
              val p1DaReceivedCmts = p1LookupReceivedCommitments.get(daId).value
              logger.debug("In total P1 received three commitments for synchronizer da")
              p1DaReceivedCmts.size shouldBe 3

              p1LookupReceivedCommitments
            }

            eventually() {
              val p2SentCommitments = participant2.commitments.lookup_sent_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    daId,
                    Some(
                      TimeRange(
                        commitmentPeriod1da.fromExclusive.forgetRefinement,
                        commitmentPeriod3da.toInclusive.forgetRefinement,
                      )
                    ),
                  )
                ),
                counterParticipants = Seq.empty,
                commitmentState = Seq.empty,
                verboseMode = true,
              )

              val p1DaReceivedCommitments = p1ReceivedCommitments.get(daId).value
              val lastP1DaReceivedCommitment = p1DaReceivedCommitments(2)
              val lastP2DaSentCommitment = p2SentCommitments.get(daId).value(2)
              lastP1DaReceivedCommitment.receivedCommitment.value shouldBe lastP2DaSentCommitment.sentCommitment.value

              lastP1DaReceivedCommitment.state should (be(ReceivedCmtState.Outstanding) or be(
                ReceivedCmtState.Mismatch
              ))

              lastP1DaReceivedCommitment.originCounterParticipant shouldBe participant2.id
            }

            logger.debug(s"The last commitment sent by P2 is never a match")
            always() {
              val lastP1DaSentCommitment = p1SentCommitments.get(daId).value(2)
              val lastP1DaReceivedCommitment = p1ReceivedCommitments.get(daId).value(2)
              lastP1DaSentCommitment.state should not be SentCmtState.Match
              lastP1DaReceivedCommitment.state should not be ReceivedCmtState.Match
            }

            logger.debug("Get P1 and P2 back in sync by purging the contract on P1 as well")
            participant1.synchronizers.disconnect_all()
            eventually() {
              participant1.synchronizers.list_connected() shouldBe empty
            }
            participant1.repair.purge(daName, Seq(cids1da.headOption.value.id.toLf))
            participant1.synchronizers.reconnect_all()
            eventually() {
              participant1.synchronizers
                .list_connected()
                .map(_.physicalSynchronizerId) should contain(daId)
            }
            alreadyDeployedContracts.set(
              alreadyDeployedContracts.get().toSet.removedAll(Set(cids1da.head)).toSeq
            )
          },
          logs => {
            forAtLeast(1, logs)(m => m.message should include(CommitmentsMismatch.id))
          },
        )
    }
  }

  "Commitment and mismatch inspection" should {
    "participant can open a commitment it previously sent" in { implicit env =>
      import env.*

      val deployedIouContractsAndCommitment =
        deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)
      val contractsAndReassignmentCounters = participant1.commitments.open_commitment(
        deployedIouContractsAndCommitment.commitment,
        daId,
        deployedIouContractsAndCommitment.commitmentPeriod.toInclusive.forgetRefinement,
        participant2,
      )
      val returnedCids = contractsAndReassignmentCounters.map(c => c.cid)
      returnedCids should contain theSameElementsAs alreadyDeployedContracts
        .get()
        .map(c => c.id.toLf)
    }

    "participant can open a commitment spanning multiple intervals" in { implicit env =>
      import env.*

      val simClock = environment.simClock.value

      deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

      logger.info(
        "Advance time five reconciliation intervals, remembering the tick after three reconciliation intervals."
      )
      simClock.advance(interval.multipliedBy(3))
      val midTick = tickAfter(simClock.uniqueTime())
      simClock.advanceTo(midTick.forgetRefinement.immediateSuccessor)
      simClock.advance(interval.multipliedBy(2))
      val cmtTick = tickAfter(simClock.uniqueTime())
      simClock.advanceTo(cmtTick.forgetRefinement.immediateSuccessor)

      logger.info("Send a commitment, which will cover the five intervals.")
      participant1.testing.fetch_synchronizer_times()

      val (_, _, cmtOverFiveIntervals) = eventually() {
        val p1Computed = participant1.commitments
          .computed(
            daName,
            cmtTick.toInstant.minusMillis(1),
            cmtTick.toInstant,
            Some(participant2),
          )
        p1Computed.size shouldBe 1
        p1Computed
      }.loneElement

      val contractsAndTransferCounters = participant1.commitments.open_commitment(
        cmtOverFiveIntervals,
        daId,
        midTick.forgetRefinement,
        participant2,
      )

      val returnedCids = contractsAndTransferCounters.map(c => c.cid)
      returnedCids should contain theSameElementsAs alreadyDeployedContracts
        .get()
        .map(c => c.id.toLf)
    }

    "opening a commitment fails when" should {
      "the participant didn't send the given commitment" in { implicit env =>
        import env.*

        val deployedIouContractsAndCommitment =
          deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)
        val notSentCmt = LtHash16().getByteString()
        val hashedNotSentCmd = AcsCommitment.hashCommitment(notSentCmt)
        // give wrong commitment but correct timestamp and counter-participant
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            hashedNotSentCmd,
            daId,
            deployedIouContractsAndCommitment.commitmentPeriod.toInclusive.forgetRefinement,
            participant2,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "because the participant has not computed such a commitment at the given tick timestamp for the given counter participant"
            ))
            logEntry.shouldBeCantonErrorCode(ParticipantInspectionServiceError.IllegalArgumentError)
          },
        )
      }

      "the participant sent the commitment but not at the given tick in the past" in {
        implicit env =>
          import env.*

          val IouContractsAndCommitment(_, commitmentPeriod, commitment) =
            deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

          // give wrong timestamp but a computed commitment and correct counter-participant
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant2.commitments.open_commitment(
              commitment,
              daId,
              tickBeforeOrAt(
                commitmentPeriod.toInclusive.forgetRefinement.immediatePredecessor
              ).forgetRefinement,
              participant1,
            ),
            logEntry => {
              logEntry.errorMessage should (include(
                "The participant cannot open commitment"
              ) and include(
                "because the participant has not computed such a commitment at the given tick timestamp for the given counter participant"
              ))
              logEntry.shouldBeCantonErrorCode(
                ParticipantInspectionServiceError.IllegalArgumentError
              )
            },
          )
      }

      "the given counter-participant is incorrect" in { implicit env =>
        import env.*

        val IouContractsAndCommitment(_, commitmentPeriod, commitment) =
          deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

        // give wrong counter-participant but a computed commitment and its correct timestamp
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            commitment,
            daId,
            commitmentPeriod.toInclusive.forgetRefinement,
            participant1,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "because the participant has not computed such a commitment at the given tick timestamp for the given counter participant"
            ))
            logEntry.shouldBeCantonErrorCode(ParticipantInspectionServiceError.IllegalArgumentError)
          },
        )
      }

      "the given timestamp is not a reconciliation interval tick" in { implicit env =>
        import env.*

        val IouContractsAndCommitment(_, commitmentPeriod, commitment) =
          deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

        // this test assumes that the reconciliation interval is not 1 second for the given opening commitment
        // timestamp to not fall on a reconciliation interval boundary
        assert(interval.getSeconds != 1)

        // give timestamp that does not correspond to a reconciliation interval boundary
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.commitments.open_commitment(
            commitment,
            daId,
            // this test assumes that the reconciliation interval is not 1 second for the timestamp below to not fall
            // on an interval reconciliation boundary
            commitmentPeriod.toInclusive.forgetRefinement.minus(JDuration.ofSeconds(1)),
            participant1,
          ),
          logEntry => {
            logEntry.errorMessage should (include(
              "The participant cannot open commitment"
            ) and include(
              "is not a valid reconciliation interval tick"
            ))
            logEntry.shouldBeCantonErrorCode(ParticipantInspectionServiceError.IllegalArgumentError)
          },
        )
      }
    }

    "acs pruning beyond the timestamp prevents opening a commitment" in { implicit env =>
      import cats.syntax.either.*
      import env.*

      val simClock = environment.simClock.value

      val IouContractsAndCommitment(_, commitmentPeriod, commitment) =
        deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

      logger.info(
        "Participant1 waits to receive counter-commitment, so that it can prune past data"
      )
      eventually() {
        participant1.commitments
          .received(
            synchronizerAlias = daName,
            start = commitmentPeriod.toInclusive.toInstant.minusMillis(1),
            end = commitmentPeriod.toInclusive.toInstant,
            counterParticipant = Some(participant2),
          )
          .size shouldBe 1
      }

      simClock.advance(pruningTimeout)
      logger.info(
        "Participant1 deploy some more contracts to advance the clean replay, so that it can prune past data"
      )
      deployThreeContractsAndCheck(daId, alreadyDeployedContracts, participant1, participant2)

      logger.info("Wait that ACS background pruning advanced past the timestamp of the commitment")
      eventually() {
        val pruningTs = participant1.testing.state_inspection.acsPruningStatus(daId)
        pruningTs.map(_.lastSuccess.forall(_ >= commitmentPeriod.toInclusive)) shouldBe Some(true)
      }

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Either
          .catchOnly[CommandFailure] {
            participant1.commitments.open_commitment(
              commitment,
              daId,
              commitmentPeriod.toInclusive.forgetRefinement,
              participant2,
            )
          }
          .left
          .value
          .getMessage should include("Command execution failed"),
        logEntries => {
          forExactly(1, logEntries)(logEntry =>
            logEntry.message should (include(
              s"Active contract store for synchronizer"
            ) and include(
              "which is after the requested time of change"
            ))
          )
        },
      )
    }

    "inspect commitment contracts" should {
      def getCleanReqTs(
          participant: LocalParticipantReference,
          synchronizerId: SynchronizerId,
      )(implicit ec: ExecutionContext): Option[CantonTimestamp] = {
        val cleanReqTs = eventually() {
          participant.underlying.value.sync.participantNodePersistentState.value.ledgerApiStore
            .cleanSynchronizerIndex(synchronizerId)
            .futureValueUS
            .flatMap(_.sequencerIndex)
        }
        cleanReqTs
      }

      "inspect created and archived contracts at the current timestamp" in { implicit env =>
        import env.*

        logger.info("Create three contracts on synchronizer da")
        val createdDaCids =
          deployThreeContractsAndCheck(
            daId,
            alreadyDeployedContracts,
            participant1,
            participant2,
          ).contracts

        // archive one of these created contracts
        logger.info("Archive one of these contracts")
        val archivedCid = createdDaCids.headOption.getOrElse(fail("No created contract found")).id
        participant1.ledger_api.javaapi.commands.submit(
          Seq(participant1.id.adminParty),
          archivedCid.exerciseArchive().commands.asScala.toSeq,
          Some(daId),
        )

        eventually() {
          participants.all.foreach(p =>
            p.ledger_api.state.acs.of_all().map(_.contractId) should not contain archivedCid
          )
        }

        val tsAfterArchival =
          getCleanReqTs(participant1, daId).getOrElse(fail("No clean request timestamp found"))

        val queriedCids = createdDaCids.map(_.id.toLf)
        val mismatchTimestamp = tsAfterArchival
        val synchronizer1Id = daId
        val inspectContracts = participant1.commitments.inspect_commitment_contracts(
          contracts = queriedCids,
          timestamp = mismatchTimestamp,
          synchronizer1Id,
          downloadPayload = true,
        )

        logger.info(
          s"Inspect the contract state after archival timestamp $tsAfterArchival valid on synchronizer da"
        )

        logger.info(s"The result contains exactly one entry per contract")
        inspectContracts.size shouldBe queriedCids.size
        inspectContracts.map(_.cid) should contain theSameElementsAs queriedCids

        logger.info(s"The result contains a payload for all contracts")
        inspectContracts.map(
          _.contract.map(_.contractId)
        ) should contain theSameElementsAs queriedCids.map(c => Some(c))

        logger.info(s"Non-archived contracts have a single state (created) and are active on da")
        inspectContracts.filter { e =>
          e.activeOnExpectedSynchronizer &&
          e.state.sizeIs == 1 && e.state.forall(_.contractState.isInstanceOf[ContractCreated])
        } should have size (queriedCids.size - 1).toLong

        logger.info(
          s"The archived contract has two states (created and archived) and is not active on da"
        )
        inspectContracts.filter { e =>
          e.cid == archivedCid.toLf && !e.activeOnExpectedSynchronizer &&
          e.state.sizeIs == 2 && e.state.count(
            _.contractState.isInstanceOf[ContractArchived]
          ) == 1 && e.state.count(
            _.contractState.isInstanceOf[ContractCreated]
          ) == 1
        } should have size 1
      }

      "do not retrieve payloads works" in { implicit env =>
        import env.*

        logger.info("Create three contracts on synchronizer da")
        val createdDaCids =
          deployThreeContractsAndCheck(
            daId,
            alreadyDeployedContracts,
            participant1,
            participant2,
          ).contracts
        val ts =
          getCleanReqTs(participant1, daId).getOrElse(fail("No clean request timestamp found"))

        val queriedCids = createdDaCids.map(_.id.toLf)
        val inspectContracts = participant1.commitments.inspect_commitment_contracts(
          queriedCids,
          ts,
          daId,
          downloadPayload = false,
        )

        logger.info(
          s"Inspect the contract state after creation timestamp $ts valid on synchronizer da"
        )

        logger.info(s"The result contains exactly one entry per contract")
        inspectContracts.size shouldBe queriedCids.size
        inspectContracts.map(_.cid) should contain theSameElementsAs queriedCids

        logger.info(s"The result does not contains a payload for any contracts")
        forAll(inspectContracts)(inspectContract => inspectContract.contract shouldBe None)
      }

      "inspect contract state for a past unpruned timestamp and more synchronizers" in {
        implicit env =>
          import env.*

          participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

          logger.info("Create three contracts on synchronizer da")
          val createdDaCids =
            deployThreeContractsAndCheck(
              daId,
              alreadyDeployedContracts,
              participant1,
              participant2,
            ).contracts

          val tsBeforeReassign =
            getCleanReqTs(participant1, daId).getOrElse(fail("No clean request timestamp found"))

          logger.info("Reassign one contract from da to acme")
          val reassignedCid =
            createdDaCids.headOption.getOrElse(fail("No created contract found")).id.toLf
          val unassignedWrapper =
            participant1.ledger_api.commands
              .submit_unassign(
                participant1.id.adminParty,
                Seq(reassignedCid),
                daId,
                acmeId,
              )

          def reassignmentStore(
              participant: LocalParticipantReference,
              synchronizerId: SynchronizerId,
          ): ReassignmentStore =
            participant.underlying.value.sync.syncPersistentStateManager
              .reassignmentStore(synchronizerId)
              .value

          // Retrieve the reassignment data
          val reassignmentStoreP1Acme = reassignmentStore(participant1, acmeId)
          val incompleteUnassignment = eventually() {
            reassignmentStoreP1Acme
              .lookup(ReassignmentId.tryCreate(unassignedWrapper.reassignmentId))
              .value
              .futureValueUS
              .value
          }

          participant1.ledger_api.commands.submit_assign(
            participant1.id.adminParty,
            unassignedWrapper.reassignmentId,
            daId,
            acmeId,
          )

          logger.info("Check that reassignment has completed")
          assertInAcsSync(Seq(participant1), acmeName, reassignedCid)

          // We do the following pings to ensure that the clean request timestamp on the two synchronizers is past the "common time"
          // when the assignment happens on the destination synchronizer.
          participant1.health.ping(participant2, synchronizerId = Some(daId))
          participant1.health.ping(participant2, synchronizerId = Some(acmeId))

          logger.info("Create three contracts on synchronizer acme")
          val c1 =
            deployOnTwoParticipantsAndCheckContract(acmeId, iouContract, participant1, participant2)
          val c2 =
            deployOnTwoParticipantsAndCheckContract(acmeId, iouContract, participant1, participant2)
          val c3 =
            deployOnTwoParticipantsAndCheckContract(acmeId, iouContract, participant1, participant2)
          val createdCidsAcme = Seq(c1, c2, c3)

          logger.info(
            s"Inspect the states of the contracts on acme and the reassigned contract w.r.t. timestamp before" +
              s"the reassign $tsBeforeReassign and synchronizer da"
          )
          val queriedContracts = createdCidsAcme.map(_.id.toLf) ++ Seq(reassignedCid)
          val inspectContracts = participant1.commitments.inspect_commitment_contracts(
            queriedContracts,
            tsBeforeReassign,
            daId,
            downloadPayload = true,
          )

          logger.info(
            s"The result contains one entry per acme contracts and two entries for the reassigned contract (one per synchronizer)"
          )
          inspectContracts.size shouldBe createdCidsAcme.size + Seq(reassignedCid).size * 2

          logger.info(
            s"All acme contracts have one state (created), have a payload, and do not exist on da"
          )
          inspectContracts.filter(contract =>
            !contract.activeOnExpectedSynchronizer &&
              contract.contract.isDefined &&
              contract.state.sizeIs == 1 &&
              contract.state.count(state =>
                state.contractState
                  .isInstanceOf[ContractCreated] && state.synchronizerId == acmeId.logical
              ) == 1
          ) should have size (createdCidsAcme.size).toLong

          logger.info(
            s"The reassigned contract has two states (created and unassigned) on da, has a payload, and is" +
              s"active on da at the queried time"
          )
          val reassigned = inspectContracts.filter(c => c.cid == reassignedCid)
          val reassignedCounter =
            incompleteUnassignment.contractsBatch.contractIdCounters.toMap.apply(reassignedCid)
          reassigned.size shouldBe 2
          reassigned.filter(states =>
            states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractCreated] && s.synchronizerId == daId.logical
              ) == 1 &&
              states.state.count(s =>
                s.contractState
                  .isInstanceOf[ContractUnassigned] && s.synchronizerId == daId.logical &&
                  s.contractState
                    .asInstanceOf[ContractUnassigned]
                    .reassignmentId
                    .contains(incompleteUnassignment.reassignmentId) &&
                  s.contractState
                    .asInstanceOf[ContractUnassigned]
                    .reassignmentCounterSrc == reassignedCounter - 1
              ) == 1 &&
              states.state.sizeIs == 2
          ) should have size 1

          logger.info(
            s"The reassigned contract has one state (assigned) on acme, has a payload, and is" +
              s"active on da at the queried time"
          )
          reassigned.filter(states =>
            states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState
                  .isInstanceOf[ContractAssigned] && s.synchronizerId == acmeId.logical &&
                  s.contractState
                    .asInstanceOf[ContractAssigned]
                    .reassignmentId
                    .contains(incompleteUnassignment.reassignmentId) &&
                  s.contractState
                    .asInstanceOf[ContractAssigned]
                    .reassignmentCounterTarget == reassignedCounter
              ) == 1 &&
              states.state.sizeIs == 1
          ) should have size 1
          reassigned.filter(c => c.contract.isDefined) should have size 2

          logger.info(
            s"Inspect the states of the reassigned contract w.r.t. timestamp after unassign and before assign" +
              s"${incompleteUnassignment.unassignmentTs} and synchronizer da"
          )
          val inspectContracts2 = participant1.commitments.inspect_commitment_contracts(
            Seq(reassignedCid),
            incompleteUnassignment.unassignmentTs,
            daId,
            downloadPayload = true,
          )

          logger.info(
            s"The reassigned contract has two states (created and unassigned) on da, has a payload, and is" +
              s"not active on da at the queried time"
          )
          val reassigned2 = inspectContracts2.filter(c => c.cid == reassignedCid)

          reassigned2.size shouldBe 2
          reassigned2.filter(states =>
            !states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractCreated] && s.synchronizerId == daId.logical
              ) == 1 &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractUnassigned] && s.synchronizerId == daId.logical
              ) == 1 &&
              states.state.sizeIs == 2
          ) should have size 1

          logger.info(
            s"The reassigned contract has one state (assigned) on acme, has a payload, and is" +
              s"not active on da at the queried time"
          )
          reassigned2.filter(states =>
            !states.activeOnExpectedSynchronizer &&
              states.state.count(s =>
                s.contractState.isInstanceOf[ContractAssigned] && s.synchronizerId == acmeId.logical
              ) == 1 &&
              states.state.sizeIs == 1
          ) should have size 1
          reassigned2.filter(c => c.contract.isDefined) should have size 2
      }
    }
  }
}

class AcsCommitmentToolingIntegrationTestPostgres extends AcsCommitmentToolingIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    // TODO(#29603): This test fails to advance time properly with BFT sequencer, too flaky (>50% failures).
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

//class AcsCommitmentToolingIntegrationTestH2 extends AcsCommitmentToolingIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseBftSequencer(
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer(
//        Seq(
//          Set(InstanceName.tryCreate("sequencer1")),
//          Set(InstanceName.tryCreate("sequencer2")),
//        )
//      ),
//    )
//  )
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}
