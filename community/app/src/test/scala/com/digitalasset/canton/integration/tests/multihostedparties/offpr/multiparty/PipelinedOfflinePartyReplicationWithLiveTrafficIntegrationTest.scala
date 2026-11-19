// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr.multiparty

import com.daml.ledger.javaapi.data.{Command, Transaction}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.FlagNotSet
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.sequencing.protocol.AllMembersOfSynchronizer
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Party, PartyId}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, blocking}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** Scenario: four parties are replicated step by step, while the source and a third participant
  * keep transacting with them during the replication and after it. Each host also hosts
  * counterparties. Each contract is signed by two of the replicated parties and observed by two
  * more and by a counterparty; each transaction replaces a contract. The sequencer holds back each
  * of the source's topology broadcasts until both participants have transacted, so transactions
  * happen between the activations.
  *
  * Runs with wall-clock time, so that topology transactions take effect the default topology change
  * delay after they are sequenced. The decision timeout is 30 seconds instead of the default 60, as
  * each onboarding flag clearance waits for it in real time.
  *
  * Goal: the target ends up with the source's ACS, and then confirms for the parties. A transaction
  * that races an activation is rejected (`NO_VIEW_WITH_VALID_RECIPIENTS`), not missed.
  */
final class PipelinedOfflinePartyReplicationWithLiveTrafficIntegrationTest
    extends PipelinedOfflinePartyReplicationIntegrationTestBase
    with HasProgrammableSequencer {

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        participants.local.dars.upload(BaseTest.CantonTestsPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(
              reconciliationInterval = reconciliationInterval,
              confirmationResponseTimeout = NonNegativeFiniteDuration.ofSeconds(15),
              mediatorReactionTimeout = NonNegativeFiniteDuration.ofSeconds(15),
            ),
          )
        source = participant1
        target = participant3
      }

  override protected def clearOnboardingFlags(
      parties: Seq[Party],
      targetLedgerEnd: Long,
      to: LocalParticipantReference,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    eventually(timeUntilSuccess = 4.minutes, maxPollInterval = 5.seconds) {
      parties.foreach(
        to.parties.clear_party_onboarding_flag(_, daId.logical, targetLedgerEnd) shouldBe FlagNotSet
      )
    }
  }

  "Parties hosted only on the source get the source's ACS while the source and another participant keep transacting with them" in {
    implicit env =>
      replicateWithLiveTraffic("Single", thresholdOnBothHosts = None)
  }

  "Parties that another participant also hosts, with threshold 1, get the source's ACS while both hosts keep transacting with them" in {
    implicit env =>
      replicateWithLiveTraffic("One", thresholdOnBothHosts = Some(PositiveInt.one))
  }

  "Parties that another participant also hosts, with threshold 2, get the source's ACS while both hosts keep transacting with them" in {
    implicit env =>
      replicateWithLiveTraffic("Two", thresholdOnBothHosts = Some(PositiveInt.two))
  }

  /** @param thresholdOnBothHosts
    *   If defined, the third participant hosts the replicated parties too, with this confirmation
    *   threshold, from before the parties have contracts. Otherwise only the source hosts them. A
    *   replicated party submits only where it is hosted with threshold 1; otherwise a counterparty
    *   on the same participant acts on the contract.
    */
  private def replicateWithLiveTraffic(prefix: String, thresholdOnBothHosts: Option[PositiveInt])(
      implicit env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val third = participant2
    val replicated = (1 to 4).map(i =>
      source.parties.enable(s"${prefix}Replicated$i", synchronizeParticipants = Seq(target, third))
    )
    val sourceCounterparties = (1 to 2).map(i =>
      source.parties
        .enable(s"${prefix}SourceCounterparty$i", synchronizeParticipants = Seq(target, third))
    )
    val thirdCounterparties = (1 to 2).map(i =>
      third.parties
        .enable(s"${prefix}ThirdCounterparty$i", synchronizeParticipants = Seq(source, target))
    )
    def hostReplicatedOnBoth(threshold: PositiveInt): Unit = replicated.foreach(party =>
      PartyToParticipantDeclarative.forParty(Set(source, third, target), daId)(
        source.id,
        party,
        threshold,
        Set(
          source.id -> ParticipantPermission.Submission,
          third.id -> ParticipantPermission.Submission,
        ),
      )(parallelExecutionContext, env)
    )
    if (thresholdOnBothHosts.isDefined) hostReplicatedOnBoth(PositiveInt.one)
    def r(i: Int): PartyId = replicated(i % replicated.size)
    def actorOn(host: LocalParticipantReference, counterparties: Seq[PartyId])(i: Int): PartyId = {
      val replicatedCanSubmit =
        thresholdOnBothHosts.forall(_ == PositiveInt.one) &&
          (host == source || thresholdOnBothHosts.isDefined)
      if (replicatedCanSubmit) r(i + 4) else counterparties(i % counterparties.size)
    }

    def createContracts(
        creator: LocalParticipantReference,
        actorAt: Int => PartyId,
        otherCounterparties: Seq[PartyId],
        shift: Int,
    ): Seq[UniversalContract.Contract] = replicated.indices.map { index =>
      val i = index + shift
      val actor = actorAt(i)
      val signatories = Seq(r(i), r(i + 1))
      val contract = new UniversalContract(
        List.empty[String].asJava,
        signatories.map(_.toProtoPrimitive).asJava,
        Seq(r(i + 2), r(i + 3), otherCounterparties(i % otherCounterparties.size), actor)
          .map(_.toProtoPrimitive)
          .asJava,
        List(actor.toProtoPrimitive).asJava,
      )
      JavaDecodeUtil
        .decodeAllCreated(UniversalContract.COMPANION)(
          creator.ledger_api.javaapi.commands
            .submit(signatories, contract.create().commands().asScala.toSeq)
        )
        .loneElement
    }
    val onSource = new Workload(
      source,
      createContracts(
        source,
        actorOn(source, sourceCounterparties),
        thirdCounterparties,
        shift = 0,
      ),
      observersAt = step => Seq(r(step + 2), r(step + 5), thirdCounterparties(step % 2)),
    )
    val onThird = new Workload(
      third,
      // Only a host of the replicated parties can create contracts they sign
      createContracts(
        if (thresholdOnBothHosts.isDefined) third else source,
        actorOn(third, thirdCounterparties),
        sourceCounterparties,
        shift = 3,
      ),
      observersAt = step => Seq(r(step + 3), r(step + 4), sourceCounterparties(step % 2)),
    )
    // With a higher threshold, the replicated parties can no longer submit, only confirm
    thresholdOnBothHosts.filter(_ > PositiveInt.one).foreach(hostReplicatedOnBoth)
    val workloads = Seq(onSource, onThird)
    workloads.foreach(_.start())
    def afterProgress(steps: Int): Future[Unit] =
      onSource
        .afterMoreSteps(steps)
        .zipWith(onThird.afterMoreSteps(steps))((_, _) => ())(parallelExecutionContext)

    val (activationOffsets, activatedAt) = loggerFactory.assertLogsUnorderedOptional(
      {
        proposeOnTarget(replicated)
        target.synchronizers.disconnect_all()
        val beforeActivationOffset = source.ledger_api.state.end()
        val activationOffsets = getProgrammableSequencer(sequencer1.name).withSendPolicy_(
          "hold back the source's topology broadcasts until both participants have transacted",
          {
            case request
                if request.sender == source.id &&
                  request.batch.allRecipients.contains(AllMembersOfSynchronizer) =>
              SendDecision.HoldBack(afterProgress(2))
            case _ => SendDecision.Process
          }: SendPolicyWithoutTraceContext,
        )(authorizeOnSourceOneByOne(replicated, beforeActivationOffset))
        val activatedAt = replicated.map(party =>
          CantonTimestamp
            .fromInstant(
              source.topology.party_to_participant_mappings
                .list(daId, filterParty = party.toProtoPrimitive)
                .loneElement
                .context
                .validFrom
            )
            .value
        )
        val targetLedgerEnd = importAndReconnect(exportAll(replicated, beforeActivationOffset))
        // The target catches up while both participants go on transacting
        afterProgress(3).futureValue
        workloads.foreach(_.stop())
        clearOnboardingFlags(replicated, targetLedgerEnd)
        (activationOffsets, activatedAt)
      },
      (
        LogEntryOptionality.OptionalMany,
        (entry: LogEntry) => entry.errorMessage should include("NO_VIEW_WITH_VALID_RECIPIENTS"),
      ),
    )

    partiesSharingAnOffset(activationOffsets) shouldBe empty
    val activations = activatedAt.sorted
    val recordTimes = workloads.flatMap(_.recordTimes)
    withClue("Transactions between consecutive activations: ") {
      all(activations.zip(activations.tail).map { case (from, to) =>
        recordTimes.count(time => from < time && time < to)
      }) should be > 0
    }
    assertSameAcsOnSourceAndTarget(replicated)

    workloads.foreach(_.replaceAllWaitingForAllHosts())
    assertSameAcsOnSourceAndTarget(replicated)
    if (thresholdOnBothHosts.isDefined)
      replicated.foreach(party =>
        activeContracts(third, party) shouldBe activeContracts(source, party)
      )
  }

  /** Replaces the oldest of its contracts in a loop until stopped, with the step's observers.
    * Submissions do not wait for other participants, as the target may be disconnected.
    *
    * A rejected submission is skipped, and its contract replaced later.
    */
  private final class Workload(
      participant: LocalParticipantReference,
      initiallyActive: Seq[UniversalContract.Contract],
      observersAt: Int => Seq[PartyId],
  ) {
    private val running = new AtomicBoolean(true)
    private val steps = new AtomicInteger(0)
    private val recordTimesQueue = new ConcurrentLinkedQueue[CantonTimestamp]()
    private lazy val done: Future[Queue[UniversalContract.Contract]] =
      Future(loop(0, Queue.from(initiallyActive)))

    def start(): Unit = done.discard

    def recordTimes: Seq[CantonTimestamp] = recordTimesQueue.asScala.toSeq

    def afterMoreSteps(count: Int): Future[Unit] = {
      val stepsToReach = steps.get() + count
      Future(blocking(eventually(timeUntilSuccess = 60.seconds, maxPollInterval = 100.millis) {
        steps.get() should be >= stepsToReach
      }))
    }

    def stop(): Unit = {
      running.set(false)
      done.futureValue.discard
    }

    /** Replaces each active contract once more, waiting until all hosts have seen it. */
    def replaceAllWaitingForAllHosts(): Unit =
      done.futureValue.zipWithIndex.foreach { case (contract, index) =>
        participant.ledger_api.javaapi.commands
          .submit(Seq(actorOf(contract)), replace(contract, index).commands().asScala.toSeq)
          .discard
      }

    @tailrec
    private def loop(
        step: Int,
        active: Queue[UniversalContract.Contract],
    ): Queue[UniversalContract.Contract] =
      if (running.get()) {
        val (oldest, rest) = active.dequeue
        val replaced = submit(actorOf(oldest), replace(oldest, step).commands())
          .map(JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(_).loneElement)
        steps.incrementAndGet().discard
        loop(step + 1, rest.enqueue(replaced.getOrElse(oldest)))
      } else active

    private def actorOf(contract: UniversalContract.Contract): PartyId =
      PartyId.tryFromProtoPrimitive(contract.data.actors.asScala.loneElement)

    private def replace(contract: UniversalContract.Contract, step: Int) =
      contract.id.exerciseReplace(
        contract.data.maintainers,
        contract.data.signatories,
        (observersAt(step) :+ actorOf(contract)).map(_.toProtoPrimitive).asJava,
        contract.data.actors,
        List.empty[String].asJava,
      )

    private def submit(submitter: PartyId, commands: java.util.List[Command]): Option[Transaction] =
      Try(
        participant.ledger_api.javaapi.commands
          .submit(Seq(submitter), commands.asScala.toSeq, optTimeout = None)
      ) match {
        case Success(transaction) =>
          recordTimesQueue
            .add(CantonTimestamp.fromInstant(transaction.getRecordTime).value)
            .discard
          Some(transaction)
        case Failure(_: CommandFailure) => None
        case Failure(other) => throw other
      }
  }
}
