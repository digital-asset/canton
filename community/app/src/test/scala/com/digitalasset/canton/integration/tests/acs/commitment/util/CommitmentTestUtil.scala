// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.acs.commitment.util

import com.daml.ledger.javaapi.data.Contract
import com.daml.ledger.javaapi.data.codegen.{Contract as ContractWithId, ContractId}
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.acs.commitment.util.ContractsAndCommitment.{
  IouCommitment,
  IouCommitmentWithContracts,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.ReceivedCmtState
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  CommitmentContractMetadata,
  SortedReconciliationIntervals,
  SortedReconciliationIntervalsHelpers,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}

import java.time.Duration as JDuration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

sealed trait ContractsAndCommitment {
  def contracts: Seq[Contract]
  def commitmentPeriod: CommitmentPeriod
  def commitment: AcsCommitment.HashedCommitmentType
  def temporaryContracts: Seq[Contract]
}

object ContractsAndCommitment {
  // For saving memory on large sets, big sequence of contracts shouldn't be stored (these classes are used after computation to return the contracts and commitment with its period), so
  // the active - and temporary contracts are empty lists
  final case class IouCommitment(
      commitmentPeriod: CommitmentPeriod,
      commitment: AcsCommitment.HashedCommitmentType,
  ) extends ContractsAndCommitment {
    override val contracts: Seq[Iou.Contract] = List.empty
    override val temporaryContracts: Seq[Iou.Contract] = List.empty
  }

  final case class IouCommitmentWithContracts(
      contracts: Seq[Iou.Contract],
      commitmentPeriod: CommitmentPeriod,
      commitment: AcsCommitment.HashedCommitmentType,
      temporaryContracts: Seq[Iou.Contract] = List.empty,
  ) extends ContractsAndCommitment
}

final case class IntervalDuration(interval: JDuration) extends AnyVal

trait ContractDeployAndPurge {

  /** Purge one contract on the given participant and then check/return the related computed ACS
    * commitment
    *
    * @param onParticipant
    *   the participant on which we perform the purge
    * @param synchronizerAlias
    *   the synchronizer where the participant is connected to
    * @param synchronizerId
    *   the identifier of the connected synchronizer
    * @param contract
    *   the contract that we purge
    * @param env
    *   console environment
    * @param traceContext
    *   trace context
    * @param interval
    *   the interval that can be used for advancing a simClock
    * @return
    *   the computed contract - and commitment information
    */
  def purgeOneContractAndComputeAcsCommitment(onParticipant: LocalParticipantReference)(
      synchronizerAlias: SynchronizerAlias,
      contract: ContractWithId[? <: ContractId[?], ?],
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
      interval: IntervalDuration,
  ): ContractsAndCommitment

  /** Deploy one contract between two participants and then check/return the related computed ACS
    * commitment
    *
    * @param synchronizerId
    *   the identifier of the synchronizer for which the participants are connected to
    * @param synchronizerAlias
    *   alias of the synchronizer for which the participants are connected to
    * @param firstParticipant
    *   the first participant (aka in its short for, P1), one of the signatories of the contract
    * @param secondParticipant
    *   the second participant (aka in its short form, P2), one of the signatories of the contract
    * @param observers
    *   list of observers (by default empty)
    * @param env
    *   console environment. Useful to use configuration values
    * @param traceContext
    *   trace context. Useful for being able to trace some points of the code
    * @param intervalDuration
    *   the interval that can be used for advancing a simClock
    * @return
    *   the computed contract - and commitment information (with period)
    */
  protected def deployOneContractAndComputeAcsCommitment(
      synchronizerId: SynchronizerId,
      synchronizerAlias: SynchronizerAlias,
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
      observers: Seq[LocalParticipantReference] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
      intervalDuration: IntervalDuration,
  ): ContractsAndCommitment
}

trait CommitmentTestUtil
    extends BaseTest
    with JFRTestPhase
    with ContractDeployAndPurge
    with SortedReconciliationIntervalsHelpers {
  // advance the time sufficiently past the topology transaction registration timeout,
  // so that the ticks to detect a timeout for topology submissions does not seep into the following tests
  protected def passTopologyRegistrationTimeout()(implicit
      environment: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): Unit =
    environment.environment.simClock.value.advance(
      environment.participant1.config.topology.topologyTransactionRegistrationTimeout.asFiniteApproximation.toJava
    )

  protected def deployOnTwoParticipantsAndCheckContract(
      synchronizerId: SynchronizerId,
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
      observers: Seq[LocalParticipantReference] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): Iou.Contract = {
    import env.*

    logger.info(s"Deploying the iou contract on both participants")
    val iou = IouSyntax
      .createIou(firstParticipant, Some(synchronizerId))(
        firstParticipant.adminParty,
        secondParticipant.adminParty,
        observers = observers.toList.map(_.adminParty),
      )

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      (Seq(firstParticipant, secondParticipant) ++ observers).foreach(p =>
        p.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) should not be empty
      )
    }

    iou
  }

  protected def deployOne(
      synchronizerId: SynchronizerId,
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
      observers: Seq[LocalParticipantReference] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): Iou.Contract = {
    import env.*

    logger.info(s"Deploying the iou contract on both participants")
    val iou = IouSyntax
      .createIou(firstParticipant, Some(synchronizerId))(
        firstParticipant.adminParty,
        secondParticipant.adminParty,
        observers = observers.toList.map(_.adminParty),
      )

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      (Seq(firstParticipant, secondParticipant) ++ observers).foreach(p =>
        p.ledger_api.state.acs
          .find_generic(p.adminParty, _.contractId == iou.id.contractId)
          .entry
          .isActiveContract shouldBe true
      )
    }

    iou
  }

  def purgeOneContractAndComputeAcsCommitment(onParticipant: LocalParticipantReference)(
      synchronizerAlias: SynchronizerAlias,
      contract: ContractWithId[? <: ContractId[?], ?],
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
      interval: IntervalDuration,
  ): IouCommitment = {
    import env.*

    // Advance to T1+1 microsecond, where the purging happens and get T1
    val tick1 = advanceAndGetNextTick()

    val purgeStartedAt = System.nanoTime()

    logger.debug(
      s"Purging one contract from ${onParticipant.name} that involves disconnect from synchronizers, purge and then reconnect"
    )
    logger.debug(s"${onParticipant.name} is disconnecting from its synchronizer...")
    onParticipant.synchronizers.disconnect_all()
    eventually() {
      onParticipant.synchronizers.list_connected() shouldBe empty
    }
    logger.debug(s"${onParticipant.name} is disconnected from synchronizer!")

    logger.debug(s"${onParticipant.name} is purging one contract from its store")
    onParticipant.repair.purge(synchronizerAlias, List(contract.id.toLf))

    logger.debug(s"${onParticipant.name} is reconnecting to its synchronizer...")
    onParticipant.synchronizers.reconnect_all()

    // T2 - Advance to T2+1 microsecond and get T2
    val tick2 = advanceAndGetNextTick()

    eventually() {
      onParticipant.synchronizers
        .list_connected()
        .map(_.synchronizerAlias) should contain(synchronizerAlias)
    }
    logger.debug(s"${onParticipant.name} is reconnected to synchronizer!")

    onParticipant.testing.fetch_synchronizer_times()
    onParticipant.health.ping(onParticipant)

    // Eventually we need the computed ACS for T1
    // We purge here only one contract so the ACS commitment computations shouldn't take long time
    // However, during the tests I observed a computation that took more than 2 minutes
    val maximumWaitTime = Math.max(interval.interval.toMillis, 2.minutes.toMillis)
    val (commitmentPeriod, _, commitment) = jfrPhase("compute-on-one-purged-contract") {
      eventually(timeUntilSuccess = maximumWaitTime.millis) {
        onParticipant.commitments
          .computed(
            synchronizerAlias,
            tick1.toInstant,
            tick2.toInstant,
          )
          .loneElement
      }
    }
    val computationEndedAt = System.nanoTime()
    logger.info(
      s"Commitment computation - purging a contract from a participant - took ${TimeUnit.NANOSECONDS
          .toMillis(computationEndedAt - purgeStartedAt)}ms!"
    )

    IouCommitment(commitmentPeriod, commitment)
  }

  override protected def deployOneContractAndComputeAcsCommitment(
      synchronizerId: SynchronizerId,
      synchronizerAlias: SynchronizerAlias,
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
      observers: Seq[LocalParticipantReference] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
      intervalDuration: IntervalDuration,
  ): IouCommitmentWithContracts = {
    import env.*

    // Record T1 and advance to T1+1 microsecond
    val tick1 = advanceAndGetNextTick()

    // This means a submission of a message to the sequencer so essentially the sequencer catches up
    // and so we don't hit the situation when the time is too way behind in Bft sequencer
    firstParticipant.parties.enable(
      UUID.randomUUID().toString,
      synchronizer = Some(synchronizerAlias),
    )
    firstParticipant.testing.fetch_synchronizer_times()
    secondParticipant.testing.fetch_synchronizer_times()

    val deployStartedAt = System.nanoTime()
    // Contract is created at T1
    val deployedContract = deployOne(
      synchronizerId,
      firstParticipant,
      secondParticipant,
      observers,
    )

    // Advance to T2+1 microsecond and get T2
    val tick2 = advanceAndGetNextTick()

    firstParticipant.testing.fetch_synchronizer_times()
    secondParticipant.testing.fetch_synchronizer_times()
    secondParticipant.health.ping(secondParticipant)

    val maximumWaitTime = Math.max(intervalDuration.interval.toMillis, 1.minutes.toMillis)
    val p1Computed = jfrPhase("compute-on-one-deployed-contract-p1") {
      eventually(timeUntilSuccess = maximumWaitTime.millis) {
        val p1Computed = firstParticipant.commitments
          .computed(
            synchronizerAlias,
            tick1.toInstant,
            tick2.toInstant,
            Some(secondParticipant),
          )
        p1Computed.size shouldBe 1
        p1Computed
      }
    }

    jfrPhase("compute-on-one-deployed-contract-p1") {
      eventually(timeUntilSuccess = maximumWaitTime.millis) {
        val p2Computed = secondParticipant.commitments
          .computed(
            synchronizerAlias,
            tick1.toInstant,
            tick2.toInstant,
            Some(firstParticipant),
          )
        p2Computed.size shouldBe 1
      }
    }

    val computationEndedAt = System.nanoTime()
    logger.info(
      s"Commitment computation for deploying an extra contract on two participants, took ${TimeUnit.NANOSECONDS
          .toMillis(computationEndedAt - deployStartedAt)}ms!"
    )

    val (commitmentPeriod, _, commitment) = p1Computed.loneElement

    IouCommitmentWithContracts(List(deployedContract), commitmentPeriod, commitment)
  }

  protected def advanceAndGetNextTick()(implicit
      duration: IntervalDuration,
      env: TestConsoleEnvironment,
  ): CantonTimestampSecond = {
    import env.*

    val simClock = environment.simClock.getOrElse(
      fail("Test failure because we should have a simClock in the environment!")
    )

    val nextTick = tickAfter(simClock.uniqueTime())

    simClock.advanceTo(nextTick.forgetRefinement.immediateSuccessor)
    nextTick
  }

  protected def awaitNextTick(
      participant: LocalParticipantReference,
      counterparticipant: ParticipantReference,
  )(implicit env: TestConsoleEnvironment, intervalDuration: IntervalDuration): CommitmentPeriod = {
    import env.*
    val simClock = environment.simClock.value

    val tick1 = tickAfter(simClock.uniqueTime())
    simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)
    // Await the synchronizer time. Internally this will trigger a fetch of the synchronizer time.
    participant.testing.await_synchronizer_time(daId, tick1.forgetRefinement.immediateSuccessor)

    val p1Computed = eventually() {
      val p1Computed = participant.commitments.computed(
        daName,
        tick1.toInstant.minusMillis(1),
        tick1.toInstant,
        Some(counterparticipant.id),
      )
      p1Computed should have size 1L
      p1Computed
    }

    val (period, _participant, commitment) = p1Computed.loneElement
    period
  }

  protected def checkReceivedCommitment(
      period: CommitmentPeriod,
      participant: ParticipantReference,
      synchronizer: SynchronizerId,
      state: ReceivedCmtState,
      expected: Int = 1,
  ): Unit =
    eventually() {
      val timeRange =
        TimeRange(period.fromExclusive.forgetRefinement, period.toInclusive.forgetRefinement)
      val receivedCommitments = participant.commitments.lookup_received_acs_commitments(
        synchronizerTimeRanges = Seq(SynchronizerTimeRange(synchronizer, Some(timeRange))),
        counterParticipants = Seq.empty,
        commitmentState = Seq.empty,
        verboseMode = false,
      )

      val receivedCommitmentsOnSynchronizer = receivedCommitments.get(synchronizer).value
      receivedCommitmentsOnSynchronizer.size should be >= expected
      forAll(receivedCommitmentsOnSynchronizer)(_.state shouldBe state)
    }

  protected def tickBeforeOrAt(
      timestamp: CantonTimestamp
  )(implicit duration: IntervalDuration): CantonTimestampSecond =
    SortedReconciliationIntervals
      .create(
        Seq(mkParameters(CantonTimestamp.MinValue, duration.interval.getSeconds)),
        CantonTimestamp.MaxValue,
      )
      .value
      .tickBeforeOrAt(timestamp)
      .value

  protected def tickAfter(timestamp: CantonTimestamp)(implicit
      duration: IntervalDuration
  ): CantonTimestampSecond =
    tickBeforeOrAt(timestamp) + PositiveSeconds.tryOfSeconds(duration.interval.getSeconds)

  def deployThreeContractsAndCheck(
      synchronizerId: SynchronizerId,
      alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      intervalDuration: IntervalDuration,
  ): IouCommitmentWithContracts =
    deployContractsAndCheck(
      synchronizerId,
      PositiveInt.three,
      alreadyDeployedContracts,
      firstParticipant,
      secondParticipant,
    )

  def deployOneContractAndCheck(
      synchronizerId: SynchronizerId,
      alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      intervalDuration: IntervalDuration,
  ): IouCommitmentWithContracts =
    deployContractsAndCheck(
      synchronizerId,
      PositiveInt.one,
      alreadyDeployedContracts,
      firstParticipant,
      secondParticipant,
    )

  def deployContractsAndCheck(
      synchronizerId: SynchronizerId,
      nContracts: PositiveInt,
      alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      intervalDuration: IntervalDuration,
  ): IouCommitmentWithContracts = {
    import env.*

    val simClock = environment.simClock.value
    simClock.advanceTo(simClock.uniqueTime().immediateSuccessor)

    val createdCids =
      (1 to nContracts.value).map(_ =>
        deployOnTwoParticipantsAndCheckContract(
          synchronizerId,
          firstParticipant,
          secondParticipant,
        )
      )

    createdCids.size shouldEqual nContracts.value

    val tick1 = tickAfter(simClock.uniqueTime())
    simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)
    // just fetch_synchronizer_times() is sometimes not enough to trigger commitment generation
    firstParticipant.health.ping(firstParticipant)

    firstParticipant.testing.fetch_synchronizer_times()

    val p1Computed = eventually() {
      val p1Computed = firstParticipant.commitments
        .computed(
          daName,
          tick1.toInstant.minusMillis(1),
          tick1.toInstant,
          Some(secondParticipant),
        )
      p1Computed.size shouldBe 1
      p1Computed
    }

    val (commitmentPeriod, _participant, commitment) = p1Computed.loneElement

    alreadyDeployedContracts.set(alreadyDeployedContracts.get().concat(createdCids))
    IouCommitmentWithContracts(createdCids, commitmentPeriod, commitment)
  }
}

// TODO(#30827): rename and move out from integration test to make it available for the admin console, see an invalid usage in Optional step 3 under: https://docs.digitalasset.com/operate/3.4/howtos/troubleshoot/commitments.html#handle-slow-commitment-computation
object CommitmentTestUtil {
  def computeHashedCommitment(
      contracts: Seq[CommitmentContractMetadata]
  ): AcsCommitment.HashedCommitmentType = {
    val h = LtHash16()
    contracts.map(item => item.cid -> item.reassignmentCounter).toMap.foreach {
      case (cid, reassignmentCounter) =>
        AcsCommitmentProcessor.addContractToCommitmentDigest(h, cid, reassignmentCounter)
    }

    val commitment = h.getByteString()
    val sumHash = LtHash16()
    sumHash.add(commitment.toByteArray)

    AcsCommitment.hashCommitment(sumHash.getByteString())
  }
}
