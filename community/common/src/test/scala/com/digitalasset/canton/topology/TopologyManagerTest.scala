// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{BatchAggregatorConfig, TopologyConfig}
import com.digitalasset.canton.crypto.BaseCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{TopologyStoreId, ValidatedTopologyTransaction}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  OwnerToKeyMapping,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class TopologyManagerTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)
  import Factory.*

  "TemporaryTopologyManager" should {
    behave like permittingMissingSigningKeySignatures(
      createTemporaryTopologyManager()
    )
  }
  "AuthorizedTopologyManager" should {
    behave like rejectingMissingSigningKeySignatures(
      createAuthorizedTopologyManager()
    )
  }
  "SynchronizerTopologyManager" should {
    behave like rejectingMissingSigningKeySignatures(
      createSynchronizerTopologyManager()._2
    )
    behave like (backpressureOnFullQueue _).tupled(
      createSynchronizerTopologyManager(maxUnsentQueueSize = NonNegativeInt.one)
    )

    behave like ((closingOutboxQueue _).tupled) (createSynchronizerTopologyManager())

    "reject change from Active to Onboarding state unless the optional checks are disabled" in {
      // Protocol version 36 creates a v30 transaction, in which the synchronizer accepts this
      // change on every protocol version
      def mkPtp(serial: PositiveInt, state: HostingState) = mkTrans(
        TopologyTransaction.tryCreate(
          TopologyChangeOp.Replace,
          serial,
          PartyToParticipant.tryCreate(
            party1b,
            PositiveInt.one,
            Seq(HostingParticipant(participant1, ParticipantPermission.Submission, state)),
          ),
          ProtocolVersion.v36,
        )
      )

      def addChangeToOnboarding(disableOptionalTopologyChecks: Boolean) = {
        val (_, manager) = createSynchronizerTopologyManager(
          disableOptionalTopologyChecks = disableOptionalTopologyChecks
        )
        manager.store
          .update(
            SequencedTime(CantonTimestamp.Epoch),
            EffectiveTime(CantonTimestamp.Epoch),
            removals = Map.empty,
            additions = Seq(ns1k1_k1, mkPtp(PositiveInt.one, HostingState.Active))
              .map(ValidatedTopologyTransaction(_)),
          )
          .futureValueUS
        manager
          .add(
            Seq(mkPtp(PositiveInt.two, HostingState.Onboarding)),
            ForceFlags.none,
            expectFullAuthorization = true,
          )
          .futureValueUS
      }

      addChangeToOnboarding(disableOptionalTopologyChecks = true) shouldBe a[Right[?, ?]]
      inside(addChangeToOnboarding(disableOptionalTopologyChecks = false)) { case Left(error) =>
        error.code shouldBe TopologyManagerError.InvalidTopologyMapping
        error.cause should include(
          s"Invalid hosting state transitions for party $party1b: $participant1: Active -> Onboarding"
        )
      }
    }

  }

  private def closingOutboxQueue(
      outbox: SynchronizerOutboxQueue,
      manager: SynchronizerTopologyManager,
  ): Unit = "close the outbox queue" in {
    // put one transaction into the outbox queue
    val asyncResult1 = manager
      .add(Seq(ns1k1_k1), ForceFlags.none, expectFullAuthorization = true)
      .futureValueUS
      .value
    outbox.numUnsentTransactions shouldBe 1

    // dequeue to move the transaction from unsent to inProcess
    outbox.dequeue(limit = PositiveInt.MaxValue).failOnShutdown
    outbox.numUnsentTransactions shouldBe 0
    outbox.numInProcessTransactions shouldBe 1

    // add another transaction to unsent
    val asyncResult2 = manager
      .add(Seq(ns2k2_k2), ForceFlags.none, expectFullAuthorization = true)
      .futureValueUS
      .value
    outbox.numUnsentTransactions shouldBe 1
    outbox.numInProcessTransactions shouldBe 1

    // now close the manager and therefore also the queue
    manager.close()

    outbox.numUnsentTransactions shouldBe 0
    outbox.numInProcessTransactions shouldBe 0

    // the pending async results should be notified of the shutdown
    asyncResult1.unwrap.unwrap.futureValue shouldBe AbortedDueToShutdown
    asyncResult2.unwrap.unwrap.futureValue shouldBe AbortedDueToShutdown

    // attempting to use the queue after the shutdown results in AbortedDueToShutdown
    outbox.enqueue(Seq(ns1k1_k1)).unwrap.unwrap.futureValue shouldBe AbortedDueToShutdown
    outbox.dequeue(limit = PositiveInt.MaxValue) shouldBe AbortedDueToShutdown
    outbox.completeCycle() shouldBe AbortedDueToShutdown

  }

  private def backpressureOnFullQueue(
      outbox: SynchronizerOutboxQueue,
      manager: SynchronizerTopologyManager,
  ): Unit =
    "backpressure if too many changes are pending in the outbox" in {

      val res1 = manager
        .add(Seq(ns1k1_k1), ForceFlags.none, expectFullAuthorization = true)
        .futureValueUS
      outbox.numUnsentTransactions shouldBe 1
      val res2 = manager
        .add(Seq(ns2k2_k2), ForceFlags.none, expectFullAuthorization = true)
        .futureValueUS
      outbox.numUnsentTransactions shouldBe 1
      val _ = outbox.dequeue(PositiveInt.one)
      outbox.numUnsentTransactions shouldBe 0
      val res3 = manager
        .add(Seq(ns2k2_k2), ForceFlags.none, expectFullAuthorization = true)
        .futureValueUS
      res1 shouldBe a[Right[?, ?]]
      res2 shouldBe Left(TopologyManagerError.TooManyPendingTopologyTransactions.Backpressure())
      res3 shouldBe a[Right[?, ?]]

    }

  private def permittingMissingSigningKeySignatures(
      topologyManager: TopologyManager[TopologyStoreId, BaseCrypto]
  ): Unit =
    "permit OwnerToKeyMappings with missing signing key signatures" in {
      val okmS1k7_k1_missing_k7 =
        okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value

      topologyManager
        .add(
          Seq(ns1k1_k1, okmS1k7_k1_missing_k7),
          ForceFlags.none,
          expectFullAuthorization = true,
        )
        .futureValueUS
        .value
        .unwrap
        .futureValueUS

      val tx = topologyManager.store
        .findPositiveTransactions(
          CantonTimestamp.MaxValue,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(Code.OwnerToKeyMapping),
          filterUid = None,
          filterNamespace = None,
        )
        .futureValueUS
        .result
        .loneElement
        .transaction

      // check that the mapping only has key7 as target key
      tx.selectMapping[OwnerToKeyMapping]
        .value
        .mapping
        .keys
        .forgetNE
        .loneElement shouldBe SigningKeys.key7

      // check that the only signature is from key1
      tx.signatures.forgetNE.loneElement.authorizingLongTermKey shouldBe SigningKeys.key1.fingerprint
    }

  private def rejectingMissingSigningKeySignatures(
      topologyManager: TopologyManager[TopologyStoreId, BaseCrypto]
  ): Unit =
    "permit OwnerToKeyMappings with missing signing key signatures" in {
      val okmS1k7_k1_missing_k7 =
        okmS1k7_k1.removeSignatures(Set(SigningKeys.key7.fingerprint)).value

      val error = loggerFactory.assertLogs(
        topologyManager
          .add(
            Seq(ns1k1_k1, okmS1k7_k1_missing_k7),
            ForceFlags.none,
            expectFullAuthorization = true,
          )
          .swap
          .futureValueUS
          .value,
        _.shouldBeCantonError(
          TopologyManagerError.UnauthorizedTransaction,
          _ should include("Topology transaction is missing authorizations by"),
        ),
      )
      error.code shouldBe TopologyManagerError.UnauthorizedTransaction
      error.cause should include("Topology transaction is missing authorizations by")
    }

  private def createAuthorizedTopologyManager() =
    new AuthorizedTopologyManager(
      Factory.sequencer1.uid,
      wallClock,
      Factory.crypto,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting,
      new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        predecessor = None,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      ),
      exitOnFatalFailures = exitOnFatal,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )

  private def createTemporaryTopologyManager() =
    new TemporaryTopologyManager(
      Factory.sequencer1.uid,
      wallClock,
      Factory.crypto,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting,
      new InMemoryTopologyStore(
        TopologyStoreId.TemporaryStore.tryCreate("test"),
        predecessor = None,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      ),
      managerVersion = TopologyManager.PV(testedProtocolVersion),
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )

  private def createSynchronizerTopologyManager(
      maxUnsentQueueSize: NonNegativeInt = NonNegativeInt.tryCreate(10),
      disableOptionalTopologyChecks: Boolean = false,
  ) = {
    val outbox = new SynchronizerOutboxQueue(timeouts, loggerFactory)
    val manager = new SynchronizerTopologyManager(
      Factory.sequencer1.uid,
      wallClock,
      Factory.syncCryptoClient.crypto,
      defaultStaticSynchronizerParameters,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting,
      new InMemoryTopologyStore(
        TopologyStoreId.SynchronizerStore(Factory.physicalSynchronizerId1),
        predecessor = None,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      ),
      outbox,
      dispatchQueueBackpressureLimit = maxUnsentQueueSize,
      disableOptionalTopologyChecks = disableOptionalTopologyChecks,
      exitOnFatalFailures = exitOnFatal,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = loggerFactory,
    )
    (outbox, manager)
  }

}
