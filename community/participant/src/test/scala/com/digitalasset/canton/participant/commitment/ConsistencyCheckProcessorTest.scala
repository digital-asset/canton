// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.commitment.ConsistencyCheckProcessor.{
  DigestInconsistency,
  UnexpectedDigestsInStore,
}
import com.digitalasset.canton.participant.commitment.DigestProcessorTestBase.acsDigest
import com.digitalasset.canton.participant.commitment.InMemoryDigestAccumulator.{
  ParticipantDigestIdentifier,
  PartyDigestIdentifier,
}
import com.digitalasset.canton.participant.config.{AcsCommitmentConfig, AcsDigestTracingMode}
import com.digitalasset.canton.participant.digest.{DigestOps, DigestOpsUtil}
import com.digitalasset.canton.participant.metrics.{CommitmentMetrics, ParticipantTestMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.participant.store.{AcsDigestStore, AcsDigestTestBase}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, TestingTopology}
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, LfPartyId}
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import scala.util.ChainingSyntax

class ConsistencyCheckProcessorTest
    extends DigestProcessorTestBase
    with HasExecutionContext
    with HasActorSystem
    with ChainingSyntax
    with AcsDigestTestBase {

  import DigestProcessorTestBase.*
  import ConsistencyCheckProcessorTest.*

  private def mkConsistencyCheckProcessor(
      participant: ParticipantId = thisParticipant,
      indexService: InternalIndexService = mkIndexService(),
      acsDigestStore: AcsDigestStore = mkInMemoryDigestStore(),
      counterpartyBatchSize: Int = 10,
      // default is 1, so that testing is deterministic
      contractChangeClassificationBatchSize: Int = 1,
      writeJournalTombstonesBatchSize: PositiveInt = PositiveInt.tryCreate(5),
      metrics: CommitmentMetrics = ParticipantTestMetrics.synchronizer.commitments,
  ): ConsistencyCheckProcessor = {
    val testSynchronizerId = DefaultTestIdentities.synchronizerId

    new ConsistencyCheckProcessor(
      thisParticipantId = participant,
      synchronizerId = testSynchronizerId,
      indexService = indexService,
      stringInterningEval = Eval.always(mockStringInterning),
      acsDigestStore = acsDigestStore,
      digestAccumulatorStoreFactory = () =>
        InMemoryAcsDigestStore
          .create(
            stringInterning = Eval.always(mockStringInterning),
            loggerFactory = loggerFactory,
          ),
      digestAccumulatorFactory = digestAccumulatorStore => {
        new SequentialDigestAccumulator(
          acsDigestStore = digestAccumulatorStore,
          stringInterning = mockStringInterning,
          tracingMode = AcsDigestTracingMode.Full,
          metrics = metrics,
          loggerFactory = loggerFactory,
        )
      },
      acsCommitmentConfig = AcsCommitmentConfig(
        counterpartyBatchSize = PositiveInt.tryCreate(counterpartyBatchSize),
        reinitializingJournalTombstonesBatchSize = writeJournalTombstonesBatchSize,
        tracing = AcsDigestTracingMode.Full,
        contractChangeClassificationBatchSize =
          PositiveInt.tryCreate(contractChangeClassificationBatchSize),
      ),
      enableAdditionalConsistencyChecks = true,
      metrics = metrics,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
    )
  }

  "counterpartyBatches source" should {
    "produce no batches when there are no counterparties" in {
      val processor = mkConsistencyCheckProcessor()

      val counterparties = processor.counterpartyBatches(tp100).runWith(Sink.seq).futureValue
      counterparties shouldBe Seq.empty
    }

    "produce batches of counterparties of the expected size" in {
      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob)),
        (off(2), cid(2), Seq(alice, bob, charlie)),
        (off(3), cid(3), Seq(david, eve)),
        (off(4), cid(4), Seq(charlie, david)),
      )
      val indexService = mkIndexService(contracts*)

      val processor = mkConsistencyCheckProcessor(
        counterpartyBatchSize = 2,
        indexService = indexService,
      )

      val counterpartyBatches = processor.counterpartyBatches(tp100).runWith(Sink.seq).futureValue

      counterpartyBatches.length shouldBe 3 // 5 parties split into max batch of 2

      counterpartyBatches.foreach { counterpartyBatch =>
        counterpartyBatch.length should be <= 2
      }

      // The implementation of index service uses Set underneath, so we can't rely on specific order of parties
      counterpartyBatches.flatten should contain theSameElementsAs Seq(
        alice,
        bob,
        charlie,
        david,
        eve,
      )
    }

    "filter out the counterparties with contracts after the timepoint" in {
      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob)),
        (off(2), cid(2), Seq(alice, bob, charlie)),

        // The contracts below should be filtered out
        (off(3), cid(3), Seq(david, eve)),
        (off(4), cid(4), Seq(charlie, david)),
      )
      val indexService = mkIndexService(contracts*)

      val processor = mkConsistencyCheckProcessor(
        indexService = indexService
      )

      val counterparties = processor.counterpartyBatches(tp(2)).runWith(Sink.seq).futureValue

      counterparties.length shouldBe 1

      // The implementation of index service uses Set underneath, so we can't rely on specific order of parties
      counterparties.loneElement should contain theSameElementsAs Seq(alice, bob, charlie)
    }
  }

  "partyDigestMissingAndMismatchedInconsistencies source" should {
    "detect nothing when there are no inconsistencies" in {
      val topologySnapshot = defaultTopologySnapshot()

      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob, charlie))
      )
      val indexService = mkIndexService(contracts*)

      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(alice),
          contractId = cid(1),
          partyPairs = Seq(alice -> alice, alice -> bob, alice -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(bob),
          contractId = cid(1),
          partyPairs = Seq(alice -> bob, bob -> bob, bob -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(charlie),
          contractId = cid(1),
          partyPairs = Seq(alice -> charlie, bob -> charlie, charlie -> charlie),
        ),
      )

      acsDigestStore.party.upsertDigestUpdates(partyDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        counterpartyBatchSize = 1,
        indexService = indexService,
        acsDigestStore = acsDigestStore,
      )

      val ((allPartiesF, participantDigestsF), inconsistenciesF) =
        processor
          .partyDigestMissingAndMismatchedInconsistencies(tp100, topologySnapshot)
          .toMat(Sink.seq)(Keep.both)
          .run()

      val allParties = allPartiesF.futureValue
      val participantDigests = participantDigestsF.futureValue
      val inconsistencies = inconsistenciesF.futureValue

      allParties shouldBe Set(alice, bob, charlie).map(internedPartyId)

      participantDigests shouldBe Map(
        internedParticipantId(p1.toLf) -> DigestOpsUtil
          .makeExpectedDigest(
            contractId = cid(1),
            partyPairs = Seq(
              alice -> alice,
              alice -> bob,
              alice -> charlie,
              alice -> bob,
              bob -> bob,
              bob -> charlie,
              alice -> charlie,
              bob -> charlie,
              charlie -> charlie,
            ),
            enableTracing = true,
          )
          .digest
          .getByteString,
        internedParticipantId(p2.toLf) -> DigestOpsUtil
          .makeExpectedDigest(
            contractId = cid(1),
            partyPairs = Seq(
              alice -> bob,
              bob -> bob,
              bob -> charlie,
              alice -> charlie,
              bob -> charlie,
              charlie -> charlie,
            ),
            enableTracing = true,
          )
          .digest
          .getByteString,
      )

      inconsistencies shouldBe empty
    }

    "detect inconsistencies" in {
      val topologySnapshot = defaultTopologySnapshot()

      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob, charlie))
      )
      val indexService = mkIndexService(contracts*)

      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        // Missing the update for Alice on purpose
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(bob),
          contractId = cid(1),
          partyPairs = Seq(alice -> bob, bob -> bob, bob -> charlie),
        ),
        AcsDigestUpdate(
          acsDigest(
            1,
            internedPartyId(charlie),
            genRawDigest(0x1a).some, // Invalid digest on purpose
          ),
          None,
        ),
      )

      acsDigestStore.party.upsertDigestUpdates(partyDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        indexService = indexService,
        acsDigestStore = acsDigestStore,
      )

      val inconsistencies = processor
        .partyDigestMissingAndMismatchedInconsistencies(tp100, topologySnapshot)
        .runWith(Sink.seq)
        .futureValue

      inconsistencies should contain theSameElementsAs Seq[
        ConsistencyCheckProcessor.DigestInconsistency
      ](
        ConsistencyCheckProcessor.MissingDigestsInStore(
          Set(PartyDigestIdentifier(internedPartyId(alice)))
        ),
        ConsistencyCheckProcessor.DigestValueMismatch(
          PartyDigestIdentifier(internedPartyId(charlie))
        ),
      )
    }
  }

  "unexpectedPartyDigestsInStore source" should {
    "detect inconsistencies" in {
      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob, charlie))
      )
      val indexService = mkIndexService(contracts*)
      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(alice),
          contractId = cid(1),
          partyPairs = Seq(alice -> alice, alice -> bob, alice -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(bob),
          contractId = cid(1),
          partyPairs = Seq(alice -> bob, bob -> bob, bob -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(charlie),
          contractId = cid(1),
          partyPairs = Seq(alice -> charlie, bob -> charlie, charlie -> charlie),
        ),
      )

      acsDigestStore.party.upsertDigestUpdates(partyDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        indexService = indexService,
        acsDigestStore = acsDigestStore,
      )

      val inconsistencies = processor
        .unexpectedPartyDigestsInStore(
          Set(
            internedPartyId(alice),
            internedPartyId(eve),
          ),
          tp100.offset,
        )
        .runWith(Sink.seq)
        .futureValue

      inconsistencies shouldEqual Seq(
        UnexpectedDigestsInStore(
          Set(bob, charlie).map(partyId => PartyDigestIdentifier(internedPartyId(partyId)))
        )
      )
    }

    "detect nothing if there are no unexpected party digests in the store" in {
      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob, charlie))
      )
      val indexService = mkIndexService(contracts*)
      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(alice),
          contractId = cid(1),
          partyPairs = Seq(alice -> alice, alice -> bob, alice -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(bob),
          contractId = cid(1),
          partyPairs = Seq(alice -> bob, bob -> bob, bob -> charlie),
        ),
      )

      acsDigestStore.party.upsertDigestUpdates(partyDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        indexService = indexService,
        acsDigestStore = acsDigestStore,
      )

      val inconsistencies = processor
        .unexpectedPartyDigestsInStore(
          Set(
            internedPartyId(alice),
            internedPartyId(bob),
          ),
          tp100.offset,
        )
        .runWith(Sink.seq)
        .futureValue

      inconsistencies shouldEqual Seq.empty
    }
  }

  "participantDigestInconsistencies source" should {
    "detect inconsistencies" in {
      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        // Missing digest for p1 on purpose
        // Invalid digest for p2
        AcsDigestUpdate(
          acsDigest(
            1,
            internedParticipantId(p2.toLf),
            genRawDigest(0x1a).some,
          ),
          None,
        ),
        // Unexpected digest for p3
        AcsDigestUpdate(
          acsDigest(
            1,
            internedParticipantId(p3.toLf),
            genRawDigest(0x1a).some,
          ),
          None,
        ),
      )

      acsDigestStore.participant.upsertDigestUpdates(partyDigestUpdates).futureValueUS

      val recalculatedDigests = Map(
        internedParticipantId(p1.toLf) -> DigestOpsUtil
          .makeExpectedDigest(
            contractId = cid(1),
            partyPairs = Seq(
              alice -> alice,
              alice -> bob,
              alice -> charlie,
              alice -> bob,
              bob -> bob,
              bob -> charlie,
              alice -> charlie,
              bob -> charlie,
              charlie -> charlie,
            ),
            enableTracing = true,
          )
          .digest
          .getByteString,
        internedParticipantId(p2.toLf) -> DigestOpsUtil
          .makeExpectedDigest(
            contractId = cid(1),
            partyPairs = Seq(
              alice -> bob,
              bob -> bob,
              bob -> charlie,
              alice -> charlie,
              bob -> charlie,
              charlie -> charlie,
            ),
            enableTracing = true,
          )
          .digest
          .getByteString,
      )

      val processor = mkConsistencyCheckProcessor(
        acsDigestStore = acsDigestStore
      )

      val inconsistencies = processor
        .participantDigestInconsistencies(
          recalculatedDigests,
          tp100,
        )
        .futureValue

      val expected: Seq[ConsistencyCheckProcessor.DigestInconsistency] = Seq(
        ConsistencyCheckProcessor.MissingDigestsInStore(
          Set(
            ParticipantDigestIdentifier(internedParticipantId(p1.toLf))
          )
        ),
        ConsistencyCheckProcessor.DigestValueMismatch(
          ParticipantDigestIdentifier(internedParticipantId(p2.toLf))
        ),
        ConsistencyCheckProcessor.UnexpectedDigestsInStore(
          Set(
            ParticipantDigestIdentifier(internedParticipantId(p3.toLf))
          )
        ),
      )

      inconsistencies should contain theSameElementsAs expected
    }

    "detect nothing if there are no inconsistencies" in {
      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val participantDigestUpdates = Seq(
        acsDigestUpdate(
          at = 1,
          key = internedParticipantId(p1.toLf),
          contractId = cid(1),
          partyPairs = Seq(
            alice -> alice,
            alice -> bob,
            alice -> charlie,
            alice -> bob,
            bob -> bob,
            bob -> charlie,
            alice -> charlie,
            bob -> charlie,
            charlie -> charlie,
          ),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedParticipantId(p2.toLf),
          contractId = cid(1),
          partyPairs = Seq(
            alice -> bob,
            bob -> bob,
            bob -> charlie,
            alice -> charlie,
            bob -> charlie,
            charlie -> charlie,
          ),
        ),
      )

      acsDigestStore.participant.upsertDigestUpdates(participantDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        acsDigestStore = acsDigestStore
      )

      val recalculatedDigests = Map(
        internedParticipantId(p1.toLf) -> DigestOpsUtil
          .makeExpectedDigest(
            contractId = cid(1),
            partyPairs = Seq(
              alice -> alice,
              alice -> bob,
              alice -> charlie,
              alice -> bob,
              bob -> bob,
              bob -> charlie,
              alice -> charlie,
              bob -> charlie,
              charlie -> charlie,
            ),
            enableTracing = true,
          )
          .digest
          .getByteString,
        internedParticipantId(p2.toLf) -> DigestOpsUtil
          .makeExpectedDigest(
            contractId = cid(1),
            partyPairs = Seq(
              alice -> bob,
              bob -> bob,
              bob -> charlie,
              alice -> charlie,
              bob -> charlie,
              charlie -> charlie,
            ),
            enableTracing = true,
          )
          .digest
          .getByteString,
      )

      val inconsistencies = processor
        .participantDigestInconsistencies(
          recalculatedDigests,
          tp100,
        )
        .futureValue

      inconsistencies shouldEqual Seq.empty
    }
  }

  "The entire consistency check" should {
    "log all the inconsistencies if the inconsistencies are found" in {
      val topologySnapshot = defaultTopologySnapshot()
      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob, charlie))
      )
      val indexService = mkIndexService(contracts*)
      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        // Missing party digest for alice
        AcsDigestUpdate(
          acsDigest(
            1,
            internedPartyId(bob),
            genRawDigest(0x1a).some, // Invalid party digest for bob
          ),
          None,
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(charlie),
          contractId = cid(1),
          partyPairs =
            Seq(alice -> charlie, bob -> charlie, charlie -> charlie), // Valid digest for Charlie
        ),
        // Unexpected party digest for david
        AcsDigestUpdate(
          acsDigest(
            1,
            internedPartyId(david),
            genRawDigest(0x1a).some,
          ),
          None,
        ),
      )

      val participantDigestUpdates = Seq(
        // Missing participant digest for p1
        AcsDigestUpdate(
          acsDigest(
            1,
            internedParticipantId(p2.toLf),
            genRawDigest(0x1a).some, // Invalid digest value for p2
          ),
          None,
        ),
        // Unexpected participant digest for p3
        AcsDigestUpdate(
          acsDigest(
            1,
            internedParticipantId(p3.toLf),
            genRawDigest(0x1a).some,
          ),
          None,
        ),
      )

      acsDigestStore.party.upsertDigestUpdates(partyDigestUpdates).futureValueUS
      acsDigestStore.participant.upsertDigestUpdates(participantDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        indexService = indexService,
        acsDigestStore = acsDigestStore,
        counterpartyBatchSize = 2,
      )

      val expectedLoggedInconsistencies: Seq[(DigestInconsistency, String)] = Seq(
        (
          ConsistencyCheckProcessor
            .MissingDigestsInStore(Set(PartyDigestIdentifier(internedPartyId(alice)))),
          "Missing digest for Alice",
        ),
        (
          ConsistencyCheckProcessor
            .DigestValueMismatch(PartyDigestIdentifier(internedPartyId(bob))),
          "Mismatched digest value for Bob",
        ),
        (
          ConsistencyCheckProcessor
            .UnexpectedDigestsInStore(Set(PartyDigestIdentifier(internedPartyId(david)))),
          "Unexpected digest for David",
        ),
        (
          ConsistencyCheckProcessor
            .MissingDigestsInStore(
              Set(ParticipantDigestIdentifier(internedParticipantId(p1.toLf)))
            ),
          "Missing digest for p1",
        ),
        (
          ConsistencyCheckProcessor
            .DigestValueMismatch(
              ParticipantDigestIdentifier(internedParticipantId(p2.toLf))
            ),
          "Mismatched digest value for p2",
        ),
        (
          ConsistencyCheckProcessor
            .UnexpectedDigestsInStore(
              Set(ParticipantDigestIdentifier(internedParticipantId(p3.toLf)))
            ),
          "Unexpected digest for p3",
        ),
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        processor.runConsistencyCheck(tp100, topologySnapshot).futureValue,
        LogEntry.assertLogSeq(
          mustContainWithClue = expectedLoggedInconsistencies.map { case (inconsistency, clue) =>
            (
              _.warningMessage should include(inconsistency.toString),
              clue,
            )
          }
        ),
      )
    }

    "log nothing if no inconsistencies are found" in {
      val topologySnapshot = defaultTopologySnapshot()
      val contracts = Seq(
        (off(1), cid(1), Seq(alice, bob, charlie))
      )
      val indexService = mkIndexService(contracts*)
      val acsDigestStore = mkInMemoryDigestStore(mockStringInterning)

      val partyDigestUpdates = Seq(
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(alice),
          contractId = cid(1),
          partyPairs = Seq(alice -> alice, alice -> bob, alice -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(bob),
          contractId = cid(1),
          partyPairs = Seq(alice -> bob, bob -> bob, bob -> charlie),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedPartyId(charlie),
          contractId = cid(1),
          partyPairs = Seq(alice -> charlie, bob -> charlie, charlie -> charlie),
        ),
      )

      val participantDigestUpdates = Seq(
        acsDigestUpdate(
          at = 1,
          key = internedParticipantId(p1.toLf),
          contractId = cid(1),
          partyPairs = Seq(
            alice -> alice,
            alice -> bob,
            alice -> charlie,
            alice -> bob,
            bob -> bob,
            bob -> charlie,
            alice -> charlie,
            bob -> charlie,
            charlie -> charlie,
          ),
        ),
        acsDigestUpdate(
          at = 1,
          key = internedParticipantId(p2.toLf),
          contractId = cid(1),
          partyPairs = Seq(
            alice -> bob,
            bob -> bob,
            bob -> charlie,
            alice -> charlie,
            bob -> charlie,
            charlie -> charlie,
          ),
        ),
      )

      acsDigestStore.party.upsertDigestUpdates(partyDigestUpdates).futureValueUS
      acsDigestStore.participant.upsertDigestUpdates(participantDigestUpdates).futureValueUS

      val processor = mkConsistencyCheckProcessor(
        indexService = indexService,
        acsDigestStore = acsDigestStore,
        counterpartyBatchSize = 2,
      )

      processor.runConsistencyCheck(tp100, topologySnapshot).futureValue
    }
  }

  "ensureUniqueAndMergeParticipantDigestMaps" should {
    "return the combined digests when the map keys are unique" in {
      val processor = mkConsistencyCheckProcessor()
      val digestMap1 = Map(
        internedParticipantId(p1.toLf) -> genRawDigest(0x1a),
        internedParticipantId(p2.toLf) -> genRawDigest(0x2a),
      )

      val digestMap2 = Map(
        internedParticipantId(p3.toLf) -> genRawDigest(0x3a),
        internedParticipantId(p4.toLf) -> genRawDigest(0x4a),
      )

      processor.ensureUniqueAndMergeParticipantDigestMaps(digestMap1, digestMap2) shouldBe Map(
        internedParticipantId(p1.toLf) -> genRawDigest(0x1a),
        internedParticipantId(p2.toLf) -> genRawDigest(0x2a),
        internedParticipantId(p3.toLf) -> genRawDigest(0x3a),
        internedParticipantId(p4.toLf) -> genRawDigest(0x4a),
      )
    }

    "throw an exception when the map keys are not unique" in {
      val processor = mkConsistencyCheckProcessor()
      val digestMap1 = Map(
        internedParticipantId(p1.toLf) -> genRawDigest(0x1a),
        internedParticipantId(p2.toLf) -> genRawDigest(0x2a),
      )

      val digestMap2 = Map(
        internedParticipantId(p2.toLf) -> genRawDigest(0x3a),
        internedParticipantId(p3.toLf) -> genRawDigest(0x4a),
      )

      loggerFactory.assertThrowsAndLogs[IllegalStateException](
        processor.ensureUniqueAndMergeParticipantDigestMaps(digestMap1, digestMap2),
        _.throwable.value.getMessage shouldBe s"Unexpected repeated participant digests for participants ${Set(p2.toLf)}",
      )
    }
  }

  "Merging digest maps" should {
    "return the expected result" in {
      val digestMap1 = Map(
        p1.toLf -> genRawDigest(0x1a),
        p2.toLf -> genRawDigest(0x2a),
      )

      val digestMap2 = Map(
        p2.toLf -> genRawDigest(0x3a),
        p3.toLf -> genRawDigest(0x4a),
      )

      ConsistencyCheckProcessor.mergeDigestMaps(digestMap1, digestMap2) shouldBe Map(
        p1.toLf -> genRawDigest(0x1a),
        p2.toLf -> DigestOps
          .combineDigests(
            Seq(
              genTracedLtHash(0x2a),
              genTracedLtHash(0x3a),
            )
          )
          .digest
          .getByteString,
        p3.toLf -> genRawDigest(0x4a),
      )
    }
  }

  private def defaultTopologySnapshot(): TopologySnapshot = {
    val topologySnapshotFactory = TestingTopology(topology =
      Map(
        partyHosting(alice)(p1),
        partyHosting(bob)(p1, p2),
        partyHosting(charlie)(p1, p2),
      )
    ).build()

    topologySnapshotFactory.topologySnapshot(timestampOfSnapshot = tp100.recordTime)
  }
}

object ConsistencyCheckProcessorTest {
  private def acsDigestUpdate[K](
      at: Int,
      key: K,
      contractId: LfContractId,
      partyPairs: Seq[(LfPartyId, LfPartyId)],
  ) =
    AcsDigestUpdate(
      acsDigest(
        at,
        key,
        DigestOpsUtil
          .makeExpectedDigest(
            contractId = contractId,
            partyPairs = partyPairs,
            enableTracing = true,
          )
          .digest
          .getByteString
          .some,
      ),
      None,
    )
}
