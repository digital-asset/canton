// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.digest

import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.AcsUpdate
import com.digitalasset.canton.participant.commitment.{
  AcsDigestTrace,
  SingleTrace,
  TraceElement,
  TracedLtHash16Blake3,
}
import com.digitalasset.canton.participant.digest.DigestOperation.Add
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  LocalPartyFirst,
  PartyAndOrder,
  RemotePartyFirst,
}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.{BaseTest, LfPartyId, ReassignmentCounter}
import org.scalatest.wordspec.AnyWordSpec

class DigestOpsTest extends AnyWordSpec with BaseTest {

  import DigestOpsTest.*

  def runTests(enableDigestTracing: Boolean) = {

    val tracingString = s"With tracing ${if (enableDigestTracing) "enabled" else "disabled"}"

    def assertTraces(actual: TracedLtHash16Blake3, expected: TraceElement*): Unit =
      if (enableDigestTracing)
        actual.trace.value.traces should contain theSameElementsInOrderAs (expected)
      else actual.trace shouldBe empty

    s"$tracingString, calculating a single digest" when {
      "return the expected result in a simple scenario" in {
        val expectedDigestHexString =
          """02ba192d55152d188526ad8245b6de1f8ed15ade3e71cc84e52416a7c60e5194a0837d8c95a7b5dcb3bdcc4dfa95d74c7a3840e4abf4e9b7e55bf5215fc86ac9be429da5a3e81160872db1fbd949d04e3202a1c904e3eb463ed8168f68608076c9b4aa8b48fd73811f698a3d2ec9c6d8b5f688d0cbcebc87583beda176b61aa4c2f35925275e40e90c9996bbf5a8f2e7a92f2c8832ecce49161ceca6c4232a90394a806d5025278c567647eb4bab482c2fb4a23944f270ce5efb1dacb404cbbd4f024d8010d92d9913f18c45489363c32add50422b2c8bdfb2fffc6ca6039a3ab1c1dd52949fc711cb28b3d6b6d1979c0243cfc9a9a996ffcdc8a8996fc2112da901226d1dbc2beb562f5699600b0fbaadaa734882887e80803b541142121b90690c5b268043712136a17528c73fbb2352076831e5f7e6ed5cf32188b293d001b559dd36d8b97afc26ca3e320019fc2c37998fd6427e41e13b4a19a458790aea4c2b2806755640d84bba68d9ce66045581555fe7e9a9bc6a554e7f8207cd65fb2198d6ba66b33c688ada474c6d0d80000022492424d65f2ddce3af98519394da3e6f0a6b08990e035aeed0db0ea881ecdcf93d1725c06a775f5f6270175908c122cbabfa745fd3a77e6a92bb44cb0eecb5c97a5dc7873030e530a0abfa3d760fad7022d60fe4a98a17e61ffb1c16941a48a35ad27c47b6ec93f2ada1ccfe60949007c7328b1320459603cca7a50fba85bbd4ff40311278dd4f48977c64e8ca29aae81bd605a11a2bb0ea0a1b087d7faf9ef8fae1325f6ae2a6d05b53295ed0de59638bc02deb9a56de27b014e2495bf6cdd810aad4d7ae5d7b145f335949b89bbb294e130739a71ff92940a455f9d5733f76951aaf10ee8f835b99eebbb1958f50be42915c9cbe79c5e8c79a68f2c21ea065f5b2b640c8777706e84fcebe83fa9fdf5258dd3d48f508c7b52d8434042ec4526a27b24d31082590eacdb209c756c9c04aaceea65463f46976c2736953267911115379f11e119b7c8475f69091ae79dc971bf516e1f21e7628fa78aa3ca63bf27056467866d9434f969efd5397c3e12a846265578fc66050e03a23a0d5a189fb48e64033b67551bb2dda032201a318a0872243734c083ac1029d618ec24fe15883e010a8f3539ae7106f5c5df5cd4780d707dbf34fec8e76db5959865b19a8287203f6260771472fae0da6b175347e6a7f57700a5aca6759d0338c47c08a798870ec1d81a26866bac05cef9eb001336640089008da25f5fbaf60d587238605af4de5190567dd984ce5efbeb7ff317e951318e7bb9a9e5f47d62dd37016c55fe7aceb23075176e51e8928b095c0612eb69cf3aa7043e6788dd5c9b960a49ec67bd3824fbcf4c33cbc295bd9a020d5b12f7ff268423b688e1cc413a9437ed064c91f9abb718b2646d1280a1c00a699fed459df6564d82dc885a5177e0af5ba3b77091c13c32bffdfbc4cf8d0f82e6290fd0a565ac2374701066871e80033fde9f450d974c6ed041c7fa3277aa1d3e97982f41de44676e77bc4a61bbe2c5d3d2057ae1df2e1ab11f493113be388c622f47cff0e1d0d0d2d88e30537ee59aa82ad506fb2625e71e9928037696abe36ac181d47eaaca8f375e7404940adff78dcf171795ee46717335181f02387277f581f0ed928b449b45148c4b894d4a781f6459c3b74cb6860e81388eb296503c1f2d3fa2ef766d15efb7fe194e2707a7fdfa382c596c45f1e408fb97a37c0a72f6a1f51b774b495d6c0b756dc61923f48003a3b48d5c9ca31b89548b398cd383271b84fa68198a76ab0361a9fb36c0b4a438efdb97b0c7aecd7c28cd31ddb10aac84f0d6dac5e3416332f034165ff31687415d59fcd4f0edbd98e25c72edaeaa44bc8a2eccb5a89e723b5a8f4688463a965e7bdd51a8dbf063c0bde599b42e534bbb5d52203da1af55d679264888a9030b387e4e9e7188cf359abd6342cf350c9080d3cf55dbc0eb22ba866b4dcc2b96a20b46ab054d08d69215f964bc35dfdb0c1e813b849739d67f544566ec490f1129a03a06408b42cd49c6c937344b2e1d6e0f732de8f321a987099c610b25f6170a182e3ba25d4640ff005b931615be17133c975a979b680c427e220c5ea0886d348028856506e1d95b0f8cc9728a3645ba3524e120623a254bf74a0b5a494234fb758c8fef525862c6afe4c0c07b2d5e159b2241b7151f8aab530d557686ca840e4adcdb2e3b45d61ca920a6199accedc94842a4111e726eabcb7b2281f7abbc5446da2a0d51dfba7f2d3d6b390637bf76844d8501ad27f72fc652172cc4ff380ba6c5f8168092d56bf8e82b4781fa1559cf8e95ed2caf46d89678bfa4c490f61930374b8be431ac52e6a06e72f29ae053df9fbea88573b4216e1bf4b198f19256fe62cd452f34c3b1d6aaf702fa51c379d7a97d37d6b3bbb092c8016d457d2469d040988b99161516df022f538c6e8254a2e57ae916c81da042fbd387e9445d3181be52e68dd8919721924e2a58f8013c95d09b71c420479789942307dcf7794f8f79d4cb7d8eb17808f0802426127d74eac1d5695ca37c920be4b8d32358264add45eaefdeba8bdebc4d1a28716b5b2895bfee62414129b7248ac8dca87904921a60ac2287406a02564e5f1b47527f4e077d04701fdcb204a3dfdd177fb04b6cb94df54bd55179861957b10b75fd2c5375a9b6528f2263000b35acae1df0574e479cf5d48ee40a3c48851944a71a19ad3f63e0077c018fe11a3d26e9ff4427b920421bf912459ee251911da404c0f1e9acb32ce31551757db893273922e50519e1fa7c5837ad450634d0cdc288d43330fb08a490f3083a9d65940f5447af820599972826a48836a0734b9f4f1052c695f"""

        val digest = DigestOps.singleDigest(
          contractId = contractId1,
          reassignmentCounter = ReassignmentCounter.Genesis,
          partyId1 = party1,
          partyId2 = party2,
          isActivation = true,
          traceChanges = enableDigestTracing,
        )

        digest.digest.hexString() shouldBe expectedDigestHexString

        assertTraces(
          digest,
          SingleTrace(
            contractId1,
            ReassignmentCounter.Genesis,
            party1,
            party2,
            isActivation = true,
          ),
        )
      }
    }

    s"$tracingString, calculating multiple digests" when {
      "return the same result regardless of the order" in {
        val digests1 = Seq(
          DigestOps.singleDigest(
            contractId = contractId1,
            reassignmentCounter = ReassignmentCounter.Genesis,
            partyId1 = party1,
            partyId2 = party2,
            isActivation = true,
            traceChanges = enableDigestTracing,
          ),
          DigestOps.singleDigest(
            contractId = contractId1,
            reassignmentCounter = ReassignmentCounter.Genesis,
            partyId1 = party2,
            partyId2 = party3,
            isActivation = true,
            traceChanges = enableDigestTracing,
          ),
          DigestOps.singleDigest(
            contractId = contractId1,
            reassignmentCounter = ReassignmentCounter.Genesis,
            partyId1 = party3,
            partyId2 = party1,
            isActivation = true,
            traceChanges = enableDigestTracing,
          ),
        )

        val expectedTraces = Seq(
          SingleTrace(
            contractId = contractId1,
            reassignmentCounter = ReassignmentCounter.Genesis,
            partyId1 = party1,
            partyId2 = party2,
            isActivation = true,
          ),
          SingleTrace(
            contractId = contractId1,
            reassignmentCounter = ReassignmentCounter.Genesis,
            partyId1 = party2,
            partyId2 = party3,
            isActivation = true,
          ),
          SingleTrace(
            contractId = contractId1,
            reassignmentCounter = ReassignmentCounter.Genesis,
            partyId1 = party3,
            partyId2 = party1,
            isActivation = true,
          ),
        )

        val digests2 = digests1.reverse

        val combined1 = DigestOps.combineDigests(digests1)
        val combined2 = DigestOps.combineDigests(digests2)

        combined1.digest.hexString() shouldBe combined2.digest.hexString()
        assertTraces(combined1, expectedTraces*)
        assertTraces(combined2, expectedTraces.reverse*)

      }
    }

    s"$tracingString, calculating the digest delta" when {
      "return the expected result for contract activation with a single counter-participant" in {
        val reassignmentCounter = ReassignmentCounter.Genesis

        val actualDeltas = DigestOps.computeDeltas(
          participant1,
          AcsUpdate(
            stakeholders = Map(
              party1 -> Set(participant1),
              party2 -> Set(participant1, participant2),
              party3 -> Set(participant1, participant2),
            ),
            locallyHostedStakeholders = Seq(party1, party2, party3),
            cid = contractId1,
            rc = reassignmentCounter,
            isActivation = true,
          ),
          traceChanges = enableDigestTracing,
        )

        val expectedDeltas = Seq[DigestDelta](
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party1, LocalPartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(party1 -> party1, party2 -> party1, party3 -> party1),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Add,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party1, RemotePartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(party1 -> party1, party1 -> party2, party1 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Add,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party2, LocalPartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(party1 -> party2, party2 -> party2, party3 -> party2),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Add,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party2, RemotePartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(party2 -> party1, party2 -> party2, party2 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Add,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party3, LocalPartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(party1 -> party3, party2 -> party3, party3 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Add,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party3, RemotePartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(party3 -> party1, party3 -> party2, party3 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Add,
          ),
          DigestDelta.Participant(
            participantId = participant1,
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(
                party1 -> party1,
                party1 -> party2,
                party1 -> party3,
                party2 -> party1,
                party2 -> party2,
                party2 -> party3,
                party3 -> party1,
                party3 -> party2,
                party3 -> party3,
              ),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = Add,
          ),
          DigestDelta.Participant(
            participantId = participant2,
            digest = makeExpectedDigest(
              contractId = contractId1,
              partyPairs = Set(
                party1 -> party2,
                party2 -> party2,
                party3 -> party2,
                party1 -> party3,
                party2 -> party3,
                party3 -> party3,
              ),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = Add,
          ),
        )

        actualDeltas should contain theSameElementsAs (expectedDeltas)

        forAll(actualDeltas)(delta =>
          if (enableDigestTracing) delta.digest.trace.value.traces should not be empty
          else delta.digest.trace shouldBe None
        )
      }

      "return the expected result for contract archivization with two counter-participant (before and after)" in {

        // Let's use something different than zero
        val reassignmentCounter = ReassignmentCounter.One

        val actualDeltas = DigestOps.computeDeltas(
          participant2,
          AcsUpdate(
            stakeholders = Map(
              party3 -> Set(participant1, participant2, participant3),
              party4 -> Set(participant1, participant3),
            ),
            locallyHostedStakeholders = Seq(party3),
            cid = contractId2,
            rc = reassignmentCounter,
            isActivation = false,
          ),
          traceChanges = enableDigestTracing,
        )

        val expectedDeltas = Seq[DigestDelta](
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party3, LocalPartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party3 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party4, LocalPartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party3 -> party4),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party3, RemotePartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party3 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
          DigestDelta.Party(
            partyAndOrder = PartyAndOrder(party4, RemotePartyFirst),
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party4 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
          DigestDelta.Participant(
            participantId = participant3,
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party3 -> party3, party3 -> party4),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
          DigestDelta.Participant(
            participantId = participant2,
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party3 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
          DigestDelta.Participant(
            participantId = participant1,
            digest = makeExpectedDigest(
              contractId = contractId2,
              partyPairs = Set(party3 -> party3, party4 -> party3),
              reassignmentCounter = reassignmentCounter,
              enableTracing = enableDigestTracing,
            ),
            operation = DigestOperation.Remove,
          ),
        )

        actualDeltas should contain theSameElementsAs expectedDeltas
      }

      "return the same result for the counter-participant" in {
        val reassignmentCounter = ReassignmentCounter.Genesis

        val deltaForParticipant2 = DigestOps
          .computeDeltas(
            participant1,
            AcsUpdate(
              stakeholders = Map(
                party1 -> Set(participant1),
                party2 -> Set(participant1, participant2),
                party3 -> Set(participant1, participant2),
              ),
              locallyHostedStakeholders = Seq(party1, party2, party3),
              cid = contractId1,
              rc = reassignmentCounter,
              isActivation = true,
            ),
            traceChanges = enableDigestTracing,
          )
          .collectFirst {
            case delta: DigestDelta.Participant if delta.participantId == participant2 => delta
          }
          .value

        val deltaForParticipant1 = DigestOps
          .computeDeltas(
            participant2,
            AcsUpdate(
              stakeholders = Map(
                party1 -> Set(participant1),
                party2 -> Set(participant1, participant2),
                party3 -> Set(participant1, participant2),
              ),
              locallyHostedStakeholders = Seq(party2, party3),
              cid = contractId1,
              rc = reassignmentCounter,
              isActivation = true,
            ),
            traceChanges = !enableDigestTracing,
          )
          .collectFirst {
            case delta: DigestDelta.Participant if delta.participantId == participant1 => delta
          }
          .value

        deltaForParticipant2.digest.digest shouldBe deltaForParticipant1.digest.digest
      }
    }
  }

  runTests(enableDigestTracing = false)
  runTests(enableDigestTracing = true)

  "Map inversion method" should {
    "return the expected result" in {

      val originalMap = Map(
        "X" -> Set(100, 200, 300),
        "Y" -> Set(100),
        "Z" -> Set(200),
      )

      val expected = Map(
        100 -> Set("X", "Y"),
        200 -> Set("X", "Z"),
        300 -> Set("X"),
      )

      DigestOps.invertMap(originalMap) shouldBe expected
    }
  }
}

object DigestOpsTest {

  private val contractId1 = ExampleTransactionFactory.unsuffixedId(1)
  private val contractId2 = ExampleTransactionFactory.unsuffixedId(2)

  private val (party1, party2, party3, party4) = (
    DefaultTestIdentities.party1.toLf,
    DefaultTestIdentities.party2.toLf,
    DefaultTestIdentities.party3.toLf,
    DefaultTestIdentities.party4.toLf,
  )

  private val (participant1, participant2, participant3) = (
    DefaultTestIdentities.participant1.toLf,
    DefaultTestIdentities.participant2.toLf,
    DefaultTestIdentities.participant3.toLf,
  )

  def makeExpectedDigest(
      contractId: LfContractId,
      partyPairs: Set[(LfPartyId, LfPartyId)],
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis,
      isActivation: Boolean = true,
      enableTracing: Boolean,
  ): TracedLtHash16Blake3 =
    DigestOps.combineDigests(partyPairs.map { case (partyId1, partyId2) =>
      DigestOps.singleDigest(
        contractId = contractId,
        reassignmentCounter = reassignmentCounter,
        partyId1 = partyId1,
        partyId2 = partyId2,
        isActivation = isActivation,
        traceChanges = enableTracing,
      )
    }.toSeq)

  def makeExpectedTrace(
      contractId: LfContractId,
      partyPairs: Set[(LfPartyId, LfPartyId)],
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis,
      isActivation: Boolean = true,
  ): AcsDigestTrace =
    AcsDigestTrace(partyPairs.view.map { case (party1, party2) =>
      SingleTrace(contractId, reassignmentCounter, party1, party2, isActivation)
    }.toSeq)

}
