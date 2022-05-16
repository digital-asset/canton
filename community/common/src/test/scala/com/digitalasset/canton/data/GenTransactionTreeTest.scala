// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import com.digitalasset.canton.data.MerkleTree.RevealIfNeedBe
import com.digitalasset.canton.topology.{ParticipantId, TestingIdentityFactory}
import com.digitalasset.canton.topology.transaction.{
  ParticipantAttributes,
  ParticipantPermission,
  TrustLevel,
}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.sequencing.protocol.{Recipients, RecipientsTree}
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
@nowarn("msg=match may not be exhaustive")
class GenTransactionTreeTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  forEvery(factory.standardHappyCases) { example =>
    s"$example" can {
      val transactionTree = example.transactionTree

      "compute the correct sequence of transaction view trees" in {
        transactionTree.allTransactionViewTrees shouldEqual example.transactionViewTrees
      }

      forEvery(example.transactionViewTrees.zip(example.viewWithSubviews).zipWithIndex) {
        case ((expectedTransactionViewTree, (expectedView, _)), index) =>
          s"blind the transaction tree to the $index-th transaction view tree" in {
            transactionTree.transactionViewTree(
              expectedTransactionViewTree.viewHash.toRootHash,
              expectedTransactionViewTree.isTopLevel,
            ) shouldEqual expectedTransactionViewTree
          }

          s"yield the correct view for the $index-th transaction view tree" in {
            expectedTransactionViewTree.view shouldEqual expectedView
          }

          val topLevelExpected =
            example.rootTransactionViewTrees.contains(expectedTransactionViewTree)
          s"yield that the $index-th transaction view tree has isTopLevel=$topLevelExpected" in {
            expectedTransactionViewTree.isTopLevel shouldEqual topLevelExpected
          }
      }

      val fullInformeeTree = transactionTree.fullInformeeTree

      val expectedInformeesAndThresholdByView = example.viewWithSubviews.map { case (view, _) =>
        val viewCommonData = view.viewCommonData.tryUnwrap
        ViewHash
          .fromRootHash(view.rootHash) -> ((viewCommonData.informees, viewCommonData.threshold))
      }.toMap

      "be converted between informee and full informee tree" in {
        example.fullInformeeTree.toInformeeTree.tryToFullInformeeTree shouldEqual example.fullInformeeTree
      }

      "compute the set of informees" in {
        example.fullInformeeTree.allInformees shouldEqual example.allInformees
      }

      "compute the full informee tree" in {
        fullInformeeTree should equal(example.fullInformeeTree)

        fullInformeeTree.informeeTreeUnblindedFor(example.allInformees) should equal(
          example.fullInformeeTree.toInformeeTree
        )

        fullInformeeTree.informeesAndThresholdByView shouldEqual expectedInformeesAndThresholdByView
      }

      "compute a partially blinded informee tree" in {
        val (parties, expectedInformeeTree) = example.informeeTreeBlindedFor

        fullInformeeTree.informeeTreeUnblindedFor(parties) should equal(expectedInformeeTree)

        val expectedInformeesByView = expectedInformeesAndThresholdByView
          .map { case (viewHash, (informees, _)) => viewHash -> informees }
          .filter { case (_, informees) =>
            informees.exists(i => parties.contains(i.party))
          }

        expectedInformeeTree.informeesByView shouldEqual expectedInformeesByView
      }

      "be serialized and deserialized" in {
        val transactionTree = example.transactionTree
        GenTransactionTree
          .fromByteString(
            factory.cryptoOps,
            transactionTree.toProtoV0.toByteString,
          ) shouldEqual Right(transactionTree)

        val fullInformeeTree = example.fullInformeeTree
        FullInformeeTree
          .fromByteString(factory.cryptoOps)(
            fullInformeeTree.toByteString(ProtocolVersion.latestForTest)
          ) shouldEqual Right(fullInformeeTree)

        val (_, informeeTree) = example.informeeTreeBlindedFor
        InformeeTree
          .fromByteString(factory.cryptoOps)(
            informeeTree.toByteString(ProtocolVersion.latestForTest)
          ) shouldEqual Right(informeeTree)

        forAll(example.transactionTree.allLightTransactionViewTrees) { lt =>
          LightTransactionViewTree.fromByteString(example.cryptoOps)(
            lt.toByteString(ProtocolVersion.latestForTest)
          ) shouldBe Right(lt)
        }
      }

      "correctly reconstruct the full transaction view trees from the lightweight ones" in {
        val neAllLightTrees = NonEmpty.from(example.transactionTree.allLightTransactionViewTrees)
        val neAllTrees = NonEmpty.from(example.transactionTree.allTransactionViewTrees)
        neAllLightTrees.flatMap(lvts =>
          LightTransactionViewTree.toAllFullViewTrees(lvts).toOption
        ) shouldBe neAllTrees
      }

      "correctly reconstruct the top-level transaction view trees from the lightweight ones" in {
        val allLightTrees = example.transactionTree.allLightTransactionViewTrees
        val neAllLightTrees = NonEmpty.from(allLightTrees)
        val neAllTrees =
          NonEmpty.from(example.transactionTree.allTransactionViewTrees.filter(_.isTopLevel))

        neAllLightTrees.flatMap(lvts =>
          LightTransactionViewTree.toToplevelFullViewTrees(lvts).toOption
        ) shouldBe neAllTrees
      }

      "correctly reconstruct the top-level transaction view trees from the lightweight ones for each informee" in {
        val seedLength = example.cryptoOps.defaultHashAlgorithm.length
        val seed = example.cryptoOps.generateSecureRandomness(seedLength.toInt)
        val hkdfOps = ExampleTransactionFactory.hkdfOps

        val allLightTrees = example.transactionTree
          .allLightTransactionViewTreesWithWitnessesAndSeeds(
            seed,
            hkdfOps,
            ProtocolVersion.latestForTest,
          )
          .valueOrFail("Cant get the light transaction trees")
        val allTrees = example.transactionTree.allTransactionViewTrees.toList
        val allInformees = allLightTrees.map(_._1.informees).fold(Set.empty)(_.union(_))

        forAll(allInformees) { inf =>
          val topLevelHashesForInf = allLightTrees
            .filter(lts =>
              lts._2.unwrap.head.contains(inf) && lts._2.unwrap.tail.forall(!_.contains(inf))
            )
            .map(_._1.viewHash)
            .toSet
          val topLevelForInf = allTrees.filter(t => topLevelHashesForInf.contains(t.viewHash))
          val allLightWeightForInf =
            allLightTrees.filter(_._2.flatten.contains(inf)).map(_._1).toList
          val res =
            LightTransactionViewTree
              .toToplevelFullViewTrees(NonEmptyUtil.fromUnsafe(allLightWeightForInf))
              .value
          res.toList shouldBe topLevelForInf
        }

      }
    }
  }

  "A transaction tree" when {

    val singleCreateView = factory.SingleCreate(ExampleTransactionFactory.lfHash(0)).rootViews.head

    // First check that the normal thing does not throw an exception.
    GenTransactionTree(factory.cryptoOps)(
      factory.submitterMetadata,
      factory.commonMetadata,
      factory.participantMetadata,
      MerkleSeq.fromSeq(factory.cryptoOps)(Seq(singleCreateView)),
    )

    "several root views have the same hash" must {
      "prevent creation" in {
        GenTransactionTree.create(factory.cryptoOps)(
          factory.submitterMetadata,
          factory.commonMetadata,
          factory.participantMetadata,
          MerkleSeq.fromSeq(factory.cryptoOps)(Seq(singleCreateView, singleCreateView)),
        ) should matchPattern {
          case Left(message: String)
              if message.matches(
                "A transaction tree must contain a hash at most once\\. " +
                  "Found the hash .* twice\\."
              ) =>
        }
      }
    }

    "a view and a subview have the same hash" must {
      "prevent creation" in {
        val childViewCommonData =
          singleCreateView.viewCommonData.tryUnwrap.copy(salt = factory.commonDataSalt(1))
        val childView = singleCreateView.copy(viewCommonData = childViewCommonData)
        val parentView = singleCreateView.copy(subviews = Seq(childView))

        GenTransactionTree.create(factory.cryptoOps)(
          factory.submitterMetadata,
          factory.commonMetadata,
          factory.participantMetadata,
          MerkleSeq.fromSeq(factory.cryptoOps)(Seq(parentView)),
        ) should matchPattern {
          case Left(message: String)
              if message.matches(
                "A transaction tree must contain a hash at most once\\. " +
                  "Found the hash .* twice\\."
              ) =>
        }
      }
    }
  }

  "A transaction view tree" when {

    val example = factory.MultipleRootsAndViewNestings

    val rootViewTree = example.rootTransactionViewTrees(1)
    val nonRootViewTree = example.transactionViewTrees(2)

    "everything is ok" must {
      "pass sanity tests" in {
        assert(rootViewTree.isTopLevel)
        assert(!nonRootViewTree.isTopLevel)
      }
    }

    "fully blinded" must {
      "reject creation" in {
        val fullyBlindedTree = example.transactionTree.blind {
          case _: GenTransactionTree => MerkleTree.RevealIfNeedBe
          case _: CommonMetadata => MerkleTree.RevealSubtree
          case _: ParticipantMetadata => MerkleTree.RevealSubtree
          case _ => MerkleTree.BlindSubtree
        }.tryUnwrap

        TransactionViewTree.create(fullyBlindedTree) shouldEqual Left(
          "A transaction view tree must contain an unblinded view."
        )
      }
    }

    "fully unblinded" must {
      "reject creation" in {
        TransactionViewTree.create(example.transactionTree).left.value should startWith(
          "A transaction view tree must not contain several unblinded views: "
        )
      }
    }

    "a subview of the represented view is blinded" must {
      "reject creation" in {
        val onlyView1Unblinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case v: TransactionView =>
            if (v == rootViewTree.view) MerkleTree.RevealIfNeedBe else MerkleTree.BlindSubtree
          case _: MerkleTreeLeaf[_] => MerkleTree.RevealSubtree
        }.tryUnwrap

        TransactionViewTree.create(onlyView1Unblinded).left.value should startWith(
          "A transaction view tree must contain a fully unblinded view:"
        )
      }
    }

    "the submitter metadata is blinded, although view is top level" must {
      "reject creation" in {
        val submitterMetadataBlinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case _: SubmitterMetadata => MerkleTree.BlindSubtree
          case _: TransactionView => MerkleTree.RevealSubtree
          case _: MerkleTreeLeaf[_] => MerkleTree.RevealSubtree
        }.tryUnwrap

        TransactionViewTree
          .create(submitterMetadataBlinded) shouldEqual Left(
          "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
            "Submitter metadata: blinded, isTopLevel: true"
        )
      }
    }

    "the submitter metadata is unblinded, although view is not top level" must {
      "reject creation" in {
        val submitterMetadata = example.transactionTree.submitterMetadata

        val submitterMetadataUnblinded =
          nonRootViewTree.tree.copy(submitterMetadata = submitterMetadata)(factory.cryptoOps)

        TransactionViewTree.create(submitterMetadataUnblinded) shouldEqual Left(
          "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
            "Submitter metadata: unblinded, isTopLevel: false"
        )
      }
    }

    "the common metadata is blinded" must {
      "reject creation" in {
        val commonMetadataBlinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case _: CommonMetadata => MerkleTree.BlindSubtree
          case _ => MerkleTree.RevealSubtree
        }.tryUnwrap

        TransactionViewTree.create(commonMetadataBlinded) shouldEqual Left(
          "The common metadata of a transaction view tree must be unblinded."
        )
      }
    }

    "the participant metadata is blinded" must {
      "reject creation" in {
        val participantMetadataBlinded = rootViewTree.tree.blind {
          case _: GenTransactionTree => RevealIfNeedBe
          case _: ParticipantMetadata => MerkleTree.BlindSubtree
          case _ => MerkleTree.RevealSubtree
        }.tryUnwrap

        TransactionViewTree.create(participantMetadataBlinded) shouldEqual Left(
          "The participant metadata of a transaction view tree must be unblinded."
        )
      }
    }
  }

  "An informee tree" when {

    val example = factory.MultipleRootsAndViewNestings

    "global metadata is incorrectly blinded" must {
      "reject creation" in {
        def corruptGlobalMetadataBlinding(informeeTree: GenTransactionTree): GenTransactionTree =
          informeeTree.copy(
            submitterMetadata = factory.submitterMetadata,
            commonMetadata = ExampleTransactionFactory.blinded(factory.commonMetadata),
            participantMetadata = factory.participantMetadata,
          )(factory.cryptoOps)

        val corruptedGlobalMetadataMessage = Left(
          "The submitter metadata of an informee tree must be blinded. " +
            "The common metadata of an informee tree must be unblinded. " +
            "The participant metadata of an informee tree must be blinded."
        )

        val globalMetadataIncorrectlyBlinded1 =
          corruptGlobalMetadataBlinding(example.informeeTreeBlindedFor._2.tree)
        InformeeTree.create(
          globalMetadataIncorrectlyBlinded1
        ) shouldEqual corruptedGlobalMetadataMessage

        val globalMetadataIncorrectlyBlinded2 =
          corruptGlobalMetadataBlinding(example.fullInformeeTree.tree)
        FullInformeeTree.create(
          globalMetadataIncorrectlyBlinded2
        ) shouldEqual corruptedGlobalMetadataMessage
      }
    }

    "view metadata is incorrectly unblinded" must {
      "reject creation" in {
        val Seq(_, view1Unblinded) = example.transactionTree.rootViews.unblindedElements
        val informeeTree = example.fullInformeeTree.toInformeeTree.tree
        val Seq(_, view1) = informeeTree.rootViews.unblindedElements

        val view1WithParticipantDataUnblinded =
          view1.copy(viewParticipantData = view1Unblinded.viewParticipantData)
        val rootViews = MerkleSeq.fromSeq(factory.cryptoOps)(Seq(view1WithParticipantDataUnblinded))

        val treeWithViewMetadataUnblinded =
          informeeTree.copy(rootViews = rootViews)(factory.cryptoOps)

        val corruptedViewMetadataMessage = "(?s)" +
          "The view participant data in an informee tree must be blinded\\. Found .*\\."

        InformeeTree
          .create(treeWithViewMetadataUnblinded)
          .left
          .value should fullyMatch regex corruptedViewMetadataMessage

        FullInformeeTree
          .create(treeWithViewMetadataUnblinded)
          .left
          .value should fullyMatch regex corruptedViewMetadataMessage
      }
    }
  }

  "A full informee tree" when {

    val example = factory.MultipleRootsAndViewNestings

    "a view is blinded" should {
      "reject creation" in {
        val allBlinded = example.fullInformeeTree.informeeTreeUnblindedFor(Set.empty).tree

        FullInformeeTree
          .create(allBlinded)
          .left
          .value should fullyMatch regex "(?s)All views in a full informee tree must be unblinded\\. Found .*\\."
      }
    }

    "a view common data is blinded" should {
      "reject creation" in {
        val fullInformeeTree = example.fullInformeeTree.tree
        val rootViews = fullInformeeTree.rootViews.unblindedElements

        val rootViewsWithCommonDataBlinded =
          rootViews.map(view =>
            view.copy(viewCommonData = ExampleTransactionFactory.blinded(view.viewCommonData))
          )

        val viewCommonDataBlinded =
          fullInformeeTree.copy(rootViews =
            MerkleSeq.fromSeq(factory.cryptoOps)(rootViewsWithCommonDataBlinded)
          )(factory.cryptoOps)

        FullInformeeTree
          .create(viewCommonDataBlinded)
          .left
          .value should fullyMatch regex "(?s)The view common data in a full informee tree must be unblinded\\. Found .*\\.\n" +
          "The view common data in a full informee tree must be unblinded\\. Found .*\\."
      }
    }
  }

  "Witnesses" must {
    import GenTransactionTreeTest._

    "correctly compute recipients from witnesses" in {
      def mkWitnesses(setup: Seq[Set[Int]]): Witnesses = Witnesses(setup.map(_.map(informee)))
      // Maps parties to participants; parties have IDs that start at 1, participants have IDs that start at 11
      def topology =
        TestingIdentityFactory(
          loggerFactory,
          topology = Map(
            1 -> Set(11),
            2 -> Set(12),
            3 -> Set(13),
            4 -> Set(14),
            5 -> Set(11, 12, 13, 15),
            6 -> Set(16),
          ).map { case (partyId, participantIds) =>
            party(partyId) -> participantIds
              .map(id =>
                participant(id) -> ParticipantAttributes(
                  ParticipantPermission.Submission,
                  TrustLevel.Ordinary,
                )
              )
              .toMap
          },
        ).topologySnapshot()
      val witnesses = mkWitnesses(
        Seq(
          Set(1, 2),
          Set(1, 3),
          Set(2, 4),
          Set(1, 2, 5),
          Set(6),
        )
      )

      witnesses
        .toRecipients(topology)
        .valueOr(err => fail(err.message))
        .futureValue shouldBe Recipients(
        NonEmpty(
          Seq,
          RecipientsTree(
            NonEmpty.mk(Set, participant(16)),
            Seq(
              RecipientsTree(
                NonEmpty(Set, 11, 12, 13, 15).map(participant),
                Seq(
                  RecipientsTree(
                    NonEmpty.mk(Set, participant(12), participant(14)),
                    Seq(
                      RecipientsTree(
                        NonEmpty.mk(Set, participant(11), participant(13)),
                        Seq(
                          RecipientsTree.leaf(NonEmpty.mk(Set, participant(11), participant(12)))
                        ),
                      )
                    ),
                  )
                ),
              )
            ),
          ),
        )
      )
    }
  }
}

object GenTransactionTreeTest {
  def party(i: Int): LfPartyId = LfPartyId.assertFromString(s"party$i::1")
  def informee(i: Int): Informee = PlainInformee(party(i))
  def participant(i: Int): ParticipantId = ParticipantId(s"participant$i")
}
