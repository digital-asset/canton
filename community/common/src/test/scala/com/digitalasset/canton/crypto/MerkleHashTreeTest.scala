// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class MerkleHashTreeTest extends AnyWordSpec with BaseTest {

  private def hashes(n: Int): NonEmpty[Seq[Hash]] =
    NonEmpty.from((0 until n).map(TestHash.digest)).value

  private def tree(n: Int): MerkleHashTree = MerkleHashTree.tryCreate(hashes(n))

  private def rootFromPath(hash: Hash, path: InclusionPath): Hash =
    MerkleHashTree.rootFromPath(hash, path)

  private def inclusionPath(index: Int, siblings: Seq[Hash]): InclusionPath =
    InclusionPath.tryCreate(index, siblings)

  // The inner nodes of the tree, to pin down the structure of the tree. The node hashes themselves
  // are pinned down by the test vector below.
  private def inner(left: Hash, right: Hash): Hash = MerkleHashTree.innerNode(left, right)

  private def ceilLog2(n: Int): Int = (0 to 31).find(d => (1L << d) >= n).value

  "MerkleHashTree" should {

    "use the hash itself as root and a path without siblings for a single hash" in {
      val hash = TestHash.digest("single")
      val singleton = MerkleHashTree.tryCreate(NonEmpty(Seq, hash))

      singleton.root shouldBe hash
      singleton.pathFor(0) shouldBe Some(inclusionPath(0, Seq.empty))
      rootFromPath(hash, inclusionPath(0, Seq.empty)) shouldBe hash
    }

    "pair only as many hashes on the lowest level as needed for a perfect tree above" in {
      val h2 = hashes(2).forgetNE
      tree(2).root shouldBe inner(h2(0), h2(1))

      val h3 = hashes(3).forgetNE
      tree(3).root shouldBe inner(inner(h3(0), h3(1)), h3(2))

      // Only the first 2 hashes are paired, so the next level has 4 nodes
      val h5 = hashes(5).forgetNE
      tree(5).root shouldBe inner(inner(inner(h5(0), h5(1)), h5(2)), inner(h5(3), h5(4)))

      // The first 4 hashes are paired, the last two move up
      val h6 = hashes(6).forgetNE
      tree(6).root shouldBe
        inner(inner(inner(h6(0), h6(1)), inner(h6(2), h6(3))), inner(h6(4), h6(5)))
    }

    "minimize the total size of the inclusion paths" in {
      def totalSiblings(n: Int): Int = (0 until n).map(tree(n).pathFor(_).value.siblings.size).sum
      // Two hashes at depth 3 and three at depth 2
      totalSiblings(5) shouldBe 12
      totalSiblings(65) shouldBe 392
    }

    "compute the expected root for a fixed sequence of hashes" in {
      // Test vector: changes to the node hashes or the tree structure change this root
      tree(5).root.toHexString shouldBe
        "122036234ee66613560e82e10eb5c081194ab82ff69d38eb358957b2fb13723a5c12"
    }

    "use the sides at the levels with a sibling as index of the inclusion path" in {
      val h3 = hashes(3).forgetNE
      tree(3).pathFor(0).value shouldBe inclusionPath(0, Seq(h3(1), h3(2)))
      tree(3).pathFor(1).value shouldBe inclusionPath(1, Seq(h3(0), h3(2)))
      // The hash at index 2 = 0b10 has no sibling at the lowest level
      tree(3).pathFor(2).value shouldBe inclusionPath(1, Seq(inner(h3(0), h3(1))))

      // The hash at index 5 = 0b101 is not paired at the lowest level, so the index of the path is
      // 0b11
      val h6 = hashes(6).forgetNE
      tree(6).pathFor(5).value shouldBe
        inclusionPath(3, Seq(h6(4), inner(inner(h6(0), h6(1)), inner(h6(2), h6(3)))))
      tree(6).pathFor(4).value shouldBe
        inclusionPath(2, Seq(h6(5), inner(inner(h6(0), h6(1)), inner(h6(2), h6(3)))))
    }

    "use the position of the hash as index of the inclusion path in a tree of size 2^k" in {
      val t = tree(8)
      forAll(0 until 8) { index =>
        val path = t.pathFor(index).value
        path.index shouldBe index
        path.siblings should have size 3
      }
    }

    (1 to 17).foreach { n =>
      s"prove the inclusion of every hash in a tree of size $n" in {
        val t = tree(n)

        forAll(t.hashes.forgetNE.zipWithIndex) { case (hash, index) =>
          val path = t.pathFor(index).value
          // Every hash is a leaf at depth ceil(log2(n)) or one less
          path.siblings.size should (be <= ceilLog2(n) and be >= ceilLog2(n) - 1)
          path.index should be < (1 << path.siblings.size)
          rootFromPath(hash, path) shouldBe t.root
        }
      }
    }

    "support large trees" in {
      val n = (1 << 16) + 1
      val t = tree(n)
      // Only the first two hashes are paired, so the last hash is a leaf at depth 16
      val lastPath = t.pathFor(n - 1).value
      lastPath.siblings should have size 16
      rootFromPath(t.hashes.last1, lastPath) shouldBe t.root
      // The first two hashes are paired, so they are leaves at depth 17
      val firstPath = t.pathFor(0).value
      firstPath.siblings should have size 17
      rootFromPath(t.hashes.head1, firstPath) shouldBe t.root
    }

    "print the index of an inclusion path in binary with one bit per sibling" in {
      tree(6).pathFor(5).value.toString should include("index = 0b11,")
      tree(8).pathFor(2).value.toString should include("index = 0b010,")
      tree(1).pathFor(0).value.toString should include("index = 0b0,")
    }

    "print the number of siblings of an inclusion path before the siblings" in {
      val printed = tree(6).pathFor(5).value.toString
      printed should include("siblingCount = 2,")
      printed.indexOf("siblingCount") should be < printed.indexOf("siblings =")
    }

    "omit the siblings of an inclusion path without siblings when printing" in {
      val printed = tree(1).pathFor(0).value.toString
      printed should include("siblingCount = 0")
      printed should not include "siblings ="
    }

    "reject more than the maximum number of hashes" in {
      val hash = TestHash.digest("hash")
      val tooMany = NonEmpty.from(Seq.fill(MerkleHashTree.MaxSize + 1)(hash)).value

      MerkleHashTree.create(tooMany).left.value should include("at most")
      MerkleHashTree.rootOf(tooMany).left.value should include("at most")
    }

    "not return a path for an index out of range" in {
      val t = tree(5)
      t.pathFor(-1) shouldBe None
      t.pathFor(5) shouldBe None
    }

    "depend on the order of the hashes" in {
      val (a, b) = (TestHash.digest("a"), TestHash.digest("b"))
      MerkleHashTree.tryCreate(NonEmpty(Seq, a, b)).root should not be
        MerkleHashTree.tryCreate(NonEmpty(Seq, b, a)).root
    }

    "support duplicate hashes at separate positions" in {
      val a = TestHash.digest("a")
      val t = MerkleHashTree.tryCreate(NonEmpty(Seq, a, a, TestHash.digest("b")))

      rootFromPath(a, t.pathFor(0).value) shouldBe t.root
      rootFromPath(a, t.pathFor(1).value) shouldBe t.root
      t.pathFor(0) should not be t.pathFor(1)
    }

    "commit to the number of hashes" in {
      val abc = hashes(3).forgetNE
      tree(3).root should not be MerkleHashTree
        .tryCreate(NonEmpty(Seq, abc(0), abc(1), abc(2), abc(2)))
        .root
      tree(3).root should not be MerkleHashTree.tryCreate(NonEmpty(Seq, abc(0), abc(1))).root
    }

    "support hashes with different hash purposes" in {
      val mixed = NonEmpty(
        Seq,
        TestHash.digest("a"),
        Hash.digest(HashPurpose.AcsCommitment, TestHash.digest("b").unwrap, HashAlgorithm.Sha256),
        Hash.digest(
          HashPurpose.SignedProtocolMessageSignature,
          TestHash.digest("c").unwrap,
          HashAlgorithm.Sha256,
        ),
      )
      val t = MerkleHashTree.tryCreate(mixed)
      forAll(mixed.forgetNE.zipWithIndex) { case (hash, index) =>
        rootFromPath(hash, t.pathFor(index).value) shouldBe t.root
      }
    }

    "reject tampered inclusion paths" in {
      val t = tree(8)
      val hash = t.hashes.head1
      val path = t.pathFor(0).value

      rootFromPath(t.hashes.last1, path) should not be t.root
      rootFromPath(hash, inclusionPath(1, path.siblings)) should not be t.root
      rootFromPath(
        hash,
        inclusionPath(0, path.siblings.updated(0, TestHash.digest("x"))),
      ) should not be t.root
      rootFromPath(hash, inclusionPath(0, path.siblings.drop(1))) should not be t.root
      rootFromPath(
        hash,
        inclusionPath(0, path.siblings :+ TestHash.digest("x")),
      ) should not be t.root
      rootFromPath(hash, tree(9).pathFor(0).value) should not be t.root
    }

    "accept an inner node with the inclusion path of its subtree" in {
      // The leaves are not hashed again, so an inner node can be shown to be included like a leaf.
      // This is harmless, as an inner node is not the hash of any other object.
      val t = tree(4)
      val path = t.pathFor(0).value
      val innerNode = rootFromPath(t.hashes.head1, inclusionPath(0, path.siblings.take(1)))

      innerNode shouldBe inner(t.hashes.head1, t.hashes.forgetNE(1))
      rootFromPath(innerNode, inclusionPath(0, path.siblings.drop(1))) shouldBe t.root
    }

    "prove the position of a hash" in {
      // In a tree over 3 hashes, the hash at index 2 is the right child of the root
      val t = tree(3)
      val hash = t.hashes.forgetNE(2)
      val path = t.pathFor(2).value
      rootFromPath(hash, path) shouldBe t.root

      // Another position with the same siblings yields another root
      rootFromPath(hash, inclusionPath(0, path.siblings)) should not be t.root
      // The original index and other aliases with the same lowest bits are not valid paths
      InclusionPath.create(2, path.siblings).left.value should include("out of range")
      InclusionPath.create(3, path.siblings).left.value should include("out of range")
    }
  }

  "InclusionPath" should {
    val sibling = TestHash.digest("sibling")

    "accept at most the maximum number of siblings" in {
      InclusionPath.create(0, Seq.fill(MerkleHashTree.MaxDepth)(sibling)).value.siblings should
        have size MerkleHashTree.MaxDepth.toLong
      InclusionPath
        .create(0, Seq.fill(MerkleHashTree.MaxDepth + 1)(sibling))
        .left
        .value should include("exceeds")
    }

    "require all bits of the index above the bits for the siblings to be 0" in {
      InclusionPath.create(3, Seq(sibling, sibling)).value.index shouldBe 3
      InclusionPath.create(0, Seq.empty).value.index shouldBe 0
      InclusionPath.create(4, Seq(sibling, sibling)).left.value should include("out of range")
      InclusionPath.create(1, Seq.empty).left.value should include("out of range")
      InclusionPath.create(-1, Seq(sibling)).left.value should include("out of range")
      InclusionPath.create(Int.MinValue, Seq(sibling)).left.value should include("out of range")
    }
  }
}
