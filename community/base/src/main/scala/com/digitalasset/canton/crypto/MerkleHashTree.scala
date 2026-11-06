// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import com.google.common.annotations.VisibleForTesting

import scala.annotation.tailrec

/** The inclusion path of the hash at position `index` in a [[MerkleHashTree]].
  *
  * @param index
  *   the position of the hash among the leaves of the tree, restricted to the levels at which the
  *   path has a sibling: bit `t` of the index tells whether the node on the path next to the `t`-th
  *   sibling (counted from the leaf) is the left (0) or right (1) child of its parent. In a tree
  *   whose size is a power of two, the index is the position of the hash in the sequence of hashes.
  * @param siblings
  *   the siblings of the nodes on the path from the leaf to the root, starting at the leaf
  *
  * An inclusion path has at most [[MerkleHashTree.MaxDepth]] siblings, and its index satisfies `0
  * <= index < 2^siblings.size`, i.e., all bits of the index above the bits for the siblings are 0.
  * Hence, every inclusion path has a unique representation.
  */
final case class InclusionPath private (index: Int, siblings: Seq[Hash])
    extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[InclusionPath] = InclusionPath
}

object InclusionPath extends PrettyPrintingCompanion[InclusionPath] {
  // Prints the index in binary with one bit per sibling, to show the sides. The rightmost bit belongs
  // to the first sibling, i.e., the one at the leaf. The number of siblings comes before the
  // siblings, as long sequences may be truncated when printed.
  override protected val pretty: Pretty[InclusionPath] = prettyOfClass(
    param(
      "index",
      path =>
        s"0b${path.index.toBinaryString.reverse.padTo(path.siblings.size, '0').reverse}".unquoted,
    ),
    param("siblingCount", _.siblings.size),
    paramIfNonEmpty("siblings", _.siblings),
  )

  /** Creates the inclusion path, or fails if it has more than [[MerkleHashTree.MaxDepth]] siblings
    * or the index is not in the range `[0, 2^siblings.size)`
    */
  def create(index: Int, siblings: Seq[Hash]): Either[String, InclusionPath] = {
    val depth = siblings.size
    for {
      _ <- Either.cond(
        depth <= MerkleHashTree.MaxDepth,
        (),
        s"Inclusion path with $depth siblings exceeds the maximum depth ${MerkleHashTree.MaxDepth}",
      )
      _ <- Either.cond(
        index >= 0 && index < (1 << depth),
        (),
        s"Index $index is out of range for an inclusion path with $depth siblings",
      )
    } yield new InclusionPath(index, siblings)
  }

  /** Creates the inclusion path. Throws if [[create]] fails. */
  def tryCreate(index: Int, siblings: Seq[Hash]): InclusionPath =
    create(index, siblings).valueOr(err => throw new IllegalArgumentException(err))
}

/** A binary Merkle tree over a non-empty sequence of hashes. The root of the tree is a single hash
  * that depends on all hashes and their order, and the inclusion path of each hash, of logarithmic
  * size, links that hash to the root.
  *
  * The tree is built bottom-up with the hashes themselves as leaves, in their order, and is as
  * balanced as possible: for `n >= 2` hashes and `d = ceil(log2(n))`, the lowest level pairs only
  * its first `2 * (n - 2^(d-1))` hashes from left to right into the inner nodes
  * `H(MerkleHashTreeInnerNode, left, right)`, and the other hashes move up to the next level
  * unchanged. The next level therefore has exactly `2^(d-1)` nodes, and the levels above pair all
  * their nodes, so that every hash is a leaf at depth `d` or `d - 1`. This minimizes the total size
  * of all inclusion paths, and no padding is needed.
  *
  * The leaves are not hashed again, as the hashes already keep them apart from the inner nodes: the
  * hashes must have been computed with a [[HashPurpose]] other than
  * [[HashPurpose.MerkleHashTreeInnerNode]], so a hash equals an inner node only in case of a hash
  * collision. For the same reason, the tree only takes hashes and not arbitrary byte strings. An
  * inner node of a tree can therefore be shown to be included in the tree like a leaf, but it is
  * not the hash of any other object. Accordingly, a verifier must compute the hash of the object
  * itself instead of taking the hash from an untrusted source.
  *
  * Security requirement: as the leaves are not hashed again, a signature over a hash with the
  * purpose [[HashPurpose.MerkleHashTreeInnerNode]] is a signature over the leaves below it. This
  * purpose must therefore only be used for the inner nodes of this tree, and such a hash must only
  * be signed as the root of a tree whose leaves the signer intends to sign, i.e., through
  * `signHashes`. This keeps the tree extensible: the root of one tree can be a leaf of another, and
  * their inclusion paths can be concatenated.
  *
  * A node that is paired at position `p` of a level has its sibling at position `p xor 1`, and the
  * inclusion path of a hash has these siblings and the sides `p & 1` of the levels at which its
  * path is paired as index. Every inclusion path has `ceil(log2(n))` or `ceil(log2(n)) - 1`
  * siblings for `n` hashes. Computing the root from a hash and its inclusion path therefore only
  * needs the index and the siblings. As the index must be smaller than `2^siblings.size`, each hash
  * of the tree has exactly one valid inclusion path. The shape of the tree, and hence the root,
  * depends on the number of hashes.
  *
  * In particular, a tree over a single hash has that hash as its root and an inclusion path without
  * siblings.
  *
  * All nodes are hashed with SHA-256, and the hashes of the tree must be SHA-256 hashes, so the
  * nodes hash the raw 32 bytes of their inputs without the multi-hash prefix. Duplicate hashes are
  * allowed and occupy separate positions. A tree has at most [[MerkleHashTree.MaxSize]] hashes.
  */
final class MerkleHashTree private (
    val hashes: NonEmpty[Seq[Hash]],
    // From the hashes up to the root: the first level holds the hashes, the last one only the root
    levels: NonEmpty[Seq[NonEmpty[IndexedSeq[Hash]]]],
) extends PrettyPrintingFromCompanion {
  import MerkleHashTree.*

  def size: Int = hashes.size

  val root: Hash = levels.last1.head1

  // The number of hashes that are paired on the lowest level. The other hashes move up unchanged.
  private val pairedHashes: Int = pairedOnLowestLevel(size)

  /** Returns the inclusion path of the hash at the given index, or [[scala.None$]] if the index is
    * out of range.
    */
  def pathFor(index: Int): Option[InclusionPath] =
    Option.when(index >= 0 && index < size) {
      // Follows the position of the node on the path through the levels below the root, and collects
      // the side and the sibling at each level at which the node is paired. On the lowest level, a
      // hash that is not paired moves up to the position after the `pairedHashes / 2` inner nodes.
      val (_, steps) =
        levels.forgetNE.dropRight(1).zipWithIndex.foldLeft((index, List.empty[(Int, Hash)])) {
          case ((position, steps), (level, height)) =>
            if (height == 0 && position >= pairedHashes)
              (position - pairedHashes / 2, steps)
            else (position >> 1, (position & 1, level(position ^ 1)) :: steps)
        }
      val sidesAndSiblings = steps.reverse
      val pathIndex = sidesAndSiblings.zipWithIndex.map { case ((side, _), t) => side << t }.sum
      // Safe: a tree has at most `2^MaxDepth` hashes, so a path has at most `MaxDepth` siblings, and
      // the index has one bit per sibling
      checked(
        InclusionPath.tryCreate(pathIndex, sidesAndSiblings.map { case (_, sibling) => sibling })
      )
    }

  override def prettyCompanion: PrettyPrintingCompanion[MerkleHashTree] = MerkleHashTree
}

object MerkleHashTree extends PrettyPrintingCompanion[MerkleHashTree] {

  override protected val pretty: Pretty[MerkleHashTree] = prettyOfClass(
    param("root", _.root),
    param("size", _.size),
  )

  /** Maximum number of siblings in an inclusion path. A tree has at most `2^MaxDepth` hashes, so
    * that the inclusion paths of all its hashes stay within this limit.
    *
    * The limit bounds the work for inclusion paths received from untrusted sources: computing the
    * root from a path takes at most `MaxDepth` SHA-256 computations, and a path has at most
    * `MaxDepth` siblings. It also bounds the memory of a tree, which stores about `2 * n` nodes for
    * `n` hashes. `2^MaxDepth` (about one million) is far above the expected number of hashes signed
    * together. The limit must stay below 31, as the index is an `Int`.
    */
  val MaxDepth: Int = 20

  /** Maximum number of hashes in a tree */
  val MaxSize: Int = 1 << MaxDepth

  /** Creates the tree over the given hashes, or fails if there are more than [[MaxSize]] hashes */
  def create(hashes: NonEmpty[Seq[Hash]]): Either[String, MerkleHashTree] =
    Either.cond(
      hashes.sizeIs <= MaxSize,
      // Safe: `hashes` is non-empty
      new MerkleHashTree(
        hashes,
        build(checked(NonEmptyUtil.fromUnsafe(hashes.forgetNE.toIndexedSeq))),
      ),
      s"A Merkle hash tree has at most $MaxSize hashes, got ${hashes.size}",
    )

  /** Creates the tree over the given hashes. Throws if there are more than [[MaxSize]] hashes. */
  def tryCreate(hashes: NonEmpty[Seq[Hash]]): MerkleHashTree =
    create(hashes).valueOr(err => throw new IllegalArgumentException(err))

  /** Computes the root of the tree that contains `hash` at the position described by `path`. A path
    * without siblings yields `hash` itself.
    */
  def rootFromPath(hash: Hash, path: InclusionPath): Hash =
    // Bit `t` of the index gives the side of the node next to the sibling at position `t`
    path.siblings.zipWithIndex.foldLeft(hash) { case (node, (sibling, t)) =>
      if (((path.index >> t) & 1) == 0) innerNode(node, sibling)
      else innerNode(sibling, node)
    }

  /** Computes the root of the tree over `hashes`, or fails if there are more than [[MaxSize]]
    * hashes
    */
  def rootOf(hashes: NonEmpty[Seq[Hash]]): Either[String, Hash] = create(hashes).map(_.root)

  @VisibleForTesting
  private[crypto] def innerNode(left: Hash, right: Hash): Hash =
    Hash
      .build(HashPurpose.MerkleHashTreeInnerNode, HashAlgorithm.Sha256)
      // All nodes are SHA-256 hashes and therefore of fixed length, so neither a length prefix nor
      // the multi-hash prefix is needed.
      .addWithoutLengthPrefix(left.unwrap)
      .addWithoutLengthPrefix(right.unwrap)
      .finish()

  /** Returns the levels of the tree, from the given leaves up to the level that holds only the
    * root. A tree over a single hash has a single level, whose only node is the root.
    */
  private def build(
      leaves: NonEmpty[IndexedSeq[Hash]]
  ): NonEmpty[Seq[NonEmpty[IndexedSeq[Hash]]]] = {
    @tailrec
    def go(
        level: NonEmpty[IndexedSeq[Hash]],
        lowerLevels: List[NonEmpty[IndexedSeq[Hash]]],
    ): NonEmpty[Seq[NonEmpty[IndexedSeq[Hash]]]] =
      if (level.sizeIs == 1) NonEmpty.mk(Seq, level, lowerLevels*).reverse
      else go(pairAll(level), level :: lowerLevels)

    if (leaves.sizeIs == 1) NonEmpty(Seq, leaves)
    else go(pairLowestLevel(leaves), List(leaves))
  }

  /** The number of hashes that are paired on the lowest level of a tree over `n` hashes: `2 * (n -
    * 2^(d-1))` for `d = ceil(log2(n))`, so that the next level has `2^(d-1)` nodes
    */
  private def pairedOnLowestLevel(n: Int): Int =
    if (n == 1) 0 else 2 * (n - Integer.highestOneBit(n - 1))

  /** Pairs the first [[pairedOnLowestLevel]] leaves from left to right, and moves the other leaves
    * up unchanged
    */
  private def pairLowestLevel(leaves: NonEmpty[IndexedSeq[Hash]]): NonEmpty[IndexedSeq[Hash]] = {
    val pairs = pairedOnLowestLevel(leaves.size) / 2
    // Safe: the next level has `2^(d-1) >= 1` nodes
    checked(NonEmptyUtil.fromUnsafe(IndexedSeq.tabulate(leaves.size - pairs) { p =>
      if (p < pairs) innerNode(leaves(2 * p), leaves(2 * p + 1)) else leaves(p + pairs)
    }))
  }

  /** Pairs all nodes of a level above the lowest one, whose number of nodes is a power of two */
  private def pairAll(level: NonEmpty[IndexedSeq[Hash]]): NonEmpty[IndexedSeq[Hash]] =
    // Safe: the level has at least two nodes, as it is not the level of the root
    checked(NonEmptyUtil.fromUnsafe(IndexedSeq.tabulate(level.size / 2) { p =>
      innerNode(level(2 * p), level(2 * p + 1))
    }))
}
