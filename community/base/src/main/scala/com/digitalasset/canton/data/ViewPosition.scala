// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.Order
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v2
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.util.ByteStringUtil
import com.google.protobuf.ByteString

/** A position encodes the path from a view in a transaction tree to its root.
  * The encoding must not depend on the hashes of the nodes.
  *
  * @param position The path from the view to the root as a singly-linked list.
  *                 The path starts at the view rather than the root so that paths to the root can
  *                 be shared.
  */
final case class ViewPosition(position: List[MerklePathElement]) extends PrettyPrinting {

  /** Adds a [[ViewPosition.MerklePathElement]] at the start of the path. */
  def +:(index: MerklePathElement): ViewPosition = new ViewPosition(index :: position)

  def encodeDeterministically: ByteString =
    DeterministicEncoding.encodeSeqWith(position)(_.encodeDeterministically)

  /** Reverse the position, as well as all contained MerkleSeqIndex path elements */
  def reverse: ViewPositionFromRoot = ViewPositionFromRoot(position.reverse.map(_.reverse))

  def toProtoV2: v2.ViewPosition = v2.ViewPosition(position = position.map(_.toProtoV2))

  override def pretty: Pretty[ViewPosition] = prettyOfClass(unnamedParam(_.position.mkShow()))
}

/** Same as [[ViewPosition]], with the position directed from the root to the leaf */
final case class ViewPositionFromRoot(position: List[MerklePathElement]) extends AnyVal {
  def isTopLevel: Boolean = position.size == 1
  def isEmpty: Boolean = position.isEmpty
}

object ViewPosition {

  /** The root [[ViewPosition]] has an empty path. */
  val root: ViewPosition = new ViewPosition(List.empty[MerklePathElement])

  implicit def prettyViewPosition: Pretty[ViewPosition] = {
    import com.digitalasset.canton.logging.pretty.Pretty.*
    prettyOfClass(unnamedParam(_.position))
  }

  implicit val orderViewPosition: Order[ViewPosition] =
    Order.by[ViewPosition, ByteString](_.encodeDeterministically)(ByteStringUtil.orderByteString)

  def fromProtoV2(viewPositionP: v2.ViewPosition): ViewPosition = {
    val v2.ViewPosition(positionP) = viewPositionP
    val position = positionP.map(MerkleSeqIndex.fromProtoV2).toList
    ViewPosition(position)
  }

  /** A single element on a path through a Merkle tree. */
  sealed trait MerklePathElement extends Product with Serializable with PrettyPrinting {
    def encodeDeterministically: ByteString
    def reverse: MerklePathElement

    def toProtoV2: v2.MerkleSeqIndex
  }

  /** For [[MerkleTreeInnerNode]]s which branch to a list of subviews,
    * the subtree is identified by the index in the list of subviews.
    */
  final case class ListIndex(index: Int) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      DeterministicEncoding
        .encodeByte(MerklePathElement.ListIndexPrefix)
        .concat(DeterministicEncoding.encodeInt(index))

    override def pretty: Pretty[ListIndex] = prettyOfString(_.index.toString)

    override lazy val reverse: ListIndex = this

    override def toProtoV2: v2.MerkleSeqIndex =
      throw new UnsupportedOperationException(
        "ListIndex is for legacy use only and should not be serialized"
      )
  }

  /** A leaf position in a [[MerkleSeq]], encodes as a path of directions from the leaf to the root.
    * The path is directed from the leaf to the root such that common subpaths can be shared.
    */
  final case class MerkleSeqIndex(index: List[Direction]) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      DeterministicEncoding
        .encodeByte(MerklePathElement.MerkleSeqIndexPrefix)
        .concat(DeterministicEncoding.encodeSeqWith(index)(_.encodeDeterministically))

    override def pretty: Pretty[MerkleSeqIndex] =
      prettyOfString(_ => index.reverse.map(_.show).mkString(""))

    override lazy val reverse: MerkleSeqIndexFromRoot = MerkleSeqIndexFromRoot(index.reverse)

    override def toProtoV2: v2.MerkleSeqIndex =
      v2.MerkleSeqIndex(isRight = index.map(_ == Direction.Right))
  }

  /** Same as [[MerkleSeqIndex]], with the position directed from the root to the leaf */
  final case class MerkleSeqIndexFromRoot(index: List[Direction]) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      throw new UnsupportedOperationException(
        "MerkleSeqIndexFromRoot is for internal use only and should not be encoded"
      )

    override def pretty: Pretty[MerkleSeqIndexFromRoot] =
      prettyOfString(_ => index.map(_.show).mkString(""))

    override lazy val reverse: MerkleSeqIndex = MerkleSeqIndex(index.reverse)

    def toProtoV2: v2.MerkleSeqIndex = throw new UnsupportedOperationException(
      "MerkleSeqIndexFromRoot is for internal use only and should not be serialized"
    )
  }

  object MerklePathElement {
    // Prefixes for the deterministic encoding of Merkle child indices.
    // Must be unique to prevent collisions of view position encodings
    private[ViewPosition] val ListIndexPrefix: Byte = 1
    private[ViewPosition] val MerkleSeqIndexPrefix: Byte = 2
  }

  object MerkleSeqIndex {
    sealed trait Direction extends Product with Serializable with PrettyPrinting {
      def encodeDeterministically: ByteString
    }
    object Direction {

      case object Left extends Direction {
        override def encodeDeterministically: ByteString = DeterministicEncoding.encodeByte(0)

        override def pretty: Pretty[Left.type] = prettyOfString(_ => "L")
      }

      case object Right extends Direction {
        override def encodeDeterministically: ByteString = DeterministicEncoding.encodeByte(1)

        override def pretty: Pretty[Right.type] = prettyOfString(_ => "R")
      }
    }

    def fromProtoV2(merkleSeqIndexP: v2.MerkleSeqIndex): MerkleSeqIndex = {
      val v2.MerkleSeqIndex(isRightP) = merkleSeqIndexP
      MerkleSeqIndex(isRightP.map(if (_) Direction.Right else Direction.Left).toList)
    }
  }

  def isDescendant(descendant: ViewPosition, ancestor: ViewPosition): Boolean = {
    descendant.position.size >= ancestor.position.size &&
    descendant.position.drop(descendant.position.size - ancestor.position.size) == ancestor.position
  }
}
