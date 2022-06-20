// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.google.protobuf.ByteString
import pprint.Tree

/** A position encodes the path from a view in a transaction tree to its root.
  * The encoding must not depend on the hashes of the nodes.
  *
  * @param position The path from the view to the root as a singly-linked list.
  *                 The path starts at the view rather than the root so that paths to the root can
  *                 be shared.
  */
case class ViewPosition(position: List[MerklePathElement]) extends AnyVal {

  /** Adds a [[ViewPosition.MerklePathElement]] at the start of the path. */
  def +:(index: MerklePathElement): ViewPosition = new ViewPosition(index :: position)

  def encodeDeterministically: ByteString =
    DeterministicEncoding.encodeSeqWith(position)(_.encodeDeterministically)
}

object ViewPosition {

  /** The root [[ViewPosition]] has an empty path. */
  val root: ViewPosition = new ViewPosition(List.empty[MerklePathElement])

  implicit def prettyViewPosition: Pretty[ViewPosition] = { pos =>
    import com.digitalasset.canton.logging.pretty.Pretty.PrettyOps
    implicit val prettyMPE: Pretty[MerklePathElement] = prettyMerklePathElement
    Tree.Apply("", Iterator(pos.position.toTree))
  }

  private val prettyMerklePathElement: Pretty[MerklePathElement] = {
    case ListIndex(index) => Tree.Literal(index.toString)
    case MerkleSeqIndex(index) =>
      val sb = new StringBuilder(index.length)
      index.reverse.foreach { dir =>
        val dirChar = dir match {
          case Direction.Left => "L"
          case Direction.Right => "R"
        }
        sb.append(dirChar)
      }
      Tree.Literal(sb.toString())
  }

  /** A single element on a path through a Merkle tree. */
  sealed trait MerklePathElement extends Product with Serializable {
    def encodeDeterministically: ByteString
  }

  /** For [[MerkleTreeInnerNode]]s which branch to a list of subviews,
    * the subtree is identified by the index in the list of subviews.
    */
  case class ListIndex(index: Int) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      DeterministicEncoding
        .encodeByte(MerklePathElement.ListIndexPrefix)
        .concat(DeterministicEncoding.encodeInt(index))
  }

  /** A leaf position in a [[MerkleSeq]], encodes as a path of directions from the leaf to the root.
    * The path is directed from the leaf to the root such that common subpaths can be shared.
    */
  case class MerkleSeqIndex(index: List[Direction]) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      DeterministicEncoding
        .encodeByte(MerklePathElement.MerkleSeqIndexPrefix)
        .concat(DeterministicEncoding.encodeSeqWith(index)(_.encodeDeterministically))
  }

  object MerklePathElement {
    // Prefixes for the deterministic encoding of Merkle child indices.
    // Must be unique to prevent collisions of view position encodings
    private[ViewPosition] val ListIndexPrefix: Byte = 1
    private[ViewPosition] val MerkleSeqIndexPrefix: Byte = 2
  }

  object MerkleSeqIndex {
    sealed trait Direction extends Product with Serializable {
      def encodeDeterministically: ByteString
    }
    object Direction {

      case object Left extends Direction {
        override def encodeDeterministically: ByteString = DeterministicEncoding.encodeByte(0)
      }

      case object Right extends Direction {
        override def encodeDeterministically: ByteString = DeterministicEncoding.encodeByte(1)
      }
    }
  }
}
