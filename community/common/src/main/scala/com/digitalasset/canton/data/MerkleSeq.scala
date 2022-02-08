// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data
import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.MerkleSeq.MerkleSeqElement
import com.digitalasset.canton.data.MerkleTree.{
  BlindSubtree,
  BlindingCommand,
  RevealIfNeedBe,
  RevealSubtree,
}
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.data.ViewPosition.{MerklePathElement, MerkleSeqIndex}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RootHash, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.util.{HasVersionedToByteString, HasProtoV0}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.annotation.tailrec

/** Wraps a sequence that is also a [[MerkleTree]].
  * Elements are arranged in a balanced binary tree. As a result, if all except one element are blinded, the
  * resulting MerkleSeq has size logarithmic in the size of the fully unblinded MerkleSeq.
  *
  * @param rootOrEmpty the root element or `None` if the sequence is empty
  * @tparam M the type of elements
  */
case class MerkleSeq[+M <: MerkleTree[_] with HasVersionedToByteString](
    rootOrEmpty: Option[MerkleTree[MerkleSeqElement[M]]]
)(hashOps: HashOps)
    extends HasProtoV0[v0.MerkleSeq]
    with PrettyPrinting {

  lazy val unblindedElementsWithIndex: Seq[(M, MerklePathElement)] = rootOrEmpty match {
    case Some(root) =>
      root.unwrap match {
        case Right(unblindedRoot) => unblindedRoot.unblindedElements
        case Left(_) => Seq.empty
      }
    case None => Seq.empty
  }

  lazy val unblindedElements: Seq[M] = unblindedElementsWithIndex.map(_._1)

  lazy val blindedElements: Seq[RootHash] = rootOrEmpty match {
    case Some(root) => root.unwrap.fold(Seq(_), _.blindedElements)
    case None => Seq.empty
  }

  private[data] def doBlind(
      optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
  ): MerkleSeq[M] =
    rootOrEmpty match {
      case Some(root) =>
        optimizedBlindingPolicy(root.rootHash) match {
          case BlindSubtree =>
            MerkleSeq(Some(BlindedNode[MerkleSeqElement[M]](root.rootHash)))(hashOps)
          case RevealSubtree => this
          case RevealIfNeedBe =>
            val blindedRoot = root.withBlindedSubtrees(optimizedBlindingPolicy)
            MerkleSeq(Some(blindedRoot))(hashOps)
        }
      case None => this
    }

  override def toProtoV0: v0.MerkleSeq =
    v0.MerkleSeq(rootOrEmpty = rootOrEmpty.map(MerkleTree.toBlindableNode))

  override def pretty: Pretty[MerkleSeq.this.type] = prettyOfClass(
    unnamedParamIfDefined(_.rootOrEmpty)
  )

  def mapM[A <: MerkleTree[A] with HasVersionedToByteString](f: M => A): MerkleSeq[A] = {
    this.copy(rootOrEmpty = rootOrEmpty.map(_.unwrap.fold(BlindedNode(_), seq => seq.mapM(f))))(
      hashOps
    )
  }
}

object MerkleSeq {

  private type Path = List[Direction]
  private def emptyPath: Path = List.empty[Direction]

  sealed trait MerkleSeqElement[+M <: MerkleTree[_] with HasVersionedToByteString]
      extends MerkleTree[MerkleSeqElement[M]]
      with HasProtoV0[v0.MerkleSeqElement]
      with HasVersionedToByteString
      with Product
      with Serializable {
    lazy val unblindedElements: Seq[(M, MerklePathElement)] = computeUnblindedElements()

    // Doing this in a separate method, as Idea would otherwise complain about a covariant type parameter
    // being used in a contravariant position.
    private def computeUnblindedElements(): Seq[(M, MerklePathElement)] = {
      val builder = Seq.newBuilder[(M, MerklePathElement)]
      foreachUnblindedElement(emptyPath)((x, path) => builder.+=(x -> MerkleSeqIndex(path)))
      builder.result()
    }

    private[MerkleSeq] def foreachUnblindedElement(path: Path)(body: (M, Path) => Unit): Unit

    def blindedElements: Seq[RootHash] = {
      val builder = scala.collection.mutable.Seq.newBuilder[RootHash]
      foreachBlindedElement(builder.+=(_))
      builder.result()
    }.toSeq

    private[MerkleSeq] def foreachBlindedElement(body: RootHash => Unit): Unit

    // We repeat this here to enforce a more specific return type.
    override private[data] def withBlindedSubtrees(
        optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
    ): MerkleSeqElement[M]

    def mapM[A <: MerkleTree[A] with HasVersionedToByteString](f: M => A): MerkleSeqElement[A]

    def toProtoV0: v0.MerkleSeqElement

    override def toByteString(version: ProtocolVersion): ByteString = toProtoV0.toByteString
  }

  private[data] case class Branch[+M <: MerkleTree[_] with HasVersionedToByteString](
      first: MerkleTree[MerkleSeqElement[M]],
      second: MerkleTree[MerkleSeqElement[M]],
  )(hashOps: HashOps)
      extends MerkleTreeInnerNode[Branch[M]](hashOps)
      with MerkleSeqElement[M] {

    override def subtrees: Seq[MerkleTree[_]] = Seq(first, second)

    override private[data] def withBlindedSubtrees(
        optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
    ): Branch[M] =
      Branch(first.doBlind(optimizedBlindingPolicy), second.doBlind(optimizedBlindingPolicy))(
        hashOps
      )

    override private[MerkleSeq] def foreachUnblindedElement(
        path: Path
    )(body: (M, Path) => Unit): Unit = {
      first.unwrap.foreach(_.foreachUnblindedElement(Direction.Left :: path)(body))
      second.unwrap.foreach(_.foreachUnblindedElement(Direction.Right :: path)(body))
    }

    override private[MerkleSeq] def foreachBlindedElement(body: RootHash => Unit): Unit = {
      first.unwrap.fold(body, _.foreachBlindedElement(body))
      second.unwrap.fold(body, _.foreachBlindedElement(body))
    }

    def toProtoV0: v0.MerkleSeqElement =
      v0.MerkleSeqElement(
        first = Some(MerkleTree.toBlindableNode(first)),
        second = Some(MerkleTree.toBlindableNode(second)),
        data = None,
      )

    override def pretty: Pretty[Branch.this.type] = prettyOfClass(
      param("first", _.first),
      param("second", _.second),
    )

    override def mapM[A <: MerkleTree[A] with HasVersionedToByteString](
        f: M => A
    ): MerkleSeqElement[A] = {
      val newFirst: MerkleTree[MerkleSeqElement[A]] =
        first.unwrap.fold(h => BlindedNode(h), _.mapM(f))
      val newSecond = second.unwrap.fold(h => BlindedNode(h), _.mapM(f))
      Branch(newFirst, newSecond)(hashOps)
    }

  }

  private[data] case class Singleton[+M <: MerkleTree[_] with HasVersionedToByteString](
      data: MerkleTree[M]
  )(hashOps: HashOps)
      extends MerkleTreeInnerNode[Singleton[M]](hashOps)
      with MerkleSeqElement[M] {
    // Singleton is a subtype of MerkleTree[_], because otherwise we would leak the size of the MerkleSeq in some cases
    // (e.g., if there is exactly one element).
    //
    // data is of type MerkleTree[_], because otherwise we would have to come up with a "surprising" implementation
    // of "withBlindedSubtrees" (i.e., blind the Singleton if the data is blinded).

    override def subtrees: Seq[MerkleTree[_]] = Seq(data)

    override private[data] def withBlindedSubtrees(
        optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
    ): Singleton[M] =
      Singleton[M](data.doBlind(optimizedBlindingPolicy))(hashOps)

    override private[MerkleSeq] def foreachUnblindedElement(path: Path)(
        body: (M, Path) => Unit
    ): Unit =
      data.unwrap.foreach(body(_, path))

    override private[MerkleSeq] def foreachBlindedElement(body: RootHash => Unit): Unit =
      data.unwrap.fold(body, _ => ())

    def toProtoV0: v0.MerkleSeqElement =
      v0.MerkleSeqElement(
        first = None,
        second = None,
        data = Some(MerkleTree.toBlindableNode(data)),
      )

    override def pretty: Pretty[Singleton.this.type] = prettyOfClass(unnamedParam(_.data))

    override def mapM[A <: MerkleTree[A] with HasVersionedToByteString](
        f: M => A
    ): MerkleSeqElement[A] = {
      val newData: MerkleTree[A] = data.unwrap.fold(h => BlindedNode(h), f(_))
      Singleton(newData)(hashOps)
    }
  }

  object MerkleSeqElement {

    private[MerkleSeq] def fromByteString[M <: MerkleTree[_] with HasVersionedToByteString](
        hashOps: HashOps,
        dataFromByteString: ByteString => Either[String, MerkleTree[
          M with HasVersionedToByteString
        ]],
    )(bytes: ByteString): Either[String, MerkleSeqElement[M]] = {
      for {
        merkleSeqElementP <- ProtoConverter
          .protoParser(v0.MerkleSeqElement.parseFrom)(bytes)
          .leftMap(_.toString)
        merkleSeqElement <- fromProtoV0(hashOps, dataFromByteString)(merkleSeqElementP)
      } yield merkleSeqElement
    }

    private[MerkleSeq] def fromProtoV0[M <: MerkleTree[_] with HasVersionedToByteString](
        hashOps: HashOps,
        dataFromByteString: ByteString => Either[String, MerkleTree[
          M with HasVersionedToByteString
        ]],
    )(merkleSeqElementP: v0.MerkleSeqElement): Either[String, MerkleSeqElement[M]] = {
      val v0.MerkleSeqElement(maybeFirstP, maybeSecondP, maybeDataP) = merkleSeqElementP

      def branchChildFromMaybeProtoBlindableNode(
          maybeNodeP: Option[v0.BlindableNode]
      ): Either[String, Option[MerkleTree[MerkleSeqElement[M]]]] =
        maybeNodeP.traverse(nodeP =>
          MerkleTree.fromProtoOption(Some(nodeP), fromByteString(hashOps, dataFromByteString))
        )

      def singletonDataFromMaybeProtoBlindableNode(
          maybeDataP: Option[v0.BlindableNode]
      ): Either[String, Option[MerkleTree[M with HasVersionedToByteString]]] =
        maybeDataP.traverse(dataP => MerkleTree.fromProtoOption(Some(dataP), dataFromByteString))

      for {
        maybeFirst <- branchChildFromMaybeProtoBlindableNode(maybeFirstP)
        maybeSecond <- branchChildFromMaybeProtoBlindableNode(maybeSecondP)
        maybeData <- singletonDataFromMaybeProtoBlindableNode(maybeDataP)

        merkleSeqElement <- (maybeFirst, maybeSecond, maybeData) match {
          case (Some(first), Some(second), None) => Right(Branch(first, second)(hashOps))
          case (None, None, Some(data)) => Right(Singleton[M](data)(hashOps))
          case (None, None, None) =>
            Left(s"Unable to create MerkleSeqElement, as all fields are undefined.")
          case (Some(_), Some(_), Some(_)) =>
            Left(
              s"Unable to create MerkleSeqElement, as both the fields for a Branch and a Singleton are defined."
            )
          case (_, _, _) =>
            // maybeFirst.isDefined != maybeSecond.isDefined
            def mkState: Option[_] => String = _.fold("undefined")(_ => "defined")

            Left(
              s"Unable to create MerkleSeqElement, as first is ${mkState(maybeFirst)} and second is ${mkState(maybeSecond)}."
            )
        }

      } yield merkleSeqElement
    }
  }

  def fromProtoV0[M <: MerkleTree[_] with HasVersionedToByteString](
      hashOps: HashOps,
      dataFromByteString: ByteString => Either[String, MerkleTree[M with HasVersionedToByteString]],
  )(merkleSeqP: v0.MerkleSeq): Either[String, MerkleSeq[M]] = {
    val v0.MerkleSeq(maybeRootP) = merkleSeqP
    for {
      rootOrEmpty <- maybeRootP.traverse(_ =>
        MerkleTree.fromProtoOption(
          maybeRootP,
          MerkleSeqElement.fromByteString[M](hashOps, dataFromByteString),
        )
      )
    } yield MerkleSeq(rootOrEmpty)(hashOps)
  }

  def fromSeq[M <: MerkleTree[_] with HasVersionedToByteString](
      hashOps: HashOps
  )(elements: Seq[MerkleTree[M]]): MerkleSeq[M] = {
    if (elements.isEmpty) {
      MerkleSeq[M](None)(hashOps)
    } else {
      // elements is non-empty

      // Arrange elements in a balanced binary tree
      val merkleSeqElements = elements.iterator
        .map(Singleton(_)(hashOps)) // Wrap elements in singletons
        .map { // Blind singletons, if the enclosed element is blinded
          case singleton @ Singleton(BlindedNode(_)) => BlindedNode(singleton.rootHash)
          case singleton => singleton
        }

      val root = mkTree[MerkleTree[MerkleSeqElement[M]]](merkleSeqElements, elements.size) {
        (first, second) =>
          val branch = Branch(first, second)(hashOps)
          if (first.isBlinded && second.isBlinded) BlindedNode(branch.rootHash) else branch
      }

      MerkleSeq(Some(root))(hashOps)
    }
  }

  /** Arranges a non-empty sequence of `elements` in a balanced binary tree.
    *
    * @param size The [[scala.collection.Iterator.length]] of `elements`.
    *             We take the size as a separate parameter because the implementation of the method relies on
    *             [[scala.collection.Iterator.grouped]] and computing the size of an iterator takes linear time.
    * @param combine The function to construct an inner node of the binary tree from two subtrees.
    */
  @tailrec
  private def mkTree[E](elements: Iterator[E], size: Int)(combine: (E, E) => E): E = {
    require(size > 0, "This method must be called with a positive size.")

    if (size == 1) {
      elements.next()
    } else {
      // size > 1

      val newElements = elements.grouped(2).map { // Create the next layer of the tree
        case Seq(first, second) => combine(first, second)
        case Seq(single) => single
        case group => throw new IllegalStateException(s"Unexpected group size: ${group.size}")
      }

      val newSize = size / 2 + size % 2 // half of size, rounded upwards
      mkTree(newElements, newSize)(combine)
    }
  }

  /** Computes the [[ViewPosition.MerkleSeqIndex]]es for all leaves in a [[MerkleSeq]] of the given size.
    * The returned indices are in sequence.
    */
  // takes O(size) runtime and memory due to sharing albeit there are O(size * log(size)) directions
  def indicesFromSeq(size: Int): Seq[MerkleSeqIndex] = {
    require(size >= 0, "A sequence cannot have negative size")

    if (size == 0) Seq.empty[MerkleSeqIndex]
    else {
      val tree = mkTree[Node](Iterator.fill[Node](size)(Leaf), size)(Inner)
      // enumerate all paths in the tree from left to right
      tree.addTo(emptyPath, List.empty[MerkleSeqIndex])
    }
  }

  // Helper classes for indicesFromSeq
  private trait Node extends Product with Serializable {

    /** Prefixes `subsequentIndices` with all paths from the leaves of this subtree to the root
      *
      * @param pathFromRoot The path from this node to the root.
      */
    def addTo(pathFromRoot: Path, subsequentIndices: List[MerkleSeqIndex]): List[MerkleSeqIndex]
  }
  private case object Leaf extends Node {
    override def addTo(
        pathFromRoot: Path,
        subsequentPaths: List[MerkleSeqIndex],
    ): List[MerkleSeqIndex] =
      MerkleSeqIndex(pathFromRoot) :: subsequentPaths
  }
  private case class Inner(left: Node, right: Node) extends Node {
    override def addTo(
        pathFromRoot: Path,
        subsequentPaths: List[MerkleSeqIndex],
    ): List[MerkleSeqIndex] =
      left.addTo(
        Direction.Left :: pathFromRoot,
        right.addTo(Direction.Right :: pathFromRoot, subsequentPaths),
      )
  }
}
