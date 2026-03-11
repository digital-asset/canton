// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.SerializationVersion
import com.digitalasset.daml.lf.transaction.{NodeId, Transaction}
import com.digitalasset.daml.lf.value.{Value => V}

class NormalizeRollbacksSpec extends AnyWordSpec with Matchers with Inside {

  import NormalizeRollbackSpec._

  // TODO:
  // Below we test a set of hand-constructed testcases. Hopefully we got all edge cases!
  // But for more confidence we might consider adding scalacheck based testing.

  // TODO:
  // We should test that the `meaning` of a transaction is preserved by normalization.

  def test(name: String)(orig: Shape.Top, expected: Shape.Top): Unit =
    testEither(name)(orig, Right(expected), shouldDropRollbacks = false)

  def testRollbackSucceeds(name: String)(orig: Shape.Top, expected: Shape.Top): Unit =
    testEither(name)(orig, Right(expected), shouldDropRollbacks = true)

  def testRollbackFails(name: String)(orig: Shape.Top, expectedErrorMessage: String): Unit =
    testEither(name)(orig, Left(expectedErrorMessage), true)

  def testEither(name: String)(orig: Shape.Top, expected: Either[String, Shape.Top], shouldDropRollbacks: Boolean = false): Unit = {
    s"normalize (${if (shouldDropRollbacks) "without" else "with"} rollbacks): ($orig) -- $name" should {
      expected match {
        case Right(expected) => {
          val tx = Shape.toTransaction(orig)
          val (txN, _) = NormalizeRollbacks.normalizeTx(tx, shouldDropRollbacks) // code under test
          val shapeN = Shape.ofTransaction(txN)
          "be as expected" in {
            assert(shapeN == expected)
          }
          "be a normal form" in {
            assert(isNormalized(txN))
          }
          "have increasing node-ids when listed in pre-order" in {
            assert(preOrderNidsOfTxIsIncreasingFromZero(txN))
          }
        }
        case Left(expectedErrorMessage) => {
          "throw exception with expected message" in {
            try {
              val tx = Shape.toTransaction(orig)
              val (txN, _) = NormalizeRollbacks.normalizeTx(tx, shouldDropRollbacks) // code under test
              fail(s"Should throw an exception with message $expectedErrorMessage, got a traction of the following shape instead: $txN")
            } catch {
              case actualError: RuntimeException => actualError.getMessage should include(expectedErrorMessage)
            }
          }
        }
      }
    }
  }

  // multi arg construction for example convenience
  def Top(xs: Shape*) = Shape.Top(xs.toList)
  def E(xs: Shape*) = Shape.Exercise(true, xs.toList)
  def EN(xs: Shape*) = Shape.Exercise(false, xs.toList)
  def R(xs: Shape*) = Shape.Rollback(xs.toList)

  val List(c1, c2, c3, c4) = List[Long](1, 2, 3, 4).map(Shape.Create)
  val List(f1, f2, f3, f4) = List[Long](1, 2, 3, 4).map(Shape.Fetch)
  val List(ec1, ec2) = List(List(), List()).map(Shape.Exercise(true, _))
  val List(en1, en2) = List(List(), List()).map(Shape.Exercise(false, _))

  // no normalization required
  test("empty tx")(
    Top(),
    Top(),
  )
  test("one create")(
    Top(c1),
    Top(c1),
  )
  test("two creates")(
    Top(c1, c2),
    Top(c1, c2),
  )
  test("rollback-create")(
    Top(R(c1)),
    Top(R(c1)),
  )
  testRollbackFails("rollback-create")(
    Top(R(c1)),
    "Create node inside Rollback node cannot be removed.",
  )
  test("non empty rollback-create between creates")(
    Top(c1, R(c2), c3),
    Top(c1, R(c2), c3),
  )
  testRollbackFails("non empty rollback-create between creates")(
    Top(c1, R(c2), c3),
    "Create node inside Rollback node cannot be removed.",
  )
  test("empty exercise")(
    Top(E()),
    Top(E()),
  )
  test("exercise and creates")(
    Top(E(c1, E(), c2)),
    Top(E(c1, E(), c2)),
  )

  // normalization rule #1
  test("empty rollback")(
    Top(R()),
    Top(),
  )
  test("empty rollback after create")(
    Top(c1, R()),
    Top(c1),
  )
  test("empty rollback before create")(
    Top(R(), c1),
    Top(c1),
  )
  test("empty rollback between creates")(
    Top(c1, R(), c2),
    Top(c1, c2),
  )
  test("sibling empty rollback")(
    Top(R(), R()),
    Top(),
  )
  test("nested inner empty rollback")(
    Top(R(c1, R(), c2)),
    Top(R(c1, c2)),
  )
  test("inner empty rollback, within exercise")(
    Top(E(c1, R(), c2)),
    Top(E(c1, c2)),
  )
  test("nested empty rollback")(
    Top(R(R())),
    Top(),
  )
  test("nested sibling empty rollback")(
    Top(R(R(), R())),
    Top(),
  )

  // normalization rules #2 or #3
  test("nested-rollback")(
    Top(R(R(c1))),
    Top(R(c1)),
  )

  // normalization rules #2
  test("head-nested-rollback")(
    Top(R(R(c1), c2)),
    Top(R(c1), R(c2)),
  )
  test("head-nested-rollback-cascade")(
    Top(R(R(R(c1), c2), c3)),
    Top(R(c1), R(c2), R(c3)),
  )

  // normalization rules #3
  test("tail-nested-rollback")(
    Top(R(c1, R(c2))),
    Top(R(c1, c2)),
  )
  test("tail-nested-rollback-cascade")(
    Top(R(c1, R(c2, R(c3)))),
    Top(R(c1, c2, c3)),
  )
  test("tail-nested-rollback-cascade-complex")(
    Top(R(c1, c2, R(c3, R(c4)))),
    Top(R(c1, c2, c3, c4)),
  )

  // normalization rules #2 and #3
  test("mixed-rollback-nesting-A")(
    Top(R(R(c1), c2, R(c3))),
    Top(R(c1), R(c2, c3)),
  )
  test("mixed-rollback-nesting-B")(
    Top(R(R(c1), c2, c3, R(c4))),
    Top(R(c1), R(c2, c3, c4)),
  )
  test("mixed-extra-1")(
    Top(R(c1, R(R(c2), c3))),
    Top(R(c1, R(c2), c3)),
  )
  test("mixed-extra-2")(
    Top(R(R(c1, R(c2)), c3)),
    Top(R(c1, c2), R(c3)),
  )

  // drop rollback nodes
  testRollbackFails("rollback-create")(
    Top(R(R(c1), c2)),
    "Create node inside Rollback node cannot be removed.",
  )

  testRollbackFails("rollback-consuming-exercise")(
    Top(R(ec1)),
    "Consuming exercise node inside Rollback node cannot be removed.",
  )

  testRollbackSucceeds("rollback-nonconsuming-exercise")(
    Top(R(en1)),
    Top(en1),
  )

  testRollbackSucceeds("rollback-fetch")(
    Top(R(f1)),
    Top(f1),
  )

  testRollbackSucceeds("rollback-no-side-effect")(
    Top(R(f1, en1), c1, ec1),
    Top(f1, en1, c1, ec1),
  )

  testRollbackFails("rollback-one-side-effect")(
    Top(c1, R(f1, c1, en1), ec1),
    "Create node inside Rollback node cannot be removed.",
  )

  testRollbackSucceeds("rollback-nested-no-side-effect")(
    Top(c1, R(R(f1, en1), R(f2, en2, R(f3, f4))), ec1),
    Top(c1, f1, en1, f2, en2, f3, f4, ec1),
  )

  testRollbackFails("rollback-nested-one-side-effect")(
    Top(c1, R(R(f1, en1), R(f2, en2, R(f3, c1, f4))), ec1),
    "Create node inside Rollback node cannot be removed.",
  )
}

object NormalizeRollbackSpec {

  type Cid = V.ContractId
  type TX = Transaction

  def preOrderNidsOfTxIsIncreasingFromZero(tx: TX): Boolean = {
    def check(x1: Int, xs: List[Int]): Boolean = {
      xs match {
        case Nil => true
        case x2 :: xs => x1 < x2 && check(x2, xs)
      }
    }
    preOrderNidsOfTx(tx) match {
      case Nil => true
      case x :: xs => x == 0 && check(x, xs)
    }
  }

  def preOrderNidsOfTx(tx: TX): List[Int] = {
    def fromNids(acc: List[Int], xs: List[NodeId]): List[Int] = {
      xs match {
        case Nil => acc
        case x :: xs =>
          val node = tx.nodes(x)
          fromNids(fromNode(x.index :: acc, node), xs)
      }
    }
    def fromNode(acc: List[Int], node: Node): List[Int] = {
      node match {
        case _: Node.LeafOnlyAction => acc
        case node: Node.Exercise => fromNids(acc, node.children.toList)
        case node: Node.Rollback => fromNids(acc, node.children.toList)
      }
    }
    fromNids(Nil, tx.roots.toList).reverse
  }

  def forallNode(tx: TX)(pred: Node => Boolean): Boolean = {
    tx.fold[Boolean](true) { case (acc, (_, node)) =>
      acc && pred(node)
    }
  }

  def forallRB(tx: TX)(pred: Node.Rollback => Boolean): Boolean = {
    forallNode(tx) {
      case rb: Node.Rollback => pred(rb)
      case _ => true
    }
  }

  def isNormalized(tx: TX): Boolean = {
    tx match {
      case Transaction(nodes, _) =>
        def isRB(node: Node): Boolean = {
          node match {
            case _: Node.Rollback => true
            case _ => false
          }
        }
        def check(rb: Node.Rollback): Boolean = {
          val n = rb.children.length
          (n > 0) && // Normalization rule #1
          !isRB(nodes(rb.children(0))) && // Normalization rule #2
          !isRB(nodes(rb.children(n - 1))) // Normalization rule #3
        }
        forallRB(tx) { rb =>
          check(rb)
        }
    }
  }

  // Shape: description of a transaction, with conversions to and from a real tx
  sealed trait Shape
  object Shape {

    final case class Top(xs: List[Shape])
    final case class Create(x: Long) extends Shape
    final case class Fetch(n: Long) extends Shape
    final case class Exercise(consuming: Boolean, x: List[Shape]) extends Shape
    final case class Rollback(x: List[Shape]) extends Shape

    def toTransaction(top: Top): TX = {
      val ids = Iterator.from(0).map(NodeId(_))
      var nodes: Map[NodeId, Node] = Map.empty
      def add(node: Node): NodeId = {
        val nodeId = ids.next()
        nodes += (nodeId -> node)
        nodeId
      }
      def toNid(shape: Shape): NodeId = {
        shape match {
          case Create(n) => add(dummyCreateNode(n))
          case Fetch(n) => add(dummyFetchNode(n))
          case Exercise(consuming, shapes) =>
            val children = shapes.map(toNid)
            add(dummyExerciseNode(consuming, children.to(ImmArray)))
          case Rollback(shapes) =>
            val children = shapes.map(toNid)
            add(Node.Rollback(children = children.to(ImmArray)))
        }
      }
      val roots: List[NodeId] = top.xs.map(toNid)
      Transaction(nodes, roots.to(ImmArray))
    }

    def ofTransaction(tx: TX): Top = {
      def ofNid(nid: NodeId): Shape = {
        tx.nodes(nid) match {
          case create: Node.Create =>
            create.arg match {
              case V.ValueInt64(n) => Create(n)
              case _ => sys.error(s"unexpected create.arg: ${create.arg}")
            }
          case fetch: Node.Fetch => {
            Fetch(fetch.packageName.toString.split("-")(2).toLong)
            //sys.error(s"Shape.ofTransaction, unexpected leaf: $leaf")
          }
          case node: Node.Exercise => Exercise(node.consuming, node.children.toList.map(ofNid))
          case node: Node.Rollback => Rollback(node.children.toList.map(ofNid))
        }
      }
      Top(tx.roots.toList.map(nid => ofNid(nid)))
    }
  }

  private def toCid(s: String): V.ContractId.V1 =
    V.ContractId.V1(crypto.Hash.hashPrivateKey(s))

  private def dummyCreateNode(n: Long): Node.Create =
    Node.Create(
      coid = toCid("dummyCid"),
      packageName = Ref.PackageName.assertFromString("-pkgName-"),
      templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
      arg = V.ValueInt64(n),
      signatories = Set.empty,
      stakeholders = Set.empty,
      keyOpt = None,
      version = SerializationVersion.minVersion,
    )

  private def dummyFetchNode(n: Long): Node.Fetch =
    Node.Fetch(
      coid = toCid("dummyTargetCoid"),
      packageName = Ref.PackageName.assertFromString(s"-pkgName-$n"),
      templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
      actingParties = Set.empty,
      signatories = Set.empty,
      stakeholders = Set.empty,
      keyOpt = None,
      byKey = false,
      interfaceId = None,
      version = SerializationVersion.minVersion,
    )

  private def dummyExerciseNode(
      consuming: Boolean,
      children: ImmArray[NodeId]
  ): Node.Exercise =
    Node.Exercise(
      targetCoid = toCid("dummyTargetCoid"),
      packageName = Ref.PackageName.assertFromString("-pkgName-"),
      templateId = Ref.Identifier(
        Ref.PackageId.assertFromString("-dummyPkg-"),
        Ref.QualifiedName.assertFromString("DummyModule:dummyName"),
      ),
      interfaceId = None,
      choiceId = Ref.Name.assertFromString("dummyChoice"),
      consuming = consuming,
      actingParties = Set.empty,
      chosenValue = V.ValueUnit,
      stakeholders = Set.empty,
      signatories = Set.empty,
      choiceObservers = Set.empty,
      choiceAuthorizers = None,
      children = children,
      exerciseResult = None,
      keyOpt = None,
      byKey = false,
      version = SerializationVersion.minVersion,
    )
}
