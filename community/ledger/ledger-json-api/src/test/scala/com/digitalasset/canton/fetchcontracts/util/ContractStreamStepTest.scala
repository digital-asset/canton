// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts.util

import com.daml.scalatest.FlatSpecCheckLaws
import com.digitalasset.canton.Generators.boundedListGen
import com.digitalasset.canton.fetchcontracts.Offset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.scalacheck.ScalaCheckBinding.*
import scalaz.scalacheck.ScalazProperties
import scalaz.syntax.apply.*
import scalaz.syntax.semigroup.*
import scalaz.{@@, Equal, Tag}

import scala.annotation.nowarn

@nowarn("msg=match may not be exhaustive")
class ContractStreamStepTest
    extends AnyFlatSpec
    with FlatSpecCheckLaws
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  import ContractStreamStepTest.*, ContractStreamStep.*
  import InsertDeleteStepTest.*

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  behavior of "append"

  it should "be associative for valid streams" in forAll(validStreamGen) { csses =>
    whenever(csses.sizeIs >= 3) {
      forEvery(
        Table(("a", "b", "c"), csses.sliding(3).map { case Seq(a, b, c) => (a, b, c) }.toSeq*)
      ) { case (a, b, c) =>
        (a |+| (b |+| c)) should ===((a |+| b) |+| c)
      }
    }
  }

  it should "report the last offset" in forAll { (a: CSS, b: CSS) =>
    def off(css: ContractStreamStep[_, _]) = css match {
      case Acs(_) => None
      case LiveBegin(off) => off.toOption
      case Txn(_, off) => Some(off)
    }

    off(a |+| b) should ===(off(b) orElse off(a))
  }

  it should "preserve append across toInsertDelete" in forAll { (a: CSS, b: CSS) =>
    (a |+| b).toInsertDelete should ===(a.toInsertDelete |+| b.toInsertDelete)
  }

  behavior of "append semigroup"

  checkLaws(ScalazProperties.semigroup.laws[CSS])
}

object ContractStreamStepTest {
  import InsertDeleteStepTest.*, InsertDeleteStep.Inserts, ContractStreamStep.*
  import org.scalacheck.{Arbitrary, Gen}
  import Arbitrary.arbitrary

  type CSS = ContractStreamStep[Unit, Cid]

  private val offGen: Gen[Offset] = Tag subst Tag.unsubst(arbitrary[String @@ Alpha])
  private val acsGen = arbitrary[Inserts[Cid]] map (Acs(_))
  private val noAcsLBGen = Gen const LiveBegin(ParticipantBegin)
  private val postAcsGen = offGen map (o => LiveBegin(AbsoluteBookmark(o)))
  private val txnGen = ^(arbitrary[IDS], offGen)(Txn(_, _))

  private val validStreamGen: Gen[Seq[CSS]] = for {
    beforeAfter <- Gen.zip(
      boundedListGen(acsGen),
      boundedListGen(txnGen),
    )
    (acsSeq, txnSeq) = beforeAfter
    liveBegin <- if (acsSeq.isEmpty) noAcsLBGen else postAcsGen
  } yield (acsSeq :+ liveBegin) ++ txnSeq

  private implicit val `CSS eq`: Equal[CSS] = Equal.equalA

  private implicit val `anyCSS arb`: Arbitrary[CSS] =
    Arbitrary(Gen.frequency((4, acsGen), (1, noAcsLBGen), (1, postAcsGen), (4, txnGen)))
}
