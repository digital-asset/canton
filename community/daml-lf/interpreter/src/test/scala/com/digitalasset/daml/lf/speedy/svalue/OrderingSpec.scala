// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy
package svalue

import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, Ref, Time}
import com.digitalasset.daml.lf.interpretation.Error.ContractIdComparability
import com.digitalasset.daml.lf.speedy.SValue.*
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.test.ValueGenerators.comparableCoidsGen
import org.scalacheck.Gen
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.{
  Checkers,
  ScalaCheckDrivenPropertyChecks,
  ScalaCheckPropertyChecks,
}

import scala.language.implicitConversions
import scala.util.{Failure, Try}

class OrderingSpec
    extends AnyWordSpec
    with Checkers
    with Inside
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with ScalaCheckPropertyChecks
    with TableDrivenPropertyChecks {

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  implicit def toTypeConId(s: String): Ref.TypeConId =
    Ref.TypeConId(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  // Each generator produces values from a single universe of mutually-comparable SValues.
  // Values from different universes are not comparable, so the ordering laws only hold within one.
  private val comparableSValueGens: Seq[Gen[SValue]] = {
    import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
    def g(va: VA)(sv: va.Inj => SValue): Gen[SValue] = va.injarb.arbitrary.map(sv)
    Seq(
      g(VA.int64)(SInt64.apply),
      g(VA.text)(SText.apply),
      g(VA.list(VA.optional(VA.int64))) { loi =>
        SList(loi.map(oi => SOptional(oi map SInt64.apply)).to(FrontStack))
      },
    ) ++ comparableCoidsGen.map(_.map(SContractId.apply))
  }

  "Ordering.compare" should {
    "be a lawful total order within each comparable subset" in {
      // Draw all three values from the same universe so that they are mutually comparable.
      val triples =
        for {
          gen <- Gen.oneOf(comparableSValueGens)
          a <- gen
          b <- gen
          c <- gen
        } yield (a, b, c)
      forAll(triples, minSuccessful(500)) { case (a, b, c) =>
        // reflexivity
        Ordering.compare(a, a) should ===(0)
        // antisymmetry
        Integer.signum(Ordering.compare(a, b)) should ===(-Integer.signum(Ordering.compare(b, a)))
        // transitivity
        if (Ordering.compare(a, b) <= 0 && Ordering.compare(b, c) <= 0)
          Ordering.compare(a, c) should be <= 0
      }
    }
  }

  // A problem in this test *usually* indicates changes that need to be made
  // in Value.orderInstance or TypedValueGenerators, rather than to svalue.Ordering.
  // The tests are here as this is difficult to test outside daml-lf/interpreter.
  "txn Value Ordering" should {
    "match global ContractId ordering" in {
      // Draw both contract IDs from the same universe so that they are mutually comparable.
      val pairs =
        for {
          gen <- Gen.oneOf(comparableCoidsGen)
          a <- gen
          b <- gen
        } yield (a, b)
      forAll(pairs, minSuccessful(150)) { case (a, b) =>
        Integer.signum(Value.ContractId.`Cid Ordering`.compare(a, b)) should ===(
          Integer.signum(Ordering.compare(SContractId(a), SContractId(b)))
        )
      }
    }

    "fail when trying to compare local/relative contract ID with global/relative/absolute contract ID with same prefix" in {

      val discriminator1 = crypto.Hash.hashPrivateKey("discriminator1")
      val discriminator2 = crypto.Hash.hashPrivateKey("discriminator2")
      val suffix1 = Bytes.assertFromString("00")
      val suffix2 = Bytes.assertFromString("80")
      val suffix3 = Bytes.assertFromString("11")

      val cid10 = Value.ContractId.V1(discriminator1, Bytes.Empty)
      val cid11 = Value.ContractId.V1(discriminator1, suffix1)
      val cid12 = Value.ContractId.V1(discriminator1, suffix2)
      val cid21 = Value.ContractId.V1(discriminator2, suffix1)

      val local1 = Value.ContractId.V2.unsuffixed(Time.Timestamp.MinValue, discriminator1).local
      val local2 = Value.ContractId.V2.unsuffixed(Time.Timestamp.MinValue, discriminator2).local
      val cid30 = Value.ContractId.V2(local1, Bytes.Empty)
      val cid31 = Value.ContractId.V2(local1, suffix1)
      val cid32 = Value.ContractId.V2(local1, suffix2)
      val cid41 = Value.ContractId.V2(local2, suffix1)
      val cid43 = Value.ContractId.V2(local2, suffix3)

      val List(vCid10, vCid11, vCid12, vCid21, vCid30, vCid31, vCid32, vCid41, vCid43) =
        List(cid10, cid11, cid12, cid21, cid30, cid31, cid32, cid41, cid43).map(SContractId.apply)

      val negativeTestCases =
        Table(
          "cid1" -> "cid2",
          vCid10 -> vCid10,
          vCid11 -> vCid11,
          vCid11 -> vCid12,
          vCid11 -> vCid21,
          vCid30 -> vCid30,
          vCid31 -> vCid31,
          vCid31 -> vCid41,
          vCid31 -> vCid43,
          vCid32 -> vCid43,
          vCid10 -> vCid30,
          vCid10 -> vCid31,
          vCid11 -> vCid30,
        )

      val positiveLocalTestCases = Table(
        "local Cid" -> "global/absolute/relative Cid",
        vCid10 -> cid11,
        vCid10 -> cid12,
        vCid30 -> cid31,
        vCid30 -> cid32,
      )

      val positiveRelativeTestCases = Table(
        "relative Cid" -> "absolute/relative Cid",
        // relative -> absolute
        cid31 -> cid32,
        // relative -> relative
        cid41 -> cid43,
      )

      forEvery(negativeTestCases) { (cid1, cid2) =>
        (Ordering.compare(cid1, cid2) == 0) shouldBe (cid1 == cid2)
        (Ordering.compare(cid2, cid1) == 0) shouldBe (cid1 == cid2)
      }

      forEvery(positiveLocalTestCases) { (localCid, globalCid) =>
        Try(Ordering.compare(localCid, SContractId(globalCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
        Try(Ordering.compare(SContractId(globalCid), localCid)) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
      }

      forEvery(positiveRelativeTestCases) { (relativeCid, otherCid) =>
        Try(Ordering.compare(SContractId(relativeCid), SContractId(otherCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(relativeCid)))
        Try(Ordering.compare(SContractId(otherCid), SContractId(relativeCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(otherCid)))
      }
    }
  }
}
