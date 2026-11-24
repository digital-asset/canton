// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest.testedProtocolVersionValidation
import com.digitalasset.canton.ProtoDeserializationError.{
  InvariantViolation,
  StringConversionError,
  ValueConversionError,
}
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersionValidation
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TagsTest extends AnyWordSpec with Matchers {

  "ReassignmentId" when {
    val sync1 = SynchronizerId.tryFromString("sync1::namespace")
    val sync2 = SynchronizerId.tryFromString("sync2::namespace")
    val ts = CantonTimestamp.Epoch
    val cid1 = LfContractId.assertFromString("00" + "00" * 32 + "01")
    val cid2 = LfContractId.assertFromString("00" + "00" * 32 + "02")
    val rac1 = ReassignmentCounter(1)
    val rac2 = ReassignmentCounter(2)

    val examples = for {
      source <- Seq(sync1, sync2)
      target <- Seq(sync1, sync2)
      timestamp <- Seq(ts, ts.plusSeconds(1))
      cidCtrs <- Seq(Seq.empty, Seq(cid1 -> rac1, cid2 -> rac2))
    } yield (Source(source), Target(target), timestamp, cidCtrs)

    val mk = ReassignmentId.apply.tupled

    "different args produce different ids" should {
      examples.zipWithIndex.foreach { case (a, i) =>
        examples.zipWithIndex.foreach { case (b, j) =>
          if (i == j) () // skip comparing the same arguments
          else withClue(s"comparing args: $a with $b")(mk(a) should not equal mk(b))
        }
      }
    }

    "equivalent args produce the same id" should {
      val source = Source(sync1)
      val target = Target(sync2)
      "ignore order of contract id counters" in {
        val oneThenTwo = ReassignmentId(source, target, ts, Seq(cid1 -> rac1, cid2 -> rac2))
        val twoThenOne = ReassignmentId(source, target, ts, Seq(cid2 -> rac2, cid1 -> rac1))
        oneThenTwo should equal(twoThenOne)
      }
    }

    "fromBytes of toBytes" should {
      "roundtrip" in {
        examples.map(mk).foreach { id =>
          ReassignmentId.fromBytes(id.toBytes) shouldBe id
        }
      }
    }

    "fromProtoPrimitive of toProtoPrimitive" should {
      "roundtrip" in {
        examples.map(mk).foreach { id =>
          ReassignmentId.fromProtoPrimitive(
            id.toProtoPrimitive,
            testedProtocolVersionValidation,
          ) shouldBe Right(id)
        }
      }
    }

    "fromProtoPrimitive" should {
      "fail if empty" in {
        ReassignmentId.fromProtoPrimitive("", testedProtocolVersionValidation) shouldBe Left(
          ValueConversionError("reassignment_id", "no ReassignmentId version")
        )
      }
      "fail if invalid version prefix" in {
        ReassignmentId.fromProtoPrimitive("FFFF", testedProtocolVersionValidation) shouldBe Left(
          ValueConversionError("reassignment_id", "invalid version: -1")
        )
      }
      "fail if invalid hex format" in {
        ReassignmentId.fromProtoPrimitive(
          "this ain't hex",
          testedProtocolVersionValidation,
        ) shouldBe Left(
          StringConversionError("invalid hex", Some("reassignment_id"))
        )
      }
      "succeed for valid values" in {
        ReassignmentId
          .fromProtoPrimitive("0001", testedProtocolVersionValidation)
          .toOption isDefined
      }
      "produce ids of 70 hex characters" in {
        examples.map(mk).foreach(_.toProtoPrimitive should have length 70)
      }
      "accept a payload of the size of a SHA-256 multi-hash" in {
        val id = "00" + "12" * 34
        ReassignmentId
          .fromProtoPrimitive(id, testedProtocolVersionValidation)
          .map(_.toProtoPrimitive) shouldBe Right(id)
      }
      "fail if longer than 70 hex characters, before decoding the hex" in {
        // valid version, but the rest is not valid hex: the length is checked first
        val id = "00" + "z" * 69
        ReassignmentId.fromProtoPrimitive(
          id,
          ProtocolVersionValidation.AlwaysValidation,
        ) shouldBe Left(
          InvariantViolation(
            field = Some("reassignment_id"),
            error = "repeated or map field has 71 elements, exceeding the maximum of 70",
          )
        )
      }
      "fail if the version is not valid hex" in {
        ReassignmentId.fromProtoPrimitive("z", testedProtocolVersionValidation) shouldBe Left(
          StringConversionError("invalid hex", Some("reassignment_id"))
        )
      }
    }

    "toString" should {
      "be sensible" in {
        ReassignmentId
          .fromProtoPrimitive("0001", testedProtocolVersionValidation)
          .map(_.toString) shouldBe Right(
          "ReassignmentId(0001)"
        )
      }
    }
  }
}
