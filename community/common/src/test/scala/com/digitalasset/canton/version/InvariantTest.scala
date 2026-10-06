// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import org.scalatest.wordspec.AnyWordSpec

final class InvariantTest extends AnyWordSpec with BaseTest {
  import InvariantTest.*

  private def rpv(protoVersion: Int): RepresentativeProtocolVersion[TestValue.type] =
    TestValue.protocolVersionRepresentativeFor(ProtoVersion(protoVersion)).value

  private def rpvFor(pv: ProtocolVersion): RepresentativeProtocolVersion[TestValue.type] =
    TestValue.protocolVersionRepresentativeFor(pv)

  // Protocol versions that differ from their representative, to check which of the two is reported
  private val pvRepresentedByRpv0 = ProtocolVersion(rpv(0).representative.v + 1)
  private val pvRepresentedByRpv1 = ProtocolVersion(rpv(1).representative.v + 1)

  // Invariants do not constrain the value class, so they are checked on plain values

  "DefaultValueUntilExclusive" should {
    val invariant =
      DefaultValueUntilExclusive[Double, TestValue.type, Double](identity, "value", rpv(1), 0.0)

    "hold for the default value before and for any value from the threshold on" in {
      invariant.validateInstance(0.0, rpv(0)) shouldBe Right(())
      invariant.validateInstance(1.0, rpv(1)) shouldBe Right(())
    }

    "report the representative protocol version, the expected and the found value" in {
      rpvFor(pvRepresentedByRpv0) shouldBe rpv(0)

      invariant.validateInstance(1.0, rpvFor(pvRepresentedByRpv0)).left.value shouldBe
        s"value must have its default value for representative protocol version ${rpv(0).representative} " +
        s"(default required for representative protocol versions < ${rpv(1).representative}): " +
        "expected 0.0, found 1.0"
    }

    "report the protocol version when validating against one" in {
      invariant.validate(1.0, pvRepresentedByRpv0).left.value shouldBe
        s"value must have its default value for protocol version $pvRepresentedByRpv0 " +
        s"(default required for protocol versions < ${rpv(1).representative}): " +
        "expected 0.0, found 1.0"
    }
  }

  "DefaultValueFromInclusive" should {
    val invariant =
      DefaultValueFromInclusive[Double, TestValue.type, Double](identity, "value", rpv(1), 0.0)

    "report the representative protocol version, the expected and the found value" in {
      rpvFor(pvRepresentedByRpv1) shouldBe rpv(1)

      invariant.validateInstance(1.0, rpvFor(pvRepresentedByRpv1)).left.value shouldBe
        s"value must have its default value for representative protocol version ${rpv(1).representative} " +
        s"(default required for representative protocol versions >= ${rpv(1).representative}): " +
        "expected 0.0, found 1.0"
    }

    "report the protocol version when validating against one" in {
      invariant.validate(1.0, pvRepresentedByRpv1).left.value shouldBe
        s"value must have its default value for protocol version $pvRepresentedByRpv1 " +
        s"(default required for protocol versions >= ${rpv(1).representative}): " +
        "expected 0.0, found 1.0"
    }
  }
}

object InvariantTest {

  /** Only provides representative protocol versions:
    * {{{
    *   proto              0                 1
    *   protocolVersion    min    min + 1    min + 2    min + 3    ...
    * }}}
    */
  final case class TestValue(msg: String)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[TestValue.type]
  ) extends HasProtocolVersionedWrapper[TestValue] {
    @transient override protected lazy val companionObj: TestValue.type = TestValue

    def toProtoV0: VersionedMessageV0 = VersionedMessageV0(msg)
    def toProtoV1: VersionedMessageV1 = VersionedMessageV1(msg, value = 0)
  }

  object TestValue
      extends VersioningCompanion[TestValue]
      with IgnoreInSerializationTestExhaustivenessCheck {
    override def name: String = "test value"

    private val minimum = ProtocolVersion.minimum

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(0) -> VersionedProtoCodec(ProtocolVersion.createStable(minimum.v))(
        VersionedMessageV0
      )(
        supportedProtoVersion(_)(proto => fromProto(proto.msg, ProtoVersion(0))),
        _.toProtoV0,
      ),
      ProtoVersion(1) -> VersionedProtoCodec(ProtocolVersion.createAlpha(minimum.v + 2))(
        VersionedMessageV1
      )(
        supportedProtoVersion(_)(proto => fromProto(proto.msg, ProtoVersion(1))),
        _.toProtoV1,
      ),
    )

    private def fromProto(msg: String, protoVersion: ProtoVersion): ParsingResult[TestValue] =
      protocolVersionRepresentativeFor(protoVersion).map(TestValue(msg)(_))
  }
}
