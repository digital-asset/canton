// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.version.RepresentativeProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

trait HasCryptographicEvidenceTest { this: AnyWordSpec =>
  def hasCryptographicEvidenceSerialization[M <: HasCryptographicEvidence](
      sut1: M,
      sut2: M,
      hint: String = "",
  ): Unit = {
    val bytes1 = sut1.getCryptographicEvidence
    "always produce the same serialization" + hint in {
      val bytes1a = sut1.getCryptographicEvidence
      assert(bytes1 === bytes1a)
    }

    if (sut1 != sut2) {
      "different objects produce different serializations" + hint in {
        assert(bytes1 !== sut2.getCryptographicEvidence)
      }
    }
  }

  def memoizedNondeterministicDeserialization[M <: ProtocolVersionedMemoizedEvidence](
      sut: M,
      ser: ByteString,
      hint: String = "",
  )(deserialize: ByteString => M): Unit = {
    hasCryptographicEvidenceDeserialization(sut, ser, hint)(deserialize)
    "deserialize sets deserializedFrom correctly" + hint in {
      val deserialized = deserialize(ser)
      assert(deserialized.deserializedFrom.isDefined)
      deserialized.deserializedFrom.foreach(xx => assertResult(ser)(xx))
    }
  }

  def hasCryptographicEvidenceDeserialization[M <: HasCryptographicEvidence](
      sut: M,
      ser: ByteString,
      hint: String = "",
  )(deserialize: ByteString => M): Unit = {
    val bytes = sut.getCryptographicEvidence

    "deserialize to an equal object" + hint in {
      val deserialized = deserialize(bytes)
      assert(sut === deserialized)
    }

    "serialize to the deserialization it was constructed from" + hint in {
      val deserialized = deserialize(bytes)
      assert(bytes === deserialized.getCryptographicEvidence)
      assert(ser === deserialize(ser).getCryptographicEvidence)
    }
  }

  def tryDeserializer[M](
      deserializer: ByteString => Either[DeserializationError, M]
  ): ByteString => M =
    bytes =>
      deserializer(bytes) match {
        case Right(m) => m
        case Left(err) => fail(err.toString)
      }

}

class MemoizedEvidenceTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {

  val mst2: MemoizedEvidenceSUT = MemoizedEvidenceSUT(2)
  val mst3: MemoizedEvidenceSUT = MemoizedEvidenceSUT(3)
  val bytes = ByteString.copyFrom(Array[Byte](10, 5))

  "MemoizedEvidence" should {
    behave like hasCryptographicEvidenceSerialization(mst2, mst3)
    behave like hasCryptographicEvidenceDeserialization(mst3, bytes)(
      MemoizedEvidenceSUT.fromByteString
    )
  }
}

sealed case class MemoizedEvidenceSUT(b: Byte)(
    val representativeProtocolVersion: RepresentativeProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends ProtocolVersionedMemoizedEvidence {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var counter: Byte = 0

  protected override def toByteStringUnmemoized: ByteString = {
    counter = (counter + 1).toByte
    ByteString.copyFrom(Array(counter, b))
  }
}

object MemoizedEvidenceSUT {
  private[this] def apply(b: Byte)(deserializedFrom: Option[ByteString]): MemoizedEvidenceSUT =
    throw new UnsupportedOperationException("Use the public apply method instead")

  def apply(b: Byte): MemoizedEvidenceSUT =
    new MemoizedEvidenceSUT(b)(RepresentativeProtocolVersion.v2, None)

  def fromByteString(bytes: ByteString): MemoizedEvidenceSUT = {
    if (bytes.size() != 2)
      throw new IllegalArgumentException(s"Only two bytes expected, got: ${bytes.toString}")

    new MemoizedEvidenceSUT(bytes.byteAt(1))(RepresentativeProtocolVersion.v2, Some(bytes))
  }
}

class MemoizedEvidenceWithFailureTest
    extends AnyWordSpec
    with BaseTest
    with HasCryptographicEvidenceTest {

  val msft2: MemoizedEvidenceWithFailureSUT = MemoizedEvidenceWithFailureSUT(2)(fail = false)
  val msft3: MemoizedEvidenceWithFailureSUT = MemoizedEvidenceWithFailureSUT(3)(fail = false)

  val bytes = ByteString.copyFrom(Array[Byte](10, 5))

  "MemoizedEvidenceWithFailure" should {
    behave like hasCryptographicEvidenceSerialization(msft2, msft3)
    behave like hasCryptographicEvidenceDeserialization(msft3, bytes)(
      MemoizedEvidenceWithFailureSUT.fromByteString
    )

    "throw an exception if serialization fails" in {
      assertThrows[SerializationCheckFailed[Unit]](MemoizedEvidenceWithFailureSUT(5)(fail = true))
    }
  }
}

sealed case class MemoizedEvidenceWithFailureSUT private (b: Byte)(
    fail: Boolean,
    override val deserializedFrom: Option[ByteString],
) extends MemoizedEvidenceWithFailure[Unit] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var counter: Byte = 0

  protected[this] override def toByteStringChecked: Either[Unit, ByteString] =
    if (fail)
      Left(())
    else {
      counter = (counter + 1).toByte
      Right(ByteString.copyFrom(Array(counter, b)))
    }
}

object MemoizedEvidenceWithFailureSUT {
  private[this] def apply(
      b: Byte
  )(fail: Boolean, deserializedFrom: Option[ByteString]): MemoizedEvidenceWithFailureSUT =
    throw new UnsupportedOperationException("Use the public apply method instead")

  def apply(b: Byte)(fail: Boolean): MemoizedEvidenceWithFailureSUT =
    new MemoizedEvidenceWithFailureSUT(b)(fail, None)

  def fromByteString(bytes: ByteString): MemoizedEvidenceWithFailureSUT = {
    if (bytes.size() != 2)
      throw new IllegalArgumentException(s"Only two bytes expected, got: ${bytes.toString}")

    new MemoizedEvidenceWithFailureSUT(bytes.byteAt(1))(false, Some(bytes))
  }
}
