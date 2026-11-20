// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.MultiHashSignature.LeafSalt
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class MultiHashSignatureTest extends AnyWordSpec with BaseTest {

  private val signature = Signature.create(
    SignatureFormat.Concat,
    ByteString.copyFromUtf8("signature"),
    Fingerprint.tryFromString("signer"),
    Some(SigningAlgorithmSpec.Ed25519),
  )

  // Five signed items with their salts, and the tree over their salted leaves
  private val items = (0 until 5).map(TestHash.digest)
  private val salts = (0 until 5).map(i => LeafSalt.tryCreate(TestHash.digest(i).unwrap))
  private val leaves: Seq[Hash] =
    items.zip(salts).map { case (item, salt) => MultiHashSignature.saltedLeaf(salt, item) }
  private val tree = MerkleHashTree.tryCreate(NonEmpty.from(leaves).value)
  private val path = tree.pathFor(2).value

  private val withPath = MultiHashSignature.create(signature, path, salts(2))

  // A single item, whose salted leaf is the root of its tree
  private val singleItem = TestHash.digest("single")
  private val singleTree =
    MerkleHashTree.tryCreate(NonEmpty(Seq, MultiHashSignature.saltedLeaf(salts(0), singleItem)))
  private val single =
    MultiHashSignature.create(signature, singleTree.pathFor(0).value, salts(0))

  private def parse(proto: v30.MultiHashSignature) =
    MultiHashSignature.fromProtoV30(ProtocolVersionValidation(testedProtocolVersion), proto)

  private def withPathProto(pathP: v30.InclusionPath): v30.MultiHashSignature =
    withPath.toProtoV30.copy(inclusionPath = Some(pathP))

  "MultiHashSignature" should {

    // Round trips of generated instances are covered by `SerializationDeserializationTest`

    "not be serializable before the protocol version that introduces it" in {
      ProtocolVersion.supported.filter(_ < ProtocolVersion.multiHashSignatures).foreach { pv =>
        withPath.toByteString(pv).left.value should include("not supported before")
      }
    }

    "serialize and parse the signature of a single item, whose inclusion path is empty" in {
      single.inclusionPath shouldBe InclusionPath.empty
      single.toProtoV30.inclusionPath shouldBe Some(InclusionPath.empty.toProtoV30)
      parse(single.toProtoV30).value shouldBe single
    }

    "serialize the leaf salt as its raw bytes" in {
      withPath.toProtoV30.leafSalt shouldBe salts(2).unwrap
    }

    "serialize the siblings as multi-hashes" in {
      val expected = v30.InclusionPath(
        index = path.index,
        siblings = path.siblings.map(_.getCryptographicEvidence),
      )
      withPath.toProtoV30.inclusionPath.value.toByteString shouldBe expected.toByteString
    }

    "compare the inclusion path and the leaf salt" in {
      withPath should not be MultiHashSignature.create(
        signature,
        tree.pathFor(3).value,
        salts(2),
      )
      withPath should not be MultiHashSignature.create(
        signature,
        path,
        salts(3),
      )
    }

    "compute the root from the item's salted leaf and the inclusion path" in {
      withPath.rootHash(items(2)) shouldBe tree.root
      // With another salt or for another item, the leaf does not lead to the root
      MultiHashSignature
        .create(signature, path, salts(3))
        .rootHash(items(2)) should not be
        tree.root
      withPath.rootHash(items(3)) should not be tree.root
    }

    "use the salted leaf of a single item as the root of its tree" in {
      single.rootHash(singleItem) shouldBe singleTree.root
      single.rootHash(singleItem) should not be singleItem
    }

    "compute the salted leaf from the salt and the item's hash" in {
      // Test vector: changes to the hash purpose or the encoding of the salted leaf change this hash
      MultiHashSignature
        .saltedLeaf(salts(0), TestHash.digest(1))
        .toHexString shouldBe
        "12203c433fcdaf0f58b0ea6c6f233ff3fc4fa782ff215e517cd9b7c16f711a895e11"
    }

    "reject a missing signature" in {
      parse(withPath.toProtoV30.copy(signature = None)).left.value.message should include(
        "signature"
      )
    }

    "reject a missing inclusion path" in {
      parse(withPath.toProtoV30.copy(inclusionPath = None)).left.value.message should include(
        "inclusion_path"
      )
    }

    "reject a missing leaf salt, which is encoded as the empty default" in {
      parse(
        withPath.toProtoV30.copy(leafSalt = ByteString.EMPTY)
      ).left.value.message should include(
        "leaf_salt"
      )
      parse(single.toProtoV30.copy(leafSalt = ByteString.EMPTY)).left.value.message should include(
        "leaf_salt"
      )
    }

    "reject a leaf salt of the wrong length" in {
      LeafSalt.create(ByteString.copyFromUtf8("short")).left.value should include("32 bytes")
      parse(
        withPath.toProtoV30.copy(leafSalt = ByteString.copyFromUtf8("short"))
      ).left.value.message should include("leaf_salt")
    }

    "reject more than the maximum number of siblings" in {
      val tooMany = Seq.fill(MerkleHashTree.MaxDepth + 1)(TestHash.digest("sibling"))
      parse(
        withPathProto(
          v30.InclusionPath(
            index = 0,
            siblings = tooMany.map(_.getCryptographicEvidence),
          )
        )
      ).left.value.message should include("siblings")
    }

    "reject an index out of range" in {
      val siblings = path.toProtoV30.siblings
      parse(
        withPathProto(v30.InclusionPath(index = 1 << siblings.size, siblings = siblings))
      ).left.value.message should include("out of range")
      // A `uint32` index of at least 2^31 is parsed as a negative `Int`
      parse(
        withPathProto(v30.InclusionPath(index = Int.MinValue, siblings = siblings))
      ).left.value.message should include("out of range")
    }

    "reject a sibling that is not a hash" in {
      parse(
        withPathProto(
          v30.InclusionPath(
            index = 0,
            siblings = Seq(ByteString.copyFromUtf8("not a hash")),
          )
        )
      ) shouldBe a[Left[?, ?]]
    }

    "print the inclusion path and the leaf salt" in {
      withPath.toString should (include("inclusionPath") and include("leafSalt"))
    }
  }
}
