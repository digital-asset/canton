// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.crypto.{Fingerprint, Signature, SignatureFormat, TestHash}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.{
  MultiTransactionSignature,
  SignedTopologyTransaction,
  SingleTransactionSignature,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.AbstractSet

class MultiHashTopologyTransactionsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  object Factory
      extends TopologyTransactionTestFactory(
        loggerFactory,
        parallelExecutionContext,
        multiHash = true,
      )

  private val transactionHash = Factory.dns1.transaction.hash

  val txHashes = NonEmpty.mk(Seq, transactionHash, TxHash(TestHash.digest("test")))

  private val multiHash = MultiTransactionSignature(
    transactionHashes = txHashes.toSet,
    signature = makeSig("multi-sig", "multi-sig-fingerprint"),
  )

  private def makeSig(content: String, fingerprint: String) =
    Signature.create(
      SignatureFormat.Symbolic,
      ByteString.copyFromUtf8(content),
      Fingerprint.tryFromString(fingerprint),
      None,
    )

  "multi transaction signature" should {
    "not hash the covered transaction hashes" in {
      // Hashing the set once per signature makes deserialization quadratic
      val unhashableHashes = new AbstractSet[TxHash] {
        override def hashCode(): Int = fail("the covered transaction hashes must not be hashed")
        override def iterator: Iterator[TxHash] = multiHash.transactionHashes.iterator
        override def contains(elem: TxHash): Boolean = multiHash.transactionHashes.contains(elem)
        override def incl(elem: TxHash): Set[TxHash] = multiHash.transactionHashes.incl(elem)
        override def excl(elem: TxHash): Set[TxHash] = multiHash.transactionHashes.excl(elem)
      }
      // Need at least size 10 for Scala's Set to switch to a hash-based implementation
      val signatures = (1 to 10).map(i =>
        MultiTransactionSignature(
          NonEmptyUtil.fromUnsafe[Set[TxHash]](unhashableHashes),
          makeSig(s"sig$i", s"fingerprint$i"),
        )
      )

      // Call `.toSet` to trigger the hash code computation
      signatures.toSet should have size 10
    }

    "have the same hash code as an equal signature" in {
      val equalSignature = multiHash.copy(
        // Built in a different order, so that this is a different but equal set
        transactionHashes = txHashes.reverse.toSet
      )

      equalSignature shouldBe multiHash
      equalSignature.hashCode shouldBe multiHash.hashCode
    }

    "differ from a signature covering other transaction hashes" in {
      val otherMultiHash = multiHash.copy(transactionHashes =
        NonEmpty.mk(Set, transactionHash, TxHash(TestHash.digest("other")))
      )

      otherMultiHash should not be multiHash
      Set(multiHash, otherMultiHash) should have size 2
    }
  }

  "signed topology transaction" should {
    "only return valid signatures" in {
      val signature1 = makeSig("sig1", "fingerprint1")
      val signature2 = makeSig("sig2", "fingerprint2")

      val hashes = NonEmpty.mk(Set, TxHash(TestHash.digest("test")))
      val multiHash = MultiTransactionSignature(
        transactionHashes = hashes,
        signature = signature2,
      )

      val signedTx = SignedTopologyTransaction
        .withSignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, signature1, Signature.noSignature),
          isProposal = false,
          testedProtocolVersion,
        )
        .addSignatures(NonEmpty.mk(Set, multiHash))

      // Critically, multiHash is not there because it does not cover the transaction
      signedTx.allUnvalidatedSignaturesCoveringHash.map(
        _.signature
      ) should contain theSameElementsAs Set(
        signature1,
        Signature.noSignature,
      )
    }

    "successfully merge single into single signature" in {
      val signedTx = SignedTopologyTransaction.withSignatures(
        Factory.dns1.transaction,
        NonEmpty.mk(Seq, Signature.noSignature),
        isProposal = false,
        testedProtocolVersion,
      )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint-2")

      signedTx.addSingleSignatures(
        NonEmpty.mk(Set, newSingleSignature)
      ) shouldBe SignedTopologyTransaction.withSignatures(
        Factory.dns1.transaction,
        NonEmpty.mk(
          Seq,
          Signature.noSignature,
          newSingleSignature,
        ),
        isProposal = false,
        testedProtocolVersion,
      )
    }

    "successfully merge multi into single signature" in {
      val signedTx = SignedTopologyTransaction.withSignatures(
        Factory.dns1.transaction,
        NonEmpty.mk(Seq, Signature.noSignature),
        isProposal = false,
        testedProtocolVersion,
      )

      signedTx
        .addSignatures(NonEmpty.mk(Set, multiHash)) shouldBe SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(
            Seq,
            multiHash,
            SingleTransactionSignature(Factory.dns1.transaction.hash, Signature.noSignature),
          ),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }

    "successfully merge single into multi signature" in {
      val signedTx = SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, multiHash),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint-2")

      signedTx.addSingleSignatures(
        NonEmpty.mk(Set, newSingleSignature)
      ) shouldBe SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(
            Seq,
            multiHash,
            SingleTransactionSignature(Factory.dns1.transaction.hash, newSingleSignature),
          ),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }

    "successfully merge multi into multi signature" in {
      val signedTx = SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, multiHash),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )

      val newSingleSignature = makeSig("new_sig", "no-fingerprint-2")

      val multiHash2 = MultiTransactionSignature(
        transactionHashes =
          NonEmpty.mk(Set, Factory.dns1.transaction.hash, TxHash(TestHash.digest("test"))),
        signature = newSingleSignature,
      )

      signedTx
        .addSignatures(NonEmpty.mk(Set, multiHash2)) shouldBe SignedTopologyTransaction
        .withTopologySignatures(
          Factory.dns1.transaction,
          NonEmpty.mk(Seq, multiHash, multiHash2),
          isProposal = false,
          BaseTest.testedProtocolVersion,
        )
    }
  }
}
