// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.version.VersionedMessage
import com.google.protobuf.ByteString

import scala.util.Random

object TestProtoBuilder {

  private val random = new Random()

  private def unblindedNode(bytes: ByteString) =
    v30.BlindableNode(v30.BlindableNode.BlindedOrNot.Unblinded(bytes))

  def versionedMessage(gm: scalapb.GeneratedMessage): ByteString =
    VersionedMessage(gm.toByteString, 1).toByteString

  private val v30BlindedNode = v30.BlindableNode(
    v30.BlindableNode.BlindedOrNot.BlindedHash(TestHash.dummyRootHash.toProtoPrimitive)
  )

  private def wrapAsBranch(child: v30.MerkleSeqElement): v30.MerkleSeqElement =
    v30.MerkleSeqElement(
      first = Some(unblindedNode(versionedMessage(child))),
      second = Some(v30BlindedNode),
      data = None,
    )

  def wrapAsSubviewsOfViewNode(child: v30.MerkleSeqElement): v30.ViewNode =
    v30.ViewNode(
      viewCommonData = Some(v30BlindedNode),
      viewParticipantData = Some(v30BlindedNode),
      subviews = Some(v30.MerkleSeq(Some(unblindedNode(versionedMessage(child))))),
    )

  private def wrapViewNodeAsSingleton(child: v30.ViewNode): v30.MerkleSeqElement =
    v30.MerkleSeqElement(
      first = None,
      second = None,
      data = Some(unblindedNode(versionedMessage(child))),
    )

  private def wrapInViewNodeElement(child: v30.MerkleSeqElement): v30.MerkleSeqElement =
    wrapViewNodeAsSingleton(wrapAsSubviewsOfViewNode(child))

  private val initNode = v30.ViewNode(
    viewCommonData = Some(v30BlindedNode),
    viewParticipantData = Some(v30BlindedNode),
    subviews = Some(v30.MerkleSeq(None)),
  )

  private val initElement: v30.MerkleSeqElement = wrapViewNodeAsSingleton(initNode)

  def buildDeepViewNode(depth: Int): v30.ViewNode =
    (1 to depth).foldLeft(initNode) { (child, _) =>
      wrapAsSubviewsOfViewNode(wrapViewNodeAsSingleton(child))
    }

  def buildDeepMerkleSeq(depth: Int): v30.MerkleSeq = {
    val finalElement = (1 to depth).foldLeft(initElement) { (child, _) =>
      wrapAsBranch(child)
    }
    v30.MerkleSeq(Some(unblindedNode(versionedMessage(finalElement))))
  }

  // It is assumed that the singleton is not nested (so has depth of 1)
  def buildDeepMerkleSeq(depth: Int, singleton: v30.MerkleSeqElement): v30.MerkleSeq = {
    val finalElement = (1 until depth).foldLeft(singleton) { (child, _) =>
      wrapAsBranch(child)
    }
    v30.MerkleSeq(Some(unblindedNode(versionedMessage(finalElement))))
  }

  // Build a MerkleSeqElement nested in random layest of view nodes and MerkleSeq branches.
  def buildDeepMerkleSeqElement(depth: Int): v30.MerkleSeqElement =
    (1 until depth).foldLeft(initElement) { (child, _) =>
      if (random.nextBoolean()) {
        wrapInViewNodeElement(child)
      } else {
        wrapAsBranch(child)
      }
    }

}
