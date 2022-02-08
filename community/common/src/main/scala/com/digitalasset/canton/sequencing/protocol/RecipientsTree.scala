// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.NonEmptySet
import cats.syntax.foldable._
import cats.syntax.semigroup._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0

/** A tree representation of the recipients for a batch.
  * Each member receiving the batch should see only subtrees of recipients from a node containing
  * the member. If a member is present in a subtree A and a sub-subtree of A then it should only see
  * the top-level subtree A.
  */
case class RecipientsTree(recipientGroup: NonEmptySet[Member], children: List[RecipientsTree])
    extends PrettyPrinting
    with HasProtoV0[v0.RecipientsTree] {

  override def pretty: Pretty[RecipientsTree] =
    prettyOfClass(param("value", _.recipientGroup.toList), param("children", _.children))

  lazy val allRecipients: Set[Member] = {
    val tail: Set[Member] = children.flatMap(t => t.allRecipients).toSet
    recipientGroup.toSortedSet ++ tail
  }

  def forMember(member: Member): List[RecipientsTree] = {
    if (recipientGroup.contains(member)) {
      List(this)
    } else {
      children.flatMap(c => c.forMember(member))
    }
  }

  lazy val leafMembers: NonEmptySet[Member] = children match {
    case Nil => recipientGroup
    case c :: cs => cs.foldLeft(c.leafMembers)(_ |+| _.leafMembers)
  }

  override def toProtoV0: v0.RecipientsTree = {
    val recipientsP = recipientGroup.toList.map(member => member.toProtoPrimitive)
    val childrenP = children.map { t: RecipientsTree =>
      t.toProtoV0
    }
    new v0.RecipientsTree(recipientsP, childrenP)
  }
}

object RecipientsTree {

  def fromProtoV0(
      treeProto: v0.RecipientsTree
  ): ParsingResult[RecipientsTree] = {
    for {
      members <- treeProto.recipients.toList.traverse(str =>
        Member.fromProtoPrimitive(str, "RecipientsTreeProto.recipients")
      )
      membersNonEmpty <- {
        members match {
          case Nil =>
            Left(
              ProtoDeserializationError.ValueConversionError(
                "RecipientsTree.recipients",
                s"RecipientsTree.recipients must be non-empty",
              )
            )
          case x :: xs => Right(NonEmptySet.of[Member](x, xs: _*))
        }
      }
      children = treeProto.children
      childTrees <- children.toList.traverse(fromProtoV0)
    } yield RecipientsTree(membersNonEmpty, childTrees)
  }

}
