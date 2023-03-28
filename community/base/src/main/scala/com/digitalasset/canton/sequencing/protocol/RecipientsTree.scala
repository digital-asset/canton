// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.reducible.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** A tree representation of the recipients for a batch.
  * Each member receiving the batch should see only subtrees of recipients from a node containing
  * the member. If a member is present in a subtree A and a sub-subtree of A then it should only see
  * the top-level subtree A.
  */
final case class RecipientsTree(
    recipientGroup: NonEmpty[Set[Member]],
    children: Seq[RecipientsTree],
) extends PrettyPrinting {

  override def pretty: Pretty[RecipientsTree] =
    prettyOfClass(param("recipient group", _.recipientGroup.toList), param("children", _.children))

  lazy val allRecipients: NonEmpty[Set[Member]] = {
    val tail: Set[Member] = children.flatMap(t => t.allRecipients).toSet
    recipientGroup ++ tail
  }

  def allPaths: NonEmpty[Seq[NonEmpty[Seq[NonEmpty[Set[Member]]]]]] =
    NonEmpty.from(children) match {
      case Some(childrenNE) =>
        childrenNE.flatMap { child =>
          child.allPaths.map(p => recipientGroup +: p)
        }
      case None => NonEmpty(Seq, NonEmpty(Seq, recipientGroup))
    }

  def forMember(member: Member): Seq[RecipientsTree] = {
    if (recipientGroup.contains(member)) {
      Seq(this)
    } else {
      children.flatMap(c => c.forMember(member))
    }
  }

  lazy val leafMembers: NonEmpty[Set[Member]] = children match {
    case NonEmpty(cs) => cs.toNEF.reduceLeftTo(_.leafMembers)(_ ++ _.leafMembers)
    case _ => recipientGroup
  }

  def toProtoV0: v0.RecipientsTree = {
    val recipientsP = recipientGroup.toSeq.map(member => member.toProtoPrimitive)
    val childrenP = children.map(_.toProtoV0)
    new v0.RecipientsTree(recipientsP, childrenP)
  }
}

object RecipientsTree {

  def leaf(group: NonEmpty[Set[Member]]): RecipientsTree = RecipientsTree(group, Seq.empty)

  def fromProtoV0(
      treeProto: v0.RecipientsTree
  ): ParsingResult[RecipientsTree] = {
    for {
      members <- treeProto.recipients.traverse(str =>
        Member.fromProtoPrimitive(str, "RecipientsTreeProto.recipients")
      )
      membersNonEmpty <- NonEmpty
        .from(members)
        .toRight(
          ProtoDeserializationError.ValueConversionError(
            "RecipientsTree.recipients",
            s"RecipientsTree.recipients must be non-empty",
          )
        )
      children = treeProto.children
      childTrees <- children.toList.traverse(fromProtoV0)
    } yield RecipientsTree(membersNonEmpty.toSet, childTrees)
  }

}
