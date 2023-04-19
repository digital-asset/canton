// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.reducible.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.sequencing.protocol.RecipientsTree.{MemberRecipient, Recipient}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}

/** A tree representation of the recipients for a batch.
  * Each member receiving the batch should see only subtrees of recipients from a node containing
  * the member. If a member is present in a subtree A and a sub-subtree of A then it should only see
  * the top-level subtree A.
  */
final case class RecipientsTree(
    recipientGroup: NonEmpty[Set[Recipient]],
    children: Seq[RecipientsTree],
) extends PrettyPrinting {

  override def pretty: Pretty[RecipientsTree] =
    prettyOfClass(
      param("recipient group", _.recipientGroup.toList),
      paramIfNonEmpty("children", _.children),
    )

  lazy val allRecipients: Set[Member] = {
    val tail: Set[Member] = children.flatMap(t => t.allRecipients).toSet
    recipientGroup
      .collect { case MemberRecipient(member) =>
        member
      }
      ++ tail
  }

  def allPaths: NonEmpty[Seq[NonEmpty[Seq[NonEmpty[Set[Recipient]]]]]] =
    NonEmpty.from(children) match {
      case Some(childrenNE) =>
        childrenNE.flatMap { child =>
          child.allPaths.map(p => recipientGroup +: p)
        }
      case None => NonEmpty(Seq, NonEmpty(Seq, recipientGroup))
    }

  def forMember(member: Member): Seq[RecipientsTree] = {
    // TODO(#12360): The projection for a member should include all the group addresses that include this member,
    //  using the appropriate topology snapshot for resolving the group addresses to members.
    if (
      recipientGroup
        .collect { case MemberRecipient(member) =>
          member
        }
        .contains(member)
    ) {
      Seq(this)
    } else {
      children.flatMap(c => c.forMember(member))
    }
  }

  lazy val leafRecipients: NonEmpty[Set[Recipient]] = children match {
    case NonEmpty(cs) => cs.toNEF.reduceLeftTo(_.leafRecipients)(_ ++ _.leafRecipients)
    case _ => recipientGroup.map(m => m: Recipient)
  }

  def toProtoV0: v0.RecipientsTree = {
    val recipientsP =
      recipientGroup.toSeq.collect { case MemberRecipient(member) =>
        member.toProtoPrimitive
      }.sorted
    val childrenP = children.map(_.toProtoV0)
    new v0.RecipientsTree(recipientsP, childrenP)
  }
}

object RecipientsTree {

  def ofMembers(
      recipientGroup: NonEmpty[Set[Member]],
      children: Seq[RecipientsTree],
  ) = RecipientsTree(recipientGroup.map(MemberRecipient), children)

  sealed trait Recipient extends Product with Serializable with PrettyPrinting

  final case class MemberRecipient(member: Member) extends Recipient {
    override def pretty: Pretty[MemberRecipient] =
      prettyOfClass(
        param("member", _.member)
      )
  }

  final case class ParticipantsOfParty(party: LfPartyId) extends Recipient {
    override def pretty: Pretty[ParticipantsOfParty] =
      prettyOfClass(
        param("party", _.party)
      )
  }

  final case class SequencersOfDomain(domain: DomainId) extends Recipient {
    override def pretty: Pretty[SequencersOfDomain] =
      prettyOfClass(
        param("domain", _.domain)
      )
  }

  final case class MediatorsOfDomain(domain: DomainId, group: Int) extends Recipient {
    override def pretty: Pretty[MediatorsOfDomain] =
      prettyOfClass(
        param("domain", _.domain),
        param("group", _.group),
      )
  }

  def leaf(group: NonEmpty[Set[Member]]): RecipientsTree =
    RecipientsTree(group.map(MemberRecipient), Seq.empty)

  def recipientsLeaf(group: NonEmpty[Set[Recipient]]): RecipientsTree =
    RecipientsTree(group, Seq.empty)

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
    } yield RecipientsTree(
      membersNonEmpty.map(m => MemberRecipient(m): Recipient).toSet,
      childTrees,
    )
  }

}
