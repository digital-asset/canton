// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.reducible._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.HasProtoV0

/** Recipients of a batch. Uses a list of [[com.digitalasset.canton.sequencing.protocol.RecipientsTree]]s
  * that define the members receiving a batch, and which members see which other recipients.
  */
case class Recipients private (trees: NonEmpty[Seq[RecipientsTree]])
    extends PrettyPrinting
    with HasProtoV0[v0.Recipients] {

  lazy val allRecipients: Set[Member] = {
    trees.flatMap(t => t.allRecipients).toSet
  }

  def forMember(member: Member): Option[Recipients] = {
    val ts = trees.forgetNE.flatMap(t => t.forMember(member))
    val optTs = NonEmpty.from(ts)
    optTs.map(Recipients(_))
  }

  override def toProtoV0: v0.Recipients = {
    val protoTrees = trees.map(_.toProtoV0)
    new v0.Recipients(protoTrees.toList)
  }

  override def pretty: Pretty[Recipients.this.type] =
    prettyOfClass(param("Recipient trees", _.trees.toList))

  def asSingleGroup: Option[NonEmpty[Set[Member]]] = {
    trees match {
      case Seq(RecipientsTree(group, Seq())) => Some(group)
      case _ => None
    }
  }

  /** Members that appear at the leaf of the BCC tree. For example, the informees of a view are leaf members of the
    * view message.
    */
  lazy val leafMembers: NonEmpty[Set[Member]] =
    trees.toNEF.reduceLeftTo(_.leafMembers)(_ ++ _.leafMembers)

}

object Recipients {

  def fromProtoV0(proto: v0.Recipients): ParsingResult[Recipients] = {
    for {
      trees <- proto.recipientsTree.traverse(t => RecipientsTree.fromProtoV0(t))
      recipients <- NonEmpty
        .from(trees)
        .toRight(
          ProtoDeserializationError.ValueConversionError(
            "RecipientsTree.recipients",
            s"RecipientsTree.recipients must be non-empty",
          )
        )
    } yield Recipients(recipients)
  }

  /** Create a [[com.digitalasset.canton.sequencing.protocol.Recipients]] representing a group of
    * members that "see" each other.
    */
  def cc(first: Member, others: Member*): Recipients =
    Recipients(NonEmpty(Seq, RecipientsTree.leaf(NonEmpty(Set, first, others: _*))))

  /** Create a [[com.digitalasset.canton.sequencing.protocol.Recipients]] representing independent groups of members
    * that do not "see" each other.
    */
  def groups(groups: NonEmpty[Seq[NonEmpty[Set[Member]]]]): Recipients =
    Recipients(groups.map(group => RecipientsTree.leaf(group)))

  def ofSet[T <: Member](set: Set[T]): Option[Recipients] = {
    val members = set.toList
    NonEmpty.from(members).map(list => Recipients.cc(list.head1, list.tail1: _*))
  }

}
