// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.EitherT
import cats.syntax.foldable.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.{Recipients, RecipientsTree}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient

import scala.concurrent.{ExecutionContext, Future}

/** Encodes the hierarchy of the witnesses of a view.
  *
  * By convention, the order is: the view's informees are at the head of the list, then the parent's views informees,
  * then the grandparent's, etc.
  */
final case class Witnesses(unwrap: Seq[Set[Informee]]) {
  import Witnesses.*

  def prepend(informees: Set[Informee]) = Witnesses(informees +: unwrap)

  /** Derive a recipient tree that mirrors the given hierarchy of witnesses. */
  def toRecipients(
      topology: PartyTopologySnapshotClient
  )(implicit ec: ExecutionContext): EitherT[Future, InvalidWitnesses, Recipients] =
    for {
      recipientsList <- unwrap.foldLeftM(Seq.empty[RecipientsTree]) { (children, informees) =>
        for {
          informeeParticipants <- topology
            .activeParticipantsOfAll(informees.map(_.party).toList)
            .leftMap(missing =>
              InvalidWitnesses(s"Found no active participants for informees: $missing")
            )
          informeeParticipantSet <- EitherT.fromOption[Future](
            NonEmpty.from(informeeParticipants.toSet[Member]),
            InvalidWitnesses(s"Empty set of witnesses given"),
          )
        } yield Seq(
          // TODO(#12382): support group addressing for informees
          RecipientsTree(informeeParticipantSet.map(RecipientsTree.MemberRecipient), children)
        )
      }
      // TODO(error handling) Why is it safe to assume that the recipient list is non-empty?
      //  It will be empty if `unwrap` is empty.
      recipients = Recipients(NonEmptyUtil.fromUnsafe(recipientsList))
    } yield recipients

  def flatten: Set[Informee] = unwrap.foldLeft(Set.empty[Informee])(_ union _)

}

case object Witnesses {
  lazy val empty: Witnesses = Witnesses(Seq.empty)

  final case class InvalidWitnesses(message: String) extends PrettyPrinting {
    override def pretty: Pretty[InvalidWitnesses] = prettyOfClass(unnamedParam(_.message.unquoted))
  }
}
