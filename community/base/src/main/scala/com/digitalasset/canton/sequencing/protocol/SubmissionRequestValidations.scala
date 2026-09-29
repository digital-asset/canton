// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

object SubmissionRequestValidations {
  def checkSenderAndRecipientsAreRegistered(
      submission: SubmissionRequest,
      snapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, MemberCheckError, Unit] = {

    val sendersET: EitherT[FutureUnlessShutdown, MemberCheckError, NonEmpty[Set[Member]]] =
      submission.aggregationRule
        .map(
          _.input
            .resolveToMembers(submission.sender, snapshot)
            .map(_ incl submission.sender)
            .leftMap(
              MemberCheckError.InvalidAggregationRule(_): MemberCheckError
            )
        )
        .getOrElse(
          EitherT.rightT[FutureUnlessShutdown, MemberCheckError](
            NonEmpty.mk(Set, submission.sender)
          )
        )
    val allRecipients = submission.batch.allMembers
    sendersET.flatMap { senders =>
      // We don't check for group members because group members are automatically added as members
      // by the sequencer.
      // This is because the notification of member changes will inform the SequencerRuntime
      // before it updates the cryptoApi (val executionOrder: Int = 1) which means
      // that a topology snapshot that delivers the updated member can only be accessed
      // after the member registration completed!
      // Therefore, we don't need to check for mediator and sequencer group members.
      val allMembers = allRecipients ++ senders
      EitherT {
        for {
          registeredMembers <- snapshot.areMembersKnown(allMembers)
        } yield {
          Either.cond(
            registeredMembers.sizeCompare(allMembers) == 0,
            (), {
              val unregisteredRecipients = allRecipients.diff(registeredMembers)
              val unregisteredSenders = senders.diff(registeredMembers)
              MemberCheckError.UnknownMembers(unregisteredRecipients, unregisteredSenders)
            },
          )
        }
      }
    }
  }

  /** A utility function to reject requests that try to send something to multiple mediators
    * (mediator groups). Mediators/groups are identified by their
    * [[com.digitalasset.canton.topology.MemberCode]]
    */
  def checkToAtMostOneMediator(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.batch.allMediatorRecipients.sizeIs <= 1

  sealed trait MemberCheckError {
    def toSequencerDeliverError: SequencerDeliverError
    def toSendAsyncClientError: SendAsyncClientError
  }
  private object MemberCheckError {

    final case class InvalidAggregationRule(str: String) extends MemberCheckError {
      override def toSequencerDeliverError: SequencerDeliverError =
        SequencerErrors.AggregateSubmissionInvalidRule(str)
      override def toSendAsyncClientError: SendAsyncClientError =
        SendAsyncClientError.RequestInvalid(s"Invalid aggregation rule $str")
    }

    final case class UnknownMembers(
        unregisteredRecipients: Set[Member],
        unregisteredSenders: Set[Member],
    ) extends MemberCheckError {
      override def toSequencerDeliverError: SequencerDeliverError =
        if (unregisteredRecipients.nonEmpty)
          SequencerErrors.UnknownRecipients(unregisteredRecipients.toSeq)
        else SequencerErrors.SenderUnknown(unregisteredSenders.toSeq)

      override def toSendAsyncClientError: SendAsyncClientError =
        SendAsyncClientError.RequestInvalid(
          s"Unregistered recipients: $unregisteredRecipients, unregistered senders: $unregisteredSenders"
        )
    }
  }
}
