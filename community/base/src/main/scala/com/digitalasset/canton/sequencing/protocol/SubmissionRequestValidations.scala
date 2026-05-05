// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

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
            .map(_.eligibleSenders.toSet incl submission.sender)
            // This is backwards compatible with pv34 as this can only fail with the aggregation
            // rules introduced into pv35.
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
      // TODO(#19476): Why we don't check group recipients here?
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
  object MemberCheckError {

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
