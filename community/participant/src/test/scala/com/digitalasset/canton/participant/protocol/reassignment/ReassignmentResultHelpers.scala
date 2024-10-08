// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

object ReassignmentResultHelpers {

  def unassignmentResult(
      sourceDomain: Source[DomainId],
      cryptoSnapshot: SyncCryptoApi,
      participantId: ParticipantId,
  )(implicit traceContext: TraceContext): DeliveredUnassignmentResult = {
    val protocolVersion = BaseTest.testedProtocolVersion

    implicit val ec: ExecutionContext = DirectExecutionContext(
      NamedLoggerFactory("test-area", "reassignment").getLogger(ReassignmentResultHelpers.getClass)
    )

    val result =
      ConfirmationResultMessage.create(
        sourceDomain.unwrap,
        ViewType.UnassignmentViewType,
        RequestId(CantonTimestamp.Epoch),
        TestHash.dummyRootHash,
        Verdict.Approve(protocolVersion),
        Set(),
        protocolVersion,
      )
    val signedResult: SignedProtocolMessage[ConfirmationResultMessage] =
      Await
        .result(
          SignedProtocolMessage.trySignAndCreate(result, cryptoSnapshot, protocolVersion),
          10.seconds,
        )
        .onShutdown(sys.error("aborted due to shutdown"))
    val batch: Batch[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] =
      Batch.of(protocolVersion, (signedResult, Recipients.cc(participantId)))
    val deliver: Deliver[OpenEnvelope[SignedProtocolMessage[ConfirmationResultMessage]]] =
      Deliver.create(
        SequencerCounter(0),
        CantonTimestamp.Epoch,
        sourceDomain.unwrap,
        Some(MessageId.tryCreate("msg-0")),
        batch,
        None,
        protocolVersion,
        Option.empty[TrafficReceipt],
      )
    val signature =
      Await
        .result(cryptoSnapshot.sign(TestHash.digest("dummySignature")).value, 10.seconds)
        .onShutdown(sys.error("aborted due to shutdown"))
        .valueOr(err => throw new RuntimeException(err.toString))
    val signedContent = SignedContent(
      deliver,
      signature,
      None,
      BaseTest.testedProtocolVersion,
    )

    DeliveredUnassignmentResult(signedContent)
  }

  def assignmentResult(targetDomain: Target[DomainId]): ConfirmationResultMessage =
    ConfirmationResultMessage.create(
      targetDomain.unwrap,
      ViewType.AssignmentViewType,
      RequestId(CantonTimestamp.Epoch),
      TestHash.dummyRootHash,
      Verdict.Approve(BaseTest.testedProtocolVersion),
      Set(),
      BaseTest.testedProtocolVersion,
    )
}
