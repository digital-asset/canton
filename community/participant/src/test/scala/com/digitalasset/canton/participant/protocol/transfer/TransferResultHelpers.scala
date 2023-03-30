// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.implicits.*
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

private[transfer] object TransferResultHelpers {

  def transferOutResult(
      sourceDomain: DomainId,
      cryptoSnapshot: SyncCryptoApi,
      participantId: ParticipantId,
  )(implicit traceContext: TraceContext): DeliveredTransferOutResult = {
    val protocolVersion = BaseTest.testedProtocolVersion

    implicit val ec: ExecutionContext = DirectExecutionContext(
      TracedLogger(
        NamedLoggerFactory("test-area", "transfer").getLogger(TransferResultHelpers.getClass)
      )
    )

    val result =
      TransferResult.create(
        RequestId(CantonTimestamp.Epoch),
        Set(),
        TransferOutDomainId(sourceDomain),
        Verdict.Approve(protocolVersion),
        protocolVersion,
      )
    val signedResult: SignedProtocolMessage[TransferOutResult] =
      Await.result(
        SignedProtocolMessage.tryCreate(result, cryptoSnapshot, protocolVersion),
        10.seconds,
      )
    val batch: Batch[OpenEnvelope[SignedProtocolMessage[TransferOutResult]]] =
      Batch.of(protocolVersion, (signedResult, Recipients.cc(participantId)))
    val deliver: Deliver[OpenEnvelope[SignedProtocolMessage[TransferOutResult]]] =
      Deliver.create(
        SequencerCounter(0),
        CantonTimestamp.Epoch,
        sourceDomain,
        Some(MessageId.tryCreate("msg-0")),
        batch,
        protocolVersion,
      )
    val signature =
      Await
        .result(cryptoSnapshot.sign(TestHash.digest("dummySignature")).value, 10.seconds)
        .valueOr(err => throw new RuntimeException(err.toString))
    val signedContent = SignedContent(
      deliver,
      signature,
      None,
      BaseTest.testedProtocolVersion,
    )

    val transferOutResult = DeliveredTransferOutResult(signedContent)
    transferOutResult
  }

  def transferInResult(targetDomain: DomainId): TransferInResult = TransferResult.create(
    RequestId(CantonTimestamp.Epoch),
    Set(),
    TransferInDomainId(targetDomain),
    Verdict.Approve(BaseTest.testedProtocolVersion),
    BaseTest.testedProtocolVersion,
  )
}
