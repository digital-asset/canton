// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.{SequencerCounter, checked}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.protocol.messages.{LocalReject, MediatorResponse}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.{MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import io.functionmeta.functionFullName

import scala.concurrent.ExecutionContext

class BadRootHashMessagesRequestProcessor(
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    participantId: ParticipantId,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends AbstractMessageProcessor(
      ephemeral,
      crypto,
      sequencerClient,
    ) {

  /** Immediately moves the request to Confirmed and
    * register a timeout handler at the decision time with the request tracker
    * to cover the case that the mediator does not send a mediator result.
    */
  def handleBadRequestWithExpectedMalformedMediatorRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      mediatorId: MediatorId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingF(functionFullName) {
      crypto.awaitIpsSnapshot(timestamp).flatMap(_.isMediatorActive(mediatorId)).flatMap {
        case true =>
          prepareForMediatorResultOfBadRequest(requestCounter, sequencerCounter, timestamp)
        case false =>
          // If the mediator is not active, then it will not send a result,
          // so we can finish off the request immediately
          invalidRequest(requestCounter, sequencerCounter, timestamp)
      }
    }

  /** Immediately moves the request to Confirmed and registers a timeout handler at the decision time with the request tracker.
    * Also sends a [[com.digitalasset.canton.protocol.messages.LocalReject.Malformed]]
    * for the given [[com.digitalasset.canton.protocol.RootHash]] with the given `rejectionReason`.
    */
  def sendRejectionAndExpectMediatorResult(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      rootHash: RootHash,
      mediatorId: MediatorId,
      rejectionReason: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingF(functionFullName) {
      val domainId = sequencerClient.domainId
      for {
        _ <- prepareForMediatorResultOfBadRequest(requestCounter, sequencerCounter, timestamp)
        snapshot <- crypto.snapshot(timestamp)
        requestId = RequestId(timestamp)
        rejection = checked(
          MediatorResponse.tryCreate(
            requestId = requestId,
            sender = participantId,
            viewHash = None,
            localVerdict = LocalReject.MalformedRejects.BadRootHashMessages.Reject(rejectionReason),
            rootHash = Some(rootHash),
            confirmingParties = Set.empty,
            domainId = domainId,
          )
        )
        signedRejection <- signResponse(snapshot, rejection)
        _ <- sendResponses(
          requestId,
          requestCounter,
          Seq(signedRejection -> Recipients.cc(mediatorId)),
        )
          .valueOr(
            // This is a best-effort response anyway, so we merely log the failure and continue
            error =>
              logger.warn(show"Failed to send best-effort rejection of malformed request: $error")
          )
      } yield ()
    }
}
