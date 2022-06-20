// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain.grpc

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.participant.domain.ParticipantDomainTopologyService
import com.digitalasset.canton.protocol.messages.{
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
}
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.transaction.{
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyElementId,
  TopologyStateUpdate,
  TopologyStateUpdateElement,
}
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ParticipantDomainTopologyServiceTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {
  private val requestId = String255.tryCreate("requestId")
  private val domainId = DomainId.tryFromString("da::default")
  private val participantId: ParticipantId = ParticipantId("p1")

  private val signedIdentityTransaction = SignedTopologyTransaction(
    TopologyStateUpdate(
      TopologyChangeOp.Add,
      TopologyStateUpdateElement(
        TopologyElementId.tryCreate("submissionId"),
        OwnerToKeyMapping(participantId, SymbolicCrypto.signingPublicKey("keyId")),
      ),
    )(defaultProtocolVersion),
    SymbolicCrypto.signingPublicKey("keyId"),
    SymbolicCrypto.emptySignature,
  )(signedTransactionProtocolVersionRepresentative, None)
  private val request = RegisterTopologyTransactionRequest
    .create(
      participantId,
      participantId,
      requestId,
      List(signedIdentityTransaction),
      domainId,
      defaultProtocolVersion,
    )
    .headOption
    .value

  private val response =
    RegisterTopologyTransactionResponse(
      participantId,
      participantId,
      requestId,
      List(
        RegisterTopologyTransactionResponse.Result(
          signedIdentityTransaction.uniquePath.toProtoPrimitive,
          RegisterTopologyTransactionResponse.State.Accepted,
        )
      ),
      domainId,
      defaultProtocolVersion,
    )

  "ParticipantDomainTopologyService" should {
    val sendRequest =
      mock[
        (
            TraceContext,
            OpenEnvelope[RegisterTopologyTransactionRequest],
        ) => EitherT[Future, SendAsyncClientError, Unit]
      ]

    when(
      sendRequest.apply(
        eqTo(traceContext),
        eqTo(
          OpenEnvelope(
            request,
            Recipients.cc(DomainTopologyManagerId(domainId)),
            defaultProtocolVersion,
          )
        ),
      )
    )
      .thenReturn(EitherT.pure[Future, SendAsyncClientError](()))

    "send request to IDM and wait to process response" in {
      val sut = new ParticipantDomainTopologyService(
        domainId,
        sendRequest,
        defaultProtocolVersion,
        ProcessingTimeout(),
        loggerFactory,
      )

      val resultF = sut.registerTopologyTransaction(request).unwrap

      // after response is processed, the future will be completed
      sut.processor.apply(
        Traced(
          List(OpenEnvelope(response, Recipients.cc(response.requestedBy), defaultProtocolVersion))
        )
      )

      resultF.map(result => result shouldBe UnlessShutdown.Outcome(response))
    }
    "send request to IDM and handle closing before response arrives" in {
      val sut = new ParticipantDomainTopologyService(
        domainId,
        sendRequest,
        defaultProtocolVersion,
        ProcessingTimeout(),
        loggerFactory,
      )

      val resultF = sut.registerTopologyTransaction(request).unwrap

      sut.close()

      resultF.map(result => result shouldBe UnlessShutdown.AbortedDueToShutdown)
    }
  }
}
