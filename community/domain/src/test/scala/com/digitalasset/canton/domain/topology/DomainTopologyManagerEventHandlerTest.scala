// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.topology.store.InMemoryRegisterTopologyTransactionResponseStore
import com.digitalasset.canton.protocol.messages.{
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
}
import com.digitalasset.canton.sequencing.client.{SendAsyncClientError, SendCallback, SendResult}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class DomainTopologyManagerEventHandlerTest extends AsyncWordSpec with BaseTest with MockitoSugar {
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
  private val domainIdentityServiceResult =
    RegisterTopologyTransactionResponse.Result(
      signedIdentityTransaction.uniquePath.toProtoPrimitive,
      RegisterTopologyTransactionResponse.State.Accepted,
    )
  private val response =
    RegisterTopologyTransactionResponse(
      participantId,
      participantId,
      requestId,
      List(domainIdentityServiceResult),
      domainId,
    )(RegisterTopologyTransactionResponse.protocolVersionRepresentativeFor(defaultProtocolVersion))

  "DomainTopologyManagerEventHandler" should {
    "handle RegisterTopologyTransactionRequests and send resulting RegisterTopologyTransactionResponse back" in {
      val store = new InMemoryRegisterTopologyTransactionResponseStore()

      val sut = {

        val requestHandler =
          mock[DomainTopologyManagerRequestService.Handler]
        when(
          requestHandler.newRequest(
            any[Member],
            any[ParticipantId],
            any[List[SignedTopologyTransaction[TopologyChangeOp]]],
          )(any[TraceContext])
        )
          .thenReturn(Future.successful(List(domainIdentityServiceResult)))

        val sequencerSendResponse = mock[
          (
              OpenEnvelope[RegisterTopologyTransactionResponse],
              SendCallback,
          ) => EitherT[Future, SendAsyncClientError, Unit]
        ]
        when(
          sequencerSendResponse.apply(
            eqTo(
              OpenEnvelope(response, Recipients.cc(response.requestedBy), defaultProtocolVersion)
            ),
            any[SendCallback],
          )
        )
          .thenAnswer(
            (_: OpenEnvelope[RegisterTopologyTransactionResponse], callback: SendCallback) => {
              callback.apply(SendResult.Success(null))
              EitherT.rightT[Future, SendAsyncClientError](())
            }
          )

        new DomainTopologyManagerEventHandler(
          store,
          requestHandler,
          sequencerSendResponse,
          defaultProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

      val result = {
        val batch =
          Batch(
            List(
              OpenEnvelope(
                request,
                Recipients.cc(DomainTopologyManagerId(response.domainId)),
                defaultProtocolVersion,
              )
            ),
            defaultProtocolVersion,
          )
        sut.apply(
          Traced(
            Seq(
              Traced(
                Deliver.create(
                  0,
                  CantonTimestamp.MinValue,
                  domainId,
                  Some(MessageId.tryCreate("messageId")),
                  batch,
                  defaultProtocolVersion,
                )
              )
            )
          )
        )
      }

      for {
        asyncResult <- result.onShutdown(fail())
        _ <- asyncResult.unwrap.onShutdown(fail())
        response <- store.getResponse(requestId)
      } yield response.isCompleted shouldBe true
    }
  }
}
