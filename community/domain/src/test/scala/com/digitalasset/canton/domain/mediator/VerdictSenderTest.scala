// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, FullInformeeTree}
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, RequestId, TestDomainParameters}
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.transaction.{
  ParticipantAttributes,
  ParticipantPermission,
  TrustLevel,
}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}

class VerdictSenderTest extends BaseTestWordSpec with HasExecutionContext {

  lazy val domainId: DomainId = DefaultTestIdentities.domainId
  lazy val factory: ExampleTransactionFactory = new ExampleTransactionFactory()(domainId = domainId)
  lazy val mediatorId: MediatorId = factory.mediatorId
  lazy val submitter: LfPartyId = ExampleTransactionFactory.submitter
  lazy val signatory: LfPartyId = ExampleTransactionFactory.signatory
  lazy val observer: LfPartyId = ExampleTransactionFactory.observer
  lazy val participant: ParticipantId = ExampleTransactionFactory.submitterParticipant

  def mkSender(topology: TestingTopology): (DefaultVerdictSender, TestSequencerClientSend) = {
    val identityFactory = TestingIdentityFactory(
      topology,
      loggerFactory,
      TestDomainParameters.defaultDynamic,
    )
    val domainSyncCryptoApi: DomainSyncCryptoClient =
      identityFactory.forOwnerAndDomain(mediatorId, domainId)

    val sequencerSend = new TestSequencerClientSend
    val verdictSender = new DefaultVerdictSender(
      sequencerSend,
      domainSyncCryptoApi,
      testedProtocolVersion,
      loggerFactory,
    )
    (verdictSender, sequencerSend)
  }

  "VerdictSender" must {
    "request rejected when informee not hosted on active participant" in {
      lazy val fullInformeeTree: FullInformeeTree = factory.SingleFetch().fullInformeeTree
      val informeeMessage = new InformeeMessage(fullInformeeTree)(testedProtocolVersion)

      val topology = TestingTopology(
        Set(domainId),
        Map(
          submitter -> Map(
            participant -> ParticipantAttributes(
              ParticipantPermission.Confirmation,
              TrustLevel.Ordinary,
            )
          )
        ),
        Set(mediatorId),
      )

      val (verdictSender, sequencerSend) = mkSender(topology)

      verdictSender
        .sendResult(
          RequestId(CantonTimestamp.Epoch),
          informeeMessage,
          Verdict.Approve,
          CantonTimestamp.ofEpochSecond(100),
        )
        .futureValue

      val request = sequencerSend.requests.loneElement
      inside(request.batch.envelopes.loneElement.protocolMessage) {
        case SignedProtocolMessage(message: MediatorResult, _) =>
          message.verdict shouldBe MediatorReject.Topology.InformeesNotHostedOnActiveParticipants
            .Reject(observer)
      }
    }
  }
}
