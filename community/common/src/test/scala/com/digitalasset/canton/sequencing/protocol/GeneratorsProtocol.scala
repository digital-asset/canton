// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.{GeneratorsVersion, ProtocolVersion}
import com.digitalasset.canton.{Generators, SequencerCounter}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsProtocol {
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.Generators.*

  implicit val acknowledgeRequestArb: Arbitrary[AcknowledgeRequest] = Arbitrary(for {
    protocolVersion <- representativeProtocolVersionGen(AcknowledgeRequest)
    ts <- implicitly[Arbitrary[CantonTimestamp]].arbitrary
    member <- implicitly[Arbitrary[Member]].arbitrary
  } yield AcknowledgeRequest(member, ts, protocolVersion.representative))

  implicit val aggregationRuleArb: Arbitrary[AggregationRule] = Arbitrary(for {
    rpv <- GeneratorsVersion.representativeProtocolVersionGen[AggregationRule](AggregationRule)
    threshold <- implicitly[Arbitrary[PositiveInt]].arbitrary
    eligibleMembers <- Generators.nonEmptyListGen[Member]
  } yield AggregationRule(eligibleMembers, threshold)(rpv))

  implicit val groupRecipientArb: Arbitrary[GroupRecipient] = genArbitrary
  implicit val recipientArb: Arbitrary[Recipient] = genArbitrary
  implicit val memberRecipientArb: Arbitrary[MemberRecipient] = genArbitrary

  private def recipientsTreeGen(
      recipientArb: Arbitrary[Recipient]
  )(depth: Int): Gen[RecipientsTree] = {
    val maxBreadth = 5
    val recipientGroupGen = nonEmptySetGen(recipientArb)

    if (depth == 0) {
      recipientGroupGen.map(RecipientsTree(_, Nil))
    } else {
      for {
        children <- Gen.listOfN(maxBreadth, recipientsTreeGen(recipientArb)(depth - 1))
        recipientGroup <- recipientGroupGen
      } yield RecipientsTree(recipientGroup, children)
    }
  }

  def recipientsArb(protocolVersion: ProtocolVersion): Arbitrary[Recipients] = {

    // For pv < ClosedEnvelope.groupAddressesSupportedSince, the recipients should contain only members
    val protocolVersionDependentRecipientGen =
      if (protocolVersion < ClosedEnvelope.groupAddressesSupportedSince.representative) {
        implicitly[Arbitrary[MemberRecipient]].arbitrary
      } else
        implicitly[Arbitrary[Recipient]].arbitrary

    Arbitrary(for {
      depths <- nonEmptyListGen(Arbitrary(Gen.choose(0, 3)))
      trees <- Gen.sequence[List[RecipientsTree], RecipientsTree](
        depths.forgetNE.map(recipientsTreeGen(Arbitrary(protocolVersionDependentRecipientGen)))
      )
    } yield Recipients(NonEmptyUtil.fromUnsafe(trees)))
  }

  implicit val closedEnvelopeArb: Arbitrary[ClosedEnvelope] = Arbitrary(for {
    bytes <- implicitly[Arbitrary[ByteString]].arbitrary

    protocolVersion <- representativeProtocolVersionGen(ClosedEnvelope)
    signatures <- defaultValueGen(protocolVersion, ClosedEnvelope.defaultSignaturesUntil)(
      Arbitrary(Gen.listOfN(5, signatureArb.arbitrary))
    )

    recipients <- recipientsArb(protocolVersion.representative).arbitrary

  } yield ClosedEnvelope.tryCreate(bytes, recipients, signatures, protocolVersion.representative))

  implicit val mediatorsOfDomainArb: Arbitrary[MediatorsOfDomain] = Arbitrary(
    implicitly[Arbitrary[NonNegativeInt]].arbitrary.map(MediatorsOfDomain(_))
  )

  implicit val messageIdArb: Arbitrary[MessageId] = Arbitrary(
    Generators.lengthLimitedStringGen(String73).map(s => MessageId.tryCreate(s.str))
  )

  def deliverGen[Env <: Envelope[_]](
      domainId: DomainId,
      batch: Batch[Env],
      protocolVersion: ProtocolVersion,
  ): Gen[Deliver[Env]] = for {
    timestamp <- implicitly[Arbitrary[CantonTimestamp]].arbitrary
    counter <- implicitly[Arbitrary[SequencerCounter]].arbitrary
    messageIdO <- Gen.option(implicitly[Arbitrary[MessageId]].arbitrary)
  } yield Deliver.create(
    counter,
    timestamp,
    domainId,
    messageIdO,
    batch,
    protocolVersion,
  )
}
