// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{Salt, TestHash}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.protocol.ConfirmationPolicy
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration

object GeneratorsData {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*
  import org.scalatest.EitherValues.*

  private val tenYears: Duration = Duration.ofDays(365 * 10)

  implicit val cantonTimestampArb: Arbitrary[CantonTimestamp] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds * 1000 * 1000).map(CantonTimestamp.ofEpochMicro)
  )
  implicit val cantonTimestampSecondArb: Arbitrary[CantonTimestampSecond] = Arbitrary(
    Gen.choose(0, tenYears.getSeconds).map(CantonTimestampSecond.ofEpochSecond)
  )

  implicit val merklePathElementArg: Arbitrary[MerklePathElement] = genArbitrary

  implicit val commonMetadataArb: Arbitrary[CommonMetadata] = Arbitrary(
    for {
      confirmationPolicy <- Arbitrary.arbitrary[ConfirmationPolicy]
      domainId <- Arbitrary.arbitrary[DomainId]

      mediatorRef <- Arbitrary.arbitrary[MediatorRef]
      singleMediatorRef <- Arbitrary.arbitrary[MediatorRef.Single]

      salt <- Arbitrary.arbitrary[Salt]
      uuid <- Gen.uuid
      protocolVersion <- representativeProtocolVersionGen(CommonMetadata)

      updatedMediatorRef =
        if (CommonMetadata.shouldHaveSingleMediator(protocolVersion)) singleMediatorRef
        else mediatorRef

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata
      .create(hashOps, protocolVersion)(
        confirmationPolicy,
        domainId,
        updatedMediatorRef,
        salt,
        uuid,
      )
      .value
  )

  implicit val participantMetadataArb: Arbitrary[ParticipantMetadata] = Arbitrary(
    for {
      ledgerTime <- Arbitrary.arbitrary[CantonTimestamp]
      submissionTime <- Arbitrary.arbitrary[CantonTimestamp]
      workflowIdO <- Gen.option(workflowIdArb.arbitrary)
      salt <- Arbitrary.arbitrary[Salt]

      protocolVersion <- representativeProtocolVersionGen(ParticipantMetadata)

      hashOps = TestHash // Not used for serialization
    } yield ParticipantMetadata(hashOps)(
      ledgerTime,
      submissionTime,
      workflowIdO,
      salt,
      protocolVersion.representative,
    )
  )

  implicit val submitterMetadataArb: Arbitrary[SubmitterMetadata] = Arbitrary(
    for {
      actAs <- nonEmptySet(lfPartyIdArb).arbitrary
      applicationId <- applicationIdArb.arbitrary
      commandId <- commandIdArb.arbitrary
      submitterParticipant <- Arbitrary.arbitrary[ParticipantId]
      salt <- Arbitrary.arbitrary[Salt]
      submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
      dedupPeriod <- Arbitrary.arbitrary[DeduplicationPeriod]
      maxSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      hashOps = TestHash // Not used for serialization
      protocolVersion <- representativeProtocolVersionGen(SubmitterMetadata)
    } yield SubmitterMetadata(
      actAs,
      applicationId,
      commandId,
      submitterParticipant,
      salt,
      submissionId,
      dedupPeriod,
      maxSequencingTime,
      hashOps,
      protocolVersion.representative,
    )
  )

  implicit val viewCommonDataArb: Arbitrary[ViewCommonData] = Arbitrary(for {
    informees <- Gen.containerOf[Set, Informee](Arbitrary.arbitrary[Informee])
    threshold <- Arbitrary.arbitrary[NonNegativeInt]
    salt <- Arbitrary.arbitrary[Salt]

    /*
      In v0 (used for pv=3,4) the confirmation policy is passed as part of the context rather than
      being read from the proto. This break the identify test in SerializationDeserializationTest.
      Fixing the test tooling currently requires too much energy compared to the benefit so filtering
      out the old protocol versions.
     */
    pv <- Gen.oneOf(ProtocolVersion.supported.forgetNE.filterNot(_.v < 5))

    hashOps = TestHash // Not used for serialization
  } yield ViewCommonData.create(hashOps)(informees, threshold, salt, pv))
}
