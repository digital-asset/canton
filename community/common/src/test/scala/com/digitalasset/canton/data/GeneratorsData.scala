// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{Salt, TestHash}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.protocol.{ConfirmationPolicy, SourceDomainId, TargetDomainId}
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import java.time.Duration

object GeneratorsData {
  import com.digitalasset.canton.Generators.*
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
      confirmationPolicy <- implicitly[Arbitrary[ConfirmationPolicy]].arbitrary
      domainId <- implicitly[Arbitrary[DomainId]].arbitrary

      mediatorRef <- implicitly[Arbitrary[MediatorRef]].arbitrary
      singleMediatorRef <- implicitly[Arbitrary[MediatorRef.Single]].arbitrary

      salt <- implicitly[Arbitrary[Salt]].arbitrary
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
      ledgerTime <- implicitly[Arbitrary[CantonTimestamp]].arbitrary
      submissionTime <- implicitly[Arbitrary[CantonTimestamp]].arbitrary
      workflowIdO <- Gen.option(workflowIdArb.arbitrary)
      salt <- implicitly[Arbitrary[Salt]].arbitrary

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
      submitterParticipant <- implicitly[Arbitrary[ParticipantId]].arbitrary
      salt <- implicitly[Arbitrary[Salt]].arbitrary
      submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
      dedupPeriod <- implicitly[Arbitrary[DeduplicationPeriod]].arbitrary
      maxSequencingTime <- implicitly[Arbitrary[CantonTimestamp]].arbitrary
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

  implicit val transferInCommonData: Arbitrary[TransferInCommonData] = Arbitrary(
    for {
      salt <- implicitly[Arbitrary[Salt]].arbitrary
      targetDomain <- implicitly[Arbitrary[TargetDomainId]].arbitrary

      targetMediator <- implicitly[Arbitrary[MediatorRef]].arbitrary
      singleTargetMediator <- implicitly[Arbitrary[MediatorRef.Single]].arbitrary

      stakeholders <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)
      uuid <- Gen.uuid
      targetProtocolVersion <- implicitly[Arbitrary[TargetProtocolVersion]].arbitrary

      updatedTargetMediator =
        if (!TransferCommonData.isGroupMediatorSupported(targetProtocolVersion.v))
          singleTargetMediator
        else targetMediator

      hashOps = TestHash // Not used for serialization

    } yield TransferInCommonData
      .create(hashOps)(
        salt,
        targetDomain,
        updatedTargetMediator,
        stakeholders,
        uuid,
        targetProtocolVersion,
      )
      .value
  )

  implicit val transferOutCommonData: Arbitrary[TransferOutCommonData] = Arbitrary(
    for {
      salt <- implicitly[Arbitrary[Salt]].arbitrary
      sourceDomain <- implicitly[Arbitrary[SourceDomainId]].arbitrary

      sourceMediator <- implicitly[Arbitrary[MediatorRef]].arbitrary
      singleSourceMediator <- implicitly[Arbitrary[MediatorRef.Single]].arbitrary

      stakeholders <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)
      adminParties <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)
      uuid <- Gen.uuid
      sourceProtocolVersion <- implicitly[Arbitrary[SourceProtocolVersion]].arbitrary

      updatedSourceMediator =
        if (!TransferCommonData.isGroupMediatorSupported(sourceProtocolVersion.v))
          singleSourceMediator
        else sourceMediator

      hashOps = TestHash // Not used for serialization

    } yield TransferOutCommonData
      .create(hashOps)(
        salt,
        sourceDomain,
        updatedSourceMediator,
        stakeholders,
        adminParties,
        uuid,
        sourceProtocolVersion,
      )
      .value
  )
}
