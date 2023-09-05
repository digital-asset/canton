// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{GeneratorsCrypto, HashPurpose, Salt, TestHash}
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  SignedProtocolMessage,
  TransferResult,
  Verdict,
}
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfTemplateId,
  RequestId,
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, SignedContent}
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.ExecutionContext

object GeneratorsTransferData {
  import com.digitalasset.canton.Generators.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import org.scalatest.EitherValues.*

  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  private implicit val ec = ExecutionContext.global

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

  private def transferInSubmitterMetadataGen(
      protocolVersion: TargetProtocolVersion
  ): Gen[TransferSubmitterMetadata] =
    for {
      submitter <- implicitly[Arbitrary[LfPartyId]].arbitrary
      applicationId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.applicationIdDefaultValue,
        applicationIdArb.arbitrary.map(_.unwrap),
      )
      submittingParticipant <- defaultValueGen(
        protocolVersion.v,
        TransferInView.submittingParticipantDefaultValue,
        implicitly[Arbitrary[ParticipantId]].arbitrary.map(_.toLf),
      )
      commandId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.commandIdDefaultValue,
        commandIdArb.arbitrary.map(_.unwrap),
      )
      submissionId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.submissionIdDefaultValue,
        Gen.option(ledgerSubmissionIdArb.arbitrary),
      )
      workflowId <- defaultValueGen(
        protocolVersion.v,
        TransferInView.workflowIdDefaultValue,
        Gen.option(workflowIdArb.arbitrary.map(_.unwrap)),
      )
    } yield TransferSubmitterMetadata(
      submitter,
      applicationId,
      submittingParticipant,
      commandId,
      submissionId,
      workflowId,
    )

  private def transferOutSubmitterMetadataGen(
      protocolVersion: SourceProtocolVersion
  ): Gen[TransferSubmitterMetadata] =
    for {
      submitter <- implicitly[Arbitrary[LfPartyId]].arbitrary
      applicationId <- defaultValueGen(
        protocolVersion.v,
        TransferOutView.applicationIdDefaultValue,
        applicationIdArb.arbitrary.map(_.unwrap),
      )
      submittingParticipant <- defaultValueGen(
        protocolVersion.v,
        TransferOutView.submittingParticipantDefaultValue,
        implicitly[Arbitrary[ParticipantId]].arbitrary.map(_.toLf),
      )
      commandId <- defaultValueGen(
        protocolVersion.v,
        TransferOutView.commandIdDefaultValue,
        commandIdArb.arbitrary.map(_.unwrap),
      )
      submissionId <- defaultValueGen(
        protocolVersion.v,
        TransferOutView.submissionIdDefaultValue,
        Gen.option(ledgerSubmissionIdArb.arbitrary),
      )
      workflowId <- defaultValueGen(
        protocolVersion.v,
        TransferOutView.workflowIdDefaultValue,
        Gen.option(workflowIdArb.arbitrary.map(_.unwrap)),
      )
    } yield TransferSubmitterMetadata(
      submitter,
      applicationId,
      submittingParticipant,
      commandId,
      submissionId,
      workflowId,
    )

  private def deliveryTransferOutResultGen(
      contract: SerializableContract,
      sourceProtocolVersion: SourceProtocolVersion,
  ): Gen[DeliveredTransferOutResult] =
    for {
      sourceDomain <- implicitly[Arbitrary[SourceDomainId]].arbitrary

      requestId <- implicitly[Arbitrary[RequestId]].arbitrary
      protocolVersion = sourceProtocolVersion.v

      verdict = Verdict.Approve(protocolVersion)
      result = TransferResult.create(
        requestId,
        contract.metadata.stakeholders,
        sourceDomain,
        verdict,
        protocolVersion,
      )

      signedResult =
        SignedProtocolMessage.tryFrom(
          result,
          protocolVersion,
          GeneratorsCrypto.sign("TransferOutResult-mediator", HashPurpose.TransferResultSignature),
        )

      recipients <- recipientsArb(protocolVersion).arbitrary

      transferOutTimestamp <- implicitly[Arbitrary[CantonTimestamp]].arbitrary

      batch = Batch.of(protocolVersion, signedResult -> recipients)
      deliver <- deliverGen(sourceDomain.unwrap, batch, protocolVersion)
    } yield DeliveredTransferOutResult {
      SignedContent(
        deliver,
        sign("TransferOutResult-sequencer", HashPurpose.TransferResultSignature),
        Some(transferOutTimestamp),
        protocolVersion,
      )
    }

  implicit val transferInViewArb: Arbitrary[TransferInView] = Arbitrary(
    for {
      salt <- implicitly[Arbitrary[Salt]].arbitrary
      targetProtocolVersion <- implicitly[Arbitrary[TargetProtocolVersion]].arbitrary

      // Default sourceProtocolVersion for pv=3
      sourceProtocolVersion <- defaultValueGen(
        targetProtocolVersion.v,
        TransferInView.sourceProtocolVersionDefaultValue,
      )

      initialContract <- implicitly[Arbitrary[SerializableContract]].arbitrary
      // No salt for pv=3
      contract =
        if (targetProtocolVersion.v < ProtocolVersion.v4) initialContract.copy(contractSalt = None)
        else initialContract

      creatingTransactionId <- implicitly[Arbitrary[TransactionId]].arbitrary
      submitterMetadata <- transferInSubmitterMetadataGen(targetProtocolVersion)
      transferOutResultEvent <- deliveryTransferOutResultGen(contract, sourceProtocolVersion)
      transferCounter <- transferCounterOGen(targetProtocolVersion.v)

      hashOps = TestHash // Not used for serialization

    } yield TransferInView
      .create(hashOps)(
        salt,
        submitterMetadata,
        contract,
        creatingTransactionId,
        transferOutResultEvent,
        sourceProtocolVersion,
        targetProtocolVersion,
        transferCounter,
      )
      .value
  )

  implicit val transferOutViewArb: Arbitrary[TransferOutView] = Arbitrary(
    for {
      salt <- implicitly[Arbitrary[Salt]].arbitrary

      sourceProtocolVersion <- implicitly[Arbitrary[SourceProtocolVersion]].arbitrary

      // Default targetProtocolVersion for pv=3
      targetProtocolVersion <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.targetProtocolVersionDefaultValue,
      )

      submitterMetadata <- transferOutSubmitterMetadataGen(sourceProtocolVersion)

      contractId <- implicitly[Arbitrary[LfContractId]].arbitrary
      templateId <- defaultValueGen(
        sourceProtocolVersion.v,
        TransferOutView.templateIdDefaultValue,
      )(implicitly[Arbitrary[LfTemplateId]])

      targetDomain <- implicitly[Arbitrary[TargetDomainId]].arbitrary
      timeProof <- implicitly[Arbitrary[TimeProof]].arbitrary
      transferCounter <- transferCounterOGen(sourceProtocolVersion.v)

      hashOps = TestHash // Not used for serialization

    } yield TransferOutView
      .create(hashOps)(
        salt,
        submitterMetadata,
        contractId,
        templateId,
        targetDomain,
        timeProof,
        sourceProtocolVersion,
        targetProtocolVersion,
        transferCounter,
      )
      .value
  )

}
