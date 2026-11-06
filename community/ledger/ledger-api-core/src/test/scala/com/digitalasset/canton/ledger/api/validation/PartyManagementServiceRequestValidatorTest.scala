// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.AllocateExternalPartyRequest.SignedTransaction
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocatePartyRequest,
  GenerateExternalPartyTopologyRequest,
  GetPartiesRequest,
  PartyDetails,
  UpdatePartyDetailsRequest,
}
import com.daml.ledger.api.v2.crypto.{Signature, SigningPublicKey}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.{
  AllocateExternalPartyRequestSignedTransactionSizeConfig,
  AllocateExternalPartyRequestSizeConfig,
  GenerateExternalPartyTopologyRequestSizeConfig,
  GetPartiesRequestSizeConfig,
  ObjectMetaSizeConfig,
  UpdatePartyDetailsRequestSizeConfig,
}
import com.digitalasset.canton.ledger.api.sizeconfig.v2.{
  SignatureSizeConfig,
  SigningPublicKeySizeConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.wordspec.AnyWordSpec

class PartyManagementServiceRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  "validating the size limits of the party management requests" should {
    val maxParties = 2
    val maxAnnotations = 3
    val maxOnboardingTransactions = 4
    val maxMultiHashSignatures = 5
    val maxTransactionBytes = 6
    val maxTransactionSignatures = 7
    val maxSignatureBytes = 8
    val maxKeyDataBytes = 9
    val maxOtherConfirmingParticipantUids = 10
    val maxObservingParticipantUids = 11
    val maxUpdateMaskBytes = 20

    val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
      publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
        signature =
          SignatureSizeConfig(signatureSizeLimit = PositiveInt.tryCreate(maxSignatureBytes)),
        signingPublicKey =
          SigningPublicKeySizeConfig(keyDataSizeLimit = PositiveInt.tryCreate(maxKeyDataBytes)),
      ),
      adminLapiSizeConfig = sizeLimitsConfig.adminLapiSizeConfig.copy(
        getPartiesRequest =
          GetPartiesRequestSizeConfig(partiesSizeLimit = PositiveInt.tryCreate(maxParties)),
        objectMeta =
          ObjectMetaSizeConfig(annotationsSizeLimit = PositiveInt.tryCreate(maxAnnotations)),
        allocateExternalPartyRequest = AllocateExternalPartyRequestSizeConfig(
          onboardingTransactionsSizeLimit = PositiveInt.tryCreate(maxOnboardingTransactions),
          multiHashSignaturesSizeLimit = PositiveInt.tryCreate(maxMultiHashSignatures),
        ),
        allocateExternalPartyRequestSignedTransaction =
          AllocateExternalPartyRequestSignedTransactionSizeConfig(
            transactionSizeLimit = PositiveInt.tryCreate(maxTransactionBytes),
            signaturesSizeLimit = PositiveInt.tryCreate(maxTransactionSignatures),
          ),
        generateExternalPartyTopologyRequest = GenerateExternalPartyTopologyRequestSizeConfig(
          otherConfirmingParticipantUidsSizeLimit =
            PositiveInt.tryCreate(maxOtherConfirmingParticipantUids),
          observingParticipantUidsSizeLimit = PositiveInt.tryCreate(maxObservingParticipantUids),
        ),
        updatePartyDetailsRequest = UpdatePartyDetailsRequestSizeConfig(
          updateMaskSizeLimit = PositiveInt.tryCreate(maxUpdateMaskBytes)
        ),
      ),
    )

    def objectMeta(numberOfAnnotations: Int): ObjectMeta =
      ObjectMeta(
        resourceVersion = "",
        annotations = (1 to numberOfAnnotations).map(i => s"key$i" -> s"value$i").toMap,
      )

    def signature(numberOfBytes: Int): Signature =
      Signature.defaultInstance.copy(signature = bytes(numberOfBytes))

    def uids(numberOfUids: Int): Seq[String] = (1 to numberOfUids).map(i => s"participant$i::id")

    cardinalityLimit("parties", maxParties)(numberOfParties =>
      PartyManagementServiceRequestValidator.validateGetPartiesRequest(
        GetPartiesRequest.defaultInstance.copy(parties = parties(numberOfParties))
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("annotations", maxAnnotations, limitName = "limit of a party allocation")(
      numberOfAnnotations =>
        PartyManagementServiceRequestValidator.validateAllocatePartyRequest(
          AllocatePartyRequest.defaultInstance.copy(localMetadata =
            Some(objectMeta(numberOfAnnotations))
          )
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("onboarding_transactions", maxOnboardingTransactions)(numberOfTransactions =>
      PartyManagementServiceRequestValidator.validateAllocateExternalPartyRequest(
        AllocateExternalPartyRequest.defaultInstance.copy(onboardingTransactions =
          Seq.fill(numberOfTransactions)(SignedTransaction.defaultInstance)
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("multi_hash_signatures", maxMultiHashSignatures)(numberOfSignatures =>
      PartyManagementServiceRequestValidator.validateAllocateExternalPartyRequest(
        AllocateExternalPartyRequest.defaultInstance.copy(multiHashSignatures =
          Seq.fill(numberOfSignatures)(signature(1))
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    byteSizeLimit("transaction", maxTransactionBytes)(numberOfBytes =>
      PartyManagementServiceRequestValidator.validateAllocateExternalPartyRequest(
        AllocateExternalPartyRequest.defaultInstance.copy(onboardingTransactions =
          Seq(SignedTransaction.defaultInstance.copy(transaction = bytes(numberOfBytes)))
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("signatures", maxTransactionSignatures)(numberOfSignatures =>
      PartyManagementServiceRequestValidator.validateAllocateExternalPartyRequest(
        AllocateExternalPartyRequest.defaultInstance.copy(onboardingTransactions =
          Seq(
            SignedTransaction.defaultInstance
              .copy(signatures = Seq.fill(numberOfSignatures)(signature(1)))
          )
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    byteSizeLimit("signature", maxSignatureBytes, limitName = "limit of a transaction signature")(
      numberOfBytes =>
        PartyManagementServiceRequestValidator.validateAllocateExternalPartyRequest(
          AllocateExternalPartyRequest.defaultInstance.copy(onboardingTransactions =
            Seq(SignedTransaction.defaultInstance.copy(signatures = Seq(signature(numberOfBytes))))
          )
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    byteSizeLimit("signature", maxSignatureBytes, limitName = "limit of a multi hash signature")(
      numberOfBytes =>
        PartyManagementServiceRequestValidator.validateAllocateExternalPartyRequest(
          AllocateExternalPartyRequest.defaultInstance.copy(multiHashSignatures =
            Seq(signature(numberOfBytes))
          )
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    byteSizeLimit("key_data", maxKeyDataBytes)(numberOfBytes =>
      PartyManagementServiceRequestValidator.validateGenerateExternalPartyTopologyRequest(
        GenerateExternalPartyTopologyRequest.defaultInstance.copy(publicKey =
          Some(SigningPublicKey.defaultInstance.copy(keyData = bytes(numberOfBytes)))
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("other_confirming_participant_uids", maxOtherConfirmingParticipantUids)(
      numberOfUids =>
        PartyManagementServiceRequestValidator.validateGenerateExternalPartyTopologyRequest(
          GenerateExternalPartyTopologyRequest.defaultInstance.copy(otherConfirmingParticipantUids =
            uids(numberOfUids)
          )
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("observing_participant_uids", maxObservingParticipantUids)(numberOfUids =>
      PartyManagementServiceRequestValidator.validateGenerateExternalPartyTopologyRequest(
        GenerateExternalPartyTopologyRequest.defaultInstance.copy(observingParticipantUids =
          uids(numberOfUids)
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("annotations", maxAnnotations, limitName = "limit of a party details update")(
      numberOfAnnotations =>
        PartyManagementServiceRequestValidator.validateUpdatePartyDetailsRequest(
          UpdatePartyDetailsRequest(
            partyDetails = Some(
              PartyDetails.defaultInstance
                .copy(party = party, localMetadata = Some(objectMeta(numberOfAnnotations)))
            ),
            updateMask = Some(FieldMask(Seq("local_metadata"))),
          )
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    serializedSizeLimit("update_mask", maxUpdateMaskBytes)(numberOfBytes =>
      PartyManagementServiceRequestValidator.validateUpdatePartyDetailsRequest(
        UpdatePartyDetailsRequest(
          partyDetails = Some(PartyDetails.defaultInstance.copy(party = party)),
          updateMask = Some(fieldMask(numberOfBytes)),
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )
  }
}
