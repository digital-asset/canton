// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocatePartyRequest,
  GenerateExternalPartyTopologyRequest,
  GetPartiesRequest,
  UpdatePartyDetailsRequest,
}
import com.digitalasset.canton.ledger.api.PartyDetails
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.user.ObjectMeta
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException

object PartyManagementServiceRequestValidator {

  def validateGetPartiesRequest(request: GetPartiesRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    validateCardinality(
      values = request.parties,
      fieldName = "parties",
      limit = sizeLimitsConfig.adminLapiSizeConfig.getPartiesRequest.partiesSizeLimit,
    )

  def validateAllocatePartyRequest(request: AllocatePartyRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    request.localMetadata.traverse_(validateObjectMetaSizeLimits)

  def validateAllocateExternalPartyRequest(request: AllocateExternalPartyRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- validateCardinality(
        values = request.onboardingTransactions,
        fieldName = "onboarding_transactions",
        limit =
          sizeLimitsConfig.adminLapiSizeConfig.allocateExternalPartyRequest.onboardingTransactionsSizeLimit,
      )
      _ <- validateCardinality(
        values = request.multiHashSignatures,
        fieldName = "multi_hash_signatures",
        limit =
          sizeLimitsConfig.adminLapiSizeConfig.allocateExternalPartyRequest.multiHashSignaturesSizeLimit,
      )
      _ <- request.onboardingTransactions.traverse_(signedTransaction =>
        for {
          _ <- validateByteSizeLimit(
            bytes = signedTransaction.transaction,
            fieldName = "transaction",
            limit =
              sizeLimitsConfig.adminLapiSizeConfig.allocateExternalPartyRequestSignedTransaction.transactionSizeLimit,
          )
          _ <- validateCardinality(
            values = signedTransaction.signatures,
            fieldName = "signatures",
            limit =
              sizeLimitsConfig.adminLapiSizeConfig.allocateExternalPartyRequestSignedTransaction.signaturesSizeLimit,
          )
          _ <- validateSignaturesByteSizeLimits(signedTransaction.signatures)
        } yield ()
      )
      _ <- validateSignaturesByteSizeLimits(request.multiHashSignatures)
    } yield ()

  def validateGenerateExternalPartyTopologyRequest(request: GenerateExternalPartyTopologyRequest)(
      implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- request.publicKey.traverse(publicKey =>
        validateByteSizeLimit(
          bytes = publicKey.keyData,
          fieldName = "key_data",
          limit = sizeLimitsConfig.publicLapiSizeConfig.signingPublicKey.keyDataSizeLimit,
        )
      )
      _ <- validateCardinality(
        values = request.otherConfirmingParticipantUids,
        fieldName = "other_confirming_participant_uids",
        limit =
          sizeLimitsConfig.adminLapiSizeConfig.generateExternalPartyTopologyRequest.otherConfirmingParticipantUidsSizeLimit,
      )
      _ <- validateCardinality(
        values = request.observingParticipantUids,
        fieldName = "observing_participant_uids",
        limit =
          sizeLimitsConfig.adminLapiSizeConfig.generateExternalPartyTopologyRequest.observingParticipantUidsSizeLimit,
      )
    } yield ()

  def validateUpdatePartyDetailsRequest(request: UpdatePartyDetailsRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, (PartyDetails, FieldMask)] =
    for {
      partyDetails <- requirePresence(
        request.partyDetails,
        "party_details",
      )
      party <- requireParty(partyDetails.party)
      metadata = partyDetails.localMetadata.getOrElse(
        ProtoObjectMeta(
          resourceVersion = "",
          annotations = Map.empty,
        )
      )
      resourceVersionNumberO <- optionalString(metadata.resourceVersion)(
        requireResourceVersion(
          _,
          "party_details.local_metadata",
        )
      )
      _ <- validateObjectMetaSizeLimits(metadata)
      annotations <- verifyMetadataAnnotations(
        annotations = metadata.annotations,
        allowEmptyValues = true,
        fieldName = "party_details.local_metadata.annotations",
      )
      updateMask <- requireUpdateMask(
        updateMask = request.updateMask,
        limit = sizeLimitsConfig.adminLapiSizeConfig.updatePartyDetailsRequest.updateMaskSizeLimit,
      )
      identityProviderId <- optionalIdentityProviderId(
        partyDetails.identityProviderId,
        "identity_provider_id",
      )
      partyRecord = PartyDetails(
        party = party,
        isLocal = partyDetails.isLocal,
        metadata = ObjectMeta(
          resourceVersionO = resourceVersionNumberO,
          annotations = annotations,
        ),
        identityProviderId = identityProviderId,
      )
    } yield (partyRecord, updateMask)
}
