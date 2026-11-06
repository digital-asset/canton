// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.foldable.*
import com.daml.ledger.api.v2.admin.user_management_service.{
  CreateUserRequest,
  GrantUserRightsRequest,
  RevokeUserRightsRequest,
  UpdateUserRequest,
}
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.user.{ObjectMeta, User}
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException

object UserManagementServiceRequestValidator {

  def validateCreateUserRequest(request: CreateUserRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- request.user
        .flatMap(_.metadata)
        .traverse_(validateObjectMetaSizeLimits)
      _ <- validateCardinality(
        values = request.rights,
        fieldName = "rights",
        limit = sizeLimitsConfig.adminLapiSizeConfig.createUserRequest.rightsSizeLimit,
      )
    } yield ()

  def validateGrantUserRightsRequest(request: GrantUserRightsRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    validateCardinality(
      values = request.rights,
      fieldName = "rights",
      limit = sizeLimitsConfig.adminLapiSizeConfig.grantUserRightsRequest.rightsSizeLimit,
    )

  def validateRevokeUserRightsRequest(request: RevokeUserRightsRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] =
    validateCardinality(
      values = request.rights,
      fieldName = "rights",
      limit = sizeLimitsConfig.adminLapiSizeConfig.revokeUserRightsRequest.rightsSizeLimit,
    )

  def validateUpdateUserRequest(request: UpdateUserRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, (User, FieldMask)] =
    for {
      pUser <- requirePresence(request.user, "user")
      pUserId <- requireUserId(pUser.id, "user.id")
      pMetadata = pUser.metadata.getOrElse(
        com.daml.ledger.api.v2.admin.object_meta.ObjectMeta(
          resourceVersion = "",
          annotations = Map.empty,
        )
      )
      pFieldMask <- requireUpdateMask(
        updateMask = request.updateMask,
        limit = sizeLimitsConfig.adminLapiSizeConfig.updateUserRequest.updateMaskSizeLimit,
      )
      pOptPrimaryParty <- optionalString(pUser.primaryParty)(requireParty)
      identityProviderId <- optionalIdentityProviderId(
        pUser.identityProviderId,
        "identity_provider_id",
      )
      pResourceVersion <- optionalString(pMetadata.resourceVersion)(
        requireResourceVersion(_, "user.metadata.resource_version")
      )
      _ <- validateObjectMetaSizeLimits(pMetadata)
      pAnnotations <- verifyMetadataAnnotations(
        annotations = pMetadata.annotations,
        allowEmptyValues = true,
        fieldName = "user.metadata.annotations",
      )
    } yield (
      User(
        id = pUserId,
        primaryParty = pOptPrimaryParty,
        isDeactivated = pUser.isDeactivated,
        metadata = ObjectMeta(
          resourceVersionO = pResourceVersion,
          annotations = pAnnotations,
        ),
        identityProviderId = identityProviderId,
        primaryPartyAuthentication = pUser.primaryPartyAuthentication,
      ),
      pFieldMask,
    )
}
