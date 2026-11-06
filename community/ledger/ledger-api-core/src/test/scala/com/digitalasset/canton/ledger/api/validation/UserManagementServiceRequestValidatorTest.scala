// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service.{
  CreateUserRequest,
  GrantUserRightsRequest,
  RevokeUserRightsRequest,
  Right,
  UpdateUserRequest,
  User,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.{
  CreateUserRequestSizeConfig,
  GrantUserRightsRequestSizeConfig,
  ObjectMetaSizeConfig,
  RevokeUserRightsRequestSizeConfig,
  UpdateUserRequestSizeConfig,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.wordspec.AnyWordSpec

class UserManagementServiceRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  "validating the size limits of the user management requests" should {
    val maxCreateUserRights = 2
    val maxGrantUserRights = 3
    val maxRevokeUserRights = 4
    val maxAnnotations = 5
    val maxUpdateMaskBytes = 20

    val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(adminLapiSizeConfig =
      sizeLimitsConfig.adminLapiSizeConfig.copy(
        createUserRequest =
          CreateUserRequestSizeConfig(rightsSizeLimit = PositiveInt.tryCreate(maxCreateUserRights)),
        grantUserRightsRequest = GrantUserRightsRequestSizeConfig(rightsSizeLimit =
          PositiveInt.tryCreate(maxGrantUserRights)
        ),
        revokeUserRightsRequest = RevokeUserRightsRequestSizeConfig(rightsSizeLimit =
          PositiveInt.tryCreate(maxRevokeUserRights)
        ),
        objectMeta =
          ObjectMetaSizeConfig(annotationsSizeLimit = PositiveInt.tryCreate(maxAnnotations)),
        updateUserRequest = UpdateUserRequestSizeConfig(updateMaskSizeLimit =
          PositiveInt.tryCreate(maxUpdateMaskBytes)
        ),
      )
    )

    def rights(numberOfRights: Int): Seq[Right] =
      (1 to numberOfRights).map(i => Right(Right.Kind.CanActAs(Right.CanActAs(s"party$i"))))

    def user(numberOfAnnotations: Int): User =
      User.defaultInstance.copy(
        id = expectedUserId,
        metadata = Some(
          ObjectMeta(
            resourceVersion = "",
            annotations = (1 to numberOfAnnotations).map(i => s"key$i" -> s"value$i").toMap,
          )
        ),
      )

    cardinalityLimit("rights", maxCreateUserRights, limitName = "limit of a user creation")(
      numberOfRights =>
        UserManagementServiceRequestValidator.validateCreateUserRequest(
          CreateUserRequest(user = Some(user(0)), rights = rights(numberOfRights))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("annotations", maxAnnotations, limitName = "limit of a user creation")(
      numberOfAnnotations =>
        UserManagementServiceRequestValidator.validateCreateUserRequest(
          CreateUserRequest(user = Some(user(numberOfAnnotations)), rights = Seq.empty)
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("rights", maxGrantUserRights, limitName = "limit of a rights grant")(
      numberOfRights =>
        UserManagementServiceRequestValidator.validateGrantUserRightsRequest(
          GrantUserRightsRequest.defaultInstance.copy(rights = rights(numberOfRights))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("rights", maxRevokeUserRights, limitName = "limit of a rights revocation")(
      numberOfRights =>
        UserManagementServiceRequestValidator.validateRevokeUserRightsRequest(
          RevokeUserRightsRequest.defaultInstance.copy(rights = rights(numberOfRights))
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    cardinalityLimit("annotations", maxAnnotations, limitName = "limit of a user update")(
      numberOfAnnotations =>
        UserManagementServiceRequestValidator.validateUpdateUserRequest(
          UpdateUserRequest(
            user = Some(user(numberOfAnnotations)),
            updateMask = Some(FieldMask(Seq("metadata"))),
          )
        )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )

    serializedSizeLimit("update_mask", maxUpdateMaskBytes)(numberOfBytes =>
      UserManagementServiceRequestValidator.validateUpdateUserRequest(
        UpdateUserRequest(user = Some(user(0)), updateMask = Some(fieldMask(numberOfBytes)))
      )(errorLoggingContext, sizeLimitsConfigWithLimits)
    )
  }
}
