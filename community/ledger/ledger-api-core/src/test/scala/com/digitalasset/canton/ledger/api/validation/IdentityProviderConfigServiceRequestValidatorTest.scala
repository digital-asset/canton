// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.identity_provider_config_service.{
  IdentityProviderConfig,
  UpdateIdentityProviderConfigRequest,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.UpdateIdentityProviderConfigRequestSizeConfig
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import org.scalatest.wordspec.AnyWordSpec

class IdentityProviderConfigServiceRequestValidatorTest
    extends AnyWordSpec
    with SizeLimitTestUtils {

  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  // The rest validations of the validator are tested in the IdentityProviderConfigServiceIT, which also shows that
  // ApiIdentityProviderConfigService uses this validator.
  "validating an update identity provider config request" should {
    val maxUpdateMaskBytes = 20

    val sizeLimitsConfigWithLimit = sizeLimitsConfig.copy(adminLapiSizeConfig =
      sizeLimitsConfig.adminLapiSizeConfig.copy(updateIdentityProviderConfigRequest =
        UpdateIdentityProviderConfigRequestSizeConfig(
          updateMaskSizeLimit = PositiveInt.tryCreate(maxUpdateMaskBytes)
        )
      )
    )

    serializedSizeLimit("update_mask", limit = maxUpdateMaskBytes)(numberOfBytes =>
      IdentityProviderConfigServiceRequestValidator.validateUpdateIdentityProviderConfigRequest(
        UpdateIdentityProviderConfigRequest(
          identityProviderConfig = Some(
            IdentityProviderConfig(
              identityProviderId = "idp1",
              isDeactivated = false,
              jwksUrl = "",
              issuer = "",
              audience = "",
            )
          ),
          updateMask = Some(fieldMask(numberOfBytes)),
        )
      )(errorLoggingContext, sizeLimitsConfigWithLimit)
    )
  }
}
