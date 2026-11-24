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
import com.google.protobuf.field_mask.FieldMask
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

    def validateUpdateIdPConfigReq(config: IdentityProviderConfig) =
      IdentityProviderConfigServiceRequestValidator.validateUpdateIdentityProviderConfigRequest(
        UpdateIdentityProviderConfigRequest(
          identityProviderConfig = Some(config),
          updateMask = Some(FieldMask(Seq("issuer", "jwks_url", "audience"))),
        )
      )(errorLoggingContext, sizeLimitsConfig)

    val jwksUrlPrefix = "http://example.com/"
    val configWithLongestValues = IdentityProviderConfig(
      identityProviderId = "idp1",
      isDeactivated = false,
      jwksUrl = jwksUrlPrefix + "a" * (4096 - jwksUrlPrefix.length),
      issuer = "i" * 4096,
      audience = "a" * 4096,
    )

    "accept an issuer, a JWKS URL and an audience of 4096 characters" in {
      validateUpdateIdPConfigReq(configWithLongestValues) shouldBe a[scala.util.Right[?, ?]]
    }

    Seq[(String, IdentityProviderConfig => IdentityProviderConfig)](
      "issuer" -> (_.copy(issuer = "i" * 4097)),
      "jwks_url" -> (config => config.copy(jwksUrl = config.jwksUrl + "a")),
      "audience" -> (_.copy(audience = "a" * 4097)),
    ).foreach { case (fieldName, tooLong) =>
      s"reject a $fieldName longer than 4096 characters" in {
        val error = validateUpdateIdPConfigReq(tooLong(configWithLongestValues)).left.value
        error.getStatus.getCode shouldBe io.grpc.Status.Code.INVALID_ARGUMENT
        error.getStatus.getDescription should (include(fieldName) and include(
          "maximum length of 4096"
        ))
      }
    }
  }
}
