// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.identity_provider_config_service.UpdateIdentityProviderConfigRequest
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.user.IdentityProviderConfigUpdate
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException

object IdentityProviderConfigServiceRequestValidator {

  def validateUpdateIdentityProviderConfigRequest(
      request: UpdateIdentityProviderConfigRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, (IdentityProviderConfigUpdate, FieldMask)] =
    for {
      config <- requirePresence(request.identityProviderConfig, "identity_provider_config")
      identityProviderId <- requireIdentityProviderId(
        config.identityProviderId,
        "identity_provider_id",
      )
      jwksUrl <- optionalString(config.jwksUrl)(requireJwksUrl(_, "jwks_url"))
      issuer <- optionalString(config.issuer)(requireNonEmptyString(_, "issuer"))
      updateMask <- requireUpdateMask(
        updateMask = request.updateMask,
        limit =
          sizeLimitsConfig.adminLapiSizeConfig.updateIdentityProviderConfigRequest.updateMaskSizeLimit,
      )
      audience <- optionalString(config.audience)(Right(_))
    } yield (
      IdentityProviderConfigUpdate(
        identityProviderId,
        Some(config.isDeactivated),
        jwksUrl,
        issuer,
        Some(audience),
      ),
      updateMask,
    )
}
