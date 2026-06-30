// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

import com.daml.jwt.JwksUrl

final case class IdentityProviderConfig(
    identityProviderId: IdentityProviderId.Id,
    isDeactivated: Boolean = false,
    jwksUrl: JwksUrl,
    issuer: String,
    audience: Option[String],
)

final case class IdentityProviderConfigUpdate(
    identityProviderId: IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[JwksUrl] = None,
    issuerUpdate: Option[String] = None,
    audienceUpdate: Option[Option[String]] = None,
)
