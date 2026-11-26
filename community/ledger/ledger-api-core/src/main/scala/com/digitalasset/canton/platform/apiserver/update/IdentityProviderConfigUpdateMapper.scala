// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.digitalasset.canton.user.{
  Audience,
  IdentityProviderConfigUpdate,
  Issuer,
  JwksUrlLengthLimited,
}

object IdentityProviderConfigUpdateMapper extends UpdateMapperBase {

  import UpdateRequestsPaths.IdentityProviderConfigPaths

  type Resource = IdentityProviderConfigUpdate
  type Update = IdentityProviderConfigUpdate

  override val fullResourceTrie: UpdatePathsTrie = IdentityProviderConfigPaths.fullUpdateTrie

  override def makeUpdateObject(
      identityProviderConfig: IdentityProviderConfigUpdate,
      updateTrie: UpdatePathsTrie,
  ): Result[IdentityProviderConfigUpdate] =
    for {
      isDeactivatedUpdate <- resolveIsDeactivatedUpdate(
        updateTrie,
        identityProviderConfig.isDeactivatedUpdate,
      )
      issuerUpdate <- resolveIssuerUpdate(updateTrie, identityProviderConfig.issuerUpdate)
      jwksUrlUpdate <- resolveJwksUrlUpdate(updateTrie, identityProviderConfig.jwksUrlUpdate)
      audienceUpdate <- resolveAudienceUpdate(
        updateTrie,
        identityProviderConfig.audienceUpdate,
      )
    } yield {
      IdentityProviderConfigUpdate(
        identityProviderId = identityProviderConfig.identityProviderId,
        isDeactivatedUpdate = isDeactivatedUpdate,
        jwksUrlUpdate = jwksUrlUpdate,
        issuerUpdate = issuerUpdate,
        audienceUpdate = audienceUpdate,
      )
    }

  def resolveAudienceUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[Option[Audience]],
  ): Result[Option[Option[Audience]]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.audience)
      .fold(noUpdate[Option[Audience]])(updateMatch =>
        makePrimitiveFieldUpdate[Option[Audience]](
          updateMatch = updateMatch,
          defaultValue = None,
          newValue = newValue.flatten,
        )
      )

  def resolveIsDeactivatedUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[Boolean],
  ): Result[Option[Boolean]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.isDeactivated)
      .fold(noUpdate[Boolean])(updateMatch =>
        if (updateMatch.isExact) {
          Right(newValue)
        } else {
          Right(None)
        }
      )

  def resolveIssuerUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[Issuer],
  ): Result[Option[Issuer]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.issuer)
      .fold(noUpdate[Issuer])(updateMatch =>
        if (updateMatch.isExact) {
          Right(newValue)
        } else {
          Right(None)
        }
      )

  def resolveJwksUrlUpdate(
      updateTrie: UpdatePathsTrie,
      newValue: Option[JwksUrlLengthLimited],
  ): Result[Option[JwksUrlLengthLimited]] =
    updateTrie
      .findMatch(IdentityProviderConfigPaths.jwksUrl)
      .fold(noUpdate[JwksUrlLengthLimited])(updateMatch =>
        if (updateMatch.isExact) {
          Right(newValue)
        } else {
          Right(None)
        }
      )

}
