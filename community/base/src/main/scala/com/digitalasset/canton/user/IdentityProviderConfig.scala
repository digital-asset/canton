// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

import com.daml.jwt.JwksUrl
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String4096,
}

final case class IdentityProviderConfig(
    identityProviderId: IdentityProviderId.Id,
    isDeactivated: Boolean = false,
    jwksUrl: JwksUrlLengthLimited,
    issuer: Issuer,
    audience: Option[Audience],
)

final case class IdentityProviderConfigUpdate(
    identityProviderId: IdentityProviderId.Id,
    isDeactivatedUpdate: Option[Boolean] = None,
    jwksUrlUpdate: Option[JwksUrlLengthLimited] = None,
    issuerUpdate: Option[Issuer] = None,
    audienceUpdate: Option[Option[Audience]] = None,
)

final case class Issuer(protected val str: String4096) extends LengthLimitedStringWrapper

object Issuer extends LengthLimitedStringWrapperCompanion[String4096, Issuer] {
  override def instanceName: String = "Issuer"
  override protected def companion: String4096.type = String4096
  override protected def factoryMethodWrapper(str: String4096): Issuer = Issuer(str)
}

final case class JwksUrlLengthLimited(protected val str: String4096)
    extends LengthLimitedStringWrapper {
  def toJwksUrl: JwksUrl = JwksUrl(unwrap)
}

object JwksUrlLengthLimited
    extends LengthLimitedStringWrapperCompanion[String4096, JwksUrlLengthLimited] {
  override def instanceName: String = "JwksUrlLimited"
  override protected def companion: String4096.type = String4096
  override protected def factoryMethodWrapper(str: String4096): JwksUrlLengthLimited =
    JwksUrlLengthLimited(str)

  def fromJwksUrl(jwksUrl: JwksUrl): Either[String, JwksUrlLengthLimited] = create(jwksUrl.value)

  def tryFromJwksUrl(jwksUrl: JwksUrl): JwksUrlLengthLimited = tryCreate(jwksUrl.value)
}

final case class Audience(protected val str: String4096) extends LengthLimitedStringWrapper

object Audience extends LengthLimitedStringWrapperCompanion[String4096, Audience] {
  override def instanceName: String = "Audience"
  override protected def companion: String4096.type = String4096
  override protected def factoryMethodWrapper(str: String4096): Audience = Audience(str)
}
