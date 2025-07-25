// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import java.time.Instant

sealed abstract class AuthorizationError {
  def reason: String
}

object AuthorizationError {

  final case class Expired(authorizedUntil: Instant, currentTime: Instant)
      extends AuthorizationError {
    override val reason =
      s"Claims were valid until $authorizedUntil, current time is $currentTime"
  }

  final case class MissingUserId(reason: String) extends AuthorizationError

  final case class InvalidParticipant(authorized: String, actual: String)
      extends AuthorizationError {
    override val reason =
      s"Claims are only valid for participantId '$authorized', actual participantId is '$actual'"
  }

  final case class InvalidUser(authorized: String, actual: String) extends AuthorizationError {
    override val reason =
      s"Claims are only valid for userId '$authorized', actual userId is '$actual'"
  }

  case object MissingPublicClaim extends AuthorizationError {
    override val reason = "Claims do not authorize the use of public services"
  }

  case object MissingAdminClaim extends AuthorizationError {
    override val reason = "Claims do not authorize the use of administrative services."
  }

  final case class MissingReadClaim(party: String) extends AuthorizationError {
    override val reason = s"Claims do not authorize to read data for party '$party'"
  }

  final object MissingReadAsAnyPartyClaim extends AuthorizationError {
    override val reason =
      s"Claims do not authorize to read data as any party (super-reader wildcard)"
  }

  final case class MissingActClaim(party: String) extends AuthorizationError {
    override val reason = s"Claims do not authorize to act as party '$party'"
  }

  final case class InvalidIdentityProviderId(identityProviderId: String)
      extends AuthorizationError {
    override val reason =
      s"identity_provider_id from the request `$identityProviderId` does not match the one provided in the authorization claims"
  }

  final case class MissingAdminOrIdpAdminOrReadClaim(party: String) extends AuthorizationError {
    override val reason =
      s"Claims do not authorize the use of administrative services nor authorize to read data for party '$party'"
  }

  final case class InvalidField(fieldName: String, reason: String) extends AuthorizationError

  final case class InternalAuthorizationError(reason: String, throwable: Throwable)
      extends AuthorizationError
}
