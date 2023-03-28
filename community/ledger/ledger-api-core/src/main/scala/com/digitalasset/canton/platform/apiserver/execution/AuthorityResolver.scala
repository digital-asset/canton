// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.data.Ref.*
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver.AuthorityResponse

import scala.concurrent.Future

trait AuthorityResolver {
  def resolve(request: AuthorityResolver.AuthorityRequest): Future[AuthorityResponse]
}

object AuthorityResolver {
  type DomainId =
    String // TODO(i11255) Make `DomainId` available on the ledger API side of the codebase.

  sealed trait AuthorityResponse
  object AuthorityResponse {
    final case object Authorized extends AuthorityResponse
    final case class MissingAuthorisation(parties: Set[Party]) extends AuthorityResponse
  }

  final case class AuthorityRequest(
      holding: Set[Party],
      requesting: Set[Party],
      domainId: Option[DomainId],
  )

  def apply(): AuthorityResolver = new TopologyUnawareAuthorityResolver

  class TopologyUnawareAuthorityResolver extends AuthorityResolver {
    override def resolve(request: AuthorityRequest): Future[AuthorityResponse] =
      Future.successful(AuthorityResponse.MissingAuthorisation(request.requesting))
  }

}
