// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver

import scala.concurrent.Future

class CantonAuthorityResolver extends AuthorityResolver {

  override def resolve(
      request: AuthorityResolver.AuthorityRequest
  ): Future[AuthorityResolver.AuthorityResponse] =
    // TODO(i11255) Implement the lookup for the authority check for the given consortium party.
    Future.successful(AuthorityResolver.AuthorityResponse.MissingAuthorisation(request.requesting))
}

object CantonAuthorityResolver {
  def topologyUnawareAuthorityResolver = new AuthorityResolver.TopologyUnawareAuthorityResolver
}
