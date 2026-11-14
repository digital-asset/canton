// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.update_service.{GetUpdatesPageRequest, UpdateServiceGrpc}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetUpdatesPageAuthIT
    extends ReadOnlyServiceCallAuthTests
    with UpdateFormatAuthTestCases {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UpdateService#GetUpdatesPage"

  private def request(updateFormat: Option[UpdateFormat]) =
    new GetUpdatesPageRequest(
      beginOffsetExclusive = None,
      endOffsetInclusive = None,
      maxPageSize = None,
      updateFormat = updateFormat,
      descendingOrder = false,
      pageToken = None,
    )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    val mainActorId = getMainActorId
    val contextWithUpdateFormat =
      if (context.updateFormat.isDefined) context
      else context.copy(updateFormat = updateFormat(Some(mainActorId)))
    stub(UpdateServiceGrpc.stub(channel), contextWithUpdateFormat.token)
      .getUpdatesPage(request(contextWithUpdateFormat.updateFormat))
  }
}
