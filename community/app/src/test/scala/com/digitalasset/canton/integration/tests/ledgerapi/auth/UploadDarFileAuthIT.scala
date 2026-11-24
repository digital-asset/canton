// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.package_management_service.*
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.util.BinaryFileUtil

import scala.concurrent.Future

final class UploadDarFileAuthIT
    extends AdminServiceCallAuthTests
    with ApiAreaScopeServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "PackageManagementService#UploadDarFile"

  override def apiAreaScopeBaseToken = adminToken
  override def apiAreaScopeRestrictions = List(
    List(ClaimApiArea.packagesWrite.area) -> true,
    List(ClaimApiArea.ledgerRead.area) -> false,
  )

  lazy private val request = {
    val darData = BinaryFileUtil
      .readByteStringFromFile(CantonExamplesPath)
      .valueOrFail("could not load examples")

    UploadDarFileRequest(
      darData,
      submissionId = "",
      UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES,
      synchronizerId = "",
    )
  }

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token).uploadDarFile(request)
}
