// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.package_management_service.*
import com.google.protobuf.ByteString

import java.io.FileInputStream
import scala.concurrent.Future

final class UploadDarAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "PackageManagementService#UploadDar"

  lazy private val request = new UploadDarFileRequest(
    ByteString.readFrom(new FileInputStream(darFile))
  )

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token).uploadDarFile(request)

}
