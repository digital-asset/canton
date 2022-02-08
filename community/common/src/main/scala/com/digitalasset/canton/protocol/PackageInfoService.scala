// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.GetResult

import scala.concurrent.Future

/** @param packageId         the unique identifier for the package
  * @param sourceDescription an informal human readable description of what the package contains
  */
case class PackageDescription(packageId: PackageId, sourceDescription: String)

object PackageDescription {

  import com.digitalasset.canton.resource.DbStorage.Implicits._

  implicit val getResult: GetResult[PackageDescription] =
    GetResult(r => PackageDescription(r.<<, r.nextString()))
}

trait PackageInfoService {

  def getDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]]

}
