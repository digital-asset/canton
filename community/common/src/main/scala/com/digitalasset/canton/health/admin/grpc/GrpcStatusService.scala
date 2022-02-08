// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.grpc

import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.admin.data
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcStatusService(status: => Future[data.NodeStatus[_]])(implicit
    ec: ExecutionContext
) extends v0.StatusServiceGrpc.StatusService {
  override def status(request: Empty): Future[v0.NodeStatus] =
    status.map {
      case data.NodeStatus.Success(status) =>
        v0.NodeStatus(v0.NodeStatus.Response.Success(status.toProtoV0))
      case data.NodeStatus.NotInitialized(active) =>
        v0.NodeStatus(v0.NodeStatus.Response.NotInitialized(v0.NodeStatus.NotInitialized(active)))
      case data.NodeStatus.Failure(_msg) =>
        // The node's status should never return a Failure here.
        v0.NodeStatus(v0.NodeStatus.Response.Empty)
    }
}
