// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAdministrationService(sequencer: Sequencer)(implicit
    executionContext: ExecutionContext
) extends v0.SequencerAdministrationServiceGrpc.SequencerAdministrationService {

  override def pruningStatus(request: Empty): Future[v0.SequencerPruningStatus] = fromGrpcContext {
    implicit traceContext =>
      sequencer.pruningStatus.map(_.toProtoV0)
  }
}
