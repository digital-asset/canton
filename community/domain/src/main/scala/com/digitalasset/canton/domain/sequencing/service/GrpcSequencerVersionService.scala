// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.domain.admin.v0.SequencerVersionServiceGrpc.SequencerVersionService
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}

class GrpcSequencerVersionService(
    protected val serverVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
) extends SequencerVersionService
    with GrpcHandshakeService
    with NamedLogging
