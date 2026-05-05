// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.TestDar

object Errors {

  sealed abstract class FrameworkException(message: String, cause: Throwable)
      extends RuntimeException(message, cause)

  final class ParticipantConnectionException(cause: Throwable)
      extends FrameworkException(
        s"Could not connect to the participant: ${cause.getMessage}",
        cause,
      )

  final class DarUploadException(dar: TestDar, cause: Throwable)
      extends FrameworkException(s"""Failed to upload DAR "$dar": ${cause.getMessage}""", cause)

}
