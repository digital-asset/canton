// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.RepairServiceErrorGroup
import com.digitalasset.canton.error.{BaseCantonError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.ProtocolVersion

sealed trait RepairServiceError extends Product with Serializable with BaseCantonError

object RepairServiceError extends RepairServiceErrorGroup {

  @Explanation("The participant does not support the requested protocol version.")
  @Resolution(
    "Specify a protocol version that the participant supports or upgrade the participant to a release that supports the requested protocol version."
  )
  object UnsupportedProtocolVersionParticipant
      extends ErrorCode(
        id = "UNSUPPORTED_PROTOCOL_VERSION_PARTICIPANT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(
        requestedProtocolVersion: ProtocolVersion,
        supportedVersions: Seq[ProtocolVersion],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not support the requested protocol version $requestedProtocolVersion"
        )
        with RepairServiceError
  }

  @Explanation(
    "The participant does not support serving an ACS snapshot at the requested timestamp, likely because some concurrent processing has not yet finished."
  )
  @Resolution(
    "Make sure that the specified timestamp has been obtained from the participant in some way. If so, retry after a bit (possibly repeatedly)."
  )
  object InvalidAcsSnapshotTimestamp
      extends ErrorCode(
        id = "INVALID_ACS_SNAPSHOT_TIMESTAMP",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Error(
        requestedTimestamp: CantonTimestamp,
        cleanTimestamp: CantonTimestamp,
        domainId: DomainId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not yet support serving an ACS snapshot at the requested timestamp $requestedTimestamp on domain $domainId"
        )
        with RepairServiceError
  }

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_REPAIR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with RepairServiceError
  }
}
