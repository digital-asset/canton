// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.error.CantonErrorGroups.ProtoSerializationErrorGroup

object ProtoSerializationError extends ProtoSerializationErrorGroup {

  /** Common Serialization error code
    *
    * USE THIS ERROR CODE ONLY WITHIN A GRPC SERVICE, PREPARING THE RESPONSE. Don't use it for
    * something like transaction processing or writing to the database.
    */
  @Explanation(
    """This error indicates that the response to an incoming administrative command could not be produced due to an internal error."""
  )
  @Resolution("Contact support")
  object ProtoSerializationFailure
      extends ErrorCode(
        id = "PROTO_SERIALIZATION_FAILURE",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {

    final case class Wrap(reason: String)
        extends CantonBaseError.Impl(
          cause = "Serialization of protobuf message failed"
        )
  }

}
