// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.digitalasset.base.error.{
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.JsonApiErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

@Explanation(
  "Errors specific for Json Ledger API."
)
object JsonApiErrors extends JsonApiErrorGroup {
  @Explanation(
    "The request body is larger than the node accepts. The HTTP status of this error is 413."
  )
  @Resolution(
    """Send a smaller request or increase the limit by changing the node configuration
      |'max-inbound-message-size' of the HTTP Ledger API."""
  )
  object RequestBodyTooLarge
      extends ErrorCode(
        id = "JSON_API_REQUEST_BODY_TOO_LARGE",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(limit: Long)(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"The request body exceeded the size limit of $limit bytes."
        )
  }

  @Explanation(
    "This error occurs when the Ledger JSON API server fails to find a suitable set of package-ids for decoding a given set of commands in a request."
  )
  @Resolution(
    """Inspect the error message and ensure that:
      |1. The requested packages are uploaded on the participant node.
      |2. The topology state satisfies the required vetting for the interested parties"""
  )
  object JsonApiPackageSelectionFailed
      extends ErrorCode(
        id = "JSON_API_PACKAGE_SELECTION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(override val cause: String)(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)
  }

  @Explanation(
    s"""This happens when the number of returned elements is equal or greater to the node limit.
       |The limit is defined by the node configuration and can be changed by the operator.
       | Check 'canton.participants.<participant-id>.http-ledger-api.websocket-config.http-list-max-elements-limit'.
       | Notice: If configured in the participant node config, the actual returned number is the minimum between the "limit" query parameter in the request and this one .
       | If the request query "limit" is the same as the mentioned configuration, it allows to return result set up to server limit without producing error.
       |"""
  )
  @Resolution("""
   |1. Preferred solution is to use websocket endpoint to get results in chunks.
   |2. It is possible to increase the limit by changing the node configuration 'http-list-max-elements-limit' but this may have
   |a severe impact on a node performance.""")
  object MaximumNumberOfElements
      extends ErrorCode(
        id = "JSON_API_MAXIMUM_LIST_ELEMENTS_NUMBER_REACHED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Reject(
        value: Int,
        limit: Long,
    )(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The number of matching elements ($value) is greater than the node limit ($limit)."
        )
  }

  @Explanation(
    """This error occurs when a websocket client authenticates with the JWT cookie, but the `Origin` header of the
      |request is missing, is the opaque `null` origin, or is not listed in
      |'canton.participants.<participant-id>.http-ledger-api.websocket-config.cookie-auth-allowed-origins'.
      |Browsers attach cookies to websocket handshakes started by any web site, so the check prevents cross-site
      |websocket hijacking. The exact reason is logged on the participant, but not given to the client for
      |security reasons."""
  )
  @Resolution(
    """Send the token in the `Authorization` header or in the `Sec-WebSocket-Protocol` header instead, or ask the
      |participant operator to add the origin of the web application to 'cookie-auth-allowed-origins'."""
  )
  object CookieAuthOriginNotAllowed
      extends ErrorCode(
        id = "JSON_API_COOKIE_AUTH_ORIGIN_NOT_ALLOWED",
        ErrorCategory.InsufficientPermission,
      ) {
    final case class Reject(override val cause: String)(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Websocket cookie authentication rejected: $cause"
        )
  }

  @Explanation(
    """This error occurs when a single inbound websocket message, including all of its fragments, exceeds the
      |maximum inbound message size of the JSON Ledger API, configured with
      |'canton.participants.<participant-id>.http-ledger-api.max-inbound-message-size'.
      |The message is rejected before it is fully received, and the websocket connection is closed."""
  )
  @Resolution(
    """Send smaller messages, or ask the participant operator to increase 'max-inbound-message-size'."""
  )
  object WebsocketMessageTooLarge
      extends ErrorCode(
        id = "JSON_API_WEBSOCKET_MESSAGE_TOO_LARGE",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject(maxMessageSizeBytes: Long)(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The inbound websocket message exceeds the maximum size of $maxMessageSizeBytes bytes."
        )
  }

  @Explanation(
    """This error occurs when an inbound websocket message is in a binary format, which is not
      |supported by the JSON Ledger API.
      |The message is rejected, and the websocket connection is closed."""
  )
  @Resolution(
    """Use a text-based format for the websocket message."""
  )
  object WebsocketMessageBinaryNotSupported
      extends ErrorCode(
        id = "JSON_API_WEBSOCKET_BINARY_MESSAGE_NOT_SUPPORTED",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Reject()(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "The inbound websocket message is in a binary format, which is not supported."
        )
  }
}
