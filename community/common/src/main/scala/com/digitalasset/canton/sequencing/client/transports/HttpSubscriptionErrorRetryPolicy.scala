// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.networking.http.HttpClientError.{
  HttpRequestNetworkError,
  HttpRequestProtocolError,
}
import com.digitalasset.canton.sequencing.client.CheckedSubscriptionErrorRetryPolicy
import com.digitalasset.canton.sequencing.client.http.HttpSequencerClientError.{
  ClientError,
  TransactionCheckFailed,
}
import com.digitalasset.canton.tracing.TraceContext

class HttpSubscriptionErrorRetryPolicy
    extends CheckedSubscriptionErrorRetryPolicy[HttpSubscriptionError] {
  override protected def retryInternal(error: HttpSubscriptionError, receivedItems: Boolean)(
      implicit traceContext: TraceContext
  ): Boolean = error match {
    case SubscriptionReadError(clientError: ClientError) =>
      clientError match {
        case ClientError(HttpRequestNetworkError(_reason)) => true
        case ClientError(HttpRequestProtocolError(code, _reason)) if code.isServerError => true
        case _ => false
      }

    // TODO(11067): when the CCF sequencer supports participant activeness check, we can remove the retry condition below
    // retry on SubscriptionReadError(TransactionCheckFailed(_,_,HttpRequestProtocolError(403,Failed to submit request: Could not find matching user certificate and status code: 403)))
    case SubscriptionReadError(_: TransactionCheckFailed) => true
    case SubscriptionReadError(_readError) => false
    case UnexpectedSubscriptionException(_exception) => false
  }
}
