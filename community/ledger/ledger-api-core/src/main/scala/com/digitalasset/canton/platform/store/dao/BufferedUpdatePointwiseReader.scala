// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.dao.BufferedUpdatePointwiseReader.{
  FetchUpdatePointwiseFromPersistence,
  ToApiResponse,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate

import scala.concurrent.Future

/** Generic class that helps serving Ledger API point-wise lookups (UpdateService.{GetUpdateById,
  * GetUpdateByOffset}) from either the in-memory fan-out buffer or from persistence.
  *
  * @param fetchFromPersistence
  *   Fetch an update by offset or id from persistence.
  * @param toApiResponse
  *   Convert a [[com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate]] to a
  *   specific API response while also filtering for visibility.
  * @tparam QueryParamType
  *   The query parameter type.
  * @tparam ApiResponse
  *   The Ledger API response type.
  */
class BufferedUpdatePointwiseReader[QueryParamType, ApiResponse](
    fetchFromPersistence: FetchUpdatePointwiseFromPersistence[QueryParamType, ApiResponse],
    fetchFromBuffer: QueryParamType => Option[TransactionLogUpdate],
    toApiResponse: ToApiResponse[QueryParamType, ApiResponse],
) {

  /** Serves processed and filtered update from the buffer by the query parameter, with fallback to
    * a persistence fetch if the update is not anymore in the buffer (i.e. it was evicted)
    *
    * @param queryParam
    *   The query parameter.
    * @param loggingContext
    *   The logging context
    * @return
    *   A future wrapping the API response if found.
    */
  def fetch(queryParam: QueryParamType)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ApiResponse]] =
    fetchFromBuffer(queryParam) match {
      case Some(value) => toApiResponse(value, queryParam, loggingContext)
      case None =>
        fetchFromPersistence(queryParam, loggingContext)
    }
}

object BufferedUpdatePointwiseReader {
  trait FetchUpdatePointwiseFromPersistence[QueryParamType, ApiResponse] {
    def apply(
        queryParam: QueryParamType,
        loggingContext: LoggingContextWithTrace,
    ): Future[Option[ApiResponse]]
  }

  trait ToApiResponse[QueryParamType, ApiResponse] {
    def apply(
        transactionAccepted: TransactionLogUpdate,
        queryParam: QueryParamType,
        loggingContext: LoggingContextWithTrace,
    ): Future[Option[ApiResponse]]
  }
}
