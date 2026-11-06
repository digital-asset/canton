// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.party_management_alpha_service.AuthorizePartyUpdateRequest
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import io.grpc.StatusRuntimeException

object PartyManagementAlphaServiceRequestValidator {

  def validateAuthorizePartyUpdateRequest(request: AuthorizePartyUpdateRequest)(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, Unit] = {
    val requestSizeConfig = sizeLimitsConfig.adminLapiSizeConfig.authorizePartyUpdateRequest
    for {
      _ <- validateByteSizeLimit(
        bytes = request.transaction,
        fieldName = "transaction",
        limit = requestSizeConfig.transactionSizeLimit,
      )
      _ <- validateCardinality(
        values = request.signatures,
        fieldName = "signatures",
        limit = requestSizeConfig.signaturesSizeLimit,
      )
      _ <- validateSignaturesByteSizeLimits(request.signatures)
    } yield ()
  }
}
