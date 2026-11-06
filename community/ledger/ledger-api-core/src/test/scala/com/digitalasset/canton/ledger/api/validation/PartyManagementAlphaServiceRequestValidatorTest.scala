// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.admin.party_management_alpha_service.AuthorizePartyUpdateRequest
import com.daml.ledger.api.v2.crypto.Signature
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.SignatureSizeConfig
import com.digitalasset.canton.ledger.api.sizeconfig.v2.admin.AuthorizePartyUpdateRequestSizeConfig
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import org.scalatest.wordspec.AnyWordSpec

class PartyManagementAlphaServiceRequestValidatorTest extends AnyWordSpec with SizeLimitTestUtils {

  private implicit val errorLoggingContext: ErrorLoggingContext = NoLogging

  "validating the size limits of an authorize party update request" should {
    val maxTransactionBytes = 4
    val maxSignatures = 2
    val maxSignatureBytes = 5

    val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
      publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(signature =
        SignatureSizeConfig(signatureSizeLimit = PositiveInt.tryCreate(maxSignatureBytes))
      ),
      adminLapiSizeConfig = sizeLimitsConfig.adminLapiSizeConfig.copy(authorizePartyUpdateRequest =
        AuthorizePartyUpdateRequestSizeConfig(
          transactionSizeLimit = PositiveInt.tryCreate(maxTransactionBytes),
          signaturesSizeLimit = PositiveInt.tryCreate(maxSignatures),
        )
      ),
    )

    def signature(numberOfBytes: Int): Signature =
      Signature.defaultInstance.copy(signature = bytes(numberOfBytes))

    def validate(request: AuthorizePartyUpdateRequest) =
      PartyManagementAlphaServiceRequestValidator.validateAuthorizePartyUpdateRequest(request)(
        errorLoggingContext,
        sizeLimitsConfigWithLimits,
      )

    byteSizeLimit("transaction", limit = maxTransactionBytes)(numberOfBytes =>
      validate(AuthorizePartyUpdateRequest.defaultInstance.copy(transaction = bytes(numberOfBytes)))
    )

    cardinalityLimit("signatures", limit = maxSignatures)(numberOfSignatures =>
      validate(
        AuthorizePartyUpdateRequest.defaultInstance
          .copy(signatures = Seq.fill(numberOfSignatures)(signature(1)))
      )
    )

    byteSizeLimit("signature", limit = maxSignatureBytes)(numberOfBytes =>
      validate(
        AuthorizePartyUpdateRequest.defaultInstance.copy(signatures = Seq(signature(numberOfBytes)))
      )
    )
  }
}
