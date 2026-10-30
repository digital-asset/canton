// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.contract_service.GetContractRequest as GrpcGetContractRequest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.GetContractRequestSizeConfig
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import org.scalatest.wordspec.AnyWordSpec

class ContractServiceRequestValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with SizeLimitTestUtils {

  private implicit val noLogging: ErrorLoggingContext = NoLogging

  private val validator = ContractServiceRequestValidator

  private val maxQueryingParties = 2
  private val sizeLimitsConfigWithLimits = sizeLimitsConfig.copy(
    publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
      getContractRequest = GetContractRequestSizeConfig(
        queryingPartiesSizeLimit = PositiveInt.tryCreate(maxQueryingParties)
      )
    )
  )

  "ContractServiceRequestValidator" when {

    "validating get contract requests size limits" should {

      cardinalityLimit(field = "querying_parties", limit = maxQueryingParties)(numberOfParties =>
        validator.validateGetContractRequest(
          GrpcGetContractRequest(
            contractId = contractId.coid,
            queryingParties = parties(numberOfParties),
          )
        )(noLogging, sizeLimitsConfigWithLimits)
      )
    }
  }
}
