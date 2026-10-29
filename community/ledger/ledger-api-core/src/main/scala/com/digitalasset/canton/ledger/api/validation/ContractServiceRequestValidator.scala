// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.contract_service.GetContractRequest as GrpcGetContractRequest
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException

object ContractServiceRequestValidator {

  import FieldValidator.*

  def validateGetContractRequest(
      request: GrpcGetContractRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Either[StatusRuntimeException, GetContractRequest] =
    for {
      contractId <- requireContractId(request.contractId, "contract_id")
      _ <- validateCardinality(
        values = request.queryingParties,
        fieldName = "querying_parties",
        limit = sizeLimitsConfig.publicLapiSizeConfig.getContractRequest.queryingPartiesSizeLimit,
      )
      queryingParties <- requireParties(request.queryingParties.toSet)
    } yield GetContractRequest(contractId, queryingParties)

  final case class GetContractRequest(
      contractId: ContractId,
      queryingParties: Set[Ref.Party],
  )
}
