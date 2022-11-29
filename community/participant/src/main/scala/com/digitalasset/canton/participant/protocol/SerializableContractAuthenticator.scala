// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersion,
  SerializableContract,
  UnicumGenerator,
}

class SerializableContractAuthenticator(unicumGenerator: UnicumGenerator) {
  def authenticate(contract: SerializableContract): Either[String, Unit] =
    contract.contractId match {
      case ContractId.V1(_discriminator, cantonContractSuffix) =>
        if (cantonContractSuffix.startsWith(AuthenticatedContractIdVersion.versionPrefixBytes))
          for {
            salt <- contract.contractSalt.toRight(
              s"Contract salt missing in serializable contract with authenticating contract id (${contract.contractId})"
            )
            recomputedUnicum <- unicumGenerator
              .recomputeUnicum(
                contractSalt = salt,
                ledgerTime = contract.ledgerCreateTime,
                suffixedContractInstance = contract.rawContractInstance,
                contractIdVersion = AuthenticatedContractIdVersion,
              )
            recomputedSuffix = recomputedUnicum.toContractIdSuffix(AuthenticatedContractIdVersion)
            _ <- Either.cond(
              recomputedSuffix == cantonContractSuffix,
              (),
              s"Mismatching contract id suffixes. expected: $recomputedSuffix vs actual: $cantonContractSuffix",
            )
          } yield ()
        else
          Right(())
    }
}
