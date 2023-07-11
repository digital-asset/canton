// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersion,
  AuthenticatedContractIdVersionV2,
  CantonContractIdVersion,
  NonAuthenticatedContractIdVersion,
  SerializableContract,
  UnicumGenerator,
}

trait SerializableContractAuthenticator {

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract instance and
    * contract salt) against the contract id, iff the contract id has a [[com.digitalasset.canton.protocol.AuthenticatedContractIdVersion]] format.
    *
    * @param contract the serializable contract
    */
  def authenticate(contract: SerializableContract): Either[String, Unit]
}

class SerializableContractAuthenticatorImpl(unicumGenerator: UnicumGenerator)
    extends SerializableContractAuthenticator {
  def authenticate(contract: SerializableContract): Either[String, Unit] = {
    val ContractId.V1(_discriminator, cantonContractSuffix) = contract.contractId
    val optContractIdVersion = CantonContractIdVersion.fromContractSuffix(cantonContractSuffix)
    optContractIdVersion match {
      case Right(AuthenticatedContractIdVersionV2) | Right(AuthenticatedContractIdVersion) =>
        for {
          contractIdVersion <- optContractIdVersion
          salt <- contract.contractSalt.toRight(
            s"Contract salt missing in serializable contract with authenticating contract id (${contract.contractId})"
          )
          recomputedUnicum <- unicumGenerator
            .recomputeUnicum(
              contractSalt = salt,
              ledgerTime = contract.ledgerCreateTime,
              metadata = contract.metadata,
              suffixedContractInstance = contract.rawContractInstance,
              contractIdVersion = contractIdVersion,
            )
          recomputedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
          _ <- Either.cond(
            recomputedSuffix == cantonContractSuffix,
            (),
            s"Mismatching contract id suffixes. expected: $recomputedSuffix vs actual: $cantonContractSuffix",
          )
        } yield ()
      // Future upgrades to the contract id scheme must also be supported
      // - hence we treat non-recognized contract id schemes as non-authenticated contract ids.
      case Left(_) | Right(NonAuthenticatedContractIdVersion) =>
        Right(())
    }
  }
}
