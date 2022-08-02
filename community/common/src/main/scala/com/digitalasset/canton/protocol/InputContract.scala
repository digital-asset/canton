// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** @param consumed Whether this contract is consumed in the core of the view this [[InputContract]] belongs to.
  *
  * @see com.digitalasset.canton.data.ViewParticipantData.coreInputs
  */
case class InputContract(contract: SerializableContract, consumed: Boolean) extends PrettyPrinting {

  def contractId: LfContractId = contract.contractId

  def contractKey: Option[LfGlobalKey] = contract.metadata.maybeKey

  def stakeholders: Set[LfPartyId] = contract.metadata.stakeholders

  def maintainers: Set[LfPartyId] = contract.metadata.maintainers

  def toProtoV0: v0.ViewParticipantData.InputContract =
    v0.ViewParticipantData.InputContract(
      contract = Some(contract.toProtoV0),
      consumed = consumed,
    )

  override def pretty: Pretty[InputContract] = prettyOfClass(
    unnamedParam(_.contract),
    paramIfTrue("consumed", _.consumed),
  )
}

object InputContract {
  def fromProtoV0(
      inputContractP: v0.ViewParticipantData.InputContract
  ): ParsingResult[InputContract] = {
    val v0.ViewParticipantData.InputContract(contractP, consumed) =
      inputContractP
    for {
      contract <- ProtoConverter
        .required("InputContract.contract", contractP)
        .flatMap(SerializableContract.fromProtoV0)
    } yield InputContract(contract, consumed)
  }
}
