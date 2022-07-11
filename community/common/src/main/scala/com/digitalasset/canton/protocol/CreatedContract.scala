// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.HasProtoV0

/** @param consumedInCore Whether the contract is consumed in the core of the view.
  *   [[com.digitalasset.canton.protocol.WellFormedTransaction]] checks that a created contract
  *   can only be used in the same or deeper rollback scopes as the create, so if `rolledBack` is true
  *   then `consumedInCore` is false.
  * @param rolledBack Whether the contract creation has a different rollback scope than the view.
  */
case class CreatedContract private (
    contract: SerializableContract,
    consumedInCore: Boolean,
    rolledBack: Boolean,
) extends HasProtoV0[v0.ViewParticipantData.CreatedContract]
    with NoCopy
    with PrettyPrinting {

  // Note that on behalf of rolledBack contracts we still send the SerializableContract along with the contract instance
  // mainly to support DAMLe.reinterpret on behalf of a top-level CreateActionDescription under a rollback node because
  // we need the contract instance to construct the LfCreateCommand.

  override def toProtoV0: v0.ViewParticipantData.CreatedContract =
    v0.ViewParticipantData.CreatedContract(
      contract = Some(contract.toProtoV0),
      consumedInCore = consumedInCore,
      rolledBack = rolledBack,
    )

  override def pretty: Pretty[CreatedContract] = prettyOfClass(
    unnamedParam(_.contract),
    paramIfTrue("consumed in core", _.consumedInCore),
    paramIfTrue("rolled back", _.rolledBack),
  )
}

object CreatedContract {
  private[this] def apply(
      contract: SerializableContract,
      consumedInCore: Boolean,
      rolledBack: Boolean,
  ): CreatedContract =
    throw new UnsupportedOperationException("Use the public create method")

  def create(
      contract: SerializableContract,
      consumedInCore: Boolean,
      rolledBack: Boolean,
  ): Either[MalformedContractId, CreatedContract] =
    ContractId
      .ensureCantonContractId(contract.contractId)
      .map(_ => new CreatedContract(contract, consumedInCore, rolledBack))

  def tryCreate(
      contract: SerializableContract,
      consumedInCore: Boolean,
      rolledBack: Boolean,
  ): CreatedContract =
    create(contract, consumedInCore, rolledBack).valueOr(err =>
      throw new IllegalArgumentException(err.toString)
    )

  def fromProtoV0(
      createdContractP: v0.ViewParticipantData.CreatedContract
  ): ParsingResult[CreatedContract] = {
    val v0.ViewParticipantData.CreatedContract(contractP, consumedInCore, rolledBack) =
      createdContractP
    for {
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV0)
      createdContract <- create(contract, consumedInCore, rolledBack).leftMap(err =>
        OtherError(err.toString)
      )
    } yield createdContract
  }
}

/** @param consumedInView Whether the contract is consumed in the view.
  *   [[com.digitalasset.canton.protocol.WellFormedTransaction]] checks that a created contract
  *   can only be used in the same or deeper rollback scopes as the create, so if `rolledBack` is true
  *   then `consumedInView` is false.
  * @param rolledBack Whether the contract creation has a different rollback scope than the view.
  */
case class CreatedContractInView(
    contract: SerializableContract,
    consumedInView: Boolean,
    rolledBack: Boolean,
)
object CreatedContractInView {
  def fromCreatedContract(created: CreatedContract): CreatedContractInView =
    CreatedContractInView(
      created.contract,
      consumedInView = created.consumedInCore,
      created.rolledBack,
    )
}
