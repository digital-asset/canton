// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ContractDeserializationError
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId, Stakeholders, v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Ref.{PackageName, TypeConId}
import com.digitalasset.nonempty.NonEmpty
import com.digitalasset.nonempty.NonEmptyReturningOps.*

final case class ContractReassignment(
    contract: ContractInstance,
    sourceValidationPackageId: Source[LfPackageId],
    targetValidationPackageId: Target[LfPackageId],
    counter: ReassignmentCounter,
) {
  def templateId: TypeConId = contract.inst.templateId
  def packageName: PackageName = contract.inst.packageName

  def toProtoV30: v30.ActiveContract = v30.ActiveContract(
    contract = contract.encoded,
    reassignmentCounter = counter.toProtoPrimitive,
  )

  def toProtoV31: v31.ActiveContract = v31.ActiveContract(
    contract = contract.encoded,
    sourceValidationPackageId = sourceValidationPackageId.unwrap,
    targetValidationPackageId = targetValidationPackageId.unwrap,
    reassignmentCounter = counter.toProtoPrimitive,
  )

}

object ContractReassignment {

  /** In v30 the validation package ids are not transmitted, so they are derived from the contract's
    * template package id.
    */
  def fromProtoV30(activeContract: v30.ActiveContract): ParsingResult[ContractReassignment] = {
    val v30.ActiveContract(contractP, reassignmentCounterP) = activeContract

    ContractInstance
      .decodeWithCreatedAt(contractP)
      .leftMap(err => ContractDeserializationError(err): ProtoDeserializationError)
      .map(contract =>
        ContractReassignment(
          contract = contract,
          sourceValidationPackageId = Source(contract.templateId.packageId),
          targetValidationPackageId = Target(contract.templateId.packageId),
          counter = ReassignmentCounter(reassignmentCounterP),
        )
      )
  }

  def fromProtoV31(
      pvv: ProtocolVersionValidation
  )(activeContract: v31.ActiveContract): ParsingResult[ContractReassignment] = {
    val v31.ActiveContract(
      contractP,
      sourceValidationPackageIdP,
      targetValidationPackageIdP,
      reassignmentCounterP,
    ) = activeContract

    for {
      sourceValidationPackageId <- ProtoValidation.validateThen(
        sourceValidationPackageIdP,
        "source_validation_package_id",
        pvv,
      )(ProtoConverter.parsePackageId)
      targetValidationPackageId <- ProtoValidation.validateThen(
        targetValidationPackageIdP,
        "target_validation_package_id",
        pvv,
      )(ProtoConverter.parsePackageId)
      contract <- ContractInstance
        .decodeWithCreatedAt(contractP)
        .leftMap(err => ContractDeserializationError(err): ProtoDeserializationError)
    } yield ContractReassignment(
      contract = contract,
      sourceValidationPackageId = Source(sourceValidationPackageId),
      targetValidationPackageId = Target(targetValidationPackageId),
      counter = ReassignmentCounter(reassignmentCounterP),
    )
  }

}

final case class ContractsReassignmentBatch private (
    contracts: NonEmpty[Seq[ContractReassignment]]
) {
  def contractIds: NonEmpty[Seq[LfContractId]] = contracts.map(_.contract.contractId)

  def contractIdCounters: NonEmpty[Seq[(LfContractId, ReassignmentCounter)]] = contracts.map {
    item => (item.contract.contractId, item.counter)
  }

  def sourcePackageIds: Source[Set[LfPackageId]] = Source(
    contracts.view.map(_.sourceValidationPackageId.unwrap).toSet
  )
  def targetPackageIds: Target[Set[LfPackageId]] = Target(
    contracts.view.map(_.targetValidationPackageId.unwrap).toSet
  )

  // This can be done because all contracts in a batch have the same stakeholders.
  def stakeholders: Stakeholders = Stakeholders(contracts.head1.contract.metadata)
}

object ContractsReassignmentBatch {
  sealed trait InvalidReassignmentBatch
  case object EmptyBatch extends InvalidReassignmentBatch
  final case class DifferingStakeholders(differing: Seq[Stakeholders])
      extends InvalidReassignmentBatch

  def apply(
      contract: ContractInstance,
      sourceValidationPackageId: Source[LfPackageId],
      targetValidationPackageId: Target[LfPackageId],
      reassignmentCounter: ReassignmentCounter,
  ): ContractsReassignmentBatch = new ContractsReassignmentBatch(
    NonEmpty.mk(
      Seq,
      ContractReassignment(
        contract,
        sourceValidationPackageId,
        targetValidationPackageId,
        reassignmentCounter,
      ),
    )
  )

  def partition(
      contracts: Seq[ContractReassignment]
  ): Seq[ContractsReassignmentBatch] =
    contracts
      .groupBy1(c => Stakeholders(c.contract.metadata))
      .values
      .map(ContractsReassignmentBatch.apply)
      .toSeq

  def create(
      contracts: Seq[ContractReassignment]
  ): Either[InvalidReassignmentBatch, ContractsReassignmentBatch] =
    partition(contracts) match {
      case Nil => Left(EmptyBatch)
      case Seq(batch) => Right(batch)
      case more => Left(DifferingStakeholders(more.map(_.stakeholders)))
    }

}
