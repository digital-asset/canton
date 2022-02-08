// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.canton.protocol.LfContractId

/** Wrapper class to make scalapb LedgerApi classes more convenient to access
  */
object LedgerApiTypeWrappers {

  /*
    Provide a few utilities methods on CreatedEvent.
    Notes:
   * We don't use an `implicit class` because it makes the use of pretty
       instances difficult (e.g. for `ledger_api.acs.of_all`).

   * Also, the name of some methods of `WrappedCreatedEvent`, such as `templataId`,
       collides with one of the underlying event.
   */
  final case class WrappedCreatedEvent(event: CreatedEvent) {

    private def corrupt: String = s"corrupt event ${event.eventId} / ${event.contractId}"

    def templateId: String = {
      event.templateId.map(x => x.moduleName + "." + x.entityName).getOrElse(corrupt)
    }

    def packageId: String = {
      event.templateId.map(_.packageId).getOrElse(corrupt)
    }

    private def flatten(prefix: Seq[String], field: RecordField): Seq[(String, Any)] = {
      def extract(args: Value.Sum): Seq[(String, Any)] =
        args match {
          case x: Value.Sum.Record => x.value.fields.flatMap(flatten(prefix :+ field.label, _))
          case x: Value.Sum.Variant => x.value.value.toList.map(_.sum).flatMap(extract)
          case x => Seq(((prefix :+ field.label).mkString("."), x.value))
        }
      field.value.map(_.sum).toList.flatMap(extract)
    }

    def arguments: Map[String, Any] =
      event.createArguments.toList.flatMap(_.fields).flatMap(flatten(Seq(), _)).toMap

    def toContractData: ContractData = {
      val templateId =
        event.templateId.getOrElse(throw new IllegalArgumentException("Template Id not specified"))
      val createArguments =
        event.createArguments.getOrElse(
          throw new IllegalArgumentException("Create Arguments not specified")
        )
      val lfContractId =
        LfContractId
          .fromString(event.contractId)
          .getOrElse(
            throw new IllegalArgumentException(s"Illegal Contract Id: ${event.contractId}")
          )
      ContractData(templateId, createArguments, event.signatories.toSet, lfContractId)
    }

  }

  /** Holder of "core" contract defining fields (particularly those relevant for importing contracts) */
  case class ContractData(
      templateId: Identifier,
      createArguments: Record,
      signatories: Set[String], // track signatories for use as auth validation by daml engine
      inheritedContractId: LfContractId,
  )

}
