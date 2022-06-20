// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.client

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1.{value => V}
import com.daml.ledger.client.binding.{Contract, Primitive => P, Template, TemplateCompanion}

object DecodeUtil {
  def decodeAllCreated[T](
      companion: TemplateCompanion[T]
  )(transaction: Transaction): Seq[Contract[T]] =
    for {
      event <- transaction.events
      created <- event.event.created.toList
      a <- decodeCreated(companion)(created).toList
    } yield a

  def decodeAllArchived[T](
      companion: TemplateCompanion[T]
  )(transaction: Transaction): Seq[P.ContractId[T]] =
    for {
      event <- transaction.events
      archive <- event.event.archived.toList
      decoded <- decodeArchived(companion)(archive).toList
    } yield decoded

  private def widenToWith[T](value: T)(implicit ev: T <:< Template[T]): T with Template[T] = {
    type W[+A] = A with T
    implicit val liftedEv: W[T] <:< W[Template[T]] = ev.liftCo[W]
    liftedEv.apply(value)
  }

  def decodeCreated[T](
      companion: TemplateCompanion[T]
  )(event: CreatedEvent): Option[Contract[T]] = {
    for {
      record <- event.createArguments: Option[V.Record]
      if event.templateId.exists(templateMatches(companion.id))
      value <- companion.fromNamedArguments(record): Option[T]
      tValue = widenToWith[T](value)(companion.describesTemplate)
    } yield Contract(
      P.ContractId(event.contractId),
      tValue,
      event.agreementText,
      event.signatories,
      event.observers,
      event.contractKey,
    )
  }

  private def templateMatches[A](expected: P.TemplateId[A])(actual: Identifier): Boolean = {
    val result = ApiTypes.TemplateId.unwrap(expected) == actual
    result
  }

  def decodeArchived[T](
      companion: TemplateCompanion[T]
  )(event: ArchivedEvent): Option[P.ContractId[T]] =
    Option(event)
      .filter(_.templateId.exists(templateMatches(companion.id)))
      .map(_.contractId)
      .map(P.ContractId.apply)

  def decodeAllCreatedTree[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionTree): Seq[Contract[T]] =
    for {
      event <- transaction.eventsById.values.toList
      created <- event.kind.created.toList
      a <- decodeCreated(companion)(created).toList
    } yield a

  def decodeAllArchivedTree[T](
      companion: TemplateCompanion[T]
  )(transaction: TransactionTree): Seq[P.ContractId[T]] =
    for {
      event <- transaction.eventsById.values.toList
      archive <- event.kind.exercised.toList.filter(e =>
        e.consuming && e.templateId.contains(companion.id)
      )
    } yield P.ContractId(archive.contractId)

}
