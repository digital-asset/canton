// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.acs

import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

trait ContractObserver {
  def reset(): Unit
  def processCreate(create: CreatedEvent, synchronizerId: String): Boolean
  def processArchive(archive: ArchivedEvent): Boolean
  def processReassign(
      create: CreatedEvent,
      synchronizerId: String,
      reassignmentCounter: Long,
  ): Boolean
}

/** Basic observer for a contract of a particular template.
  *
  * A contract observer which will filter out create / archive events of a particular template and
  * present the decoded version of that template to the typed create and archive functions.
  *
  * Is meant to be used for java-codegen generated classes.
  */
abstract class BaseContractObserver[TC <: Contract[TCid, T], TCid <: ContractId[T], T](
    companion: ContractCompanion[TC, TCid, T]
) extends ContractObserver {

  private def decode(created: CreatedEvent) =
    JavaDecodeUtil
      .decodeCreated[TC](companion)(javaapi.data.CreatedEvent.fromProto(toJavaProto(created)))

  override def processReassign(
      create: CreatedEvent,
      synchronizerId: String,
      reassignmentCounter: Long,
  ): Boolean =
    decode(create).map(processReassign_(synchronizerId, reassignmentCounter)).isDefined

  override def processCreate(create: CreatedEvent, synchronizerId: String): Boolean =
    decode(create).map(processCreate_(synchronizerId)).isDefined

  protected def processCreate_(synchronizerId: String)(create: TC): Unit
  protected def processReassign_(synchronizerId: String, reassignmentCounter: Long)(
      contract: TC
  ): Unit

  def processArchive(archive: ArchivedEvent): Boolean =
    JavaDecodeUtil
      .decodeArchived[T](companion)(
        javaapi.data.ArchivedEvent.fromProto(ArchivedEvent.toJavaProto(archive))
      )
      .map(processArchive_)
      .isDefined

  protected def processArchive_(archive: ContractId[T]): Unit

}
