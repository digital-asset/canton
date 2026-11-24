// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v2.transaction.Transaction

object TransactionHelpers {
  def archivedEvents(transaction: Transaction): Vector[ArchivedEvent] =
    events(transaction).flatMap(_.event.archived.toList).toVector

  def createdEvents(transaction: Transaction): Vector[CreatedEvent] =
    events(transaction).flatMap(_.event.created.toList).toVector

  def exercisedEvents(transaction: Transaction): Vector[ExercisedEvent] =
    events(transaction).flatMap(_.event.exercised.toList).toVector

  // Party-wildcard queries also return the data of tests running concurrently. Every test
  // allocates fresh parties, so restricting the results to its own parties isolates it.
  def createdFor(parties: Party*)(events: Vector[CreatedEvent]): Vector[CreatedEvent] = {
    val ids = parties.map(_.getValue).toSet
    events.filter(event => event.signatories.exists(ids) || event.observers.exists(ids))
  }

  def exercisedBy(parties: Party*)(events: Vector[ExercisedEvent]): Vector[ExercisedEvent] = {
    val ids = parties.map(_.getValue).toSet
    events.filter(_.actingParties.exists(ids))
  }

  def concerns(parties: Party*)(transaction: Transaction): Boolean =
    createdFor(parties*)(createdEvents(transaction)).nonEmpty ||
      exercisedBy(parties*)(exercisedEvents(transaction)).nonEmpty

  private def events(transaction: Transaction): Iterator[Event] =
    transaction.events.iterator
}
