// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.store.{IndexedDomain, IndexedMember, IndexedStringStore}
import com.digitalasset.canton.topology.{DomainId, DomainMember}
import com.digitalasset.canton.util.NoCopy
import slick.jdbc.SetParameter

import scala.concurrent.{ExecutionContext, Future}

/** We typically have a database per node but there can be many owners of a SequencerClient within that node.
  *
  * For a domain the mediator and topology manager may have their own SequencerClient instances.
  * For a participant each domain the participant connects to will have its own SequencerClient.
  * We use this discriminator to allow storing state for all sequencer clients in the same table.
  *
  * To ensure that we truly use different "static_string" indexes, we'll require the
  * indexes to be generated by code in this file so that we can discriminate the client data
  */
sealed trait SequencerClientDiscriminator extends NoCopy {

  /** indexed use within the database */
  def index: Int
}

object SequencerClientDiscriminator {

  case class DomainDiscriminator(domainId: DomainId, override val index: Int)
      extends NoCopy
      with SequencerClientDiscriminator

  case class DomainMemberDiscriminator(member: DomainMember, override val index: Int)
      extends NoCopy
      with SequencerClientDiscriminator

  implicit val setClientDiscriminatorParameter: SetParameter[SequencerClientDiscriminator] =
    (v, pp) => pp.setInt(v.index)

  def fromDomainMember(member: DomainMember, indexedStringStore: IndexedStringStore)(implicit
      ec: ExecutionContext
  ): Future[SequencerClientDiscriminator] = {
    IndexedMember.indexed(indexedStringStore)(member).map { mb =>
      DomainMemberDiscriminator(member, mb.index)
    }
  }

  def fromIndexedDomainId(domainId: IndexedDomain): DomainDiscriminator =
    DomainDiscriminator(domainId.item, domainId.index)

}
