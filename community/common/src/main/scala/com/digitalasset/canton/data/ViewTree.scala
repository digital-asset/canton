// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RootHash, ViewHash}
import com.digitalasset.canton.topology.{DomainId, MediatorId}

/** Common supertype of all view trees that are sent as [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]]s */
trait ViewTree extends PrettyPrinting {

  /** The informees of the view in the tree */
  def informees: Set[Informee]

  /** Return the hash whose signature is to be included in the [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] */
  def toBeSigned: Option[RootHash]

  /** The hash of the view */
  def viewHash: ViewHash

  /** The root hash of the view tree.
    *
    * Two view trees with the same [[rootHash]] must also have the same [[domainId]] and [[mediatorId]]
    * (except for hash collisions).
    */
  def rootHash: RootHash

  /** The domain to which the [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] should be sent to */
  def domainId: DomainId

  /** The mediator that is responsible for coordinating this request */
  def mediatorId: MediatorId

  override def pretty: Pretty[this.type]
}
