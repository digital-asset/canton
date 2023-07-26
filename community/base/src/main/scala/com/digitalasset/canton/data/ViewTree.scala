// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{LfTemplateId, RootHash, ViewHash}
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerApplicationId, LedgerCommandId, LedgerParticipantId}

/** Common supertype of all view trees that are sent as [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]]s */
trait ViewTree extends PrettyPrinting {

  /** The informees of the view in the tree */
  def informees: Set[Informee]

  /** Return the hash whose signature is to be included in the [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] */
  def toBeSigned: Option[RootHash]

  /** The hash of the view */
  def viewHash: ViewHash

  def viewPosition: ViewPosition

  /** The root hash of the view tree.
    *
    * Two view trees with the same [[rootHash]] must also have the same [[domainId]] and [[mediator]]
    * (except for hash collisions).
    */
  def rootHash: RootHash

  /** The domain to which the [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]] should be sent to */
  def domainId: DomainId

  /** The mediator that is responsible for coordinating this request */
  def mediator: MediatorRef

  override def pretty: Pretty[this.type]
}

/** Supertype of [[FullTransferOutTree]] and [[FullTransferInTree]]
  */
trait TransferViewTree extends ViewTree {
  def submitterMetadata: TransferSubmitterMetadata

  val viewPosition: ViewPosition =
    ViewPosition.root // Use a dummy value, as there is only one view.
}

object TransferViewTree {

  // Default values shared between transfer-out and transfer-in in case
  // the field is not populated as it comes from an implementation using
  // a version of the protocol where these fields were not yet included
  sealed trait Versioned[A] {
    def introducedIn: ProtocolVersion
    def default: A
  }

  implicit object VersionedLedgerParticipantId extends Versioned[LedgerParticipantId] {
    override val introducedIn: ProtocolVersion = ProtocolVersion.dev
    override val default: LedgerParticipantId =
      LedgerParticipantId.assertFromString("no-participant-id")
  }

  implicit object VersionedLedgerApplicationId extends Versioned[LedgerApplicationId] {
    override val introducedIn: ProtocolVersion = ProtocolVersion.dev
    override val default: LedgerApplicationId =
      LedgerApplicationId.assertFromString("no-application-id")
  }

  implicit object VersionedLedgerCommandId extends Versioned[LedgerCommandId] {
    override val introducedIn: ProtocolVersion = ProtocolVersion.dev
    override val default: LedgerCommandId =
      LedgerCommandId.assertFromString("no-command-id")
  }

  implicit object VersionedLfTemplateId extends Versioned[LfTemplateId] {
    override val introducedIn: ProtocolVersion = ProtocolVersion.dev
    override val default: LfTemplateId =
      LfTemplateId.assertFromString("no-package-id:no.module.name:no.entity.name")
  }

  /** @param version The version of the protocol relevant for the caller
    * @param value The value to be used if introduced at or after `version`
    * @return `value` is `version` is at or after `version`, otherwise a default value
    */
  def versionedValue[A](version: ProtocolVersion)(value: A)(implicit field: Versioned[A]): A =
    if (version >= field.introducedIn) {
      value
    } else {
      field.default
    }

}
