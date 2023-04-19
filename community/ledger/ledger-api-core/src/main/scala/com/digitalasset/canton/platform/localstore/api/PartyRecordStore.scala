// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.platform.server.api.validation.ResourceAnnotationValidation

import scala.concurrent.Future

final case class PartyDetailsUpdate(
    party: Ref.Party,
    identityProviderId: IdentityProviderId,
    displayNameUpdate: Option[Option[String]],
    isLocalUpdate: Option[Boolean],
    metadataUpdate: ObjectMetaUpdate,
)

final case class PartyRecordUpdate(
    party: Ref.Party,
    identityProviderId: IdentityProviderId,
    metadataUpdate: ObjectMetaUpdate,
)

trait PartyRecordStore {
  import PartyRecordStore.*

  def createPartyRecord(partyRecord: PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[PartyRecord]]

  def updatePartyRecord(partyRecordUpdate: PartyRecordUpdate, ledgerPartyIsLocal: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Result[PartyRecord]]

  def getPartyRecordO(party: Ref.Party)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[PartyRecord]]]

  def filterExistingParties(parties: Set[Ref.Party], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContext
  ): Future[Set[Ref.Party]]

  def filterExistingParties(parties: Set[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[Set[Ref.Party]]

}

object PartyRecordStore {
  type Result[T] = Either[Error, T]

  sealed trait Error

  final case class PartyNotFound(party: Ref.Party) extends Error
  final case class PartyRecordNotFoundFatal(party: Ref.Party) extends Error
  final case class PartyRecordExistsFatal(party: Ref.Party) extends Error
  final case class ConcurrentPartyUpdate(party: Ref.Party) extends Error
  final case class MaxAnnotationsSizeExceeded(party: Ref.Party) extends Error {
    def getReason: String = ResourceAnnotationValidation.AnnotationsSizeExceededError.reason
  }

}