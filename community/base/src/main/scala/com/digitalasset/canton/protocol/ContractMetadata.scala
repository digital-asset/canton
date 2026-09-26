// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.ContractMetadata.InvalidContractMetadata
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPartyId, LfVersioned, checked}

/** Metadata for a contract.
  *
  * @param signatories
  *   Must include the maintainers of the key if any
  * @param stakeholders
  *   Must include the signatories
  * @throws ContractMetadata.InvalidContractMetadata
  *   if some maintainers are not signatories or some signatories are not stakeholders.
  */
final case class ContractMetadata private (
    signatories: Set[LfPartyId],
    stakeholders: Set[LfPartyId],
    maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]],
) extends PrettyPrintingFromCompanion {

  {
    val nonSignatoryMaintainers = maintainers -- signatories
    if (nonSignatoryMaintainers.nonEmpty)
      throw InvalidContractMetadata(show"Maintainers are not signatories: $nonSignatoryMaintainers")
    val nonStakeholderSignatories = signatories -- stakeholders
    if (nonStakeholderSignatories.nonEmpty)
      throw InvalidContractMetadata(
        show"Signatories are not stakeholders: $nonStakeholderSignatories"
      )
  }

  def maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers] =
    maybeKeyWithMaintainersVersioned.map(_.unversioned)

  def maybeKey: Option[LfGlobalKey] = maybeKeyWithMaintainers.map(_.globalKey)

  def maintainers: Set[LfPartyId] =
    maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.maintainers)

  override def prettyCompanion: PrettyPrintingCompanion[ContractMetadata] = ContractMetadata
}

object ContractMetadata extends PrettyPrintingCompanion[ContractMetadata] {

  override protected val pretty: Pretty[ContractMetadata] = prettyOfClass(
    param("signatories", _.signatories),
    param("stakeholders", _.stakeholders),
    paramIfDefined("key", _.maybeKey),
    paramIfNonEmpty("maintainers", _.maintainers),
  )

  final case class InvalidContractMetadata(message: String) extends RuntimeException(message)

  def apply(stakeholders: Stakeholders): ContractMetadata =
    ContractMetadata(stakeholders.signatories, stakeholders.all, None)

  def tryCreate(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]],
  ): ContractMetadata =
    ContractMetadata(signatories, stakeholders, maybeKeyWithMaintainersVersioned)

  def create(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]],
  ): Either[String, ContractMetadata] =
    Either
      .catchOnly[InvalidContractMetadata](
        tryCreate(signatories, stakeholders, maybeKeyWithMaintainersVersioned)
      )
      .leftMap(_.message)

  def empty: ContractMetadata = checked(ContractMetadata.tryCreate(Set.empty, Set.empty, None))

}
