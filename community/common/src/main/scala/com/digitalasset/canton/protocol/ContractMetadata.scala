// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractMetadata.InvalidContractMetadata
import com.digitalasset.canton.protocol.version.VersionedContractMetadata
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, HasVersionedWrapperCompanion}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, checked}

/** Metadata for a contract.
  *
  * @param signatories Must include the maintainers of the key if any
  * @param stakeholders Must include the signatories
  * @throws ContractMetadata.InvalidContractMetadata if some maintainers are not signatories or some signatories are not stakeholders.
  */
case class ContractMetadata private (
    signatories: Set[LfPartyId],
    stakeholders: Set[LfPartyId],
    maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
) extends HasVersionedWrapper[VersionedContractMetadata]
    with HasProtoV0[v0.SerializableContract.Metadata]
    with PrettyPrinting {

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

  def maybeKey: Option[LfGlobalKey] = maybeKeyWithMaintainers.map(_.globalKey)

  def maintainers: Set[LfPartyId] =
    maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.maintainers)

  override def toProtoVersioned(version: ProtocolVersion): VersionedContractMetadata =
    VersionedContractMetadata(VersionedContractMetadata.Version.V0(toProtoV0))

  override def toProtoV0: v0.SerializableContract.Metadata = {
    v0.SerializableContract.Metadata(
      nonMaintainerSignatories = (signatories -- maintainers).toList,
      nonSignatoryStakeholders = (stakeholders -- signatories).toList,
      key = maybeKey.map(GlobalKeySerialization.assertToProto),
      maintainers = maintainers.toSeq,
    )
  }

  override def pretty: Pretty[ContractMetadata] = prettyOfClass(
    param("signatories", _.signatories),
    param("stakeholders", _.stakeholders),
    paramIfDefined("key", _.maybeKey),
    paramIfNonEmpty("maintainers", _.maintainers),
  )
}

object ContractMetadata
    extends HasVersionedWrapperCompanion[VersionedContractMetadata, ContractMetadata] {
  override protected def ProtoClassCompanion: VersionedContractMetadata.type =
    VersionedContractMetadata
  override protected def name: String = "contract metadata"

  case class InvalidContractMetadata(message: String) extends RuntimeException(message)

  private def apply(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
  ): ContractMetadata =
    throw new UnsupportedOperationException("Use the other factory methods instead")

  def tryCreate(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
  ): ContractMetadata =
    new ContractMetadata(signatories, stakeholders, maybeKeyWithMaintainers)

  def create(
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      maybeKeyWithMaintainers: Option[LfGlobalKeyWithMaintainers],
  ): Either[String, ContractMetadata] =
    Either
      .catchOnly[InvalidContractMetadata](
        tryCreate(signatories, stakeholders, maybeKeyWithMaintainers)
      )
      .leftMap(_.message)

  def empty: ContractMetadata = checked(ContractMetadata.tryCreate(Set.empty, Set.empty, None))

  override def fromProtoVersioned(
      metadataP: VersionedContractMetadata
  ): ParsingResult[ContractMetadata] =
    metadataP.version match {
      case VersionedContractMetadata.Version.Empty =>
        Left(FieldNotSet("VersionedContractMetadata.version"))
      case VersionedContractMetadata.Version.V0(metadata) => fromProtoV0(metadata)
    }

  def fromProtoV0(
      metadataP: v0.SerializableContract.Metadata
  ): ParsingResult[ContractMetadata] = {
    val v0.SerializableContract.Metadata(
      nonMaintainerSignatoriesP,
      nonSignatoryStakeholdersP,
      keyP,
      maintainersP,
    ) =
      metadataP
    for {
      nonMaintainerSignatories <- nonMaintainerSignatoriesP.traverse(ProtoConverter.parseLfPartyId)
      nonSignatoryStakeholders <- nonSignatoryStakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      keyO <- keyP.traverse(GlobalKeySerialization.fromProtoV0)
      maintainersList <- maintainersP.traverse(ProtoConverter.parseLfPartyId)
      _ <- Either.cond(maintainersList.isEmpty || keyO.isDefined, (), FieldNotSet("Metadata.key"))
    } yield {
      val maintainers = maintainersList.toSet
      val keyWithMaintainersO = keyO.map(key => LfGlobalKeyWithMaintainers(key, maintainers))
      val signatories = maintainers ++ nonMaintainerSignatories.toSet
      val stakeholders = signatories ++ nonSignatoryStakeholders.toSet
      checked(ContractMetadata.tryCreate(signatories, stakeholders, keyWithMaintainersO))
    }
  }
}

case class WithContractMetadata[+A](private val x: A, metadata: ContractMetadata) {
  def unwrap: A = x
}

object WithContractMetadata {
  implicit def prettyWithContractMetadata[A: Pretty]: Pretty[WithContractMetadata[A]] = {
    import Pretty._
    prettyOfClass(
      unnamedParam(_.x),
      param("metadata", _.metadata),
    )
  }
}
