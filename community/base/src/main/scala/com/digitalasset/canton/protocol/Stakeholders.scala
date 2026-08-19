// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting

// Invariant: signatories is a subset of all
final case class Stakeholders private (all: Set[LfPartyId], signatories: Set[LfPartyId])
    extends PrettyPrinting {

  val nonConfirming: Set[LfPartyId] = all -- signatories

  {
    val nonStakeholderSignatories = signatories -- all
    if (nonStakeholderSignatories.nonEmpty)
      throw new IllegalArgumentException(
        show"Signatories are not stakeholders: $nonStakeholderSignatories"
      )
  }

  override protected def pretty: Pretty[Stakeholders.this.type] = prettyOfClass(
    param("all", _.all.toSeq.sorted),
    param("signatories", _.signatories.toSeq.sorted),
  )

  def toProtoV30: v30.Stakeholders = v30.Stakeholders(
    all = all.toSeq.map(_.toProtoUnvalidated),
    signatories = signatories.toSeq.map(_.toProtoUnvalidated),
  )
}

object Stakeholders {

  def apply(metadata: ContractMetadata): Stakeholders =
    Stakeholders(all = metadata.stakeholders, signatories = metadata.signatories)

  def tryCreate(stakeholders: Set[LfPartyId], signatories: Set[LfPartyId]): Stakeholders =
    new Stakeholders(signatories = signatories, all = stakeholders)

  def withSignatories(signatories: Set[LfPartyId]): Stakeholders =
    Stakeholders(all = signatories, signatories = signatories)

  @VisibleForTesting
  def withSignatoriesAndObservers(
      signatories: Set[LfPartyId],
      observers: Set[LfPartyId],
  ): Stakeholders =
    Stakeholders(all = signatories.union(observers), signatories = signatories)

  def fromProtoV30(
      pvv: ProtocolVersionValidation,
      stakeholdersP: v30.Stakeholders,
  ): ParsingResult[Stakeholders] =
    for {
      stakeholders <- ProtoValidation
        .validateThen(stakeholdersP.all, "stakeholders", pvv, ProtoValidation.MaxCollectionSize)(
          ProtoConverter.parseLfPartyId
        )
        .map(_.toSet)
      signatories <- ProtoValidation
        .validateThen(
          stakeholdersP.signatories,
          "signatories",
          pvv,
          ProtoValidation.MaxCollectionSize,
        )(
          ProtoConverter.parseLfPartyId
        )
        .map(_.toSet)

      nonStakeholderSignatories = signatories -- stakeholders

      _ <- Either.cond(
        nonStakeholderSignatories.isEmpty,
        (),
        ProtoDeserializationError.InvariantViolation(
          "signatories",
          s"The following signatories are not stakeholders: $nonStakeholderSignatories",
        ),
      )

    } yield Stakeholders(all = stakeholders, signatories = signatories)
}
