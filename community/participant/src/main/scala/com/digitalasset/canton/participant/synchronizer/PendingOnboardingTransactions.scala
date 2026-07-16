// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.synchronizer

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.digitalasset.canton.{ProtoDeserializationError, SynchronizerAlias}
import com.digitalasset.nonempty.NonEmpty

/** Onboarding topology transactions provided by the operator when registering a synchronizer. They
  * are persisted as a pending operation so that the actual onboarding, which happens at handshake
  * or at a subsequent connect/reconnect (possibly after a node restart), can use them. The pending
  * operation is deleted when the participant connects to the synchronizer.
  */
final case class PendingOnboardingTransactions(
    transactions: NonEmpty[Seq[GenericSignedTopologyTransaction]]
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PendingOnboardingTransactions.type
    ]
) extends HasProtocolVersionedWrapper[PendingOnboardingTransactions] {
  @transient override protected lazy val companionObj: PendingOnboardingTransactions.type =
    PendingOnboardingTransactions

  private def toProtoV30: v30.PendingOnboardingTransactions =
    v30.PendingOnboardingTransactions(transactions.forgetNE.map(_.toByteString))

  def toPendingOperation(
      synchronizerId: SynchronizerId,
      alias: SynchronizerAlias,
  ): PendingOperation[PendingOnboardingTransactions, SynchronizerId] =
    PendingOperation(
      name = PendingOnboardingTransactions.operationName,
      key = PendingOnboardingTransactions.operationKey(alias),
      operation = this,
      synchronizer = synchronizerId,
    )
}

object PendingOnboardingTransactions extends VersioningCompanion[PendingOnboardingTransactions] {
  override val name: String = "PendingOnboardingTransactions"

  def apply(
      transactions: NonEmpty[Seq[GenericSignedTopologyTransaction]],
      protocolVersion: ProtocolVersion,
  ): PendingOnboardingTransactions =
    PendingOnboardingTransactions(transactions)(protocolVersionRepresentativeFor(protocolVersion))

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v34), v30.PendingOnboardingTransactions)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      )
  )

  lazy val operationName: NonEmptyString =
    NonEmptyString.tryCreate("onboarding-transactions")

  def operationKey(alias: SynchronizerAlias): String = alias.unwrap

  type Store = PendingOperationStore[PendingOnboardingTransactions, SynchronizerId]

  private def fromProtoV30(
      proto: v30.PendingOnboardingTransactions
  ): ParsingResult[PendingOnboardingTransactions] =
    for {
      transactions <- proto.transactions.traverse(
        SignedTopologyTransaction.fromTrustedByteString(ProtocolVersionValidation.NoValidation)
      )
      transactionsNE <- NonEmpty
        .from(transactions)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            Some("transactions"),
            "onboarding transactions must not be empty",
          )
        )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield PendingOnboardingTransactions(transactionsNE)(rpv)
}
