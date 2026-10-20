// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.admin.api.client.AdminApiResponseValidation
import com.digitalasset.canton.admin.api.client.data.ListPartiesResult.ParticipantSynchronizers
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.validation.ProtoValidation

final case class ListPartiesResult(
    partyResult: Party,
    participants: Seq[ParticipantSynchronizers],
) {
  def party: PartyId = partyResult.partyId
}

object ListPartiesResult {
  final case class SynchronizerPermission(
      synchronizerId: SynchronizerId,
      permission: ParticipantPermission,
  )
  final case class ParticipantSynchronizers(
      participant: ParticipantId,
      synchronizers: Seq[SynchronizerPermission],
  )

  private def fromProtoV30(
      valueP: v30.ListPartiesResponse.Result.ParticipantSynchronizers.SynchronizerPermissions
  ): ParsingResult[SynchronizerPermission] =
    for {
      synchronizerId <- ProtoValidation.validateThen(
        valueP.synchronizerId,
        "synchronizer_id",
        AdminApiResponseValidation.Trusted,
      )(SynchronizerId.fromProtoPrimitive)
      permission <- ParticipantPermission.fromProtoV30(valueP.permission)
    } yield SynchronizerPermission(synchronizerId, permission)

  private def fromProtoV30(
      value: v30.ListPartiesResponse.Result.ParticipantSynchronizers
  ): ParsingResult[ParticipantSynchronizers] =
    for {
      participantId <- ProtoValidation.validateThen(
        value.participantUid,
        "participant_uid",
        AdminApiResponseValidation.Trusted,
      )(ParticipantId.fromProtoPrimitiveUid)

      synchronizers <- ProtoValidation
        .validateLengthThen(
          value.synchronizers,
          "synchronizers",
          AdminApiResponseValidation.Trusted,
          AdminApiResponseValidation.NoLengthLimit,
        )((entry, _) => fromProtoV30(entry))
    } yield ParticipantSynchronizers(participantId, synchronizers)

  def fromProtoV30(
      value: v30.ListPartiesResponse.Result
  ): ParsingResult[ListPartiesResult] =
    for {
      partyUid <- ProtoValidation.validateThen(
        value.party,
        "party",
        AdminApiResponseValidation.Trusted,
      )(UniqueIdentifier.fromProtoPrimitive)
      participants <- ProtoValidation
        .validateLengthThen(
          value.participants,
          "participants",
          AdminApiResponseValidation.Trusted,
          AdminApiResponseValidation.NoLengthLimit,
        )((entry, _) => fromProtoV30(entry))
    } yield ListPartiesResult(PartyId(partyUid), participants)
}

final case class ListKeyOwnersResult(
    store: SynchronizerId,
    owner: Member,
    signingKeys: Seq[SigningPublicKey],
    encryptionKeys: Seq[EncryptionPublicKey],
) {
  def keys(purpose: KeyPurpose): Seq[PublicKey] = purpose match {
    case KeyPurpose.Signing => signingKeys
    case KeyPurpose.Encryption => encryptionKeys
  }
}

object ListKeyOwnersResult {
  def fromProtoV30(
      value: v30.ListKeyOwnersResponse.Result
  ): ParsingResult[ListKeyOwnersResult] =
    for {
      synchronizerId <- ProtoValidation.validateThen(
        value.synchronizerId,
        "synchronizer_id",
        AdminApiResponseValidation.Trusted,
      )(SynchronizerId.fromProtoPrimitive)
      owner <- ProtoValidation.validateThen(
        value.keyOwner,
        "keyOwner",
        AdminApiResponseValidation.Trusted,
      )(Member.fromProtoPrimitive)
      signingKeys <- ProtoValidation
        .validateLengthThen(
          value.signingKeysV30,
          "signing_keys",
          AdminApiResponseValidation.Trusted,
          AdminApiResponseValidation.NoLengthLimit,
        )((entry, _) => SigningPublicKey.fromProtoV30(entry))
      encryptionKeys <- ProtoValidation
        .validateLengthThen(
          value.encryptionKeys,
          "encryption_keys",
          AdminApiResponseValidation.Trusted,
          AdminApiResponseValidation.NoLengthLimit,
        )((entry, _) => EncryptionPublicKey.fromProtoV30(entry))
    } yield ListKeyOwnersResult(synchronizerId, owner, signingKeys, encryptionKeys)
}

final case class SynchronizerPredecessor(
    psid: PhysicalSynchronizerId,
    upgradeTime: CantonTimestamp,
    isLateUpgrade: Boolean,
)

object SynchronizerPredecessor {
  def fromProtoV30(
      proto: com.digitalasset.canton.admin.topology.v30.SynchronizerPredecessor
  ): ParsingResult[SynchronizerPredecessor] =
    for {
      psid <- PhysicalSynchronizerId.fromProtoPrimitive(
        proto.predecessorPhysicalId,
        "predecessor_physical_id",
      )
      upgradeTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "upgrade_time",
        proto.upgradeTime,
      )
    } yield SynchronizerPredecessor(
      psid = psid,
      upgradeTime = upgradeTime,
      isLateUpgrade = proto.isLateUpgrade,
    )
}
