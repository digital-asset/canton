// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.protocol.ViewHash
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import monocle.{Lens, PLens}

object EncryptedViewMessageUtils {

  /** The contents of this message are not valid, since we use the same bytes for both types of
    * objects. It can be used where the bytes do not matter or are expected to be invalid.
    */
  def exampleEncryptedSingleView(viewType: ViewType)(
      submittingParticipantSignature: Option[Signature],
      viewHash: ViewHash,
      viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
      encryptedViewBytes: ByteString,
      synchronizerId: PhysicalSynchronizerId,
      viewEncryptionScheme: SymmetricKeyScheme,
      protocolVersion: ProtocolVersion,
  ): EncryptedViewMessage[viewType.type] =
    if (protocolVersion >= ProtocolVersion.v35)
      EncryptedSingleViewMessage(
        submittingParticipantSignature,
        viewHash,
        viewEncryptionKeyRandomness,
        EncryptedView[viewType.type](viewType)(
          Encrypted.fromByteString[CompressedView[viewType.View]](encryptedViewBytes)
        ),
        synchronizerId,
        viewEncryptionScheme,
        protocolVersion,
      )
    else
      EncryptedMultipleViewsMessage(
        EncryptedMultipleViews[viewType.type](
          viewType,
          Encrypted
            .fromByteString[CompressedView[MultipleViewTrees[viewType.View]]](encryptedViewBytes),
        ),
        NonEmpty.mk(Seq, viewHash),
        viewEncryptionKeyRandomness,
        synchronizerId,
        viewEncryptionScheme,
        submittingParticipantSignature,
        protocolVersion,
      )

  object Optics {

    private def modifyRandomness[VT <: ViewType]: NonEmpty[
      Seq[AsymmetricEncrypted[SecureRandomness]]
    ] => EncryptedViewMessage[VT] => EncryptedViewMessage[VT] = { newRandomness =>
      {
        case singleViewMessage: EncryptedSingleViewMessage[VT] =>
          singleViewMessage.copy(viewEncryptionKeyRandomness = newRandomness)
        case multipleViewsMessage: EncryptedMultipleViewsMessage[VT] =>
          multipleViewsMessage.copy(viewEncryptionKeyRandomness = newRandomness)
      }
    }

    def randomnessLens[VT <: ViewType]
        : Lens[EncryptedViewMessage[VT], NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]]] =
      Lens[EncryptedViewMessage[VT], NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]]](
        _.viewEncryptionKeyRandomness
      )(
        modifyRandomness
      )

    private def modifyViewHash[VT <: ViewType]
        : ViewHash => EncryptedViewMessage[VT] => EncryptedViewMessage[VT] = { newViewHash =>
      {
        case singleViewMessage: EncryptedSingleViewMessage[VT] =>
          singleViewMessage.copy(viewHash = newViewHash)
        case multipleViewsMessage: EncryptedMultipleViewsMessage[VT] =>
          multipleViewsMessage.copy(viewHashes =
            multipleViewsMessage.viewHashes.map(_ => newViewHash)
          )
      }
    }

    def viewHashOrHashesLens[VT <: ViewType]: PLens[EncryptedViewMessage[VT], EncryptedViewMessage[
      VT
    ], NonEmpty[Seq[ViewHash]], ViewHash] =
      PLens[EncryptedViewMessage[VT], EncryptedViewMessage[VT], NonEmpty[Seq[ViewHash]], ViewHash](
        _.viewHashes
      )(modifyViewHash)

    private def modifySignature[VT <: ViewType]
        : Option[Signature] => EncryptedViewMessage[VT] => EncryptedViewMessage[VT] = {
      newSignature =>
        {
          case singleViewMessage: EncryptedSingleViewMessage[VT] =>
            singleViewMessage.copy(submittingParticipantSignature = newSignature)
          case multipleViewsMessage: EncryptedMultipleViewsMessage[VT] =>
            multipleViewsMessage.copy(submittingParticipantSignature = newSignature)
        }
    }

    def signatureLens[VT <: ViewType]: Lens[EncryptedViewMessage[VT], Option[Signature]] =
      Lens[EncryptedViewMessage[VT], Option[Signature]](_.submittingParticipantSignature)(
        modifySignature
      )
  }

}
