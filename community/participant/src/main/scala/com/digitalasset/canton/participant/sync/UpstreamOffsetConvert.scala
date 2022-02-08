// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import java.nio.{ByteBuffer, ByteOrder}
import cats.syntax.either._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.lf.data.{Ref, Bytes => LfBytes}
import com.digitalasset.canton.participant.{GlobalOffset, LedgerSyncOffset}
import com.digitalasset.canton.util.ShowUtil._
import com.google.protobuf.ByteString

/**  Conversion utility to convert back and forth between GlobalOffsets and the offsets used by the
  *  ParticipantState API ReadService still based on a byte string. Canton emits single-Long GlobalOffsets.
  */
object UpstreamOffsetConvert {

  private val versionUpstreamOffsetsAsLong: Byte = 0
  private val longBasedByteLength: Int = 9 // One byte for the version plus 8 bytes for Long

  def fromGlobalOffset(offset: GlobalOffset): LedgerSyncOffset = {
    // Ensure we don't get negative values as the 2s-complement of Long/math.BigInt would violate lexographic
    // ordering required upstream.
    if (offset <= 0L) {
      throw new IllegalArgumentException(s"Offset has to be positive and not $offset")
    }

    LedgerSyncOffset(
      LfBytes.fromByteString(
        ByteString.copyFrom(
          ByteBuffer
            .allocate(longBasedByteLength)
            .order(ByteOrder.BIG_ENDIAN)
            .put(0, versionUpstreamOffsetsAsLong)
            .putLong(1, offset)
        )
      )
    )
  }

  def toGlobalOffset(offset: LedgerSyncOffset): Either[String, GlobalOffset] = {
    val bytes = offset.bytes.toByteArray
    if (bytes.lengthCompare(longBasedByteLength) != 0) {
      if (offset == LedgerSyncOffset.beforeBegin) {
        Right(0L)
      } else {
        Left(s"Invalid canton offset length: Expected $longBasedByteLength, actual ${bytes.length}")
      }
    } else if (!bytes.headOption.contains(versionUpstreamOffsetsAsLong)) {
      Left(
        s"Unknown canton offset version: Expected $versionUpstreamOffsetsAsLong, actual ${bytes.headOption}"
      )
    } else {
      val offset =
        ByteBuffer.wrap(bytes).getLong(1) // does not throw as size check has been performed earlier
      if (offset <= 0L) {
        Left(s"Canton offsets need to be positive and not $offset")
      } else {
        Right(offset)
      }
    }
  }

  def assertToGlobalOffset(offset: LedgerSyncOffset): GlobalOffset =
    toGlobalOffset(offset)
      .leftMap[GlobalOffset](err => throw new IllegalArgumentException(err))
      .merge

  def toLedgerOffset(offset: GlobalOffset): LedgerOffset =
    LedgerOffset(LedgerOffset.Value.Absolute(fromGlobalOffset(offset).toHexString))

  def toLedgerOffset(offset: String): LedgerOffset = LedgerOffset(
    LedgerOffset.Value.Absolute(offset)
  )

  def toLedgerSyncOffset(offset: LedgerOffset): Either[String, LedgerSyncOffset] =
    for {
      absoluteOffset <- Either.cond(
        offset.value.isAbsolute,
        offset.getAbsolute,
        show"offset must be an absolute offset, but received ${offset}",
      )
      ledgerSyncOffset <- toLedgerSyncOffset(absoluteOffset)
    } yield ledgerSyncOffset

  def tryToLedgerSyncOffset(offset: LedgerOffset): LedgerSyncOffset =
    toLedgerSyncOffset(offset).valueOr(err => throw new IllegalArgumentException(err))

  def toLedgerSyncOffset(offset: String): Either[String, LedgerSyncOffset] =
    Ref.HexString.fromString(offset).map(LedgerSyncOffset.fromHexString)

}
