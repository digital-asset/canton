// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{InternedPartyId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.ParticipantId
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.collection.immutable

trait AcsDigestStore {

  import AcsDigestStore.*

  /** Stores running digests per party and order as sparse journal for a given synchronizer */
  def party: DigestJournal[PartyAndOrder[InternedPartyId], RawDigest]

  /** Stores running digests per counterparticipant as sparse journal for a given synchronizer */
  def participant: DigestJournal[InternedParticipantId, (RawDigest, HashedDigest)]

  /** Inserts the given record time as a checkpoint time.
    *
    * Must not be called concurrently with
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.upsertDigestUpdates]]
    * of [[party]] or [[participant]] whose record times are smaller than or equal to the given
    * record time.
    */
  def insertCheckpointTime(
      recordTime: RecordTime
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** First deletes all checkpoint record times that are higher than `fromExclusive`. Then deletes
    * all digest entries from [[party]] and [[participant]] whose record time is higher than
    * `fromExclusive`.
    */
  def deleteAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Deletes all checkpoint record times that are lower than `toExclusive`. Then deletes all digest
    * entries in [[party]] and [[participant]] whose record time is lower than `toExclusive` and
    * that satisfy one of the following conditions:
    *
    *   - The entry has been replaced (see
    *     [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]])
    *     by an entry with a higher record time, but still lower than `toExclusive`.
    *   - The entry's [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.digestO]]
    *     is [[scala.None$]].
    */
  def deleteUpTo(toExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Returns the most recent checkpoint time lower than or equal to `toInclusive`, if any */
  def latestCheckpointUpTo(toInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RecordTime]]

  /** Returns the first checkpoint time after `fromExclusive`, if any */
  def firstCheckpointAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RecordTime]]

  /** Checks for each key of [[party]] and [[participant]] that the
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]]
    * chaining is correct up to the latest checkpoint (inclusive).
    *
    * @see
    *   com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.checkReplacesInvariant
    */
  @VisibleForTesting
  def checkReplacesInvariant()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object AcsDigestStore {

  /** Maintains a key-value journal for keys `K` and values `V` indexed by
    * [[com.digitalasset.canton.participant.event.RecordTime]] as part of an [[AcsDigestStore]].
    */
  trait DigestJournal[K, V] {

    /** The synchronizer to which the digests belong to */
    protected def indexedSynchronizer: IndexedSynchronizer

    /** Upserts new entries for the given keys, i.e., inserts new entries or updates existing rows.
      *
      * The [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.recordTime]]s must
      * all be greater than the maximum checkpoint time that has been inserted previously with
      * [[AcsDigestStore.insertCheckpointTime]] or is being inserted concurrently. The
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]]
      * must be [[scala.None$]] if no entry for the key has ever been inserted, and otherwise the
      * record time of the previous entry for the key that this update replaces.
      */
    def upsertDigestUpdates(
        digests: immutable.Iterable[AcsDigestUpdate[K, V]]
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

    /** Deletes all digest entries whose record time is higher than `fromExclusive`.
      */
    private[AcsDigestStore] def deleteAfter(fromExclusive: RecordTime)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]

    /** Deletes all digest entries whose record time is lower than `toExclusive` and that satisfy
      * one of the following conditions:
      *
      *   - The entry has been replaced (see
      *     [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]])
      *     by an entry with a higher record time, but still lower than `toExclusive`.
      *   - The entry's
      *     [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.digestO]] is
      *     [[scala.None$]].
      */
    private[AcsDigestStore] def deleteUpTo(toExclusive: RecordTime)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]

    /** Returns the latest entry for the given key up to the given record time (inclusive), if any.
      */
    def lookup(
        key: K,
        toInclusive: RecordTime,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[AcsDigestUpdate[K, V]]]

    /** Returns the latest entry for each of the given keys up to the given record time (inclusive).
      * Keys without entries do not appear in the map.
      */
    def bulkLookup(
        keys: immutable.Iterable[K],
        toInclusive: RecordTime,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[K, AcsDigestUpdate[K, V]]]

    /** Returns a snapshot of all entries as of a `atInclusive`. The snapshot includes the latest
      * entry for each key whose record time is lower than or equal to `atInclusive`.
      *
      * @param limit
      *   The maximum number of entries to return.
      * @param token
      *   A token to continue a previous [[snapshot]] call that returned a token. Use
      *   [[scala.None$]] for the first call.
      * @return
      *   Up to `limit` many entries for keys and possibly a continuation token.
      */
    def snapshot(atInclusive: RecordTime, limit: Int, token: Option[SnapshotPaginationToken])(
        implicit traceContext: TraceContext
    ): FutureUnlessShutdown[
      (
          immutable.Iterable[AcsDigest[K, V]],
          Either[PaginationTokenDone, SnapshotPaginationToken],
      )
    ]
    type SnapshotPaginationToken

    /** For all the keys whose entries have been updated between `fromInclusive` and `toExclusive`,
      * returns the latest such update.
      *
      * @param limit
      *   The maximum number of entries to return.
      * @param token
      *   A token to continue a previous [[changesBetween]] call that returned a token. Use
      *   [[scala.None$]] for the first call.
      * @return
      *   Up to `limit` many entries with updates in the given period and possibly a continuation
      *   token.
      */
    def changesBetween(
        fromInclusive: RecordTime,
        toExclusive: RecordTime,
        limit: Int,
        token: Option[ChangesBetweenPaginationToken],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[
      (
          immutable.Iterable[AcsDigest[K, V]],
          Either[PaginationTokenDone, ChangesBetweenPaginationToken],
      )
    ]
    type ChangesBetweenPaginationToken

    /** Checks for each key that the
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]]
      * chaining is correct up to the given record time.
      *
      * If the complete list of digests for a key happen at increasing record times `rt_1`, `rt_2`,
      * ..., `rt_N` and `rt_M` is the last of the record times that is smaller or equal to
      * `upToInclusive`, then all of the following hold on the
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]]
      * at `rt_i` for all `i`:
      *
      *   1. If `i = 1`, then
      *      [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]]
      *      is `None` or some value smaller than `rt_1`.
      *
      *   1. If `1 < i <= M`, then
      *      [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesRecordTime]]
      *      is [[scala.Some$]](`rt_i-1`)
      *
      *   1. If `i > M`, there are no constraints because all data larger than `upToInclusive` is
      *      considered to be dirty.
      *
      * @return
      *   a future failed with [[java.lang.IllegalStateException]] if the chaining is incorrect for
      *   any key.
      */
    @VisibleForTesting
    def checkReplacesInvariant(upToInclusive: RecordTime)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]
  }

  /** Represents the running digest of the active contracts shared with a key at a given record
    * time. The digest is [[scala.None]] when the key's digest is deleted at the record time.
    */
  final case class AcsDigest[+K, +V](
      key: K,
      digestO: Option[V],
      recordTime: RecordTime,
  ) {
    def map[L](f: K => L): AcsDigest[L, V] = copy(key = f(key))
  }

  /** Represents an update to the running digest of the shared active contract for a key at a given
    * record time, together with a by-record-time reference to the entry it is replaces, if any.
    */
  final case class AcsDigestUpdate[+K, +V](
      digestUpdate: AcsDigest[K, V],
      replacesRecordTime: Option[RecordTime],
  ) {
    def map[L](f: K => L): AcsDigestUpdate[L, V] = copy(digestUpdate = digestUpdate.map(f))
  }

  /** Must always be 2048 long as long as we use [[com.digitalasset.canton.crypto.LtHash16]].
    */
  // May be refined by a proper type later in the future
  type RawDigest = ByteString

  /** Represents the SHA256 hash of a raw digest */
  type HashedDigest = ByteString

  final case class PartyAndOrder[+Party](party: Party, order: PartyOrder) {
    def map[A](f: Party => A): PartyAndOrder[A] = copy(party = f(party))
  }

  object PartyAndOrder {

    /** Encodes an interned party and the order in a single Int. Exploits that interned party IDs
      * are non-negative. Inverse of [[decodePartyAndOrder]]. Order is encoded into the least bit to
      * achieve locality as both orders of a party are typically accessed and updated together.
      */
    def encodePartyAndOrder(pao: PartyAndOrder[InternedPartyId]): Int = {
      val PartyAndOrder(party, order) = pao
      require(party >= 0, "Interned party IDs must be non-negative.")
      party * 2 + PartyOrder.toInt(order)
    }

    /** Inverse of [[encodePartyAndOrder]]. */
    def decodePartyAndOrder(encoded: Int): PartyAndOrder[InternedPartyId] = {
      val party = encoded / 2
      val order = if (encoded % 2 == 0) LocalPartyFirst else RemotePartyFirst
      PartyAndOrder(party, order)
    }

  }

  type PartyAcsDigest[+Party] = AcsDigest[PartyAndOrder[Party], RawDigest]
  object PartyAcsDigest {
    def internalize(
        stringInterning: StringInterningView,
        pad: PartyAcsDigest[LfPartyId],
    ): PartyAcsDigest[InternedPartyId] = pad.map(_.map(stringInterning.party.internalize))

    def externalize(
        stringInterning: StringInterningView,
        pad: PartyAcsDigest[InternedPartyId],
    ): PartyAcsDigest[LfPartyId] = pad.map(_.map(stringInterning.party.externalize))
  }

  type PartyAcsDigestUpdate[+Party] = AcsDigestUpdate[PartyAndOrder[Party], RawDigest]
  object PartyAcsDigestUpdate {
    def internalize(
        stringInterning: StringInterningView,
        pad: PartyAcsDigestUpdate[LfPartyId],
    ): PartyAcsDigestUpdate[InternedPartyId] =
      pad.map(_.map(stringInterning.party.internalize))

    def externalize(
        stringInterning: StringInterningView,
        pad: PartyAcsDigestUpdate[InternedPartyId],
    ): PartyAcsDigestUpdate[LfPartyId] =
      pad.map(_.map(stringInterning.party.externalize))
  }

  /** Indicates whether the running digest for the party annotates the active contracts with the
    * local parties first or with the remote parties first.
    */
  sealed trait PartyOrder extends Product with Serializable
  case object LocalPartyFirst extends PartyOrder
  case object RemotePartyFirst extends PartyOrder

  object PartyOrder {
    def toInt(order: PartyOrder): Int = order match {
      case LocalPartyFirst => 0
      case RemotePartyFirst => 1
    }
  }

  type InternedParticipantId = Int

  type ParticipantAcsDigest[+Participant] = AcsDigest[Participant, (RawDigest, HashedDigest)]
  object ParticipantAcsDigest {
    def internalize(
        stringInterning: StringInterningView,
        pad: ParticipantAcsDigest[ParticipantId],
    ): ParticipantAcsDigest[InternedParticipantId] =
      pad.map(stringInterning.participantId.internalize)

    def externalize(
        stringInterning: StringInterningView,
        pad: ParticipantAcsDigest[InternedParticipantId],
    ): ParticipantAcsDigest[ParticipantId] = pad.map(stringInterning.participantId.externalize)
  }

  type ParticipantAcsDigestUpdate[+Participant] =
    AcsDigestUpdate[Participant, (RawDigest, HashedDigest)]
  object ParticipantAcsDigestUpdate {
    def internalize(
        stringInterning: StringInterningView,
        pad: ParticipantAcsDigestUpdate[ParticipantId],
    ): ParticipantAcsDigestUpdate[InternedParticipantId] =
      pad.map(stringInterning.participantId.internalize)

    def externalize(
        stringInterning: StringInterningView,
        pad: ParticipantAcsDigestUpdate[InternedParticipantId],
    ): ParticipantAcsDigestUpdate[ParticipantId] =
      pad.map(stringInterning.participantId.externalize)
  }
}
