// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.bifunctor.*
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.commitment.{AcsDigestTrace, Timepoint}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.resource.ToDbPrimitive
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{InternedPartyId, LedgerParticipantId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.ParticipantId
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext

trait AcsDigestStore {

  import AcsDigestStore.*

  protected implicit def executionContext: ExecutionContext

  /** Stores running digests per party and order as sparse journal for a given synchronizer */
  def party: DigestJournal[PartyAndOrder[InternedPartyId], RawDigest] = party_
  protected def party_ : AcsDigestJournal[PartyAndOrder[InternedPartyId], RawDigest]
  @VisibleForTesting @inline
  private[store] final def partyInternal
      : AcsDigestJournal[PartyAndOrder[InternedPartyId], RawDigest] =
    party_

  /** Stores running digests per counterparticipant as sparse journal for a given synchronizer */
  def participant: DigestJournal[InternedParticipantId, (RawDigest, HashedDigest)] = participant_
  protected def participant_ : AcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)]
  @VisibleForTesting @inline
  private[store] final def participantInternal
      : AcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)] = participant_

  /** Inserts the given offset as a checkpoint.
    *
    * Must not be called concurrently with
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.upsertDigestUpdates]]
    * of [[party]] or [[participant]] whose offsets are smaller than or equal to the given offset.
    */
  def insertCheckpointTime(checkpoint: Checkpoint)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** First deletes all checkpoints that are higher than `fromExclusive`. Then deletes all digest
    * entries from [[party]] and [[participant]] whose offset is higher than `fromExclusive`.
    */
  final def deleteAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    _ <- deleteCheckpointsAfter(fromExclusive)
    _ <- party_.deleteAfter(fromExclusive)
    _ <- participant_.deleteAfter(fromExclusive)
  } yield ()

  /** Deletes the checkpoints after `fromExclusive` as part of the crash recovery sequence in
    * [[deleteAfter]].
    */
  protected def deleteCheckpointsAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
  @VisibleForTesting @inline
  private[store] final def deleteCheckpointsAfterInternal(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = deleteCheckpointsAfter(fromExclusive)

  /** Deletes all checkpoints that are lower than `toExclusive`. Then deletes all digest entries in
    * [[party]] and [[participant]] whose offset is lower than `toExclusive` and that satisfies one
    * of the following conditions:
    *
    *   - The entry has been replaced (see
    *     [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]])
    *     by an entry with a higher offset, but still lower than or equal to `toExclusive`.
    *   - The entry's [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.digestO]]
    *     is [[scala.None$]].
    */
  final def deleteUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    _ <- deleteCheckpointsUpTo(toExclusive)
    _ <- party_.deleteUpTo(toExclusive)
    _ <- participant_.deleteUpTo(toExclusive)
  } yield ()

  /** Deletes the checkpoints up to `toExclusive` as part of the pruning sequence in [[deleteUpTo]].
    */
  protected def deleteCheckpointsUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
  @VisibleForTesting @inline
  private[store] final def deleteCheckpointsUpToInternal(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = deleteCheckpointsUpTo(toExclusive)

  /** Returns the most recent checkpoint lower than or equal to `toInclusive`, if any */
  def latestCheckpointUpTo(toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]]

  /** Returns the first checkpoint offset after `fromExclusive`, if any */
  def firstCheckpointAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]]

  /** Checks for each key of [[party]] and [[participant]] that the
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
    * chaining is correct up to the latest checkpoint (inclusive).
    *
    * @see
    *   [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.checkReplacesInvariant]]
    */
  @VisibleForTesting
  final def checkReplacesInvariant()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    lastCheckpointO <- latestCheckpointUpTo(Offset.MaxValue)
    _ <- lastCheckpointO.fold(FutureUnlessShutdown.unit) {
      case Checkpoint(Timepoint(offsetInclusive), _) =>
        for {
          _ <- party.checkReplacesInvariant(offsetInclusive)
          _ <- participant.checkReplacesInvariant(offsetInclusive)
        } yield ()
    }
  } yield ()
}

object AcsDigestStore {

  /** Maintains a key-value journal for keys `K` and values `V` indexed by
    * [[com.digitalasset.canton.data.Offset]] as part of an [[AcsDigestStore]].
    */
  trait DigestJournal[K, V] {

    /** Upserts new entries for the given keys, i.e., inserts new entries or updates existing rows.
      *
      * The [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest.offset]]s must all
      * be greater than the maximum checkpoint offset that has been inserted previously with
      * [[AcsDigestStore.insertCheckpointTime]] or is being inserted concurrently. The
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
      * must be [[scala.None$]] if no entry for the key has ever been inserted, and otherwise the
      * offset of the previous entry for the key that this update replaces.
      */
    def upsertDigestUpdates(
        digests: immutable.Iterable[AcsDigestUpdate[K, V]]
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

    /** Returns the latest entry for the given key up to the given offset (inclusive), if any.
      */
    def lookup(
        key: K,
        toInclusive: Offset,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[AcsDigestUpdate[K, V]]]

    /** Returns the latest entry for each of the given keys up to the given offset (inclusive). Keys
      * without entries do not appear in the map.
      */
    def bulkLookup(
        keys: immutable.Iterable[K],
        toInclusive: Offset,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[K, AcsDigestUpdate[K, V]]]

    /** Returns a snapshot of all entries as of a given
      * [[com.digitalasset.canton.data.Offset AtInclusive]] value. The snapshot includes the latest
      * entry for each key whose offset is lower than or equal to the given
      * [[com.digitalasset.canton.data.Offset AtInclusive]] value.
      *
      * @param limit
      *   The maximum number of entries to return.
      * @param tokenOrStart
      *   Either a token to continue a previous [[snapshot]] call that returned a token or a
      *   [[com.digitalasset.canton.data.Offset]] for the inclusive first snapshot call. Use
      *   [[scala.util.Right$]] for the first call.
      * @return
      *   Up to `limit` many entries for keys and possibly a continuation token.
      */
    def snapshot(tokenOrStart: Either[SnapshotPaginationToken, AtInclusive], limit: Int)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[
      (
          immutable.Iterable[AcsDigestUpdate[K, V]],
          Either[PaginationTokenDone, SnapshotPaginationToken],
      )
    ]
    type SnapshotPaginationToken
    type AtInclusive = Offset

    /** For all the keys whose entries have been updated between the given
      * [[AcsDigestStore.ChangesBetweenOffsetRange]]'s `fromInclusive` and `toExclusive`, returns
      * the latest such update.
      *
      * @param tokenOrStart
      *   The offset range to query the data or a token to continue a previous [[changesBetween]]
      *   call that returned a token. Use [[scala.util.Right]] for the first call.
      * @param limit
      *   The maximum number of entries to return.
      * @return
      *   Up to `limit` many entries with updates in the given period and possibly a continuation
      *   token.
      */
    def changesBetween(
        tokenOrStart: Either[ChangesBetweenPaginationToken, ChangesBetweenOffsetRange],
        limit: Int,
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[
      (
          immutable.Iterable[AcsDigest[K, V]],
          Either[PaginationTokenDone, ChangesBetweenPaginationToken],
      )
    ]
    type ChangesBetweenPaginationToken

    /** Checks for each key that the
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
      * chaining is correct up to the given offset.
      *
      * If the complete list of digests for a key happen at increasing offsets `rt_1`, `rt_2`, ...,
      * `rt_N` and `rt_M` is the last of the offsets that is smaller or equal to `upToInclusive`,
      * then all the following hold on the
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
      * at `rt_i` for all `i`:
      *
      *   1. If `i = 1`, then
      *      [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
      *      is `None` or some value smaller than `rt_1`.
      *
      *   1. If `1 < i <= M`, then
      *      [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
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
    def checkReplacesInvariant(upToInclusive: Offset)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]
  }

  /** This range is specifically designed to give an offset range constrain to
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.changesBetween]].
    */
  final case class ChangesBetweenOffsetRange(fromInclusive: Offset, toExclusive: Offset) {
    require(fromInclusive < toExclusive, s"$fromInclusive should be less than $toExclusive.")
  }

  /** Represents the running digest of the active contracts shared with a key at a given offset. The
    * digest is [[scala.None]] when the key's digest is deleted at the offset.
    */
  final case class AcsDigest[+K, +V](
      key: K,
      offset: Offset,
      timestamp: CantonTimestamp,
      digestO: Option[V],
      trace: Option[AcsDigestTrace],
  ) {

    /** Returns the timepoint of the ACS digest.
      *
      * We keep `offset` and `timestamp` as separate fields in this case class because this class
      * serves as the DTO for storage and we do not want to omit `timestamp` from the `==` checks
      * for DTOs.
      */
    def timepoint: Timepoint = Timepoint(offset)(timestamp)

    def map[L](f: K => L): AcsDigest[L, V] = copy(key = f(key))

    def partitionMap[K1, K2](f: K => Either[K1, K2]): Either[AcsDigest[K1, V], AcsDigest[K2, V]] =
      f(key).bimap(k1 => copy(key = k1), k2 => copy(key = k2))

    def mapValue[W](f: V => W): AcsDigest[K, W] = copy(digestO = digestO.map(f))
  }

  trait AcsDigestCompanion {
    def apply[K, V](
        key: K,
        timepoint: Timepoint,
        digestO: Option[V],
        trace: Option[AcsDigestTrace],
    ): AcsDigest[K, V] = AcsDigest(key, timepoint.offset, timepoint.recordTime, digestO, trace)

    def empty[K, V](key: K, timepoint: Timepoint): AcsDigest[K, V] =
      AcsDigest(key, timepoint, None, None)
  }
  object AcsDigest extends AcsDigestCompanion {
    implicit def getAcsDigest[K: GetResult, V](implicit
        vO: GetResult[Option[V]]
    ): GetResult[AcsDigestStore.AcsDigest[K, V]] = GetResult { pr =>
      AcsDigestStore.AcsDigest(
        pr.<<[K],
        pr.<<[Offset],
        pr.<<[CantonTimestamp],
        pr.<<[Option[V]],
        pr.<<[Option[AcsDigestTrace]],
      )
    }
  }

  /** Represents an update to the running digest of the shared active contract for a key at a given
    * offset, together with a by-offset reference to the entry it replaces, if any.
    */
  final case class AcsDigestUpdate[+K, +V](
      digestUpdate: AcsDigest[K, V],
      replacesOffset: Option[Offset],
  ) {
    def map[L](f: K => L): AcsDigestUpdate[L, V] = copy(digestUpdate = digestUpdate.map(f))

    def partitionMap[K1, K2](
        f: K => Either[K1, K2]
    ): Either[AcsDigestUpdate[K1, V], AcsDigestUpdate[K2, V]] =
      digestUpdate
        .partitionMap(f)
        .bimap(d1 => copy(digestUpdate = d1), d2 => copy(digestUpdate = d2))

    def mapValue[W](f: V => W): AcsDigestUpdate[K, W] =
      copy(digestUpdate = digestUpdate.mapValue(f))
  }

  trait AcsDigestUpdateCompanion {
    def apply[K, V](
        digestUpdate: AcsDigest[K, V],
        replacesOffset: Option[Offset],
    ): AcsDigestUpdate[K, V] = new AcsDigestUpdate(digestUpdate, replacesOffset)

    def empty[K, V](key: K, timepoint: Timepoint): AcsDigestUpdate[K, V] =
      AcsDigestUpdate(AcsDigest.empty(key, timepoint), None)
  }
  object AcsDigestUpdate extends AcsDigestUpdateCompanion {
    implicit def getAcsDigestUpdate[K: GetResult, V](implicit
        vO: GetResult[Option[V]]
    ): GetResult[AcsDigestStore.AcsDigestUpdate[K, V]] = GetResult { pr =>
      AcsDigestStore.AcsDigestUpdate(
        digestUpdate = AcsDigest.getAcsDigest[K, V].apply(pr), // this uses pr in order
        replacesOffset = pr.<<[Option[Offset]], // thus we cannot move this before getAcsDigest call
      )
    }
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
  object PartyAcsDigest extends AcsDigestCompanion {
    def internalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigest[LfPartyId],
    ): PartyAcsDigest[InternedPartyId] = pad.map(_.map(stringInterning.party.internalize))

    def externalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigest[InternedPartyId],
    ): PartyAcsDigest[LfPartyId] = pad.map(_.map(stringInterning.party.externalize))
  }

  type PartyAcsDigestUpdate[+Party] = AcsDigestUpdate[PartyAndOrder[Party], RawDigest]
  object PartyAcsDigestUpdate extends AcsDigestUpdateCompanion {
    def internalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigestUpdate[LfPartyId],
    ): PartyAcsDigestUpdate[InternedPartyId] =
      pad.map(_.map(stringInterning.party.internalize))

    def externalize(
        stringInterning: StringInterning,
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
    def orderFor(
        localParticipant: LedgerParticipantId,
        remoteParticipant: LedgerParticipantId,
    ): PartyOrder =
      if (localParticipant < remoteParticipant) LocalPartyFirst else RemotePartyFirst
  }

  type InternedParticipantId = Int

  type ParticipantAcsDigest[+Participant] = AcsDigest[Participant, (RawDigest, HashedDigest)]
  object ParticipantAcsDigest extends AcsDigestCompanion {
    def internalize(
        stringInterning: StringInterning,
        pad: ParticipantAcsDigest[ParticipantId],
    ): ParticipantAcsDigest[InternedParticipantId] =
      pad.map(stringInterning.participantId.internalize)

    def externalize(
        stringInterning: StringInterning,
        pad: ParticipantAcsDigest[InternedParticipantId],
    ): ParticipantAcsDigest[ParticipantId] = pad.map(stringInterning.participantId.externalize)
  }

  type ParticipantAcsDigestUpdate[+Participant] =
    AcsDigestUpdate[Participant, (RawDigest, HashedDigest)]
  object ParticipantAcsDigestUpdate {
    def internalize(
        stringInterning: StringInterning,
        pad: ParticipantAcsDigestUpdate[ParticipantId],
    ): ParticipantAcsDigestUpdate[InternedParticipantId] =
      pad.map(stringInterning.participantId.internalize)

    def externalize(
        stringInterning: StringInterning,
        pad: ParticipantAcsDigestUpdate[InternedParticipantId],
    ): ParticipantAcsDigestUpdate[ParticipantId] =
      pad.map(stringInterning.participantId.externalize)
  }

  /** Represents a checkpoint of a certain `checkpointType` at the given `timepoint`.
    *
    * TODO(#34334): add index for `checkpointType` depending on the usage pattern
    */
  final case class Checkpoint(timepoint: Timepoint, checkpointType: CheckpointType) {
    def offset: Offset = timepoint.offset
    def recordTime: CantonTimestamp = timepoint.recordTime
  }

  object Checkpoint {
    def apply(
        offset: Offset,
        recordTime: CantonTimestamp,
        checkpointType: CheckpointType,
    ): Checkpoint =
      Checkpoint(Timepoint(offset)(recordTime), checkpointType)

    implicit val checkpointGetResult: GetResult[Checkpoint] =
      GetResult[Checkpoint] { rs =>
        val offset = rs.<<[Offset]
        val timestamp = rs.<<[CantonTimestamp]
        val checkpointType = rs.<<[CheckpointType]
        Checkpoint(Timepoint(offset)(timestamp), checkpointType)
      }

  }

  /** Describes the trigger of the checkpoint.
    *
    * TODO(#33084): resolve int value to human readable strings in the debug view
    */
  final case class CheckpointType private (id: Int)
      extends PrettyPrintingFromCompanion
      with Product
      with Serializable {
    override def prettyCompanion: PrettyPrintingCompanion[CheckpointType] = CheckpointType
  }

  object CheckpointType extends PrettyPrintingCompanion[CheckpointType] {

    private val ids: mutable.Map[Int, (CheckpointType, String)] =
      mutable.TreeMap.empty[Int, (CheckpointType, String)]

    protected val pretty: Pretty[CheckpointType] =
      prettyOfString(cpt =>
        // normally, an instance of CheckpointType must have been created via `apply` or
        // tryFromId, both of which throw in case of inconsistencies. Therefore the getOrElse branch shouldn't actually
        // be reached.
        ids
          .get(cpt.id)
          .map(_._2)
          .getOrElse(
            throw new IllegalStateException(
              s"CheckpointType with id ${cpt.id} was not created via CheckpointType.apply or CheckpointType.tryFromId."
            )
          )
      )

    /** Creates a new [[CheckpointType]] with a given description */
    def apply(id: Int, description: String): CheckpointType = {
      val checkpointType = new CheckpointType(id)
      ids.put(id, (checkpointType, description)).foreach { oldDescription =>
        throw new IllegalArgumentException(
          s"requirement failed: CheckpointType with id=$id already exists for ${oldDescription._2}"
        )
      }

      checkpointType
    }

    /** When a reconcilition interval boundary has been crossed.
      */
    val ReconciliationIntervalBoundary: CheckpointType =
      CheckpointType(1, "ReconciliationIntervalBoundary")

    /** When an affirmation interval boundary has been crossed.
      */
    val AffirmationIntervalBoundary: CheckpointType =
      CheckpointType(2, "AffirmationIntervalBoundary")

    /** When a certain number of events have been processed without writing a checkpoint.
      */
    val MaxEventsWithoutCheckpoint: CheckpointType = CheckpointType(3, "MaxEventsWithoutCheckpoint")

    /** When the hosting relation between a party and a participant have changed.
      */
    val PartyHostingChange: CheckpointType = CheckpointType(4, "PartyHostingChange")

    /** When a reinitialization has completed.
      */
    val Reinitialization: CheckpointType = CheckpointType(5, "Reinitialization")

    @VisibleForTesting
    def all: Set[CheckpointType] = ids.values.map { case (tpe, _) => tpe }.toSet

    /** For constructing a checkpoint from an integer. Throws an exception in case the provided
      * integer is not a known checkpoint type.
      */
    private def tryFromId(id: Int): CheckpointType =
      ids
        .getOrElse(
          id,
          throw new IllegalArgumentException(
            s"DB value '$id' doesn't map to a known checkpoint type: $ids"
          ),
        )
        ._1

    implicit val checkpointTypeSetParameter: ToDbPrimitive[CheckpointType, Int] =
      ToDbPrimitive(_.id)
    implicit val checkpointTypeGetResult: GetResult[CheckpointType] =
      GetResult { rs =>
        val dbInt = rs.<<[Int]
        tryFromId(dbInt)
      }
  }
}
