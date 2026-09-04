// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Monad
import cats.syntax.bifunctor.*
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.commitment.{AcsDigestTrace, Timepoint}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.resource.ToDbPrimitive
import com.digitalasset.canton.store.Purgeable
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{InternedPartyId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.ParticipantId
import com.digitalasset.nonempty.{NonEmpty, NonEmptyUtil}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext

trait AcsDigestStore extends AutoCloseable with Purgeable { this: NamedLogging =>

  import AcsDigestStore.*

  protected implicit def executionContext: ExecutionContext

  /** Stores running digests per party and order as sparse journal for a given synchronizer */
  def party: DigestJournal[InternedPartyId] = party_
  protected def party_ : AcsDigestJournal[InternedPartyId]
  @inline private[store] final def partyInternal: AcsDigestJournal[InternedPartyId] =
    party_

  /** Stores running digests per counterparticipant as sparse journal for a given synchronizer */
  def participant: DigestJournal[InternedParticipantId] = participant_
  protected def participant_ : AcsDigestJournal[InternedParticipantId]
  @inline private[store] final def participantInternal: AcsDigestJournal[InternedParticipantId] =
    participant_

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
  @inline
  private[store] final def deleteCheckpointsAfterInternal(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = deleteCheckpointsAfter(fromExclusive)

  /** Returns the latest ledger offset up to which the digest store has been successfully pruned via
    * [[deleteUpTo]].
    */
  def lookupLatestPruningOffset()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Offset]]

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
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Pruning the ACS digest store up to offset $toExclusive")
    for {
      latestPruningOffsetO <- lookupLatestPruningOffset()
      _ <- deleteCheckpointsUpTo(toExclusive)
      _ <- party_.deleteUpTo(toExclusive, latestPruningOffsetO)
      _ <- participant_.deleteUpTo(toExclusive, latestPruningOffsetO)
      _ <- increaseLatestPruneOffset(toExclusive)
    } yield ()
  }

  /** Deletes the checkpoints up to `toExclusive` as part of the pruning sequence in [[deleteUpTo]].
    */
  protected def deleteCheckpointsUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
  @inline private[store] final def deleteCheckpointsUpToInternal(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = deleteCheckpointsUpTo(toExclusive)

  protected def increaseLatestPruneOffset(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  @inline private[store] final def increaseLatestPruneOffsetInternal(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = increaseLatestPruneOffset(toExclusive)

  /** Deletes all data in this store */
  override final def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Purging the ACS digest store")
    for {
      _ <- purgeLatestPrune()
      _ <- purgeCheckpoints()
      _ <- party_.purge()
      _ <- participant_.purge()
    } yield ()
  }

  protected def purgeCheckpoints()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
  @inline private[store] final def purgeCheckpointsInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = purgeCheckpoints()

  protected def purgeLatestPrune()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
  @inline private[store] final def purgeLatestPruneInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    purgeLatestPrune()

  /** Returns the most recent checkpoint lower than or equal to `toInclusive`, if any. If
    * `checkpointTypes` is given, only checkpoints of the given types are considered.
    */
  def latestCheckpointUpTo(
      toInclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[CheckpointType]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]]

  /** Returns the latest checkpoint of tick type */
  def latestTickCheckpoint()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    latestCheckpointUpTo(Offset.MaxValue, checkpointTickFilter)

  /** Returns the latest checkpoint of type reconciliation */
  def latestReconciliationCheckpoint()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    latestCheckpointUpTo(Offset.MaxValue, checkpointReconciliationFilter)

  /** Returns the latest checkpoint of type reinitialization */
  def latestReinitializationCheckpoint()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    latestCheckpointUpTo(Offset.MaxValue, checkpointReinitializationFilter)

  /** Returns the first checkpoint offset after `fromExclusive`, if any. If `checkpointTypes` is
    * given, only checkpoints of the given types are considered.
    */
  def firstCheckpointAfter(
      fromExclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[CheckpointType]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]]

  /** Checks for each key of [[party]] and [[participant]] that the
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigestUpdate.replacesOffset]]
    * chaining is correct up to the latest checkpoint (inclusive) with respect to the latest pruning
    * offset.
    *
    * @see
    *   [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal.checkReplacesInvariant]]
    */
  final def checkReplacesInvariant()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    lastCheckpointO <- latestCheckpointUpTo(Offset.MaxValue, allCheckpointsFilter)
    _ <- lastCheckpointO.fold(FutureUnlessShutdown.unit) {
      case Checkpoint(Timepoint(offsetInclusive), _) =>
        for {
          latestPruningOffsetO <- lookupLatestPruningOffset()
          _ <- party.checkReplacesInvariant(offsetInclusive, latestPruningOffsetO)
          _ <- participant.checkReplacesInvariant(offsetInclusive, latestPruningOffsetO)
        } yield ()
    }
  } yield ()

  /** Truncates all data in the [[AcsDigestStore]] and possibly in all [[AcsDigestStore]]s of the
    * given node.
    *
    * This method is unsafe in that the operation may block until a concurrently running DB backup
    * has finished. It should therefore be only used in special circumstances.
    */
  final def truncateAllBlocking()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- truncateLatestPrune()
      _ <- truncateCheckpoints()
      _ <- party_.truncateAll()
      _ <- participant_.truncateAll()
    } yield ()

  protected def truncateCheckpoints()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  @inline private[store] final def truncateCheckpointsInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    truncateCheckpoints()

  protected def truncateLatestPrune()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  @inline private[store] final def truncateLatestPruneInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    truncateLatestPrune()
}

object AcsDigestStore {

  /** Maintains a key-value journal for keys `K` and values `V` indexed by
    * [[com.digitalasset.canton.data.Offset]] as part of an [[AcsDigestStore]].
    */
  trait DigestJournal[K] {

    protected def executionContext: ExecutionContext

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
        digests: immutable.Iterable[AcsDigestUpdate[K]]
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

    /** Returns the latest entry for the given key up to the given offset (inclusive), if any.
      */
    def lookup(
        key: K,
        toInclusive: Offset,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[AcsDigestUpdate[K]]]

    /** Returns the latest entry for each of the given keys up to the given offset (inclusive). Keys
      * without entries do not appear in the map.
      */
    def bulkLookup(
        keys: immutable.Iterable[K],
        toInclusive: Offset,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[K, AcsDigestUpdate[K]]] =
      bulkLookup(keys.map(key => key -> toInclusive)).map(_.map { case ((key, _), update) =>
        key -> update
      })(executionContext)

    def bulkLookup(keysUptoInclusive: immutable.Iterable[(K, Offset)])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Map[(K, Offset), AcsDigestUpdate[K]]]

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
          immutable.Iterable[AcsDigestUpdate[K]],
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
          immutable.Iterable[AcsDigest[K]],
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
      *   1. `rt_2` is at least the `latestPruningOffset` if defined.
      *
      * @return
      *   a future failed with [[java.lang.IllegalStateException]] if the chaining is incorrect for
      *   any key.
      */
    @VisibleForTesting
    def checkReplacesInvariant(upToInclusive: Offset, latestPruningOffset: Option[Offset])(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit]
  }

  object DigestJournal {

    /** Processes all active digest updates from a
      * [[com.digitalasset.canton.participant.store.AcsDigestStore.DigestJournal]] in paginated
      * batches, starting at an inclusive offset.
      *
      * Uses monadic tail-recursion (`tailRecM`) to page through journal snapshots sequentially
      * while capping memory usage and supporting an error channel via `EitherT`.
      *
      * @param journal
      *   The digest journal (party or participant) to read snapshots from.
      * @param startAtInclusive
      *   The starting offset from which to begin reading snapshots (inclusive).
      * @param pageSize
      *   The maximum number of digest updates to load per paginated database query.
      * @param processBatch
      *   The async operation returning a [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]]
      *   to apply to each paginated batch of digest updates.
      * @tparam K
      *   The key type of the journal (e.g., `PartyAndOrder` or `InternedParticipantId`).
      * @tparam V
      *   The value type stored in the journal (e.g., `RawDigest` or `(RawDigest, HashedDigest)`).
      * @tparam E
      *   The error type handled within the `Either` error channel.
      * @return
      *   A [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]] that completes with an error
      *   or unit when all batches have been processed.
      */
    def processSnapshotInBatchesE[K, E](journal: AcsDigestStore.DigestJournal[K])(
        startAtInclusive: Offset,
        pageSize: Int,
    )(
        processBatch: immutable.Iterable[AcsDigestUpdate[K]] => FutureUnlessShutdown[Unit]
    )(implicit
        traceContext: TraceContext,
        ec: ExecutionContext,
    ): FutureUnlessShutdown[Unit] = {
      type LoopState = Either[journal.SnapshotPaginationToken, journal.AtInclusive]
      val initialState: LoopState = Right(startAtInclusive)

      Monad[FutureUnlessShutdown].tailRecM[LoopState, Unit](initialState) { currentState =>
        journal
          .snapshot(currentState, pageSize)
          .flatMap { case (acsDigestUpdates, doneOrNextToken) =>
            processBatch(acsDigestUpdates).map { _ =>
              doneOrNextToken match {
                case Left(_paginationTokenDone) => Right(())
                case Right(nextToken) => Left(Left(nextToken))
              }
            }
          }
      }
    }
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
  final case class AcsDigest[+K](
      key: K,
      offset: Offset,
      timestamp: CantonTimestamp,
      digestO: Option[RawDigest],
      trace: Option[AcsDigestTrace],
  ) {

    /** Returns the timepoint of the ACS digest.
      *
      * We keep `offset` and `timestamp` as separate fields in this case class because this class
      * serves as the DTO for storage and we do not want to omit `timestamp` from the `==` checks
      * for DTOs.
      */
    def timepoint: Timepoint = Timepoint(offset)(timestamp)

    def map[L](f: K => L): AcsDigest[L] = copy(key = f(key))

    def partitionMap[K1, K2](f: K => Either[K1, K2]): Either[AcsDigest[K1], AcsDigest[K2]] =
      f(key).bimap(k1 => copy(key = k1), k2 => copy(key = k2))
  }

  trait AcsDigestCompanion {
    def apply[K](
        key: K,
        timepoint: Timepoint,
        digestO: Option[RawDigest],
        trace: Option[AcsDigestTrace],
    ): AcsDigest[K] = AcsDigest(key, timepoint.offset, timepoint.recordTime, digestO, trace)

    def empty[K](key: K, timepoint: Timepoint): AcsDigest[K] =
      AcsDigest(key, timepoint, None, None)
  }
  object AcsDigest extends AcsDigestCompanion {
    implicit def getAcsDigest[K: GetResult]: GetResult[AcsDigestStore.AcsDigest[K]] = GetResult {
      pr =>
        val key = pr.<<[K]
        val offset = pr.<<[Offset]
        val timestamp = pr.<<[CantonTimestamp]
        val digestO = pr.nextBytesOption().map(ByteString.copyFrom)
        val trace = pr.<<[Option[AcsDigestTrace]]
        AcsDigestStore.AcsDigest(key, offset, timestamp, digestO, trace)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit def setParameterAcsDigest[K: SetParameter]: SetParameter[AcsDigestStore.AcsDigest[K]] =
      SetParameter { (digest, pp) =>
        pp >> digest.key
        pp >> digest.offset
        pp >> digest.timestamp
        pp.setBytes(digest.digestO.map(_.toByteArray).orNull)
        pp >> digest.trace
      }
  }

  /** Represents an update to the running digest of the shared active contract for a key at a given
    * offset, together with a by-offset reference to the entry it replaces, if any.
    */
  final case class AcsDigestUpdate[+K](
      digestUpdate: AcsDigest[K],
      replacesOffset: Option[Offset],
  ) {
    def map[L](f: K => L): AcsDigestUpdate[L] = copy(digestUpdate = digestUpdate.map(f))

    def partitionMap[K1, K2](
        f: K => Either[K1, K2]
    ): Either[AcsDigestUpdate[K1], AcsDigestUpdate[K2]] =
      digestUpdate
        .partitionMap(f)
        .bimap(d1 => copy(digestUpdate = d1), d2 => copy(digestUpdate = d2))
  }

  trait AcsDigestUpdateCompanion {
    def apply[K](
        digestUpdate: AcsDigest[K],
        replacesOffset: Option[Offset],
    ): AcsDigestUpdate[K] = new AcsDigestUpdate(digestUpdate, replacesOffset)

    def empty[K, V](key: K, timepoint: Timepoint): AcsDigestUpdate[K] =
      AcsDigestUpdate(AcsDigest.empty(key, timepoint), None)
  }
  object AcsDigestUpdate extends AcsDigestUpdateCompanion {
    implicit def getAcsDigestUpdate[K: GetResult]: GetResult[AcsDigestStore.AcsDigestUpdate[K]] =
      GetResult { pr =>
        val digestUpdate = AcsDigest.getAcsDigest[K].apply(pr)
        val replacesOffset = pr.<<[Option[Offset]]
        AcsDigestStore.AcsDigestUpdate(digestUpdate, replacesOffset)
      }
  }

  /** Must always be 2048 long as long as we use [[com.digitalasset.canton.crypto.LtHash16]].
    */
  // May be refined by a proper type later in the future
  type RawDigest = ByteString

  /** Represents the SHA256 hash of a raw digest */
  type HashedDigest = ByteString

  type PartyAcsDigest[+Party] = AcsDigest[Party]
  object PartyAcsDigest extends AcsDigestCompanion {
    def internalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigest[LfPartyId],
    ): PartyAcsDigest[InternedPartyId] = pad.map(stringInterning.party.internalize)

    def externalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigest[InternedPartyId],
    ): PartyAcsDigest[LfPartyId] = pad.map(stringInterning.party.externalize)
  }

  type PartyAcsDigestUpdate[+Party] = AcsDigestUpdate[Party]
  object PartyAcsDigestUpdate extends AcsDigestUpdateCompanion {
    def internalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigestUpdate[LfPartyId],
    ): PartyAcsDigestUpdate[InternedPartyId] =
      pad.map(stringInterning.party.internalize)

    def externalize(
        stringInterning: StringInterning,
        pad: PartyAcsDigestUpdate[InternedPartyId],
    ): PartyAcsDigestUpdate[LfPartyId] =
      pad.map(stringInterning.party.externalize)
  }

  type InternedParticipantId = Int

  type ParticipantAcsDigestUpdate[+Participant] = AcsDigestUpdate[Participant]
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
  final case class Checkpoint(timepoint: Timepoint, checkpointType: CheckpointType)
      extends PrettyPrintingFromCompanion {
    def offset: Offset = timepoint.offset
    def recordTime: CantonTimestamp = timepoint.recordTime

    override def prettyCompanion: PrettyPrintingCompanion[Checkpoint] = Checkpoint
  }

  object Checkpoint extends PrettyPrintingCompanion[Checkpoint] {
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

    val pretty: Pretty[Checkpoint] = prettyOfClass(
      param("offset", _.offset),
      param("recordTime", _.recordTime),
      param("type", _.checkpointType),
    )

  }

  /** Describes the trigger of the checkpoint.
    */
  final case class CheckpointType private (id: Int)(val isTickCheckpoint: Boolean)
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
    def apply(id: Int, description: String, isTickCheckpoint: Boolean): CheckpointType = {
      val checkpointType = new CheckpointType(id)(isTickCheckpoint)
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
      CheckpointType(1, "ReconciliationIntervalBoundary", isTickCheckpoint = true)

    /** When an affirmation interval boundary has been crossed.
      */
    val AffirmationIntervalBoundary: CheckpointType =
      CheckpointType(2, "AffirmationIntervalBoundary", isTickCheckpoint = true)

    /** When a certain number of events have been processed without writing a checkpoint.
      */
    val MaxEventsWithoutCheckpoint: CheckpointType =
      CheckpointType(3, "MaxEventsWithoutCheckpoint", isTickCheckpoint = false)

    /** When the hosting relation between a party and a participant have changed.
      */
    val PartyHostingChange: CheckpointType =
      CheckpointType(4, "PartyHostingChange", isTickCheckpoint = false)

    /** When a reinitialization has completed.
      */
    val Reinitialization: CheckpointType =
      CheckpointType(5, "Reinitialization", isTickCheckpoint = false)

    /** When a checkpoint at the timestamp of a received commitment has been emitted to signal the
      * progression of offsets on an idle stream
      */
    val ReceivedCommitmentCheckpoint: CheckpointType =
      CheckpointType(6, "ReceivedCommitmentCheckpoint", isTickCheckpoint = false)

    // NOTICE: when adding a new checkpoint type, the debug.checkpoint_type function
    // needs to be re-created in a new SQL migration file with the added checkpoint.

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

    implicit val checkpointTypeToDbPrimitive: ToDbPrimitive[CheckpointType, Int] =
      ToDbPrimitive(_.id)
    implicit val checkpointTypeGetResult: GetResult[CheckpointType] =
      GetResult { rs =>
        val dbInt = rs.<<[Int]
        tryFromId(dbInt)
      }
  }

  val checkpointReconciliationFilter: Option[NonEmpty[Set[CheckpointType]]] =
    Some(NonEmpty(Set, CheckpointType.ReconciliationIntervalBoundary))
  val checkpointReinitializationFilter: Option[NonEmpty[Set[CheckpointType]]] =
    Some(NonEmpty(Set, CheckpointType.Reinitialization))
  val checkpointTickFilter: Option[NonEmpty[Set[CheckpointType]]] =
    Some(NonEmptyUtil.fromUnsafe(CheckpointType.all.filter(_.isTickCheckpoint)))
  def allCheckpointsFilter: Option[NonEmpty[Set[CheckpointType]]] = None
}
