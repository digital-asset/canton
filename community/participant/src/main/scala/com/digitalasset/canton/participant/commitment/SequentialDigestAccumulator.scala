// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.foldable.*
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{HashOps, HashPurpose, LtHash16Blake3}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.*
import com.digitalasset.canton.participant.digest.{DigestDelta, DigestOperation, DigestOps}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId}

import scala.concurrent.ExecutionContext

/** A digest accumulator processes
  * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor.Classification]]s and
  * updates the affected digests accordingly.
  *
  * This simplistic implementation loads each affected digest from the digest store, updates it, and
  * then immediately writes it back to the store.
  */
class SequentialDigestAccumulator(
    thisLfParticipant: LedgerParticipantId,
    acsDigestStore: AcsDigestStore,
    stringInterning: StringInterning,
    hashOps: HashOps,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit traceContext: TraceContext, ec: ExecutionContext)
    extends NamedLogging {

  def process(
      input: ProcessingContext[CheckpointFenceOr[Classification]]
  ): FutureUnlessShutdown[Option[CheckpointWritten]] =
    // for now use the offset as the tiebreaker
    input match {
      case ProcessingContext(_, _, CheckpointFence) =>
        acsDigestStore
          .insertCheckpointTime(input.offset, input.recordTime)
          .map(_ => Some(CheckpointWritten(input.recordTime, input.offset)))

      case ProcessingContext(_, _, NotCheckpointFence(_, classification)) =>
        classification match {
          case update: AcsUpdate =>
            val deltas = DigestOps.computeDeltas(thisLfParticipant, update)
            MonadUtil
              .sequentialTraverse_(deltas) {
                case DigestDelta.Party(partyAndOrder, digestDelta, operation) =>
                  updateDigest(acsDigestStore.party)(
                    input.offset,
                    input.recordTime,
                    partyAndOrder.map(stringInterning.party.internalize),
                    digestDelta,
                    operation,
                  )(
                    LtHash16Blake3.tryCreate,
                    _.getByteString,
                  )

                case DigestDelta.Participant(participant, digestDelta, operation) =>
                  updateDigest(acsDigestStore.participant)(
                    input.offset,
                    input.recordTime,
                    stringInterning.participantId.internalize(participant),
                    digestDelta,
                    operation,
                  )(
                    { case (digest, _hash) => LtHash16Blake3.tryCreate(digest) },
                    digest =>
                      (
                        digest.getByteString,
                        // calculate the hash of the digest
                        hashOps
                          .digest(HashPurpose.HashedAcsCommitment, digest.getByteString, Sha256)
                          .getCryptographicEvidence,
                      ),
                  )

              }
              .map(_ => None)

          case PartyAddedToParticipant(party, participant) =>
            handleTopologyChange(
              input.offset,
              input.recordTime,
              party,
              participant,
              isAddition = true,
            ).map(_ => None)

          case PartyRemovedFromParticipant(party, participant) =>
            handleTopologyChange(
              input.offset,
              input.recordTime,
              party,
              participant,
              isAddition = false,
            ).map(_ => None)

          case PartyOnboardingToParticipant(party, participant) =>
            FutureUnlessShutdown.pure(None)
        }
    }

  /** Generic logic for updating the digest for a party or participant.
    */
  private def updateDigest[Key, V](journal: DigestJournal[Key, V])(
      offset: Offset,
      recordTime: CantonTimestamp,
      key: Key,
      update: LtHash16Blake3,
      operation: DigestOperation,
  )(
      toLtHash16Blake3: V => LtHash16Blake3,
      toV: LtHash16Blake3 => V,
  ): FutureUnlessShutdown[Unit] =
    journal
      .lookup(key, offset)
      .flatMap { acsDigestUpdateO =>
        val (existingDigestO, existingOffsetO) = acsDigestUpdateO.flatMap { digest =>
          digest.digestUpdate.digestO
            .map { rawDigest =>
              val replacesOffset =
                // if the offset of the digest update from the store is the same as the offset currently being processed,
                // then this is another update to the digest at the same offset and we need to retain the "replaces_offset" value.
                if (digest.digestUpdate.offset == offset) digest.replacesOffset
                // otherwise, this update is a new link in the replacement chain.
                else Some(digest.digestUpdate.offset)
              toLtHash16Blake3(rawDigest) -> replacesOffset
            }
        }.unzip

        val updatedDigest = existingDigestO.getOrElse(LtHash16Blake3.empty)
        operation match {
          case DigestOperation.Add =>
            updatedDigest.union(update)
          case DigestOperation.Remove =>
            updatedDigest.removeAll(update)
        }
        if (existingDigestO.isEmpty && updatedDigest.isEmpty) {
          // if there was no previous journal entry and the computed digest is empty,
          // then there's no need to store anything
          FutureUnlessShutdown.unit
        } else {
          journal.upsertDigestUpdates(
            Seq(
              AcsDigestUpdate(
                AcsDigest(key, offset, recordTime, Some(toV(updatedDigest))),
                replacesOffset = existingOffsetO.flatten,
              )
            )
          )
        }
      }

  /** Adding a party to a participant means the party's digest needs to be added to the
    * participant's digest.
    *
    * Removing a party from a participant means the party's digest needs to be removed from the
    * participant's digest.
    */
  private def handleTopologyChange(
      offset: Offset,
      recordTime: CantonTimestamp,
      party: LfPartyId,
      participant: LedgerParticipantId,
      isAddition: Boolean,
  ): FutureUnlessShutdown[Unit] = {
    val internedPid = stringInterning.participantId.internalize(participant)
    val partyKey = PartyAndOrder(
      stringInterning.party.internalize(party),
      if (participant < thisLfParticipant) LocalPartyFirst else RemotePartyFirst,
    )
    for {
      partyDigestUpdateO <- acsDigestStore.party.lookup(partyKey, offset)
      nonEmptyPartyDigestO = partyDigestUpdateO
        .flatMap(_.digestUpdate.digestO)
        .map(LtHash16Blake3.tryCreate)
        .filter(!_.isEmpty)

      // only update the participant hash if there is a non-empty party hash
      _ <- nonEmptyPartyDigestO.traverse_(partyDigest =>
        updateDigest(acsDigestStore.participant)(
          offset,
          recordTime,
          internedPid,
          update = partyDigest,
          if (isAddition) DigestOperation.Add else DigestOperation.Remove,
        )(
          { case (digest, _hash) => LtHash16Blake3.tryCreate(digest) },
          digest =>
            (
              digest.getByteString,
              // calculate the hash of the digest
              hashOps
                .digest(HashPurpose.HashedAcsCommitment, digest.getByteString, Sha256)
                .getCryptographicEvidence,
            ),
        )
      )
    } yield ()
  }
}
