// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.BftOrdererLimitsView
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

import scala.collection.immutable.SortedSet

/** Leaders include, with their proposal of a new block, a strong quorum of signed commit messages
  * that completed the previous block. This is what makes this set canonical and ensures every
  * validator agrees on it, even if it is different from the commits a validator has seen, which
  * caused the validator to commit the respective block. Check the CometBFT documentation for the
  * original concept:
  * https://docs.cometbft.com/v0.37/spec/consensus/consensus#canonical-vs-subjective-commit.
  *
  * A correct node is required to include a non-empty canonical commit set except for:
  *   - each leader's first block in the first epoch
  *   - bottom blocks created due to a segment view change
  *   - a state-transferred node’s first block in the epoch at which it starts participating as a
  *     leader However, it's non-trivial to fully enforce it, so only best-effort validation has
  *     been introduced (see
  *     [[com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.PbftMessageValidatorImpl.validatePrePrepare]]).
  */
final case class CanonicalCommitSet(private val commits: Set[SignedMessage[Commit]]) {
  lazy val sortedCommits: Seq[SignedMessage[Commit]] = SortedSet.from(commits).toSeq

  val timestamps: Seq[CantonTimestamp] = sortedCommits.map(_.message.localTimestamp)

  def toProto: v30.CanonicalCommitSet = v30.CanonicalCommitSet(sortedCommits.map(_.toProtoV1))
}

object CanonicalCommitSet {

  val empty: CanonicalCommitSet = CanonicalCommitSet(Set.empty)

  def fromProto(
      pvv: ProtocolVersionValidation,
      limits: BftOrdererLimitsView,
      canonicalCommitSet: v30.CanonicalCommitSet,
  ): ParsingResult[CanonicalCommitSet] =
    ProtoValidation
      .validateLengthThen(
        canonicalCommitSet.canonicalCommits,
        "canonical_commits",
        pvv,
        ValidationParam(ProtocolVersion.v38 -> limits.maxCanonicalCommits),
      ) { (commit, _) =>
        SignedMessage.fromProtoWithNodeId(pvv, v30.ConsensusMessage, SupportedVersions.ProtoData30)(
          claimedOriginator =>
            Commit.fromProtoConsensusMessage(
              // Since the commits in the canonical commit set are nested messages of PrePrepares,
              //  we set their actual sender to `None` as the sender is set in the outer message
              //  and is unused for nested messages.
              claimedOriginatorAndActualSender = (claimedOriginator, None),
              _,
            )
        )(commit)
      }
      .map(x => CanonicalCommitSet(SortedSet.from(x)))
}
