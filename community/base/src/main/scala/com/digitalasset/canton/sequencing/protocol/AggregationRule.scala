// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{HashBuilder, Signature, SigningKeyUsage, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionedCompanionDbHelpers,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

/** Collecting the signatures by a sender for an inflight aggregation per envelop
  *
  * This is the "value" class in a key-value pair.
  *
  * @param sequencingTimestamp
  *   when the signatures were added to the aggregation
  * @param signatures
  *   the signatures of the sender, one for each envelope of the submission. Multiple signatures are
  *   possible but not expected.
  */
final case class AggregationBySender(
    sequencingTimestamp: CantonTimestamp,
    signatures: Seq[Seq[Signature]],
) extends PrettyPrinting {
  override protected def pretty: Pretty[this.type] = AggregationBySender.prettyInstance
}
object AggregationBySender {
  private val prettyInstance: Pretty[AggregationBySender] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass(
      param("sequencing timestamp", _.sequencingTimestamp),
      param("signatures", _.signatures),
    )
  }
}

/** Raw aggregation rule shipped on the wire
  *
  * Before PV35, we used to ship a lot of UIDs as part of our aggregation rules. With PV35, we
  * stopped this and group recipients such that we can minimize the payload (as verdict payloads
  * seemed to be ~ 2/3 made out of aggregation rules)
  *
  * This however means that we need to resolve the aggregation rules against the topology snapshot.
  * Now, which snapshot we use doesn't matter much:
  *   - If a member is added racily to a group, it will not be able to participate in the
  *     aggregation as it has not received the original request.
  *   - If a member is removed, then the member will not be able to submit their aggregation anyway.
  *
  * This means whatever happens racily, we check everything against the topology snapshot at the
  * time when we check if the aggregation is completed.
  *
  * What we don't do is to trigger a delivery of an aggregations based message on a topology change.
  * So only signed submissions can trigger the completion of an aggregation, not a topology
  * transaction.
  */
sealed trait AggregationRuleInput extends PrettyPrinting {

  def resolveToMembers(sender: Member, snapshot: TopologySnapshot)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, AggregationRuleInput.Resolved]

  /** Append info on this aggregation rule to the hash builder deterministically
    *
    * @param sender
    *   the sender of the submission request. This is only used to compute the aggregation id for
    *   sender deduplication rules, where we skip adding the same UID again to the request
    *   aggregation rule (so we interpret an empty list of eligible senders as "the sender only".
    */
  def appendForAggregationId(builder: HashBuilder, sender: Member): Unit

  /** Will compute whether the aggregation is completed
    * @param aggregatedSignatures
    *   the signatures that have been aggregated for this aggregation rule so far. Note that these
    *   signatures might no longer be valid, so we need to check them against the snapshot as well
    *   if we have enough signatures to meet the threshold.
    * @param snapshot
    *   must be the snapshot of the latest aggregation added
    * @return
    *   returns an option which is set if the aggregation completed, containing the sequencing
    *   timestamp and the remaining valid signatures (as some might have been removed because they
    *   are no longer valid)
    */
  def computeDeliveredAt(
      aggregatedSignatures: SortedMap[Member, AggregationBySender],
      snapshot: SyncCryptoApi,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]]

  def checkInvariant(
      aggregatedSenders: Map[Member, AggregationBySender],
      maxSequencingTimestamp: CantonTimestamp,
  ): Either[String, Unit] = {
    val envelopeCounts = aggregatedSenders.values.map(_.signatures.size).toSet
    for {
      _ <- Either.cond(
        envelopeCounts.sizeIs <= 1,
        (),
        show"aggregated senders have varying numbers of envelopes: $envelopeCounts",
      )
      lateSenders = aggregatedSenders.collect {
        case (sender, aggregationBySender)
            if aggregationBySender.sequencingTimestamp > maxSequencingTimestamp =>
          sender -> aggregationBySender.sequencingTimestamp
      }
      _ <- Either.cond(
        lateSenders.isEmpty,
        (),
        show"aggregated senders' sequencing timestamp is after the max sequencing time at $maxSequencingTimestamp: $aggregatedSenders",
      )
    } yield ()
  }

}

object AggregationRuleInput extends HasLoggerName {

  // Resolved doesn't have a magic number as it produces the same aggregation
  // id as before on pv34. Note that the magic numbers are not really necessary,
  // as the aggregation id is hashing the envelope.
  private val magicNumberMediatorGroup = 0x01
  private val magicNumberSequencerGroup = 0x02
  private val magicNumberSenderDedup = 0x03

  final case class Resolved(
      eligibleSenders: NonEmpty[Seq[Member]],
      threshold: PositiveInt,
  ) extends AggregationRuleInput {

    override def resolveToMembers(sender: Member, snapshot: TopologySnapshot)(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, String, AggregationRuleInput.Resolved] = EitherT.rightT(this)

    override protected def pretty: Pretty[Resolved] = prettyResolved

    def appendForAggregationId(builder: HashBuilder, sender: Member): Unit = {
      builder.addInt(eligibleSenders.size)
      eligibleSenders.foreach(member => builder.addString(member.toProtoPrimitive))
      builder.addInt(threshold.value)
    }

    /** The sequencing timestamp at which this aggregatable submission was delivered, if so. */
    override def computeDeliveredAt(
        aggregatedSignatures: SortedMap[Member, AggregationBySender],
        snapshot: SyncCryptoApi,
    )(implicit
        loggingContext: NamedLoggingContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]] =
      FutureUnlessShutdown.pure(
        Option
          .when(aggregatedSignatures.sizeCompare(threshold.value) >= 0)(
            aggregatedSignatures.values
              .map(_.sequencingTimestamp)
              // tryAggregate will reject any addition of signatures after the event is delivered.
              // This means that the max sequencing timestamp is the one when we shipped the event.
              .maxOption
              .map((_, aggregatedSignatures))
          )
          .flatten
      )

    override def checkInvariant(
        aggregatedSenders: Map[Member, AggregationBySender],
        maxSequencingTimestamp: CantonTimestamp,
    ): Either[String, Unit] = {
      val uneligibleAggregated = aggregatedSenders.keys.filterNot(eligibleSenders.contains)
      for {
        _ <- Either.cond(
          uneligibleAggregated.isEmpty,
          (),
          show"non-eligible members' submission requests have been aggregated: ${uneligibleAggregated.toSeq}",
        )
        _ <- super.checkInvariant(aggregatedSenders, maxSequencingTimestamp)
      } yield ()
    }
  }

  private val prettyResolved: Pretty[Resolved] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass[Resolved](
      param("threshold", _.threshold),
      param("eligible members", _.eligibleSenders),
    )
  }

  final case class MediatorGroup(index: NonNegativeInt) extends AggregationRuleInput {
    override def resolveToMembers(sender: Member, snapshot: TopologySnapshot)(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, String, AggregationRuleInput.Resolved] =
      EitherT(
        snapshot
          .mediatorGroup(index)
          .map(_.flatMap { group =>
            NonEmpty.from(group.active).map((_, group))
          })
          .map(
            _.toRight(s"Mediator group with index $index does not exist at ${snapshot.timestamp}")
              .map { case (active, group) =>
                Resolved(
                  eligibleSenders = active,
                  threshold = group.threshold,
                )
              }
          )
      )

    override protected def pretty: Pretty[MediatorGroup] = prettyMediatorGroup

    def appendForAggregationId(builder: HashBuilder, sender: Member): Unit = {
      builder.addInt(magicNumberMediatorGroup)
      builder.addInt(index.value)
    }

    def computeDeliveredAt(
        aggregatedSignatures: SortedMap[Member, AggregationBySender],
        snapshot: SyncCryptoApi,
    )(implicit
        loggingContext: NamedLoggingContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]] =
      snapshot.ipsSnapshot.mediatorGroup(index)(loggingContext.traceContext).flatMap {
        case None => FutureUnlessShutdown.pure(None)
        case Some(group) =>
          validateSignaturesAtThresholdAndTimestamp(
            aggregatedSignatures = aggregatedSignatures,
            eligibleSenders = group.active,
            snapshot = snapshot,
            threshold = group.threshold,
          )
      }

  }

  private val prettyMediatorGroup: Pretty[MediatorGroup] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfClass[MediatorGroup](
      param("index", _.index)
    )
  }

  /** Validates sender eligibility and cryptographic signatures for a set of aggregations.
    *
    * This method performs a multi-step validation to ensure that a batch of aggregations meets the
    * required threshold for delivery. It enforces the following rules:
    *   1. Senders must be present in the `eligibleSenders` list (e.g., not offboarded).
    *   1. Signatures must be cryptographically valid according to the provided `snapshot`.
    *   1. If any individual envelope within a sender's aggregation loses all of its valid
    *      signatures, that sender's entire aggregation is discarded.
    *
    * @note
    *   To avoid the performance overhead of deeply nested asynchronous traversals, this
    *   implementation uses a flatten-and-rebuild strategy: it extracts unique signatures into a
    *   flat list, runs cryptographic verifications in a single parallel batch, and synchronously
    *   reconstructs the nested aggregations using an O(1) Set lookup.
    *
    * @param aggregatedSignatures
    *   The raw map of senders to their respective aggregations.
    * @param eligibleSenders
    *   The list of members currently active/eligible in the topology.
    * @param snapshot
    *   The cryptographic snapshot used to verify signature key usage.
    * @param threshold
    *   The minimum number of valid sender aggregations required.
    * @return
    *   A Future containing the maximum sequencing timestamp and the pruned map of valid
    *   aggregations. Returns `None` if the final number of valid aggregations falls below the
    *   `threshold`.
    */
  private def validateSignaturesAtThresholdAndTimestamp(
      aggregatedSignatures: SortedMap[Member, AggregationBySender],
      eligibleSenders: Seq[Member],
      snapshot: SyncCryptoApi,
      threshold: PositiveInt,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]] =
    // Immediately return if we don't have enough aggregations to meet the threshold.
    if (aggregatedSignatures.sizeIs < threshold.value) {
      FutureUnlessShutdown.pure(None)
    } else {
      ErrorUtil.requireState(
        aggregatedSignatures.values
          .maxByOption(_.sequencingTimestamp)
          .map(_.sequencingTimestamp)
          .contains(snapshot.ipsSnapshot.timestamp),
        s"Snapshot does not align with aggregations? ${snapshot.ipsSnapshot.timestamp} vs ${aggregatedSignatures.values
            .map(_.sequencingTimestamp)}",
      )

      val eligibleMembersSet = eligibleSenders.toSet
      val eligibleAggregations: List[(Member, AggregationBySender)] = aggregatedSignatures.view
        // Keep only senders that are still eligible (e.g., filter out offboarded mediators).
        .filter { case (member, _) => eligibleMembersSet.contains(member) }.toList

      // `.distinct` omitted because Members are already unique (originating Map), and a single member's signatures
      // are almost universally distinct since they cover different envelopes. Overhead of checking for uniqueness
      // outweighs the cost of redundantly validating a rare duplicate signature.
      val signaturesToVerify: List[(Member, Signature)] = eligibleAggregations.iterator.flatMap {
        case (member, agg) => agg.signatures.flatten.map(member -> _)
      }.toList

      // TODO(#33650) - replace with unboundedTraverseFilter; safe to run unbounded for now as the flat list is
      //  bounded by topology (eligible members) * payload size (envelopes) * signatures per envelope. And because the
      //  eligible members are currently mediator/sequencer groups this is a small number
      val validSignatureSetF = signaturesToVerify
        .parTraverseFilter { case (member, signature) =>
          // filter out all signatures that are no longer valid
          snapshot
            .verifyKeyUsage(
              member,
              signature.authorizingLongTermKey,
              signature.signatureDelegation,
              usage = SigningKeyUsage.ProtocolOnly,
            )(loggingContext.traceContext)
            .value
            .map {
              case Right(()) => Some((member, signature))
              case Left(_) =>
                loggingContext.info(
                  s"Signature of member $member for aggregation is no longer valid at ${snapshot.ipsSnapshot.timestamp} and will not be carried forward: $signature"
                )
                None
            }
        }
        .map(_.toSet)

      validSignatureSetF.map { validSignatureSet =>
        val validAggregations = eligibleAggregations.flatMap { case (member, agg) =>
          val allEnvelopesWithSignaturesO =
            agg.signatures.traverse { sigs =>
              val filtered = sigs.filter(sig => validSignatureSet.contains(member -> sig))
              Option.when(filtered.nonEmpty)(filtered)
            }

          // If any envelope loses all of its valid signatures, we cannot include this sender's aggregations anymore.
          // Normally, the same key is used for all envelopes, but this is not guaranteed.
          allEnvelopesWithSignaturesO.map(validEnvelopes =>
            member -> agg.copy(signatures = validEnvelopes)
          )
        }

        // Finally, return the result if we still have enough valid aggregations to meet the threshold,
        // together with the pruned signatures that should be included in the delivery.
        if (validAggregations.sizeIs >= threshold.value) {
          validAggregations
            .maxByOption(_._2.sequencingTimestamp)
            .map(_._2.sequencingTimestamp)
            .map((_, SortedMap.from(validAggregations)))
        } else {
          loggingContext.info(
            s"Not enough valid aggregations (had=${aggregatedSignatures.size}, valid=${validAggregations.size}, threshold=${threshold.value}) to meet the threshold, cannot deliver yet"
          )
          None
        }
      }
    }

  case object SequencerGroup extends AggregationRuleInput {
    override def resolveToMembers(sender: Member, snapshot: TopologySnapshot)(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, String, AggregationRuleInput.Resolved] =
      EitherT(
        snapshot
          .sequencerGroup()
          .map(_.flatMap { group =>
            NonEmpty.from(group.active).map((_, group))
          })
          .map(_.toRight(s"No sequencers at ${snapshot.timestamp}").map { case (active, group) =>
            Resolved(
              eligibleSenders = active,
              threshold = group.threshold,
            )
          })
      )
    override protected def pretty: Pretty[SequencerGroup.type] = prettySequencerGroup
    def appendForAggregationId(builder: HashBuilder, sender: Member): Unit =
      builder.addInt(magicNumberSequencerGroup)

    def computeDeliveredAt(
        aggregatedSignatures: SortedMap[Member, AggregationBySender],
        snapshot: SyncCryptoApi,
    )(implicit
        loggingContext: NamedLoggingContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]] =
      snapshot.ipsSnapshot.sequencerGroup()(loggingContext.traceContext).flatMap {
        case None => FutureUnlessShutdown.pure(None)
        case Some(group) =>
          validateSignaturesAtThresholdAndTimestamp(
            aggregatedSignatures = aggregatedSignatures,
            eligibleSenders = group.active,
            snapshot = snapshot,
            threshold = group.threshold,
          )
      }

  }
  private val prettySequencerGroup: Pretty[SequencerGroup.type] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfObject[SequencerGroup.type]
  }
  case object SenderDedup extends AggregationRuleInput {
    override def resolveToMembers(sender: Member, snapshot: TopologySnapshot)(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, String, AggregationRuleInput.Resolved] =
      EitherT.rightT(
        Resolved(
          eligibleSenders = NonEmpty.mk(Seq, sender),
          threshold = PositiveInt.one,
        )
      )
    override protected def pretty: Pretty[SenderDedup.type] = prettySenderDedup
    def appendForAggregationId(builder: HashBuilder, sender: Member): Unit = {
      builder.addInt(magicNumberSenderDedup)
      builder.addString(sender.toProtoPrimitive)
    }
    def computeDeliveredAt(
        aggregatedSignatures: SortedMap[Member, AggregationBySender],
        snapshot: SyncCryptoApi,
    )(implicit
        loggingContext: NamedLoggingContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]] =
      FutureUnlessShutdown.pure(aggregatedSignatures.headOption.map { case (member, aggregation) =>
        // we do not need to check the signatures here as we will perform this
        // computation immediately when receiving the event, so the signatures must be valid at this point.
        // Also, we only have one sender in this aggregation rule, so we can just take the head of the map
        (aggregation.sequencingTimestamp, SortedMap(member -> aggregation))
      })

    override def checkInvariant(
        aggregatedSenders: Map[Member, AggregationBySender],
        maxSequencingTimestamp: CantonTimestamp,
    ): Either[String, Unit] =
      for {
        _ <- super.checkInvariant(aggregatedSenders, maxSequencingTimestamp)
        _ <- Either.cond(
          aggregatedSenders.sizeIs <= 1,
          (),
          s"Multiple senders have been aggregated for a sender deduplication rule: ${aggregatedSenders.keySet}",
        )
      } yield ()

  }

  private val prettySenderDedup: Pretty[SenderDedup.type] = {
    import com.digitalasset.canton.logging.pretty.PrettyUtil.*
    prettyOfObject[SenderDedup.type]
  }

}

/** Encodes the conditions on when an aggregatable submission request's envelopes are sequenced and
  * delivered.
  *
  * Aggregatable submissions are grouped by their [[SubmissionRequest.aggregationId]]. An
  * aggregatable submission's envelopes are delivered to their recipients when the
  * [[AggregationRuleInput.Resolved.threshold]]-th submission request in its group has been
  * sequenced. The aggregatable submission request that triggers the threshold defines the
  * sequencing timestamp (and thus the sequencer counters) for all delivered envelopes. The sender
  * of an aggregatable submission request receives a receipt of delivery immediately when its
  * request was sequenced, not when its envelopes were delivered. When the envelopes are actually
  * delivered, no further delivery receipt is sent.
  *
  * So a threshold of 1 means that no aggregation takes place and the event is sequenced and
  * delivered immediately. In this case, one can completely omit the aggregation rule in the
  * submission request.
  *
  * This is used for sequencer and mediator group addressing as well as for deduplication of
  * requests that might be sent multiple times due to amplifications and retries.
  */
final case class AggregationRule(
    input: AggregationRuleInput
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[AggregationRule.type]
) extends HasProtocolVersionedWrapper[AggregationRule]
    with PrettyPrinting {
  @transient override protected lazy val companionObj: AggregationRule.type = AggregationRule

  private[canton] def toProtoV30: v30.AggregationRule =
    input match {
      case AggregationRuleInput.Resolved(eligibleSenders, threshold) =>
        v30.AggregationRule(
          eligibleMembers = eligibleSenders.map(_.toProtoPrimitive),
          threshold = threshold.value,
        )
      case AggregationRuleInput.MediatorGroup(index) =>
        v30.AggregationRule(
          eligibleMembers = Seq(MediatorGroupRecipient(index).toProtoPrimitive),
          threshold = 0, // ignored
        )
      case AggregationRuleInput.SequencerGroup =>
        v30.AggregationRule(
          eligibleMembers = Seq(SequencersOfSynchronizer.toProtoPrimitive),
          threshold = 0, // ignored
        )
      case AggregationRuleInput.SenderDedup =>
        v30.AggregationRule(
          eligibleMembers = Seq(),
          threshold = 0, // ignored
        )
    }

  override protected def pretty: Pretty[this.type] = prettyOfClass(
    param("input", _.input)
  )

}

// See https://github.com/DACH-NY/canton/pull/32193
private[sequencing] final case class LegacyUseMemberIdsAsEligibleMembers(v: Boolean) extends AnyVal

object LegacyUseMemberIdsAsEligibleMembers {
  def apply(pv: ProtocolVersion): LegacyUseMemberIdsAsEligibleMembers =
    if (pv == ProtocolVersion.v34) LegacyUseMemberIdsAsEligibleMembers(true)
    else LegacyUseMemberIdsAsEligibleMembers(false)
}

object AggregationRule
    extends VersioningCompanionContext[AggregationRule, LegacyUseMemberIdsAsEligibleMembers]
    with ProtocolVersionedCompanionDbHelpers[AggregationRule] {

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.AggregationRule)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  @VisibleForTesting
  def testing(
      eligibleSenders: NonEmpty[Seq[Member]],
      threshold: PositiveInt,
      protocolVersion: ProtocolVersion,
  ): AggregationRule =
    AggregationRule(AggregationRuleInput.Resolved(eligibleSenders, threshold))(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def senderDedup(member: Member, protocolVersion: ProtocolVersion): AggregationRule = {
    // we stopped shipping redundant information with pv35 for speed and glory
    val input: AggregationRuleInput = if (protocolVersion == ProtocolVersion.v34) {
      AggregationRuleInput.Resolved(NonEmpty.mk(Seq, member), threshold = PositiveInt.one)
    } else {
      AggregationRuleInput.SenderDedup
    }
    AggregationRule(input)(protocolVersionRepresentativeFor(protocolVersion))
  }

  def sequencerTimeAdvancingRequest(
      sequencers: NonEmpty[Seq[SequencerId]],
      protocolVersion: ProtocolVersion,
  ): AggregationRule =
    AggregationRule(AggregationRuleInput.Resolved(sequencers, threshold = PositiveInt.one))(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def activeSequencers(
      sequencers: NonEmpty[Seq[SequencerId]],
      threshold: PositiveInt,
      protocolVersion: ProtocolVersion,
  ): AggregationRule = {
    val input: AggregationRuleInput = if (protocolVersion == ProtocolVersion.v34) {
      AggregationRuleInput.Resolved(sequencers, threshold = threshold)
    } else {
      AggregationRuleInput.SequencerGroup
    }
    AggregationRule(input)(protocolVersionRepresentativeFor(protocolVersion))
  }

  def activeMediators(
      mediators: NonEmpty[Seq[MediatorId]],
      groupIndex: MediatorGroupIndex,
      threshold: PositiveInt,
      protocolVersion: ProtocolVersion,
  ): AggregationRule = {
    val input: AggregationRuleInput = if (protocolVersion == ProtocolVersion.v34) {
      AggregationRuleInput.Resolved(mediators, threshold = threshold)
    } else {
      AggregationRuleInput.MediatorGroup(groupIndex)
    }
    AggregationRule(input)(protocolVersionRepresentativeFor(protocolVersion))
  }

  override def name: String = "AggregationRule"

  private[canton] def fromProtoV30(
      useMemberIdsAsEligibleMembers: LegacyUseMemberIdsAsEligibleMembers,
      proto: v30.AggregationRule,
  ): ParsingResult[AggregationRule] = {
    val v30.AggregationRule(eligibleMembersP, thresholdP) = proto

    if (useMemberIdsAsEligibleMembers.v) {
      for {
        eligibleMembers <- ProtoConverter.parseRequiredNonEmpty(
          Member.fromProtoPrimitive(_, "eligible_members"),
          "eligible_members",
          eligibleMembersP,
        )
        threshold <- ProtoConverter.parsePositiveInt("threshold", thresholdP)
        rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      } yield {
        AggregationRule(AggregationRuleInput.Resolved(eligibleMembers, threshold))(rpv)
      }
    } else {
      def ruleFromRecipients(recipients: List[Recipient]): ParsingResult[AggregationRuleInput] =
        recipients match {
          case MediatorGroupRecipient(index) :: Nil =>
            Right(AggregationRuleInput.MediatorGroup(index))
          case SequencersOfSynchronizer :: Nil => Right(AggregationRuleInput.SequencerGroup)
          case Nil => Right(AggregationRuleInput.SenderDedup)
          case other =>
            val members = other.collect { case member: MemberRecipient =>
              member.member
            }
            for {
              membersNE <- NonEmpty
                .from(members)
                .toRight(
                  ProtoDeserializationError.FieldNotSet(
                    s"Sequence eligible_members not set or empty"
                  )
                )
              _ <- Either.cond(
                members.sizeCompare(other) == 0,
                (),
                ProtoDeserializationError.InvariantViolation(
                  "eligible_members",
                  s"Recipients of unequal type in aggregation rule $eligibleMembersP",
                ),
              )
              threshold <- ProtoConverter.parsePositiveInt("threshold", thresholdP)
            } yield AggregationRuleInput.Resolved(membersNE, threshold)
        }

      for {
        recipients <- eligibleMembersP.traverse(Recipient.fromProtoPrimitive(_, "eligible_members"))
        rule <- ruleFromRecipients(recipients.toList)
        rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      } yield AggregationRule(rule)(rpv)
    }
  }

}
