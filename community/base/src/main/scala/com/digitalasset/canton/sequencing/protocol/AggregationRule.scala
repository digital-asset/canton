// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{HashBuilder, Signature, SigningKeyUsage, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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

  private def validateSignaturesAtThresholdAndTimestamp(
      aggregatedSignatures: SortedMap[Member, AggregationBySender],
      eligibleSenders: Seq[Member],
      snapshot: SyncCryptoApi,
      threshold: PositiveInt,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Option[(CantonTimestamp, SortedMap[Member, AggregationBySender])]] =
    // immediately return if we don't have enough signatures at all
    if (aggregatedSignatures.sizeIs < threshold.value) {
      FutureUnlessShutdown.pure(None)
    } else {
      ErrorUtil.requireState(
        aggregatedSignatures.values
          .map(_.sequencingTimestamp)
          .maxOption
          .contains(snapshot.ipsSnapshot.timestamp),
        s"snapshot does not align with aggregations? ${snapshot.ipsSnapshot.timestamp} vs ${aggregatedSignatures.values
            .map(_.sequencingTimestamp)}",
      )
      val eligibleMembersSet = eligibleSenders.toSet
      val cleanedUpSignaturesF = aggregatedSignatures.view
        // filter out all senders that are not eligible anymore (e.g. mediator
        // got offboarded)
        .filter { case (member, _) =>
          eligibleMembersSet.contains(member)
        }
        .toSeq
        .parTraverseFilter { case (member, aggregationBySender) =>
          aggregationBySender.signatures
            .parTraverse { envelopeSignatures =>
              // filter out all signatures that are no longer valid
              envelopeSignatures.parTraverseFilter { signature =>
                snapshot
                  .verifyKeyUsage(
                    member,
                    signature.authorizingLongTermKey,
                    signature.signatureDelegation,
                    usage = SigningKeyUsage.ProtocolOnly,
                  )(loggingContext.traceContext)
                  .value
                  .map {
                    case Right(()) => Some(signature)
                    case Left(_) =>
                      loggingContext.info(
                        s"Signature of member $member for aggregation is no longer valid at ${snapshot.ipsSnapshot.timestamp} and will not be carried forward: $signature"
                      )
                      None
                  }
              }
            }
            .map {
              // if we do not have enough signatures left for all envelopes, we cannot include this senders
              // aggregations anymore. Normally, the same key will be used for all envelopes, but this is
              // not guaranteed.
              case cleanedSignatures if cleanedSignatures.forall(_.nonEmpty) =>
                Some((member, aggregationBySender.copy(signatures = cleanedSignatures)))
              case _ => None
            }
        }

      cleanedUpSignaturesF.map { cleanedUpSignatures =>
        // now, return the result if we have enough signatures left after cleanup,
        // together with the pruned signatures that should be included in the delivery
        if (cleanedUpSignatures.sizeIs >= threshold.value) {
          cleanedUpSignatures
            .map(_._2.sequencingTimestamp)
            .maxOption
            .map((_, SortedMap.from(cleanedUpSignatures)))
        } else {
          loggingContext.info(
            s"Not enough valid signatures left after cleanup (had=${aggregatedSignatures.size}, cleaned=${cleanedUpSignatures.size}, threshold=${threshold.value}) to meet the threshold, cannot deliver yet"
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

object AggregationRule
    extends VersioningCompanionContext[AggregationRule, ProtocolVersion]
    with ProtocolVersionedCompanionDbHelpers[AggregationRule] {

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

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.AggregationRule)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[canton] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      proto: v30.AggregationRule,
  ): ParsingResult[AggregationRule] = {
    val v30.AggregationRule(eligibleMembersP, thresholdP) = proto
    if (expectedProtocolVersion == ProtocolVersion.v34) {
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
        rpv = protocolVersionRepresentativeFor(expectedProtocolVersion)
      } yield AggregationRule(rule)(rpv)
    }
  }

}
