// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.BooleanUtil.implicits.*

import java.time.Instant

sealed trait DisseminationStatus extends Product with Serializable with PrettyPrinting {

  import DisseminationStatus.*

  /** The membership this dissemination progress refers to. */
  val membership: Membership

  /** Whom the batch has been sent to. */
  val batchSentTo: Set[BftNodeId]

  /** Whom the batch must be sent to. */
  val sendBatchTo: Set[BftNodeId]

  /** The epoch number this dissemination progress refers to. */
  val epochNumber: EpochNumber

  // Various statistics for observability follow

  val stats: OrderingRequestBatchStats
  val availabilityEnterInstant: Option[Instant] = None
  val regressionsToSigning: Int = 0
  val disseminationRegressions: Int = 0

  lazy val recipients: Set[BftNodeId] = membership.otherNodes

  def tracedBatchId: Traced[BatchId]

  /** Acknowledgements received for this dissemination progress. */
  def acks: Set[AvailabilityAck]

  /** Acknowledgements received for this dissemination progress. */
  def previousAcks: Option[Set[AvailabilityAck]]

  /** Updates the dissemination status for a new membership. */
  def changeMembership(newMembership: Membership): DisseminationStatus = {
    val myId = newMembership.myId
    val updatedAcks = updateAcks(acks, newMembership.orderingTopology)
    val regressedToSigning =
      DisseminationStatus.ackOf(myId, updatedAcks).isEmpty
    val disseminationRegressed =
      !regressedToSigning &&
        computeSendTo(membership, acks, batchSentTo).sizeIs <
        computeSendTo(
          newMembership,
          updatedAcks,
          batchSentTo,
          previousAcks = acks,
        ).size
    val updatedRegressionsToSigning = regressionsToSigning + regressedToSigning.toInt
    val updatedDisseminationRegressions = disseminationRegressions + disseminationRegressed.toInt
    update(
      newMembership,
      updatedAcks,
      updatedRegressionsToSigning,
      updatedDisseminationRegressions,
      previousAcks = Some(acks),
    )
  }

  /** Updates the dissemination status adding sends. */
  def addSends(additionalSends: Set[BftNodeId]): DisseminationStatus =
    this match {
      case ip: InProgress => ip.copy(batchSentTo = ip.batchSentTo ++ additionalSends)
      case c: Complete => c.copy(batchSentTo = c.batchSentTo ++ additionalSends)
    }

  /** Updates the dissemination status adding an incoming ack. */
  def addAck(ack: AvailabilityAck): DisseminationStatus =
    // Since we may be re-requesting votes, we need to ensure that we don't add multiple acks with different
    //  valid signatures from the same node, because each node can contribute at most one ack
    //  to the dissemination quorum.

    //  Also, since we won't update progress if the topology hasn't changed, we must ensure that the node that sent the
    //  ack is still in the topology, otherwise we might early-validate an ack from a removed node, start its signature
    //  verification (async), shortly afterward receive the new topology that removes the node and finally
    //  happily complete signature verification of the ack from the removed node.
    //  Then, if we were to add that ack without checking again if the originating node is in the topology,
    //  this would result in an invalid PoA that would cause consensus to reject any `PrePrepare` that contains it.

    (if (membership.orderingTopology.nodes.contains(ack.from) && ackOf(ack.from).isEmpty) {
       this match {
         case ip: InProgress => ip.copy(acks = ip.acks + ack)
         case c: Complete =>
           c.copy(tracedProofOfAvailability =
             c.tracedProofOfAvailability.map(
               _.copy(acks = toOrderedAcksSeq((c.tracedProofOfAvailability.value.acks.toSet + ack)))
             )
           )
       }
     } else {
       this
     }).update()

  /** Updates the dissemination status without external events, to check if an in-progress status is
    * complete or vice versa.
    */
  def update(): DisseminationStatus =
    update(
      membership,
      acks,
      regressionsToSigning,
      disseminationRegressions,
      previousAcks,
    )

  private def update(
      newMembership: Membership,
      reviewedAcks: Set[AvailabilityAck],
      updatedRegressionsToSigning: Int,
      updatedDisseminationRegressions: Int,
      previousAcks: Option[Set[AvailabilityAck]],
  ): DisseminationStatus =
    if (
      AvailabilityModule.hasDisseminationQuorum(newMembership.orderingTopology, reviewedAcks.size)
    )
      Complete(
        newMembership,
        tracedBatchId.map(_ =>
          ProofOfAvailability(tracedBatchId.value, toOrderedAcksSeq(reviewedAcks), epochNumber)
        ),
        epochNumber,
        batchSentTo,
        stats,
        availabilityEnterInstant,
        readyForOrderingInstant = this match {
          case _: InProgress => Some(Instant.now())
          case dc: Complete => dc.readyForOrderingInstant
        },
        updatedRegressionsToSigning,
        disseminationRegressions, // No additional regressions if the dissemination is complete
        previousAcks,
      )
    else
      InProgress(
        newMembership,
        tracedBatchId,
        reviewedAcks,
        epochNumber,
        batchSentTo,
        stats,
        availabilityEnterInstant,
        updatedRegressionsToSigning,
        updatedDisseminationRegressions,
        previousAcks,
      )

  def ackOf(nodeId: BftNodeId): Option[AvailabilityAck] =
    DisseminationStatus.ackOf(nodeId, acks)

  def needsSigning: Boolean

  def toEither: Either[InProgress, Complete] = this match {
    case ip: InProgress => Left(ip)
    case c: Complete => Right(c)
  }

  def asComplete: Option[Complete] =
    this match {
      case c: Complete => Some(c)
      case _: InProgress => None
    }

  def asInProgress: Option[InProgress] =
    this match {
      case _: Complete => None
      case ip: InProgress => Some(ip)
    }

  // Used by metrics emission
  def resetRegressions(): DisseminationStatus

  override def pretty: Pretty[DisseminationStatus] =
    prettyOfClass(
      param("membership", _.membership),
      param("batchId", _.tracedBatchId.value.toString.doubleQuoted),
      param("acks", _.acks),
      param("previousAcks", _.previousAcks),
      param("epochNumber", _.epochNumber),
      param("recipients", _.recipients.map(_.doubleQuoted)),
      param("batchSentTo", _.batchSentTo.map(_.doubleQuoted)),
      param("stats", _.stats),
      param("availabilityEnterInstant", _.availabilityEnterInstant),
      param("regressionsToSigning", _.regressionsToSigning),
      param("disseminationRegressions", _.disseminationRegressions),
    )
}

object DisseminationStatus {

  /** Progress of a dissemination that is not complete yet. */
  final case class InProgress(
      override val membership: Membership,
      override val tracedBatchId: Traced[BatchId],
      override val acks: Set[AvailabilityAck],
      override val epochNumber: EpochNumber,
      override val batchSentTo: Set[BftNodeId] = Set.empty,
      override val stats: OrderingRequestBatchStats,
      override val availabilityEnterInstant: Option[Instant] = None,
      override val regressionsToSigning: Int = 0,
      override val disseminationRegressions: Int = 0,
      override val previousAcks: Option[Set[AvailabilityAck]] = None,
  ) extends DisseminationStatus {

    // We allow dissemination progress with acks not in the topology to allow dissemination
    //  when the node is not part of the topology yet during onboarding, but we always
    //  produce valid PoAs.

    override lazy val sendBatchTo: Set[BftNodeId] =
      computeSendTo(membership, acks, batchSentTo, previousAcks.getOrElse(acks))

    override lazy val needsSigning: Boolean =
      this.ackOf(membership.myId).isEmpty

    override def resetRegressions(): InProgress =
      copy(regressionsToSigning = 0, disseminationRegressions = 0)
  }

  /** A complete dissemination.
    *
    * @param tracedProofOfAvailability
    *   The proof of availability for the completely disseminated batch.
    * @param readyForOrderingInstant
    *   The instant when the dissemination has become complete.
    */
  final case class Complete(
      override val membership: Membership,
      tracedProofOfAvailability: Traced[ProofOfAvailability],
      override val epochNumber: EpochNumber,
      override val batchSentTo: Set[BftNodeId] = Set.empty,
      override val stats: OrderingRequestBatchStats,
      override val availabilityEnterInstant: Option[Instant] = None,
      readyForOrderingInstant: Option[Instant] = None,
      override val regressionsToSigning: Int = 0,
      override val disseminationRegressions: Int = 0,
      override val previousAcks: Option[Set[AvailabilityAck]] = None,
  ) extends DisseminationStatus {

    override lazy val sendBatchTo: Set[BftNodeId] =
      // If F == 0, no other nodes are required to store the batch because there is no fault tolerance,
      //  so batches are ready for ordering immediately after being signed and stored locally,
      //  i.e., without them being sent at all.
      //  However, we still want to send the batch to other nodes to minimize fetches at the output phase.
      if (batchSentTo.isEmpty)
        recipients
      else
        Set.empty

    override val needsSigning: Boolean = false

    override def tracedBatchId: Traced[BatchId] = tracedProofOfAvailability.map(_.batchId)

    override def acks: Set[AvailabilityAck] = tracedProofOfAvailability.value.acks.toSet

    override def resetRegressions(): Complete =
      copy(regressionsToSigning = 0, disseminationRegressions = 0)
  }

  def updateAcks(
      acks: Iterable[AvailabilityAck],
      currentOrderingTopology: OrderingTopology,
  ): Set[AvailabilityAck] =
    acks.filter(_.validateIn(currentOrderingTopology).isRight).toSet

  private def ackOf(
      nodeId: BftNodeId,
      acks: Set[AvailabilityAck],
  ): Option[AvailabilityAck] =
    acks.find(_.from == nodeId)

  private def computeSendTo(
      membership: Membership,
      acks: Set[AvailabilityAck],
      batchSentTo: Set[BftNodeId],
      previousAcks: Set[AvailabilityAck] = Set.empty,
  ): Set[BftNodeId] = {
    val lostExternalAcksFrom =
      previousAcks
        .map(_.from)
        .diff(acks.map(_.from))
        .intersect(membership.otherNodes)
    membership.otherNodes.diff(batchSentTo).diff(acks.map(_.from)) ++ lostExternalAcksFrom
  }

  private def toOrderedAcksSeq(acks: Set[AvailabilityAck]) =
    acks.toSeq.sortBy(_.from)
}
