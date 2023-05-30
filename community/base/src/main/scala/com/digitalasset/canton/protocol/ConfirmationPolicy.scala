// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{ConfirmingParty, Informee, PlainInformee}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, TrustLevel}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

sealed trait ConfirmationPolicy extends Product with Serializable with PrettyPrinting {
  protected val name: String
  protected val index: Int

  def toProtoPrimitive: ByteString = DeterministicEncoding.encodeString(name)

  /** @param submittingAdminPartyO admin party of the submitting participant; defined only for top-level views
    */
  def informeesAndThreshold(
      actionNode: LfActionNode,
      submittingAdminPartyO: Option[LfPartyId],
      topologySnapshot: TopologySnapshot,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContext
  ): Future[(Set[Informee], NonNegativeInt)] = if (protocolVersion < ProtocolVersion.v5)
    informeesAndThresholdV0(actionNode, topologySnapshot)
  else informeesAndThresholdV5(actionNode, submittingAdminPartyO, topologySnapshot)

  def informeesAndThresholdV0(
      actionNode: LfActionNode,
      topologySnapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext
  ): Future[(Set[Informee], NonNegativeInt)]

  def informeesAndThresholdV5(
      actionNode: LfActionNode,
      submittingAdminPartyO: Option[LfPartyId],
      topologySnapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext
  ): Future[(Set[Informee], NonNegativeInt)]

  /** The minimal acceptable trust level of the sender of mediator response */
  def requiredTrustLevel: TrustLevel

  /** The minimum threshold for views of requests with this policy.
    * The mediator checks that all views have at least the given threshold.
    */
  def minimumThreshold(informees: Set[Informee]): NonNegativeInt = NonNegativeInt.one

  override def pretty: Pretty[ConfirmationPolicy] = prettyOfObject[ConfirmationPolicy]
}

object ConfirmationPolicy {

  private val havingVip: ParticipantAttributes => Boolean = _.trustLevel == TrustLevel.Vip
  private val havingConfirmer: ParticipantAttributes => Boolean = _.permission.canConfirm

  private def toInformeesAndThreshold(
      confirmingParties: Set[LfPartyId],
      plainInformees: Set[LfPartyId],
      requiredTrustLevel: TrustLevel,
  ): (Set[Informee], NonNegativeInt) = {
    // We make sure that the threshold is at least 1 so that a transaction is not vacuously approved if the confirming parties are empty.
    val threshold = NonNegativeInt.tryCreate(Math.max(confirmingParties.size, 1))
    val informees =
      confirmingParties.map(ConfirmingParty(_, 1, requiredTrustLevel): Informee) ++
        plainInformees.map(PlainInformee)
    (informees, threshold)
  }

  case object Vip extends ConfirmationPolicy {
    override val name = "Vip"
    protected override val index: Int = 0

    override def informeesAndThresholdV0(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val stateVerifiers = LfTransactionUtil.stateKnownTo(node)
      val confirmingPartiesF = stateVerifiers.toList
        .parTraverseFilter { partyId =>
          topologySnapshot
            .activeParticipantsOf(partyId)
            .map(participants => participants.values.find(havingVip).map(_ => partyId))
        }
        .map(_.toSet)
      confirmingPartiesF.map { confirmingParties =>
        val plainInformees = LfTransactionUtil.informees(node) -- confirmingParties
        val informees =
          confirmingParties.map(ConfirmingParty(_, 1, TrustLevel.Vip)) ++
            plainInformees.map(PlainInformee)
        // As all VIP participants are trusted, it suffices that one of them confirms.
        (informees, NonNegativeInt.one)
      }
    }

    override def informeesAndThresholdV5(
        node: LfActionNode,
        submittingAdminPartyO: Option[LfPartyId],
        topologySnapshot: TopologySnapshot,
    )(implicit
        ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val stateVerifiers = LfTransactionUtil.stateKnownTo(node)

      for {
        vipParties <- stateVerifiers.toList
          .parTraverseFilter { partyId =>
            topologySnapshot
              .activeParticipantsOf(partyId)
              .map(_.values.find(havingVip).map(_ => partyId))
          }
          .map(_.toSet)
      } yield {
        val plainInformees =
          (LfTransactionUtil.informees(node) -- vipParties -- submittingAdminPartyO)
            .map(PlainInformee)

        val vipConfirmingParties =
          (vipParties -- submittingAdminPartyO).map(ConfirmingParty(_, 1, TrustLevel.Vip))

        val submittingConfirmingPartyO = submittingAdminPartyO
          .map { submittingAdminParty =>
            // Choose the highest possible required trust level.
            val requiredTrustLevel =
              if (vipParties.contains(submittingAdminParty)) TrustLevel.Vip else TrustLevel.Ordinary

            // Choose the weight of the submitting admin party so high that
            // 1. it is part of every quorum.
            // 2. it is always positive.
            //
            // Add one, if the submitting participant has VIP status so that it can attain the threshold alone.
            val weight =
              (vipParties.size + 1) + (if (requiredTrustLevel == TrustLevel.Vip) 1 else 0)
            ConfirmingParty(submittingAdminParty, weight, requiredTrustLevel)
          }

        val informees = plainInformees ++ vipConfirmingParties ++ submittingConfirmingPartyO

        // At least one VIP party and the submitting participant (if defined) need to approve.
        val threshold = 1 + submittingConfirmingPartyO.fold(0)(_ => vipParties.size + 1)

        (informees, NonNegativeInt.tryCreate(threshold))
      }
    }

    override def requiredTrustLevel: TrustLevel = TrustLevel.Vip

    override def minimumThreshold(informees: Set[Informee]): NonNegativeInt = {
      // Make sure that at least one VIP needs to approve.

      val weightOfOrdinary = informees.collect {
        case ConfirmingParty(_, weight, TrustLevel.Ordinary) => weight
      }.sum
      NonNegativeInt.tryCreate(weightOfOrdinary + 1)
    }
  }

  case object Signatory extends ConfirmationPolicy {
    override val name = "Signatory"
    protected override val index: Int = 1

    override def informeesAndThresholdV0(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val confirmingParties =
        LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
      require(
        confirmingParties.nonEmpty,
        "There must be at least one confirming party, as every node must have at least one signatory.",
      )
      val plainInformees = LfTransactionUtil.informees(node) -- confirmingParties
      Future.successful(
        toInformeesAndThreshold(confirmingParties, plainInformees, TrustLevel.Ordinary)
      )
    }

    override def informeesAndThresholdV5(
        node: LfActionNode,
        submittingAdminPartyO: Option[LfPartyId],
        topologySnapshot: TopologySnapshot,
    )(implicit
        ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val confirmingParties =
        LfTransactionUtil.signatoriesOrMaintainers(node) ++
          LfTransactionUtil.actingParties(node) ++ submittingAdminPartyO
      require(
        confirmingParties.nonEmpty,
        "There must be at least one confirming party, as every node must have at least one signatory.",
      )
      val plainInformees = LfTransactionUtil.informees(node) -- confirmingParties
      Future.successful(
        toInformeesAndThreshold(confirmingParties, plainInformees, TrustLevel.Ordinary)
      )
    }
    override def requiredTrustLevel: TrustLevel = TrustLevel.Ordinary
  }

  case object Full extends ConfirmationPolicy {
    override val name = "Full"
    protected override val index: Int = 2

    override def informeesAndThresholdV0(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val informees = LfTransactionUtil.informees(node)
      require(
        informees.nonEmpty,
        "There must be at least one informee as every node must have at least one signatory.",
      )
      Future.successful(toInformeesAndThreshold(informees, Set.empty, TrustLevel.Ordinary))
    }

    override def informeesAndThresholdV5(
        node: LfActionNode,
        submittingAdminPartyO: Option[LfPartyId],
        topologySnapshot: TopologySnapshot,
    )(implicit
        ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val informees = LfTransactionUtil.informees(node) ++ submittingAdminPartyO
      require(
        informees.nonEmpty,
        "There must be at least one informee as every node must have at least one signatory.",
      )
      Future.successful(toInformeesAndThreshold(informees, Set.empty, TrustLevel.Ordinary))
    }
    override def requiredTrustLevel: TrustLevel = TrustLevel.Ordinary
  }

  val values: Seq[ConfirmationPolicy] = Seq[ConfirmationPolicy](Vip, Signatory, Full)

  require(
    values.zipWithIndex.forall { case (policy, index) => policy.index == index },
    "Mismatching policy indices.",
  )

  /** Ordering for [[ConfirmationPolicy]] */
  implicit val orderConfirmationPolicy: Order[ConfirmationPolicy] =
    Order.by[ConfirmationPolicy, Int](_.index)

  /** Chooses appropriate confirmation policies for a transaction.
    * It chooses [[Vip]] if every node has at least one VIP who knows the state
    * It chooses [[Signatory]] if every node has a Participant that can confirm.
    * It never chooses [[Full]].
    */
  def choose(transaction: LfVersionedTransaction, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext
  ): Future[Seq[ConfirmationPolicy]] = {

    val actionNodes = transaction.nodes.values.collect { case an: LfActionNode => an }

    val vipCheckPartiesPerNode = actionNodes.map { node =>
      LfTransactionUtil.informees(node) & LfTransactionUtil.stateKnownTo(node)
    }
    val signatoriesCheckPartiesPerNode = actionNodes.map { node =>
      LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
    }
    val allParties =
      (vipCheckPartiesPerNode.flatten ++ signatoriesCheckPartiesPerNode.flatten).toSet
    // TODO(i4930) - potentially batch this lookup
    val eligibleParticipantsF =
      allParties.toList
        .parTraverse(partyId =>
          topologySnapshot.activeParticipantsOf(partyId).map { res =>
            (partyId, (res.values.exists(havingVip), res.values.exists(havingConfirmer)))
          }
        )
        .map(_.toMap)

    eligibleParticipantsF.map { eligibleParticipants =>
      val hasVipForEachNode = vipCheckPartiesPerNode.forall { _.exists(eligibleParticipants(_)._1) }
      val hasConfirmersForEachNode = signatoriesCheckPartiesPerNode.forall {
        _.exists(eligibleParticipants(_)._2)
      }
      List(hasVipForEachNode -> Vip, hasConfirmersForEachNode -> Signatory)
        .filter(_._1)
        .map(_._2)
    }
  }

  def fromProtoPrimitive(
      encodedName: ByteString
  ): Either[DeserializationError, ConfirmationPolicy] =
    DeterministicEncoding.decodeString(encodedName).flatMap {
      case (Vip.name, _) => Right(Vip)
      case (Signatory.name, _) => Right(Signatory)
      case (badName, badBytes) =>
        Left(DefaultDeserializationError(s"Invalid confirmation policy $badName"))
    }
}
