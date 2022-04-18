// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{ConfirmingParty, Informee, PlainInformee}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, TrustLevel}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.{DeserializationError, DeterministicEncoding}
import com.digitalasset.canton.util.LfTransactionUtil
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

sealed trait ConfirmationPolicy extends Product with Serializable with PrettyPrinting {
  protected val name: String
  protected val index: Int

  def toProtoPrimitive: ByteString = DeterministicEncoding.encodeString(name)

  def informeesAndThreshold(actionNode: LfActionNode, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext
  ): Future[(Set[Informee], NonNegativeInt)]

  /** The minimal acceptable trust level of the sender of mediator response */
  def requiredTrustLevel: TrustLevel

  /** The minimum threshold for views of requests with this policy.
    * The mediator checks that all views have at least the given threshold.
    */
  def minimumThreshold: NonNegativeInt

  override def pretty: Pretty[ConfirmationPolicy] = prettyOfObject[ConfirmationPolicy]
}

object ConfirmationPolicy {

  private val havingVip: ParticipantAttributes => Boolean = _.trustLevel == TrustLevel.Vip
  private val havingConfirmer: ParticipantAttributes => Boolean = _.permission.canConfirm

  private def toInformeesAndThreshold(
      confirmingParties: Set[LfPartyId],
      plainInformees: Set[LfPartyId],
  ): (Set[Informee], NonNegativeInt) = {
    // We make sure that the threshold is at least 1 so that a transaction is not vacuously approved if the confirming parties are empty.
    val threshold = NonNegativeInt.tryCreate(Math.max(confirmingParties.size, 1))
    val informees =
      confirmingParties.map(ConfirmingParty(_, 1): Informee) ++ plainInformees.map(PlainInformee)
    (informees, threshold)
  }

  case object Vip extends ConfirmationPolicy {
    override val name = "Vip"
    protected override val index: Int = 0

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val stateVerifiers = LfTransactionUtil.stateKnownTo(node)
      val confirmingPartiesF = stateVerifiers.toList
        .traverseFilter { partyId =>
          topologySnapshot
            .activeParticipantsOf(partyId)
            .map(participants => participants.values.find(havingVip).map(_ => partyId))
        }
        .map(_.toSet)
      confirmingPartiesF.map { confirmingParties =>
        val plainInformees = LfTransactionUtil.informees(node) -- confirmingParties
        val informees =
          confirmingParties.map(ConfirmingParty(_, 1)) ++ plainInformees.map(PlainInformee)
        // As all VIP participants are trusted, it suffices that one of them confirms.
        (informees, NonNegativeInt.one)
      }
    }

    override def requiredTrustLevel: TrustLevel = TrustLevel.Vip

    override def minimumThreshold: NonNegativeInt = NonNegativeInt.one
  }

  case object Signatory extends ConfirmationPolicy {
    override val name = "Signatory"
    protected override val index: Int = 1

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val confirmingParties =
        LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
      require(
        confirmingParties.nonEmpty,
        "There must be at least one confirming party, as every node must have at least one signatory.",
      )
      val plainInformees = LfTransactionUtil.informees(node) -- confirmingParties
      Future.successful(toInformeesAndThreshold(confirmingParties, plainInformees))
    }

    override def requiredTrustLevel: TrustLevel = TrustLevel.Ordinary

    override def minimumThreshold: NonNegativeInt = NonNegativeInt.one
  }

  case object Full extends ConfirmationPolicy {
    override val name = "Full"
    protected override val index: Int = 2

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val informees = LfTransactionUtil.informees(node)
      require(
        informees.nonEmpty,
        "There must be at least one informee as every node must have at least one signatory.",
      )
      Future.successful(toInformeesAndThreshold(informees, Set.empty))
    }

    override def requiredTrustLevel: TrustLevel = TrustLevel.Ordinary

    override def minimumThreshold: NonNegativeInt = NonNegativeInt.one
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

    val vipCheckParties = actionNodes.map { node =>
      LfTransactionUtil.informees(node) & LfTransactionUtil.stateKnownTo(node)
    }
    val signatoriesCheckParties = actionNodes.map { node =>
      LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
    }
    val allParties = vipCheckParties.flatten ++ signatoriesCheckParties.flatten
    // TODO(i4930) - potentially batch this lookup
    val activeParticipantsF =
      allParties.toList
        .traverse(partyId =>
          topologySnapshot.activeParticipantsOf(partyId).map { res =>
            (partyId, (res.values.exists(havingVip), res.values.exists(havingConfirmer)))
          }
        )
        .map(_.toMap)

    activeParticipantsF.map { partyData =>
      val hasVipForEachNode = vipCheckParties.forall { nodeParties =>
        nodeParties.exists(partyData.get(_).forall(_._1))
      }
      val hasConfirmersForEachNode = signatoriesCheckParties.forall { nodeParties =>
        nodeParties.exists(partyData.get(_).forall(_._2))
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
        Left(DeserializationError(s"Invalid confirmation policy $badName", badBytes))
    }
}
