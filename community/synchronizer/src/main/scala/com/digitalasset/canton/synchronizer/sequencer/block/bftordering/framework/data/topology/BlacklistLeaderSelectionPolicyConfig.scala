// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v31

/** Blacklisting leader selection policy
  *
  * With this policy, peers that fail to complete their consensus segment within the allotted time
  * in an epoch are temporarily banned from leading segments in subsequent epochs.
  * @param howLongToBlacklist
  *   The number of epochs spent on the blacklist before this peer is given another chance to lead a
  *   segment in a future Epoch. This duration is defined by
  *   [[BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist]] , and defaults to a linearly
  *   increasing penalty for peers that fail to complete their assigned segment across consecutive
  *   trials.
  * @param howManyCanWeBlacklist
  *   - The number of peers that can be simultaneously blacklisted, and defaults to the number of
  *     tolerated faults: f = (N-1)/3.
  * @note
  *   this should not exceed 2f, as this could blacklist all correct nodes in the network for a
  *   consensus, leaving only the f malicious nodes with assigned segments and potentially leading
  *   to a network-wide halt.
  */
final case class BlacklistLeaderSelectionPolicyConfig(
    howLongToBlacklist: HowLongToBlacklist,
    howManyCanWeBlacklist: HowManyCanWeBlacklist,
) extends PrettyPrintingFromCompanion {
  override def prettyCompanion: PrettyPrintingCompanion[BlacklistLeaderSelectionPolicyConfig] =
    BlacklistLeaderSelectionPolicyConfig

  def toProto: v31.BlacklistLeaderSelectionPolicy = v31.BlacklistLeaderSelectionPolicy(
    howLongToBlacklist.toProto,
    howManyCanWeBlacklist.toProto,
  )
}

object BlacklistLeaderSelectionPolicyConfig
    extends PrettyPrintingCompanion[BlacklistLeaderSelectionPolicyConfig] {

  override protected val pretty: Pretty[BlacklistLeaderSelectionPolicyConfig] =
    prettyOfClass(
      param("howLongToBlacklist", _.howLongToBlacklist),
      param("howManyCanWeBlacklist", _.howManyCanWeBlacklist),
    )

  def fromProto(
      proto: v31.BlacklistLeaderSelectionPolicy
  ): ParsingResult[BlacklistLeaderSelectionPolicyConfig] = for {
    howLongToBlacklist <- proto.howLongToBlacklist match {
      case v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("howLongToBlacklist"))
      case v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.HowLongLinear(value) =>
        ParsingResult.pure(HowLongToBlacklist.Linear(value.maximumEpochLengthBlacklisted))
      case v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.HowLongNoBlacklisting(value) =>
        ParsingResult.pure(HowLongToBlacklist.NoBlacklisting)
      case v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist
            .HowLongLinearWithParameters(value) =>
        ParsingResult.pure(
          HowLongToBlacklist.LinearWithParameters(
            slope = value.slope,
            initialValue = value.initialValue,
            maximumEpochBlacklisted = value.maximumEpochLengthBlacklisted,
          )
        )
      case v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist
            .HowLongExponential(value) =>
        ParsingResult.pure(
          HowLongToBlacklist.Exponential(
            initialValue = value.initialValue,
            maximumEpochBlacklisted = value.maximumEpochLengthBlacklisted,
          )
        )
    }
    howManyCanWeBlacklist <- proto.howManyCanWeBlacklist match {
      case v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("howManyCanWeBlacklist"))
      case v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist
            .HowManyNumFaultsTolerated(value) =>
        ParsingResult.pure(HowManyCanWeBlacklist.NumFaultsTolerated)
      case v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist
            .HowManyNoBlacklisting(value) =>
        ParsingResult.pure(HowManyCanWeBlacklist.NoBlacklisting)
    }
  } yield BlacklistLeaderSelectionPolicyConfig(
    howLongToBlacklist,
    howManyCanWeBlacklist,
  )

  sealed trait HowLongToBlacklist
      extends PrettyPrintingFromCompanion
      with Product
      with Serializable {
    def punishNodeThatFailed(failedEpochSoFar: Long): BlacklistStatus
    // Since the HowLongToBlacklist might have changed between epochs, how long we need to wait until next trial might
    // change. So this method recomputes it.
    def updateLeftUntilNextTrial(epochsLeftUntilNextTrial: Long): Long
    def toProto: v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist
  }

  object HowLongToBlacklist {

    final case class Linear(maximumEpochBlacklisted: Option[Long]) extends HowLongToBlacklist {
      override def prettyCompanion: PrettyPrintingCompanion[Linear] = Linear

      override def punishNodeThatFailed(failedEpochSoFar: Long): BlacklistStatus =
        BlacklistStatus.Blacklisted.create(
          failedAttemptsBefore = failedEpochSoFar,
          epochsLeftUntilNewTrial = updateLeftUntilNextTrial(failedEpochSoFar),
        )

      override def toProto: v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist =
        v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.HowLongLinear(
          v31.HowLongLinear(maximumEpochBlacklisted)
        )

      override def updateLeftUntilNextTrial(epochsLeftUntilNextTrial: Long): Long =
        maximumEpochBlacklisted.getOrElse(epochsLeftUntilNextTrial).min(epochsLeftUntilNextTrial)
    }

    object Linear extends PrettyPrintingCompanion[Linear] {
      override protected val pretty: Pretty[Linear] = prettyOfClass(
        param("maximumEpochBlacklisted", _.maximumEpochBlacklisted)
      )
    }

    // X |-> min(slope*X+initialValue, maximumEpochBlacklisted)
    final case class LinearWithParameters(
        maximumEpochBlacklisted: Option[Long],
        slope: Long,
        initialValue: Long,
    ) extends HowLongToBlacklist {
      override def prettyCompanion: PrettyPrintingCompanion[LinearWithParameters] =
        LinearWithParameters

      override def punishNodeThatFailed(failedEpochSoFar: Long): BlacklistStatus =
        BlacklistStatus.Blacklisted.create(
          failedAttemptsBefore = failedEpochSoFar,
          epochsLeftUntilNewTrial = updateLeftUntilNextTrial(
            slope * failedEpochSoFar + initialValue
          ),
        )

      override def updateLeftUntilNextTrial(epochsLeftUntilNextTrial: Long): Long =
        maximumEpochBlacklisted.getOrElse(epochsLeftUntilNextTrial).min(epochsLeftUntilNextTrial)

      override def toProto: v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist =
        v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.HowLongLinearWithParameters(
          v31.HowLongLinearWithParameters(
            slope = slope,
            initialValue = initialValue,
            maximumEpochLengthBlacklisted = maximumEpochBlacklisted,
          )
        )
    }

    object LinearWithParameters extends PrettyPrintingCompanion[LinearWithParameters] {
      override protected val pretty: Pretty[LinearWithParameters] = prettyOfClass(
        param("maximumEpochBlacklisted", _.maximumEpochBlacklisted),
        param("slope", _.slope),
        param("initialValue", _.initialValue),
      )
    }

    final case class Exponential(
        maximumEpochBlacklisted: Option[Long],
        initialValue: Long,
    ) extends HowLongToBlacklist {
      override def prettyCompanion: PrettyPrintingCompanion[Exponential] = Exponential

      override def punishNodeThatFailed(failedEpochSoFar: Long): BlacklistStatus =
        BlacklistStatus.Blacklisted.create(
          failedAttemptsBefore = failedEpochSoFar,
          epochsLeftUntilNewTrial = updateLeftUntilNextTrial(
            fixOverflow(
              pow(failedEpochSoFar) + initialValue
            )
          ),
        )

      private def fixOverflow(x: Long): Long =
        if (x < 0) Long.MaxValue else x

      private def pow(x: Long): Long = {
        val temp = scala.math.pow(2, x.toDouble)
        if (temp < 0.0 || temp > Long.MaxValue.toDouble) Long.MaxValue else temp.toLong
      }

      override def updateLeftUntilNextTrial(epochsLeftUntilNextTrial: Long): Long =
        maximumEpochBlacklisted.getOrElse(epochsLeftUntilNextTrial).min(epochsLeftUntilNextTrial)

      override def toProto: v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist =
        v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.HowLongExponential(
          v31.HowLongExponential(
            initialValue = initialValue,
            maximumEpochLengthBlacklisted = maximumEpochBlacklisted,
          )
        )
    }

    object Exponential extends PrettyPrintingCompanion[Exponential] {
      override protected val pretty: Pretty[Exponential] = prettyOfClass(
        param("maximumEpochBlacklisted", _.maximumEpochBlacklisted),
        param("initialValue", _.initialValue),
      )
    }

    case object NoBlacklisting extends HowLongToBlacklist {
      override final def prettyCompanion: PrettyPrintingCompanion[NoBlacklisting.this.type] =
        NoBlacklistingPrettyPrintingCompanion
      override def punishNodeThatFailed(failedEpochSoFar: Long): BlacklistStatus =
        BlacklistStatus.Clean
      override def toProto: v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist =
        v31.BlacklistLeaderSelectionPolicy.HowLongToBlacklist.HowLongNoBlacklisting(
          v31.HowLongNoBlacklisting()
        )

      override def updateLeftUntilNextTrial(epochsLeftUntilNextTrial: Long): Long = 0L
    }

    private object NoBlacklistingPrettyPrintingCompanion
        extends PrettyPrintingCompanion[NoBlacklisting.type] {
      override protected val pretty: Pretty[NoBlacklisting.type] =
        prettyOfObject[NoBlacklisting.type]
    }
  }

  sealed trait HowManyCanWeBlacklist
      extends PrettyPrintingFromCompanion
      with Product
      with Serializable {
    override final def prettyCompanion: PrettyPrintingCompanion[HowManyCanWeBlacklist] =
      HowManyCanWeBlacklist
    def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int
    def toProto: v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist
  }

  object HowManyCanWeBlacklist extends PrettyPrintingCompanion[HowManyCanWeBlacklist] {

    override protected val pretty: Pretty[HowManyCanWeBlacklist] =
      prettyOfObject[HowManyCanWeBlacklist]

    case object NumFaultsTolerated extends HowManyCanWeBlacklist {
      override def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int =
        orderingTopology.numFaultsTolerated

      override def toProto: v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist =
        v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist.HowManyNumFaultsTolerated(
          v31.HowManyNumFaultsTolerated()
        )
    }

    case object NoBlacklisting extends HowManyCanWeBlacklist {
      override def howManyCanWeBlacklist(orderingTopology: OrderingTopology): Int = 0
      override def toProto: v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist =
        v31.BlacklistLeaderSelectionPolicy.HowManyCanWeBlacklist.HowManyNoBlacklisting(
          v31.HowManyNoBlacklisting()
        )
    }
  }
}
