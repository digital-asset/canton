// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.v30.SynchronizerPredecessor as SynchronizerPredecessorProto
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  ProtocolVersionedCompanionDbHelpers,
  ReleaseProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

/** Information about the predecessor of a synchronizer.
  *
  * @param psid
  *   Id of the predecessor.
  * @param upgradeTime
  *   When the upgrade happened/is supposed to happen.
  * @param isLateUpgrade
  *   Whether the upgrade is considered a late upgrade, meaning the node is being upgraded to the
  *   new synchronizer via manual repair steps.
  */
final case class SynchronizerPredecessor private (
    psid: PhysicalSynchronizerId,
    upgradeTime: CantonTimestamp,
    isLateUpgrade: Boolean,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SynchronizerPredecessor.type
    ]
) extends HasProtocolVersionedWrapper[SynchronizerPredecessor] {
  @transient override protected lazy val companionObj: SynchronizerPredecessor.type =
    SynchronizerPredecessor

  def copy(
      psid: PhysicalSynchronizerId = psid,
      upgradeTime: CantonTimestamp = upgradeTime,
      isLateUpgrade: Boolean = isLateUpgrade,
  ): SynchronizerPredecessor =
    new SynchronizerPredecessor(psid, upgradeTime, isLateUpgrade)(representativeProtocolVersion)

  def toProtoV30: SynchronizerPredecessorProto =
    SynchronizerPredecessorProto(
      psid.toProtoPrimitive,
      Some(upgradeTime.toProtoTimestamp),
      isLateUpgrade = isLateUpgrade,
    )
}

object SynchronizerPredecessor
    extends VersioningCompanion[SynchronizerPredecessor]
    with ProtocolVersionedCompanionDbHelpers[SynchronizerPredecessor] {
  override def name: String = "SynchronizerPredecessor"

  def apply(
      psid: PhysicalSynchronizerId,
      upgradeTime: CantonTimestamp,
      isLateUpgrade: Boolean,
  ): SynchronizerPredecessor =
    new SynchronizerPredecessor(psid, upgradeTime, isLateUpgrade)(
      protocolVersionRepresentativeFor(psid.protocolVersion)
    )

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec.storage(
      ReleaseProtocolVersion(ProtocolVersion.v34),
      SynchronizerPredecessorProto,
    )(
      supportedProtoVersionPVV(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private def fromProtoV30(
      pvv: ProtocolVersionValidation,
      proto: SynchronizerPredecessorProto,
  ): ParsingResult[SynchronizerPredecessor] = {
    val SynchronizerPredecessorProto(psidP, upgradeTimePO, isLateUpgrade) = proto

    for {
      psid <- ProtoValidation.validateThen(
        psidP,
        "predecessor_physical_id",
        pvv,
      )(PhysicalSynchronizerId.fromProtoPrimitive)
      upgradeTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "upgrade_time",
        upgradeTimePO,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield new SynchronizerPredecessor(psid, upgradeTime, isLateUpgrade)(rpv)
  }
}

/** Information about the successor of a synchronizer.
  *
  * @param psid
  *   Id of the successor.
  * @param upgradeTime
  *   When the migration is supposed to happen.
  */
final case class SynchronizerSuccessor(
    psid: PhysicalSynchronizerId,
    upgradeTime: CantonTimestamp,
)

object LogicalUpgradeTime {

  /** Can the message be processed (or does it "belong" to the predecessor synchronizer).
    */
  def canProcessKnowingPredecessor(
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      sequencedTime: SequencedTime,
  ): Boolean =
    canProcessKnowingPastUpgrade(synchronizerPredecessor.map(_.upgradeTime), sequencedTime.value)

  /** Can the message be processed (or does it "belong" to the predecessor synchronizer).
    */
  def canProcessKnowingPredecessor(
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      sequencingTime: CantonTimestamp,
  ): Boolean =
    canProcessKnowingPastUpgrade(synchronizerPredecessor.map(_.upgradeTime), sequencingTime)

  /** Can the message be processed (or does it "belong" to the predecessor synchronizer).
    */
  def canProcessKnowingPastUpgrade(
      upgradeTime: Option[CantonTimestamp],
      sequencingTime: CantonTimestamp,
  ): Boolean =
    upgradeTime.fold(true)(_ < sequencingTime)

  /** Can the message be processed (or does it "belong" to the successor synchronizer).
    */
  def canProcessKnowingSuccessor(
      synchronizerSuccessor: Option[SynchronizerSuccessor],
      sequencedTime: SequencedTime,
  ): Boolean =
    canProcessKnowingFutureUpgrade(synchronizerSuccessor.map(_.upgradeTime), sequencedTime.value)

  /** Can the message be processed (or does it "belong" to the successor synchronizer).
    */
  def canProcessKnowingSuccessor(
      synchronizerSuccessor: Option[SynchronizerSuccessor],
      sequencingTime: CantonTimestamp,
  ): Boolean =
    canProcessKnowingFutureUpgrade(synchronizerSuccessor.map(_.upgradeTime), sequencingTime)

  /** Can the message be processed (or does it "belong" to the successor synchronizer).
    */
  def canProcessKnowingFutureUpgrade(
      upgradeTime: Option[CantonTimestamp],
      sequencingTime: CantonTimestamp,
  ): Boolean =
    upgradeTime.fold(true)(_ > sequencingTime)
}
