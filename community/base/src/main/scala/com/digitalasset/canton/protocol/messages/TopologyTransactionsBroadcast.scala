// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext,
}

final case class TopologyTransactionsBroadcast(
    override val synchronizerId: PhysicalSynchronizerId,
    transactions: SignedTopologyTransactions[TopologyChangeOp, TopologyMapping],
) extends UnsignedProtocolMessage
    with HasProtocolVersionedWrapper[TopologyTransactionsBroadcast] {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    TopologyTransactionsBroadcast.type
  ] = TopologyTransactionsBroadcast.protocolVersionRepresentativeFor(synchronizerId.protocolVersion)

  @transient override protected lazy val companionObj: TopologyTransactionsBroadcast.type =
    TopologyTransactionsBroadcast

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.TopologyTransactionsBroadcast(toProtoV30)

  def toProtoV30: v30.TopologyTransactionsBroadcast = v30.TopologyTransactionsBroadcast(
    synchronizerId.toProtoPrimitive,
    Some(transactions.toProtoV30),
  )

  def signedTransactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]] =
    transactions.transactions
}

object TopologyTransactionsBroadcast
    extends VersioningCompanionContext[
      TopologyTransactionsBroadcast,
      ProtocolVersion,
    ] {

  def apply(
      psid: PhysicalSynchronizerId,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): TopologyTransactionsBroadcast =
    TopologyTransactionsBroadcast(
      psid,
      SignedTopologyTransactions(transactions, psid.protocolVersion),
    )

  override def name: String = "TopologyTransactionsBroadcast"

  implicit val acceptedTopologyTransactionMessageCast
      : ProtocolMessageContentCast[TopologyTransactionsBroadcast] =
    ProtocolMessageContentCast.create[TopologyTransactionsBroadcast](
      name
    ) {
      case att: TopologyTransactionsBroadcast => Some(att)
      case _ => None
    }

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.TopologyTransactionsBroadcast
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[messages] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.TopologyTransactionsBroadcast,
  ): ParsingResult[TopologyTransactionsBroadcast] = {
    val v30.TopologyTransactionsBroadcast(synchronizerP, signedTopologyTransactionsP) = message
    for {
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(synchronizerP, "synchronizer_id")

      signedTopologyTransactions <- ProtoConverter.parseRequired(
        SignedTopologyTransactions
          .fromProtoV30(ProtocolVersionValidation.PV(expectedProtocolVersion), _),
        "signed_transactions",
        signedTopologyTransactionsP,
      )
    } yield TopologyTransactionsBroadcast(synchronizerId, signedTopologyTransactions)
  }

  /** The state of the submission of a topology transaction broadcast. In combination with the
    * sequencer client send tracker capability, State reflects that either the sequencer Accepted
    * the submission or that the submission was Rejected due to an error or a timeout. See
    * SynchronizerTopologyService.
    */
  sealed trait State extends Product with Serializable

  object State {
    case object Failed extends State

    case object Accepted extends State
  }
}
