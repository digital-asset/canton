// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError._
import com.digitalasset.canton.config.RequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.PrettyInstances._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.v0.TopologyTransaction.Transaction
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.version._
import com.google.protobuf.ByteString
import slick.jdbc.SetParameter

/** Add, Remove, Replace */
sealed trait TopologyChangeOp extends Product with Serializable with PrettyPrinting {
  def toProto: v0.TopologyChangeOp
  override def pretty: Pretty[TopologyChangeOp.this.type] = adHocPrettyInstance
}

/** +/- */
sealed abstract class AddRemoveChangeOp(val toProto: v0.TopologyChangeOp) extends TopologyChangeOp

object AddRemoveChangeOp {
  def fromProtoV0(
      protoOp: v0.TopologyChangeOp
  ): ParsingResult[AddRemoveChangeOp] =
    protoOp match {
      case v0.TopologyChangeOp.Add => Right(TopologyChangeOp.Add)
      case v0.TopologyChangeOp.Remove => Right(TopologyChangeOp.Remove)
      case v0.TopologyChangeOp.Replace => Left(InvariantViolation("Replace op is not allowed here"))
      case v0.TopologyChangeOp.Unrecognized(x) => Left(UnrecognizedEnum(protoOp.name, x))
    }
}

object TopologyChangeOp {
  sealed trait Positive extends TopologyChangeOp

  final case object Add extends AddRemoveChangeOp(v0.TopologyChangeOp.Add) with Positive
  final case object Remove extends AddRemoveChangeOp(v0.TopologyChangeOp.Remove)

  final case object Replace extends TopologyChangeOp with Positive {
    def toProto: v0.TopologyChangeOp = v0.TopologyChangeOp.Replace
  }

  type Add = Add.type
  type Remove = Remove.type
  type Replace = Replace.type

  trait OpTypeChecker[A <: TopologyChangeOp] {
    def isOfType(op: TopologyChangeOp): Boolean
  }

  implicit val topologyAddChecker = new OpTypeChecker[Add] {
    override def isOfType(op: TopologyChangeOp): Boolean = op match {
      case _: Add => true
      case _ => false
    }
  }

  implicit val topologyPositiveChecker = new OpTypeChecker[Positive] {
    override def isOfType(op: TopologyChangeOp): Boolean = op match {
      case _: Add | _: Replace => true
      case _ => false
    }
  }

  implicit val topologyRemoveChecker = new OpTypeChecker[Remove] {
    override def isOfType(op: TopologyChangeOp): Boolean = op match {
      case _: Remove => true
      case _ => false
    }
  }

  implicit val topologyReplaceChecker = new OpTypeChecker[Replace] {
    override def isOfType(op: TopologyChangeOp): Boolean = op match {
      case _: Replace => true
      case _ => false
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def select[Op <: TopologyChangeOp](transaction: SignedTopologyTransaction[TopologyChangeOp])(
      implicit checker: OpTypeChecker[Op]
  ): Option[SignedTopologyTransaction[Op]] = if (checker.isOfType(transaction.operation))
    Some(transaction.asInstanceOf[SignedTopologyTransaction[Op]])
  else None

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def select[Op <: TopologyChangeOp](
      storedTransaction: StoredTopologyTransaction[TopologyChangeOp]
  )(implicit
      checker: OpTypeChecker[Op]
  ): Option[StoredTopologyTransaction[Op]] = if (
    checker.isOfType(storedTransaction.transaction.operation)
  )
    Some(storedTransaction.asInstanceOf[StoredTopologyTransaction[Op]])
  else None

  def fromProtoV0(
      protoOp: v0.TopologyChangeOp
  ): ParsingResult[TopologyChangeOp] =
    protoOp match {
      case v0.TopologyChangeOp.Add => Right(Add)
      case v0.TopologyChangeOp.Remove => Right(Remove)
      case v0.TopologyChangeOp.Replace => Right(Replace)
      case v0.TopologyChangeOp.Unrecognized(x) => Left(UnrecognizedEnum(protoOp.name, x))
    }

  implicit val setParameterTopologyChangeOp: SetParameter[TopologyChangeOp] = (v, pp) =>
    v match {
      case Add => pp.setInt(1)
      case Remove => pp.setInt(2)
      case Replace => pp.setInt(3)
    }
}

/** Topology transaction id
  *
  * Used to distinguish topology transactions from each other such that a Remove explicitly refers to a
  * corresponding Add, such that we can support re-addition (Add, Remove, Add again).
  */
final case class TopologyElementId(override protected val str: String255)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  def toLengthLimitedString: String255 = str
  // TODO(i4933) validate strings when deserializing from proto (must be safesimplestring)

  override def pretty: Pretty[TopologyElementId] = prettyOfString(_.unwrap)
}

object TopologyElementId extends LengthLimitedStringWrapperCompanion[String255, TopologyElementId] {
  def generate(): TopologyElementId = {
    TopologyElementId(String255.tryCreate(PseudoRandom.randomAlphaNumericString(32)))
  }

  // Reuse externally supplied identifier that needs to be unique.
  def adopt(id: String255): TopologyElementId = TopologyElementId(id)

  override def instanceName: String = "TopologyElementId"

  override protected def companion: String255.type = String255

  override protected def factoryMethodWrapper(str: String255): TopologyElementId =
    TopologyElementId(str)
}

sealed trait TopologyStateElement[+M <: TopologyMapping] extends PrettyPrinting {
  def id: TopologyElementId
  def mapping: M
  def uniquePath: UniquePath
}

final case class TopologyStateUpdateElement(
    id: TopologyElementId,
    mapping: TopologyStateUpdateMapping,
) extends TopologyStateElement[TopologyStateUpdateMapping] {
  override def pretty: Pretty[TopologyStateUpdateElement] =
    prettyOfClass(param("id", _.id), param("mapping", _.mapping))

  lazy val uniquePath: UniquePath = mapping.uniquePath(id)
}

final case class DomainGovernanceElement(mapping: DomainGovernanceMapping)
    extends TopologyStateElement[DomainGovernanceMapping] {
  override def pretty: Pretty[DomainGovernanceElement] =
    prettyOfClass(param("id", _.id), param("mapping", _.mapping))

  lazy val id: TopologyElementId = TopologyElementId(mapping.domainId.toLengthLimitedString)
  lazy val uniquePath: UniquePathSignedDomainGovernanceTransaction =
    mapping.uniquePath(id) // TODO(Rafael): id is not used for the path ; improve API?
}

/** Defines the required authorization chain */
sealed trait RequiredAuth {
  def namespaces: (Seq[Namespace], Boolean)
  def uids: Seq[UniqueIdentifier]
}
object RequiredAuth {

  /** Authorization must be on the namespace level
    *
    * This implies that it must be authorized by a Namespace delegation.
    * The boolean designates if the delegation needs to be a root delegation.
    */
  case class Ns(namespace: Namespace, rootDelegation: Boolean) extends RequiredAuth {
    override def namespaces: (Seq[Namespace], Boolean) = (Seq(namespace), true)
    override def uids: Seq[UniqueIdentifier] = Seq()
  }
  case class Uid(override val uids: Seq[UniqueIdentifier]) extends RequiredAuth {
    override def namespaces: (Seq[Namespace], Boolean) =
      (uids.map(uid => uid.namespace).distinct, false)
  }
}

sealed trait TopologyTransaction[+Op <: TopologyChangeOp]
    extends ProtocolVersionedMemoizedEvidence
    with PrettyPrinting
    with HasProtocolVersionedWrapper[VersionedMessage[TopologyTransaction[TopologyChangeOp]]]
    with HasProtoV0[v0.TopologyTransaction] {
  def op: Op
  def element: TopologyStateElement[TopologyMapping]

  def reverse: TopologyTransaction[TopologyChangeOp]

  // calculate hash for signature
  def hashToSign(hashOps: HashOps): Hash =
    hashOps.digest(HashPurpose.TopologyTransactionSignature, this.getCryptographicEvidence)

  override protected def toProtoVersioned: VersionedMessage[TopologyTransaction[TopologyChangeOp]] =
    TopologyTransaction.toProtoVersioned(this)

  override def toByteStringUnmemoized: ByteString = super[HasProtocolVersionedWrapper].toByteString

  def toProtoV0: v0.TopologyTransaction
}

object TopologyTransaction
    extends HasMemoizedProtocolVersionedWrapperCompanion[TopologyTransaction[TopologyChangeOp]] {
  override val name: String = "TopologyTransaction"

  val supportedProtoVersions = SupportedProtoVersions(
    0 -> VersionedProtoConverter(
      ProtocolVersion.v2_0_0,
      supportedProtoVersionMemoized(v0.TopologyTransaction)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  /** returns true if two transactions are equivalent */
  def equivalent(
      first: TopologyTransaction[TopologyChangeOp],
      second: TopologyTransaction[TopologyChangeOp],
  ): Boolean =
    (first, second) match {
      case (
            TopologyStateUpdate(firstOp, TopologyStateUpdateElement(_id, firstMapping)),
            TopologyStateUpdate(secondOp, TopologyStateUpdateElement(_id2, secondMapping)),
          ) =>
        firstOp == secondOp && firstMapping == secondMapping

      case (
            DomainGovernanceTransaction(DomainGovernanceElement(firstMapping)),
            DomainGovernanceTransaction(DomainGovernanceElement(secondMapping)),
          ) =>
        firstMapping == secondMapping

      case (_: TopologyStateUpdate[_], _: DomainGovernanceTransaction) => false

      case (_: DomainGovernanceTransaction, _: TopologyStateUpdate[_]) => false
    }

  def fromProtoV0(transactionP: v0.TopologyTransaction)(
      bytes: ByteString
  ): ParsingResult[TopologyTransaction[TopologyChangeOp]] = transactionP.transaction match {
    case Transaction.Empty => Left(FieldNotSet("TopologyTransaction.transaction.version"))
    case Transaction.StateUpdate(stateUpdate) =>
      TopologyStateUpdate.fromProtoV0(stateUpdate, bytes)
    case Transaction.DomainGovernance(domainGovernance) =>
      DomainGovernanceTransaction.fromProtoV0(domainGovernance, bytes)
  }
}

/** +/-, X -> Y
  *
  * Every topology transaction is the combination of an operation (Add, Remove),
  * a unique element id and the state operation.
  *
  * An Add can pick a random element id. A remove needs to pick the element id of the corresponding addition.
  * Element ids are uniqueness constraints. Once removed, they can't be re-added
  * (during a configurable time window)
  */
sealed abstract case class TopologyStateUpdate[+Op <: AddRemoveChangeOp](
    op: Op,
    element: TopologyStateUpdateElement,
)(
    val representativeProtocolVersion: ProtocolVersion,
    val deserializedFrom: Option[ByteString] = None,
) extends TopologyTransaction[Op] {
  override def toProtoV0: v0.TopologyTransaction = {
    val mappingP: v0.TopologyStateUpdate.Mapping = element.mapping match {
      case x: NamespaceDelegation =>
        v0.TopologyStateUpdate.Mapping.NamespaceDelegation(x.toProtoV0)
      case x: IdentifierDelegation =>
        v0.TopologyStateUpdate.Mapping.IdentifierDelegation(x.toProtoV0)
      case x: OwnerToKeyMapping =>
        v0.TopologyStateUpdate.Mapping.OwnerToKeyMapping(x.toProtoV0)
      case x: PartyToParticipant =>
        v0.TopologyStateUpdate.Mapping.PartyToParticipant(x.toProtoV0)
      case x: SignedLegalIdentityClaim =>
        v0.TopologyStateUpdate.Mapping.SignedLegalIdentityClaim(x.toProtoV0)
      case x: ParticipantState =>
        v0.TopologyStateUpdate.Mapping.ParticipantState(x.toProtoV0)
      case x: MediatorDomainState =>
        v0.TopologyStateUpdate.Mapping.MediatorDomainState(x.toProtoV0)
      case x: VettedPackages =>
        v0.TopologyStateUpdate.Mapping.VettedPackages(x.toProtoV0)

    }

    val stateUpdate = v0.TopologyTransaction.Transaction.StateUpdate(
      v0.TopologyStateUpdate(operation = op.toProto, id = element.id.unwrap, mapping = mappingP)
    )

    v0.TopologyTransaction(stateUpdate)
  }

  /** Create reversion of this transaction
    *
    * If this transaction is an Add, we return a corresponding Remove with the same transaction id.
    * If this transaction is a Remove, we return an Add with a new transaction id.
    */
  def reverse: TopologyTransaction[TopologyChangeOp] = {
    import TopologyChangeOp._

    (op: AddRemoveChangeOp) match {
      case Add => TopologyStateUpdate(Remove, element)(representativeProtocolVersion)
      case Remove => TopologyStateUpdate.createAdd(element.mapping, representativeProtocolVersion)
    }
  }

  override def pretty: Pretty[TopologyStateUpdate.this.type] =
    prettyOfClass(param("op", _.op), param("element", _.element))
}

object TopologyStateUpdate {
  def apply[Op <: AddRemoveChangeOp](
      op: Op,
      element: TopologyStateUpdateElement,
  )(protocolVersion: ProtocolVersion): TopologyStateUpdate[Op] =
    new TopologyStateUpdate(op, element)(
      TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)
    ) {}

  def fromByteString(bytes: ByteString): ParsingResult[TopologyStateUpdate[AddRemoveChangeOp]] =
    for {
      converted <- TopologyTransaction.fromByteString(bytes)
      result <- converted match {
        case topologyStateUpdate: TopologyStateUpdate[_] =>
          Right(topologyStateUpdate)
        case _: DomainGovernanceTransaction =>
          Left(
            ProtoDeserializationError.TransactionDeserialization(
              "Expecting TopologyStateUpdate, found DomainGovernanceTransaction"
            )
          )
      }
    } yield result

  def fromProtoV0(
      protoTopologyTransaction: v0.TopologyStateUpdate,
      bytes: ByteString,
  ): ParsingResult[TopologyStateUpdate[AddRemoveChangeOp]] = {
    val mappingRes: ParsingResult[TopologyStateUpdateMapping] =
      protoTopologyTransaction.mapping match {

        case v0.TopologyStateUpdate.Mapping.IdentifierDelegation(idDelegation) =>
          IdentifierDelegation.fromProtoV0(idDelegation)

        case v0.TopologyStateUpdate.Mapping.NamespaceDelegation(nsDelegation) =>
          NamespaceDelegation.fromProtoV0(nsDelegation)

        case v0.TopologyStateUpdate.Mapping.OwnerToKeyMapping(owkm) =>
          OwnerToKeyMapping.fromProtoV0(owkm)

        case v0.TopologyStateUpdate.Mapping.PartyToParticipant(value) =>
          PartyToParticipant.fromProtoV0(value)

        case v0.TopologyStateUpdate.Mapping.SignedLegalIdentityClaim(value) =>
          SignedLegalIdentityClaim.fromProtoV0(value)

        case v0.TopologyStateUpdate.Mapping.ParticipantState(value) =>
          ParticipantState.fromProtoV0(value)

        case v0.TopologyStateUpdate.Mapping.MediatorDomainState(value) =>
          MediatorDomainState.fromProtoV0(value)

        case v0.TopologyStateUpdate.Mapping.VettedPackages(value) =>
          VettedPackages.fromProtoV0(value)

        case v0.TopologyStateUpdate.Mapping.Empty =>
          Left(UnrecognizedField("TopologyStateUpdate.Mapping is empty"))
      }
    for {
      op <- AddRemoveChangeOp.fromProtoV0(protoTopologyTransaction.operation)
      mapping <- mappingRes
      id <- TopologyElementId.fromProtoPrimitive(protoTopologyTransaction.id)
    } yield new TopologyStateUpdate(op, TopologyStateUpdateElement(id, mapping))(
      TopologyTransaction.protocolVersionRepresentativeFor(0),
      Some(bytes),
    ) {}
  }

  def createAdd(
      mapping: TopologyStateUpdateMapping,
      protocolVersion: ProtocolVersion,
  ): TopologyStateUpdate[TopologyChangeOp.Add] =
    TopologyStateUpdate(
      TopologyChangeOp.Add,
      TopologyStateUpdateElement(TopologyElementId.generate(), mapping),
    )(TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion))
}

sealed abstract case class DomainGovernanceTransaction(
    element: DomainGovernanceElement
)(
    val representativeProtocolVersion: ProtocolVersion,
    val deserializedFrom: Option[ByteString] = None,
) extends TopologyTransaction[TopologyChangeOp.Replace] {
  val op = TopologyChangeOp.Replace

  override def toProtoV0: v0.TopologyTransaction = {
    val mappingP = element.mapping match {
      case x: DomainParametersChange =>
        v0.DomainGovernanceTransaction.Mapping.DomainParametersChange(x.toProtoV0)
    }

    v0.TopologyTransaction(
      v0.TopologyTransaction.Transaction.DomainGovernance(
        v0.DomainGovernanceTransaction(mapping = mappingP)
      )
    )
  }

  override def pretty: Pretty[DomainGovernanceTransaction] = prettyOfClass(
    param("element", _.element)
  )

  def reverse: TopologyTransaction[TopologyChangeOp.Replace] = this
}

object DomainGovernanceTransaction {
  def apply(
      mapping: DomainGovernanceMapping,
      protocolVersion: ProtocolVersion,
  ): DomainGovernanceTransaction =
    new DomainGovernanceTransaction(DomainGovernanceElement(mapping))(
      TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)
    ) {}

  def apply(
      element: DomainGovernanceElement,
      protocolVersion: ProtocolVersion,
  ) = new DomainGovernanceTransaction(element)(
    TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)
  ) {}

  def fromProtoV0(
      protoTopologyTransaction: v0.DomainGovernanceTransaction,
      bytes: ByteString,
  ): ParsingResult[DomainGovernanceTransaction] = {
    val mapping: ParsingResult[DomainGovernanceMapping] = protoTopologyTransaction.mapping match {
      case v0.DomainGovernanceTransaction.Mapping.DomainParametersChange(domainParametersChange) =>
        DomainParametersChange.fromProtoV0(domainParametersChange)

      case v0.DomainGovernanceTransaction.Mapping.Empty =>
        Left(UnrecognizedField("DomainGovernanceTransaction.Mapping is empty"))
    }

    mapping.map(mapping =>
      new DomainGovernanceTransaction(DomainGovernanceElement(mapping))(
        TopologyTransaction.protocolVersionRepresentativeFor(0),
        Some(bytes),
      ) {}
    )
  }

  def fromByteString(bytes: ByteString): ParsingResult[DomainGovernanceTransaction] =
    for {
      converted <- TopologyTransaction.fromByteString(bytes)
      result <- converted match {
        case _: TopologyStateUpdate[_] =>
          Left(
            ProtoDeserializationError.TransactionDeserialization(
              "Expecting DomainGovernanceTransaction, found TopologyStateUpdate"
            )
          )
        case domainGovernanceTransaction: DomainGovernanceTransaction =>
          Right(domainGovernanceTransaction)

      }
    } yield result
}
