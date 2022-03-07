// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.kernel.Order
import cats.syntax.either._
import com.daml.ledger.client.binding.Primitive.{Party => ClientParty}
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.RequireTypes.{LengthLimitedString, String300}
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.{DomainId, LedgerParticipantId, LfPartyId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/** Top level trait representing an identity within the system */
sealed trait Identity extends PrettyPrinting {
  def uid: UniqueIdentifier

  def toProtoPrimitive: String = uid.toProtoPrimitive

  /** returns the string representation used in console filters (maps to the uid) */
  def filterString: String = uid.toProtoPrimitive

  override def pretty: Pretty[this.type] = prettyOfParam(_.uid)
}

sealed trait KeyOwnerCode {

  def threeLetterId: LengthLimitedString

  def toProtoPrimitive: String = threeLetterId.unwrap

}

object KeyOwnerCode {

  def fromProtoPrimitive_(code: String): Either[String, KeyOwnerCode] =
    LengthLimitedString.create(code, 3).flatMap {
      case MediatorId.Code.threeLetterId => Right(MediatorId.Code)
      case DomainTopologyManagerId.Code.threeLetterId => Right(DomainTopologyManagerId.Code)
      case ParticipantId.Code.threeLetterId => Right(ParticipantId.Code)
      case SequencerId.Code.threeLetterId => Right(SequencerId.Code)
      case UnauthenticatedMemberId.Code.threeLetterId => Right(UnauthenticatedMemberId.Code)
      case _ => Left(s"Unknown three letter type $code")
    }

  def fromProtoPrimitive(
      code: String,
      field: String,
  ): ParsingResult[KeyOwnerCode] =
    fromProtoPrimitive_(code).leftMap(ValueConversionError(field, _))

}

/** An identity within the system that owns a key */
sealed trait KeyOwner extends Identity {

  def code: KeyOwnerCode

  override def toProtoPrimitive: String = toLengthLimitedString.unwrap
  def toLengthLimitedString: String300 =
    String300.tryCreate(
      s"${code.threeLetterId.unwrap}${SafeSimpleString.delimiter}${uid.toProtoPrimitive}"
    )

  override def pretty: Pretty[KeyOwner] =
    prettyOfString(inst =>
      inst.code.threeLetterId.unwrap + SafeSimpleString.delimiter + inst.uid.show
    )
}

object KeyOwner {

  def fromProtoPrimitive_(keyOwner: String): Either[String, KeyOwner] = {
    // The first three letters of the string identify the type of member
    val (typ, uidS) = keyOwner.splitAt(3)

    def mapToType(code: KeyOwnerCode, uid: UniqueIdentifier): Either[String, KeyOwner] = {
      code match {
        case MediatorId.Code => Right(MediatorId(uid))
        case DomainTopologyManagerId.Code => Right(DomainTopologyManagerId(uid))
        case ParticipantId.Code => Right(ParticipantId(uid))
        case SequencerId.Code => Right(SequencerId(uid))
        case UnauthenticatedMemberId.Code => Right(UnauthenticatedMemberId(uid))
      }
    }

    // expecting COD::<uid>
    val dlen = SafeSimpleString.delimiter.length

    for {
      _ <- Either.cond(
        keyOwner.length > 3 + (2 * dlen),
        (),
        s"Invalid keyOwner `$keyOwner`, expecting <three-letter-code>::id::fingerprint.",
      )
      _ <- Either.cond(
        keyOwner.substring(3, 3 + dlen) == SafeSimpleString.delimiter,
        (),
        s"Expected delimiter after three letter code of `$keyOwner``",
      )
      code <- KeyOwnerCode.fromProtoPrimitive_(typ)
      uid <- UniqueIdentifier.fromProtoPrimitive_(uidS.substring(dlen))
      keyOwner <- mapToType(code, uid)
    } yield keyOwner
  }

  def fromProtoPrimitive(
      keyOwner: String,
      fieldName: String,
  ): ParsingResult[KeyOwner] =
    fromProtoPrimitive_(keyOwner).leftMap(ValueConversionError(fieldName, _))

}

/** A member in a domain such as a participant and or domain entities
  *
  * A member can be addressed and talked to on the transaction level
  * through the sequencer. Therefore every member is a KeyOwner. And the
  * sequencer is not a member, as he is one level below, dealing with
  * messages.
  */
sealed trait Member extends KeyOwner with Product with Serializable

object Member {

  def fromProtoPrimitive(
      member: String,
      fieldName: String,
  ): ParsingResult[Member] =
    KeyOwner.fromProtoPrimitive(member, fieldName).flatMap {
      case x: Member => Right(x)
      case _ =>
        Left(
          ProtoDeserializationError
            .ValueDeserializationError(fieldName, s"Value `$member` is not of type Member")
        )
    }

  implicit val memberOrdering: Ordering[Member] =
    Ordering.by(x => (x.code.threeLetterId.unwrap, x.uid.namespace.unwrap, x.uid.id.unwrap))

  implicit val memberOrder: Order[Member] = Order.fromOrdering

  /** Instances for slick to set and get members.
    * Not exposed by default as other types derived from [[Member]] have their own persistence schemes ([[ParticipantId]]).
    */
  object DbStorageImplicits {
    implicit val setParameterMember: SetParameter[Member] = (v: Member, pp) =>
      pp >> v.toLengthLimitedString

    implicit val getResultMember: GetResult[Member] = GetResult(r => {
      KeyOwner
        .fromProtoPrimitive_(r.nextString())
        .fold(
          err => throw new DbDeserializationException(err),
          {
            case member: Member => member
            case _ => throw new DbDeserializationException("Unknown type of member")
          },
        )
    })
  }
}

sealed trait AuthenticatedMember extends Member {
  override def code: AuthenticatedMemberCode
}

sealed trait AuthenticatedMemberCode extends KeyOwnerCode

case class UnauthenticatedMemberId(uid: UniqueIdentifier) extends Member {
  override def code: KeyOwnerCode = UnauthenticatedMemberId.Code
}

object UnauthenticatedMemberId {
  object Code extends KeyOwnerCode {
    val threeLetterId: LengthLimitedString = LengthLimitedString.tryCreate("UNM", 3)
  }

  private val RandomIdentifierNumberOfBytes = 20

  def tryCreate(namespace: Namespace): UnauthenticatedMemberId =
    UnauthenticatedMemberId(
      UniqueIdentifier.tryCreate(
        HexString.toHexString(SecureRandomness.randomByteString(RandomIdentifierNumberOfBytes)),
        namespace.fingerprint.unwrap,
      )
    )
}

/** A participant identifier */
case class ParticipantId(uid: UniqueIdentifier) extends AuthenticatedMember {

  override def code: AuthenticatedMemberCode = ParticipantId.Code

  def adminParty: PartyId = PartyId(uid)
  def toLf: LedgerParticipantId = LedgerParticipantId.assertFromString(uid.toProtoPrimitive)
}

object ParticipantId {
  object Code extends AuthenticatedMemberCode {
    val threeLetterId: LengthLimitedString = LengthLimitedString.tryCreate("PAR", 3)
  }
  def apply(identifier: Identifier, namespace: Namespace): ParticipantId =
    ParticipantId(UniqueIdentifier(identifier, namespace))

  /** create a participant from a string
    *
    * used in testing
    */
  @VisibleForTesting
  def apply(addr: String): ParticipantId = {
    ParticipantId(UniqueIdentifier.tryCreate(addr, "default"))
  }

  implicit val ordering: Ordering[ParticipantId] = Ordering.by(_.uid.toProtoPrimitive)

  def fromProtoPrimitive(
      proto: String,
      fieldName: String,
  ): ParsingResult[ParticipantId] =
    KeyOwner.fromProtoPrimitive(proto, fieldName).flatMap {
      case x: ParticipantId => Right(x)
      case y =>
        Left(
          ProtoDeserializationError
            .ValueDeserializationError(fieldName, s"Value $y is not of type `ParticipantId`")
        )
    }

  def fromLfParticipant(lfParticipant: LedgerParticipantId): Either[String, ParticipantId] =
    UniqueIdentifier.fromProtoPrimitive_(lfParticipant).map(ParticipantId(_))

  def tryFromLfParticipant(lfParticipant: LedgerParticipantId): ParticipantId =
    fromLfParticipant(lfParticipant).fold(
      e => throw new IllegalArgumentException(e),
      Predef.identity,
    )

  def tryFromProtoPrimitive(str: String): ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive(str)
  )

  // Instances for slick (db) queries
  implicit val getResultParticipantId: GetResult[ParticipantId] =
    UniqueIdentifier.getResult.andThen(ParticipantId(_))
  implicit val setParameterParticipantId: SetParameter[ParticipantId] =
    (p: ParticipantId, pp: PositionedParameters) => pp >> p.uid.toLengthLimitedString
}

/** A party identifier based on a unique identifier
  */
case class PartyId(uid: UniqueIdentifier) extends Identity {

  def toLf: LfPartyId = LfPartyId.assertFromString(uid.toProtoPrimitive)

  def toPrim: ClientParty = ClientParty(toLf)
}

object PartyId {

  implicit val ordering: Ordering[PartyId] = Ordering.by(x => x.toProtoPrimitive)
  implicit val getResultPartyId: GetResult[PartyId] =
    UniqueIdentifier.getResult.andThen(PartyId(_))
  implicit val setParameterPartyId: SetParameter[PartyId] =
    (p: PartyId, pp: PositionedParameters) => pp >> p.uid.toLengthLimitedString

  def apply(identifier: Identifier, namespace: Namespace): PartyId =
    PartyId(UniqueIdentifier(identifier, namespace))

  def fromLfParty(lfParty: LfPartyId): Either[String, PartyId] =
    UniqueIdentifier.fromProtoPrimitive_(lfParty).map(PartyId(_))

  def tryFromLfParty(lfParty: LfPartyId): PartyId =
    fromLfParty(lfParty) match {
      case Right(partyId) => partyId
      case Left(e) => throw new IllegalArgumentException(e)
    }

  def tryFromProtoPrimitive(str: String): PartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive(str)
  )

}

sealed trait DomainMember extends AuthenticatedMember

object DomainMember {

  /** List domain members for the given id, optionally including the sequencer. * */
  def list(id: DomainId, includeSequencer: Boolean): Set[DomainMember] = {
    // TODO(i7992) remove static mediator id
    val baseMembers = Set[DomainMember](DomainTopologyManagerId(id), MediatorId(id))
    if (includeSequencer) baseMembers + SequencerId(id)
    else baseMembers
  }

  /** List all domain members always including the sequencer. */
  def listAll(id: DomainId): Set[DomainMember] = list(id, includeSequencer = true)
}

case class MediatorId(uid: UniqueIdentifier) extends DomainMember {
  override def code: AuthenticatedMemberCode = MediatorId.Code
}

object MediatorId {
  object Code extends AuthenticatedMemberCode {
    val threeLetterId = LengthLimitedString.tryCreate("MED", 3)
  }

  def apply(identifier: Identifier, namespace: Namespace): MediatorId =
    MediatorId(UniqueIdentifier(identifier, namespace))

  def apply(domainId: DomainId): MediatorId = MediatorId(domainId.unwrap)

  def fromProtoPrimitive(
      mediatorId: String,
      fieldName: String,
  ): ParsingResult[MediatorId] = Member.fromProtoPrimitive(mediatorId, fieldName).flatMap {
    case medId: MediatorId => Right(medId)
    case _ =>
      Left(
        ProtoDeserializationError
          .ValueDeserializationError(fieldName, s"Value `$mediatorId` is not of type MediatorId")
      )
  }

}

/** The domain topology manager id
  *
  * The domain manager is the topology manager of the domain. The read side
  * of the domain manager is the IdentityProvidingService.
  */
case class DomainTopologyManagerId(uid: UniqueIdentifier) extends DomainMember {
  override def code: AuthenticatedMemberCode = DomainTopologyManagerId.Code
  def domainId: DomainId = DomainId(uid)
}

object DomainTopologyManagerId {

  object Code extends AuthenticatedMemberCode {
    val threeLetterId = LengthLimitedString.tryCreate("DOM", 3)
  }

  def apply(identifier: Identifier, namespace: Namespace): DomainTopologyManagerId =
    DomainTopologyManagerId(UniqueIdentifier(identifier, namespace))

  def apply(domainId: DomainId): DomainTopologyManagerId = DomainTopologyManagerId(domainId.unwrap)
}

case class SequencerId(uid: UniqueIdentifier) extends DomainMember {
  override def code: AuthenticatedMemberCode = SequencerId.Code
}

object SequencerId {

  object Code extends AuthenticatedMemberCode {
    val threeLetterId = LengthLimitedString.tryCreate("SEQ", 3)
  }

  def apply(identifier: Identifier, namespace: Namespace): SequencerId =
    SequencerId(UniqueIdentifier(identifier, namespace))

  def apply(domainId: DomainId): SequencerId = SequencerId(domainId.unwrap)
}
