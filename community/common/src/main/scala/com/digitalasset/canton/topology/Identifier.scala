// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.Order
import cats.implicits._
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.RequireTypes.{
  LengthLimitedStringWrapper,
  String185,
  String255,
}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.{LfPartyId, checked}
import io.circe.Encoder
import slick.jdbc.{GetResult, SetParameter}
import com.digitalasset.canton.util.ShowUtil._

/** utility class to ensure that strings conform to LF specification minus our internal delimiter */
object SafeSimpleString {

  val delimiter = "::"

  def fromProtoPrimitive(str: String): Either[String, String] = {
    for {
      _ <- LfPartyId.fromString(str)
      opt <- Either.cond(
        !str.contains(delimiter),
        str,
        s"String contains reserved delimiter `$delimiter`.",
      )
    } yield opt
  }

}

/** An identifier such as a random or a readable string
  */
final case class Identifier private (protected val str: String185)
    extends LengthLimitedStringWrapper
    with NoCopy
    with PrettyPrinting {
  def toLengthLimitedString: String185 = str

  override def pretty: Pretty[Identifier] = prettyOfString(_.unwrap)
}

object Identifier {

  private[this] def apply(str: String185): Identifier =
    throw new UnsupportedOperationException("Use the (try)create method instead")

  def create(str: String): Either[String, Identifier] =
    for {
      idString <- SafeSimpleString.fromProtoPrimitive(str)
      string185 <- String185.create(idString)
    } yield new Identifier(string185)

  def tryCreate(str: String): Identifier =
    create(str).valueOr(err => throw new IllegalArgumentException(s"Invalid identifier $str: $err"))

  def fromProtoPrimitive(str: String): Either[String, Identifier] = create(str)

  implicit val getResultIdentifier: GetResult[Identifier] = GetResult { r =>
    Identifier
      .fromProtoPrimitive(r.nextString())
      .valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize Identifier: $err")
      )
  }

  implicit val setParameterIdentifier: SetParameter[Identifier] = (v, pp) =>
    pp.setString(v.toProtoPrimitive)
  implicit val setParameterIdentifierOption: SetParameter[Option[Identifier]] = (v, pp) =>
    pp.setStringOption(v.map(_.toProtoPrimitive))

  implicit val namespaceOrder: Order[Identifier] = Order.by[Identifier, String](_.unwrap)

  implicit val domainAliasEncoder: Encoder[Identifier] =
    Encoder.encodeString.contramap[Identifier](_.unwrap)

}

object Namespace {
  implicit val setParameterNamespace: SetParameter[Namespace] = (v, pp) =>
    pp.setString(v.toProtoPrimitive)
  implicit val namespaceOrder: Order[Namespace] = Order.by[Namespace, String](_.unwrap)
  implicit val setParameterOptionNamespace: SetParameter[Option[Namespace]] = (v, pp) =>
    pp.setStringOption(v.map(_.toProtoPrimitive))
}

// architecture-handbook-entry-begin: UniqueIdentifier
/** A namespace spanned by the fingerprint of a pub-key
  *
  * This is based on the assumption that the fingerprint is unique to the public-key
  */
final case class Namespace(fingerprint: Fingerprint) extends PrettyPrinting {
  def unwrap: String = fingerprint.unwrap
  def toProtoPrimitive: String = fingerprint.toProtoPrimitive
  override def pretty: Pretty[Namespace] = prettyOfParam(_.fingerprint)
}

/** a unique identifier within a namespace
  * Based on the Ledger API PartyIds/LedgerStrings being limited to 255 characters, we allocate
  * - 64 + 4 characters to the namespace/fingerprint (essentially SHA256 with extra bytes),
  * - 2 characters as delimiters, and
  * - the last 185 characters for the Identifier.
  */
final case class UniqueIdentifier(id: Identifier, namespace: Namespace) extends PrettyPrinting {
// architecture-handbook-entry-end: UniqueIdentifier
  def toProtoPrimitive: String =
    id.toProtoPrimitive + SafeSimpleString.delimiter + namespace.toProtoPrimitive

  def toLengthLimitedString: String255 = checked(String255.tryCreate(toProtoPrimitive))

  override def pretty: Pretty[this.type] =
    prettyOfString(uid => uid.id.show + SafeSimpleString.delimiter + uid.namespace.show)
}

object UniqueIdentifier {

  def tryCreate(id: String, fingerprint: String): UniqueIdentifier =
    UniqueIdentifier(Identifier.tryCreate(id), Namespace(Fingerprint.tryCreate(fingerprint)))

  def tryFromProtoPrimitive(str: String): UniqueIdentifier =
    fromProtoPrimitive_(str).valueOr(e => throw new IllegalArgumentException(e))

  def fromProtoPrimitive_(str: String): Either[String, UniqueIdentifier] = {
    val pos = str.indexOf(SafeSimpleString.delimiter)
    if (pos > 0) {
      val s1 = str.substring(0, pos)
      val s2 = str.substring(pos + 2)
      for {
        idf <- Identifier
          .fromProtoPrimitive(s1)
          .leftMap(x => s"Identifier decoding of `${str.limit(200)}` failed with: $x")
        fp <- Fingerprint
          .fromProtoPrimitive(s2)
          .leftMap(x => s"Fingerprint decoding of `${str.limit(200)}` failed with: $x")
      } yield UniqueIdentifier(idf, Namespace(fp))
    } else if (pos == 0) {
      Left(s"Invalid unique identifier `$str` with empty identifier.")
    } else if (str.isEmpty) {
      Left(s"Empty string is not a valid unique identifier.")
    } else {
      Left(s"Invalid unique identifier `$str` with missing namespace.")
    }
  }

  def fromProtoPrimitive(
      uid: String,
      fieldName: String,
  ): ParsingResult[UniqueIdentifier] =
    fromProtoPrimitive_(uid).leftMap(ValueConversionError(fieldName, _))

  // slick instance for deserializing unique identifiers
  // not an implicit because we shouldn't ever need to parse a raw unique identifier
  val getResult: GetResult[UniqueIdentifier] = GetResult(r => deserializeFromDb(r.nextString()))
  val getResultO: GetResult[Option[UniqueIdentifier]] =
    GetResult(r => r.nextStringOption().map(deserializeFromDb))
  implicit val setParameterUid: SetParameter[UniqueIdentifier] = (v, pp) =>
    pp.setString(v.toProtoPrimitive)

  /** @throws com.digitalasset.canton.store.db.DbDeserializationException if the string is not a valid unqiue identifier */
  def deserializeFromDb(uid: String): UniqueIdentifier =
    fromProtoPrimitive_(uid).valueOr(err =>
      throw new DbDeserializationException(s"Failed to parse a unique ID $uid: $err")
    )
}
