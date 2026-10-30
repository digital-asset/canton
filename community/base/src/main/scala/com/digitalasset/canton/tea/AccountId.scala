// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import slick.jdbc.GetResult

/** Traffic account id: one or more non-empty segments joined by `/`, at most 255 characters in
  * total. Segments use the Ledger API user id characters plus space, so every party id is valid.
  */
final case class AccountId(str: String255) extends LengthLimitedStringWrapper

object AccountId extends LengthLimitedStringWrapperCompanion[String255, AccountId] {
  override def instanceName: String = "AccountId"
  override protected def companion: String255.type = String255
  override protected def factoryMethodWrapper(str: String255): AccountId = AccountId(str)

  private val specialChars = "@^$.!`-#+'~_|:()"
  private val segment = s"""[a-zA-Z0-9 ${specialChars.map(c => s"\\$c").mkString}]+"""
  private val pattern = s"$segment(?:/$segment)*".r.pattern

  private def checkFormat(id: AccountId): Either[String, AccountId] =
    Either.cond(
      pattern.matcher(id.unwrap).matches(),
      id,
      s"Invalid $instanceName '${id.unwrap}': expected non-empty segments separated by '/', " +
        s"made of ASCII letters, digits, spaces, and $specialChars",
    )

  override def create(str: String): Either[String, AccountId] =
    super.create(str).flatMap(checkFormat)

  override def tryCreate(str: String): AccountId =
    create(str).valueOr(err => throw new IllegalArgumentException(err))

  override def fromProtoPrimitive(str: String): ParsingResult[AccountId] =
    fromProtoPrimitive(str, instanceName)

  override def fromProtoPrimitive(str: String, field: String): ParsingResult[AccountId] =
    super
      .fromProtoPrimitive(str, field)
      .flatMap(checkFormat(_).leftMap(ValueConversionError(field, _)))

  // Some rows were stored before we added the new format, so we only check the length here.
  // All stored ids were already String255, and most are party ids that match the format anyway.
  private def fromDb(str: String): AccountId =
    String255
      .fromProtoPrimitive(str, instanceName)
      .map(AccountId(_))
      .valueOr(err => throw new DbDeserializationException(err.toString))

  override implicit val getResultWrapper: GetResult[AccountId] =
    GetResult(r => fromDb(r.nextString()))

  override implicit val getResultOptionWrapper: GetResult[Option[AccountId]] =
    GetResult(r => r.nextStringOption().map(fromDb))
}
