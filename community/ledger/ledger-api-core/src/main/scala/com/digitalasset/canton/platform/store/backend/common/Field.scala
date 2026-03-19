// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.platform.store.interning.StringInterning

import java.lang
import java.sql.PreparedStatement
import scala.reflect.ClassTag

/** @tparam From
  *   is an arbitrary type from which we can extract the data of interest for the particular column
  * @tparam To
  *   is the intermediary type of the result of the extraction. From => To functionality is intended
  *   to be injected at Schema definition time. To is not nullable, should express a clean Scala
  *   type
  * @tparam Converted
  *   is the (possibly primitive) type needed by the JDBC API To => Converted is intended to be
  *   injected at PGField definition time. Converted might be nullable, primitive, boxed-type,
  *   whatever the JDBC API requires
  */
private[backend] abstract class Field[From, To, Converted](implicit
    classTag: ClassTag[Converted]
) {
  def extract: StringInterning => From => To
  def convert: To => Converted
  def selectFieldExpression(inputFieldName: String): String = inputFieldName

  final def toArray(
      input: Vector[From],
      stringInterning: StringInterning,
  ): Array[Converted] =
    input.view
      .map(extract(stringInterning) andThen convert)
      .toArray(classTag)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  final def prepareData(preparedStatement: PreparedStatement, index: Int, value: Any): Unit =
    prepareDataTemplate(
      preparedStatement,
      index,
      value.asInstanceOf[Converted],
    ) // this cast is safe by design

  def prepareDataTemplate(
      preparedStatement: PreparedStatement,
      index: Int,
      value: Converted,
  ): Unit =
    preparedStatement.setObject(index, value)
}

private[backend] abstract class TrivialField[From, To](implicit classTag: ClassTag[To])
    extends Field[From, To, To] {
  override def convert: To => To = identity
}

private[backend] trait TrivialOptionalField[From, To >: Null <: AnyRef]
    extends Field[From, Option[To], To] {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[To] => To = _.orNull
}

private[backend] final case class StringField[From](extract: StringInterning => From => String)
    extends TrivialField[From, String]

private[backend] final case class StringOptional[From](
    extract: StringInterning => From => Option[String]
) extends TrivialOptionalField[From, String]

private[backend] final case class Bytea[From](extract: StringInterning => From => Array[Byte])
    extends TrivialField[From, Array[Byte]]

private[backend] final case class ByteaOptional[From](
    extract: StringInterning => From => Option[Array[Byte]]
) extends TrivialOptionalField[From, Array[Byte]]

private[backend] final case class Integer[From](extract: StringInterning => From => Int)
    extends TrivialField[From, Int]

private[backend] final case class BooleanField[From](extract: StringInterning => From => Boolean)
    extends TrivialField[From, Boolean]

private[backend] final case class IntOptional[From](extract: StringInterning => From => Option[Int])
    extends Field[From, Option[Int], java.lang.Integer] {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[Int] => java.lang.Integer = _.map(Int.box).orNull
}

private[backend] final case class Bigint[From](extract: StringInterning => From => Long)
    extends TrivialField[From, Long]

private[backend] final case class BigintOptional[From](
    extract: StringInterning => From => Option[Long]
) extends Field[From, Option[Long], java.lang.Long] {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[Long] => java.lang.Long = _.map(Long.box).orNull
}

private[backend] final case class Smallint[From](extract: StringInterning => From => Int)
    extends TrivialField[From, Int]

private[backend] final case class SmallintOptional[From](
    extract: StringInterning => From => Option[Int]
) extends Field[From, Option[Int], java.lang.Integer] {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[Int] => java.lang.Integer = _.map(Int.box).orNull
}

private[backend] final case class BooleanOptional[From](
    extract: StringInterning => From => Option[Boolean]
) extends Field[From, Option[Boolean], java.lang.Boolean] {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[Boolean] => lang.Boolean = _.map(Boolean.box).orNull
}

private[backend] final case class BooleanMandatory[From](
    extract: StringInterning => From => Boolean
) extends TrivialField[From, Boolean]

private[backend] final case class StringArray[From](
    extract: StringInterning => From => Iterable[String]
) extends Field[From, Iterable[String], Array[String]] {
  override def convert: Iterable[String] => Array[String] = _.toArray
}
