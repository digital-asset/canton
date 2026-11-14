// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.proto

import cats.syntax.either.*
import com.digitalasset.canton.integration.tests.manual.proto.BufImageParser.{
  cantonPackagePrefix,
  supportedCompanionExtensions,
}
import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.MessageCategory.*
import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.{
  MessageCategory,
  RequestType,
  ResponseType,
  getNonEmptyString,
}
import com.google.protobuf.descriptor.{DescriptorProto, FieldDescriptorProto}
import scalapb.options.MessageOptions

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.util.Try

/** Parse a buf binary image and extract its [[BufImageStructure]].
  *
  * The relation parent -> kid is with usage as a field: message M1 is parent of message M2 if one
  * of the field of M1 has type M2.
  *
  * The reason it is a class and not an object is readability: it removes the need of passing the
  * parameters and maps around.
  */
class BufImageParser(img: buf.alpha.image.v1.image.Image) {
  import org.scalatest.Assertions.*
  import org.scalatest.OptionValues.*

  // parent -> kids
  private val forest: mutable.Map[String, Set[String]] = mutable.Map.empty
  private val messageCategories: mutable.Map[String, Set[MessageCategory]] = mutable.Map.empty
  private val endpoints: mutable.Map[String, (RequestType, ResponseType)] = mutable.Map.empty

  private def isEmpty: AtomicBoolean = new AtomicBoolean(true)

  def parse(): BufImageStructure = {
    if (!isEmpty.compareAndSet(true, false))
      fail("Method parse should be called at most once")

    parseImage()

    val haveParent = forest.values.flatten.toSet
    val roots = forest.keySet.toSet.diff(haveParent)

    // Propagate the categories: if a node is in category C, then all its kids are as well.
    propagateCategories(roots)

    BufImageStructure(
      forest = forest.toMap,
      messageCategories = messageCategories.toMap,
      roots = roots,
      endpoints = endpoints.toMap,
    )
  }

  /** Read the image and populate the maps with initial data.
    */
  private def parseImage(): Unit =
    img.file.foreach { file =>
      val pkg = getNonEmptyString(file.`package`)

      if (pkg.startsWith(cantonPackagePrefix)) {
        // messages
        file.messageType.foreach { mT =>
          val name = getNonEmptyString(mT.name)

          parseDescriptor(s"$pkg.$name", mT)
        }

        // services
        file.service.foreach { service =>
          val serviceName = getNonEmptyString(service.name)
          val fullServiceName = s"$pkg.$serviceName"

          service.method.foreach { method =>
            val request = removeInitialDot(getNonEmptyString(method.inputType))
            val response = removeInitialDot(getNonEmptyString(method.outputType))
            val endpoint = getNonEmptyString(method.name)

            endpoints.addOne(
              (s"$fullServiceName.$endpoint", (RequestType(request), ResponseType(response)))
            )

            addToMap(messageCategories, request, Request)
            addToMap(messageCategories, response, Response)
          }
        }
      }
    }

  private def addToMap[V](map: mutable.Map[String, Set[V]], key: String, v: V): Unit =
    map.updateWith(key) {
      case Some(values) => Some(values + v)
      case None => Some(Set(v))
    }

  private def addToMap[V](
      map: mutable.Map[String, Set[V]],
      key: String,
      values: Iterable[V],
  ): Unit =
    if (values.nonEmpty)
      map.updateWith(key) {
        case Some(existingValues) => Some(existingValues ++ values)
        case None => Some(values.toSet)
      }

  /** Extract categories from the MessageOptions.
    */
  private def parseMessageOptions(messageName: String)(
      messageOptions: scalapb.options.MessageOptions
  ): Seq[MessageCategory] = messageOptions match {
    // This verbose case allows to detect if MessageOptions is changed
    case scalapb.options.MessageOptions(
          Seq(),
          companionExtends,
          Seq(),
          None,
          Seq(),
          Seq(),
          None,
          Seq(),
          None,
          Seq(),
          Seq(),
          Seq(),
          scalapb.UnknownFieldSet.empty,
        ) =>
      val (unsupportedCompanionExtensions, validCompanionExtensions) =
        companionExtends.partitionMap(v => supportedCompanionExtensions.get(v).toRight(v))

      if (unsupportedCompanionExtensions.nonEmpty)
        fail(
          s"Found unsupported companion extensions in $messageName: $unsupportedCompanionExtensions"
        )

      validCompanionExtensions

    case other =>
      fail(s"Unable to parse MessageOptions for message $messageName. Found: $other")
  }

  /** Extracts categories from the descriptor.
    */
  private def parseDescriptorOptions(
      messageName: String
  )(descriptor: DescriptorProto): Seq[MessageCategory] =
    descriptor.options match {
      case Some(options) =>
        options.unknownFields.asMap.values.flatMap {
          case scalapb.UnknownFieldSet.Field(Seq(), Seq(), Seq(), byteStrings) =>
            byteStrings.flatMap { bs =>
              parseMessageOptions(messageName)(
                Try(MessageOptions.parseFrom(bs.toByteArray)).toEither.valueOr { err =>
                  fail(s"Unable to parse message options of $messageName: $err")
                }
              )
            }

          case other => fail(s"Unable to parse options for message $messageName. Found: $other")
        }.toSeq

      case None => Nil
    }

  // protobuf prefixes some paths with .
  private def removeInitialDot(str: String): String =
    if (str.headOption.contains('.')) str.drop(1) else str

  private def parseDescriptor(fullName: String, d: DescriptorProto): Unit = {
    forest.put(fullName, Set())

    addToMap(messageCategories, fullName, parseDescriptorOptions(fullName)(d))

    // Extract parent -> kid relationship from fields
    d.field.foreach { field =>
      if (field.`type`.value == FieldDescriptorProto.Type.TYPE_MESSAGE) {
        val typeName = removeInitialDot(getNonEmptyString(field.typeName))

        if (typeName.startsWith(cantonPackagePrefix))
          addToMap(forest, fullName, typeName)
      }
    }

    // Recursive call
    d.nestedType.foreach { nestedType =>
      val nestedTypeName = getNonEmptyString(nestedType.name)
      val nestedTypeFullName = s"$fullName.$nestedTypeName"

      if (nestedTypeFullName.startsWith(cantonPackagePrefix))
        parseDescriptor(nestedTypeFullName, nestedType)
    }
  }

  /** Propagate categories transitively from parent to kids.
    */
  private def propagateCategories(roots: Set[String]): Unit = {
    def propagateCategories(
        nodes: Set[String],
        categories: Set[MessageCategory],
        visited: Set[String],
    ): Unit =
      nodes.foreach { node =>
        val nodeCategories = messageCategories.getOrElse(node, Set.empty) ++ categories
        addToMap(messageCategories, node, nodeCategories)

        // avoid infinite recursion for protos depending on themselves
        val newNodes = forest.get(node).value.diff(visited)

        propagateCategories(newNodes, nodeCategories, visited + node)
      }

    propagateCategories(roots, Set(), Set())
  }
}

object BufImageParser {
  lazy val cantonPackagePrefix = "com.digitalasset.canton"

  lazy val supportedCompanionExtensions: Map[String, MessageCategory] = Map(
    "com.digitalasset.canton.version.StorageProtoVersion" -> Storage,
    "com.digitalasset.canton.version.AlphaProtoVersion" -> Alpha,
    "com.digitalasset.canton.version.StableProtoVersion" -> Stable,
  )
}
