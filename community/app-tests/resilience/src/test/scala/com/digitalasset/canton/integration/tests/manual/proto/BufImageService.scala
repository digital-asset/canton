// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.proto

import buf.alpha.image.v1.image.ImageFile
import cats.syntax.functorFilter.*
import com.digitalasset.canton.integration.tests.manual.proto.BufImageParser.cantonPackagePrefix
import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.getNonEmptyString
import com.google.protobuf.descriptor.DescriptorProto
import monocle.macros.syntax.lens.*

/** Creates an image that can be used to detect removed endpoint/services. The image contains:
  *   - Services
  *   - The messages for requests and responses. Those messages are stripped of their fields.
  */
class BufImageService(img: buf.alpha.image.v1.image.Image, structure: BufImageStructure) {

  // Messages that we want to keep: request and response for each endpoint
  private val keep: Set[String] = structure.endpoints.flatMap { case (_, (request, response)) =>
    Set(request.str, response.str)
  }.toSet

  def create(): buf.alpha.image.v1.image.Image =
    img.focus(_.file).modify(_.mapFilter(trimFile()))

  /** Only keep the nodes that are listed in `keep` but remove their fields since this image is only
    * used to detect endpoints that are removed.
    */
  private def trimDescriptor(fullName: String, d: DescriptorProto): Option[DescriptorProto] = {
    val newNestedType = d.nestedType.mapFilter { nestedType =>
      val nestedTypeName = getNonEmptyString(nestedType.name)
      val nestedTypeFullName = s"$fullName.$nestedTypeName"

      trimDescriptor(nestedTypeFullName, nestedType)
    }

    // Keep descriptor if it is explicitly required (endpoint) or if it has nested messages (i.e. kids).
    Option.when(keep.contains(fullName) || newNestedType.nonEmpty)(
      // Keep only messages: remove fields, enum
      d.copy(field = Nil, enumType = Nil, nestedType = newNestedType)
    )
  }

  private def trimFile()(file: ImageFile): Option[ImageFile] = {
    val pkg = getNonEmptyString(file.`package`)

    // Only keep Canton files
    if (pkg.startsWith(cantonPackagePrefix)) {
      val newMessageType = file.messageType.mapFilter { mT =>
        val name = getNonEmptyString(mT.name)

        trimDescriptor(s"$pkg.$name", mT)
      }

      Some(file.copy(messageType = newMessageType))
    } else None
  }
}
