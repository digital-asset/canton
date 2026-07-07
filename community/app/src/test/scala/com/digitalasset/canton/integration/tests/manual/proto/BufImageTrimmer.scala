// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.proto

import buf.alpha.image.v1.image.ImageFile
import cats.syntax.functorFilter.*
import com.digitalasset.canton.integration.tests.manual.proto.BufImageParser.cantonPackagePrefix
import com.digitalasset.canton.integration.tests.manual.proto.BufImageStructure.{
  MessageCategory,
  getNonEmptyString,
}
import com.google.protobuf.descriptor.DescriptorProto
import monocle.macros.syntax.lens.*

/** Allow to trim an image to keep only messages of a single category.
  */
class BufImageTrimmer(img: buf.alpha.image.v1.image.Image, structure: BufImageStructure) {

  def trim(keep: MessageCategory): buf.alpha.image.v1.image.Image =
    img.focus(_.file).modify(_.map(trimFile(keep = keep)))

  private def trimDescriptor(
      keep: MessageCategory
  )(fullName: String, d: DescriptorProto): Option[DescriptorProto] = {
    // Only keep messages that have the correct category.
    val keepField = structure.messageCategories.get(fullName).fold(false)(_.contains(keep))

    val newNestedType = d.nestedType.mapFilter { nestedType =>
      val nestedTypeName = getNonEmptyString(nestedType.name)
      val nestedTypeFullName = s"$fullName.$nestedTypeName"

      trimDescriptor(keep)(nestedTypeFullName, nestedType)
    }

    Option.when(newNestedType.nonEmpty || keepField)(
      d.copy(field = d.field, nestedType = newNestedType)
    )
  }

  private def trimFile(keep: MessageCategory)(file: ImageFile): ImageFile = {
    val pkg = getNonEmptyString(file.`package`)

    if (pkg.startsWith(cantonPackagePrefix)) {
      val newMessageType = file.messageType.mapFilter { mT =>
        val name = getNonEmptyString(mT.name)

        trimDescriptor(keep)(s"$pkg.$name", mT)
      }

      file.copy(messageType = newMessageType, service = Nil)
    } else {
      // Keep non Canton files to avoid breaking dependencies
      file
    }
  }
}
