// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.http.json.v2.ProtoInfo
import com.digitalasset.canton.proto.ProtoParser
import org.scalatest.AppendedClues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

/** This test ensures that all new proto comments contain clear Required or Optional markers.
  */
class ProtoDocumentationConformanceTest extends AnyWordSpecLike with Matchers with AppendedClues {

  "new proto comments" should {
    "contain clear Required or Optional marks" in {
      val allFields = ProtoCommentsChecker.reportProto()
      val checkedFields = allFields.filterNot(_.isSkipped)
      val conflictingFields = checkedFields.filter(f => f.isOptional && f.isRequired)
      val unspecifiedFields = checkedFields.filter(f => !f.isOptional && !f.isRequired)
      conflictingFields shouldBe empty withClue
        s"Found ${conflictingFields.size} field(s) with conflicting Required and Optional markers:\n" +
        conflictingFields
          .map(f => s"- ${f.fileName} / ${f.messageName} / ${f.fieldName}: ${f.comment.trim}")
          .mkString("\n")
      unspecifiedFields shouldBe empty withClue
        s"Found ${unspecifiedFields.size} field(s) in proto files with no Required or Optional marker:\n" +
        unspecifiedFields
          .map(f => s"- ${f.fileName} / ${f.messageName} / ${f.fieldName}")
          .mkString("\n")
    }
  }

  "new rpc comments" should {
    "contain a 'Required permissions: ' line" in {
      def hasRequiredPermissions(rpc: ReportedRpc): Boolean =
        rpc.comment
          .split("\n")
          .exists(line => ProtoInfo.requiredPermissionsPattern.findFirstIn(line).isDefined)

      val allRpcs = ProtoCommentsChecker.reportRpcs().filter(_.exposed)
      val missingRpcs = allRpcs.filterNot(hasRequiredPermissions)
      if (!missingRpcs.isEmpty)
        fail(
          s"Found ${missingRpcs.size} rpc method(s) with no 'Required permissions: ' line:\n" +
            missingRpcs
              .map(r => s"- ${r.fileName} / ${r.serviceName} / ${r.methodName}")
              .mkString("\n")
        )
    }
  }
}

final case class ReportedField(
    fileName: String,
    messageName: String,
    fieldName: String,
    isOptional: Boolean,
    isRequired: Boolean,
    isSkipped: Boolean, // oneof variant field; Required/Optional doesn't apply
    comment: String,
)

final case class ReportedRpc(
    fileName: String,
    serviceName: String,
    methodName: String,
    comment: String,
) {
  def exposed = !List(
    "CommandInspectionService",
    "ParticipantPruningService",
    "PartyManagementAlphaService",
  ).contains(serviceName)
}

object ProtoCommentsChecker {

  lazy val protoInfo = ProtoInfo.loadData()

  /** (messageName, fieldName) pairs that are oneof variants and don't need Required/Optional
    * markers.
    */
  private lazy val skippedFieldKeys: Set[(String, String)] = {
    import scala.jdk.CollectionConverters.CollectionHasAsScala
    import io.protostuff.compiler.model.Message
    def collectNested(m: Message): Iterator[Message] =
      Iterator.single(m) ++ m.getMessages.asScala.iterator.flatMap(collectNested)
    val all =
      ProtoParser
        .parseProtos()
        .iterator
        .flatMap(_.getMessages.asScala.iterator)
        .flatMap(collectNested)
    (for {
      msg <- all
      field <- msg.getOneofs.asScala.flatMap(_.getFields.asScala)
    } yield msg.getName -> field.getName).toSet
  }

  def reportProto(): Seq[ReportedField] =
    protoInfo.protoComments.fileComments.toSeq.flatMap { case (fileName, fileComments) =>
      fileComments.messages.toSeq.flatMap { case (messageName, messageInfo) =>
        messageInfo.message.fieldComments.toSeq.map { case (fieldName, comment) =>
          ReportedField(
            fileName,
            messageName,
            fieldName,
            messageInfo.isFieldOptional(fieldName),
            messageInfo.isFieldRequired(fieldName),
            skippedFieldKeys.contains(messageName -> fieldName),
            comment,
          )
        }
      }
    }

  def reportRpcs(): Seq[ReportedRpc] =
    protoInfo.protoComments.fileComments.toSeq.flatMap { case (fileName, fileComments) =>
      fileComments.services.toSeq.flatMap { case (serviceName, methods) =>
        methods.map { method =>
          ReportedRpc(
            fileName,
            serviceName,
            method.name,
            method.comments.getOrElse(""),
          )
        }
      }
    }
}
