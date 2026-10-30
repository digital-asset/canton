// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.grpc.GrpcStatus
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.update
import com.digitalasset.canton.ledger.api.{CumulativeFilter, InterfaceFilter, TemplateFilter}
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import com.google.rpc.error_details
import com.google.rpc.error_details.RetryInfo
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Duration

trait ValidatorTestUtils extends Matchers with Inside with OptionValues with EitherValues {
  self: Suite =>

  protected val includedModule = "includedModule"
  protected val includedTemplate = "includedTemplate"
  protected val expectedUserId = "expectedUserId"
  protected val packageName = Ref.PackageName.assertFromString("somePackageName")
  protected val packageNameRefEncoded = Ref.PackageRef.Name(packageName).toString
  protected val templateQualifiedName =
    Ref.QualifiedName.assertFromString(s"$includedModule:$includedTemplate")
  protected val packageId = Ref.PackageId.assertFromString("packageId")
  protected val packageId2 = Ref.PackageId.assertFromString("packageId2")
  protected val offsetLong = 42L
  protected val offset = Some(Offset.tryFromLong(offsetLong))
  protected val party = Ref.Party.assertFromString("party")
  protected val party2 = Ref.Party.assertFromString("party2")
  protected val verbose = false
  protected val updateId = TestUpdateId("42")
  protected val ledgerEnd = Some(Offset.tryFromLong(1000))
  protected val contractId = ContractId.V1.assertFromString("00" * 32 + "0001")
  protected val moduleName = Ref.ModuleName.assertFromString(includedModule)
  protected val dottedName = Ref.DottedName.assertFromString(includedTemplate)
  protected val refTemplateId = Ref.Identifier(packageId, templateQualifiedName)
  protected val refTemplateId2 = Ref.Identifier(packageId2, templateQualifiedName)
  protected implicit val sizeLimitsConfig: LedgerApiSizeLimitsConfig = LedgerApiSizeLimitsConfig()

  private val expectedTemplates = Set(
    Ref.NameTypeConRef(
      Ref.PackageRef.Name(packageName),
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(includedModule),
        Ref.DottedName.assertFromString(includedTemplate),
      ),
    )
  )

  protected def hasExpectedFilters(
      req: update.GetUpdatesRequest,
      expectedTemplates: Set[Ref.NameTypeConRef] = expectedTemplates,
  ): Assertion = {
    val filtersByParty = req.updateFormat.includeTransactions.value.eventFormat.filtersByParty
    filtersByParty should have size 1
    inside(filtersByParty.headOption.value) { case (p, filters) =>
      p shouldEqual party
      filters shouldEqual
        CumulativeFilter(
          templateFilters =
            expectedTemplates.map(TemplateFilter(_, includeCreatedEventBlob = false)),
          interfaceFilters = Set(
            InterfaceFilter(
              interfaceTypeRef = Ref.NameTypeConRef(
                Ref.PackageRef.Name(packageName),
                Ref.QualifiedName(
                  Ref.DottedName.assertFromString(includedModule),
                  Ref.DottedName.assertFromString(includedTemplate),
                ),
              ),
              includeView = true,
              includeCreatedEventBlob = true,
            )
          ),
          templateWildcardFilter = None,
        )
    }
  }

  protected def requestMustFailWith(
      request: Either[StatusRuntimeException, ?],
      code: Code,
      description: String,
      metadata: Map[String, String] = Map.empty,
      retryDelay: Seq[Duration] = Seq.empty,
  ): Assertion =
    inside(request)(isError(code, description, metadata, retryDelay))
  protected def isError(
      expectedCode: Code,
      expectedDescription: String,
      metadata: Map[String, String],
      retryDelay: Seq[Duration],
  ): PartialFunction[Either[StatusRuntimeException, ?], Assertion] = { case Left(err) =>
    err.getStatus should have(Symbol("code")(expectedCode))
    err.getStatus should have(Symbol("description")(expectedDescription))
    val details = GrpcStatus
      .toProto(err.getStatus, err.getTrailers)
      .details
    val errorInfoMetadata: Map[String, String] = details
      .collect {
        case any if any.is[error_details.ErrorInfo] =>
          any.unpack[error_details.ErrorInfo].metadata
      }
      .flatten
      .toMap
    errorInfoMetadata should contain allElementsOf metadata
    val retryInfoMetadata: Seq[Duration] = details.collect {
      case any if any.is[error_details.RetryInfo] =>
        val retryDelay = any
          .unpack[RetryInfo]
          .getRetryDelay
        ProtoConverter.DurationConverter.fromProtoPrimitive(retryDelay).value
    }
    retryInfoMetadata should contain allElementsOf retryDelay
  }

}

trait SizeLimitTestUtils extends AnyWordSpecLike with ValidatorTestUtils {

  protected def cardinalityLimit(field: String, limit: Int, limitName: String = "limit")(
      validate: Int => Either[StatusRuntimeException, ?]
  ): Unit = {
    s"accept $field with cardinality exactly equal to the $limitName" in {
      validate(limit) shouldBe a[Right[?, ?]]
    }
    s"reject $field with cardinality exceeding the $limitName" in {
      val exceedingLimit = limit + 1
      requestMustFailWith(
        request = validate(exceedingLimit),
        code = Code.INVALID_ARGUMENT,
        description = cardinalityLimitExceeded(field, actual = exceedingLimit, limit = limit),
      )
    }
  }

  protected def byteSizeLimit(field: String, limit: Int, limitName: String = "limit")(
      validate: Int => Either[StatusRuntimeException, ?]
  ): Unit = {
    s"accept $field with number of bytes exactly equal to the $limitName" in {
      validate(limit) shouldBe a[Right[?, ?]]
    }
    s"reject $field with number of bytes exceeding the $limitName" in {
      requestMustFailWith(
        request = validate(limit + 1),
        code = Code.INVALID_ARGUMENT,
        description = sizeLimitExceeded(field, actual = limit + 1, limit = limit),
      )
    }
  }

  protected def cardinalityLimitExceeded(field: String, actual: Int, limit: Int): String =
    s"FIELD_CARDINALITY_LIMIT_EXCEEDED(8,0): Invariant violation in field `$field`: " +
      s"repeated field has $actual elements, exceeding the maximum of $limit"

  protected def sizeLimitExceeded(field: String, actual: Int, limit: Int): String =
    s"FIELD_SIZE_LIMIT_EXCEEDED(8,0): Invariant violation in field `$field`: " +
      s"Byte length for $field ($actual bytes) exceeds maximum limit of $limit bytes"

  protected def parties(numberOfParties: Int): Seq[String] =
    (1 to numberOfParties).map(i => s"party$i")

  protected def bytes(numberOfBytes: Int): ByteString =
    ByteString.copyFrom(Array.fill(numberOfBytes)(1.toByte))
}
