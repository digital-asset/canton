// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import cats.implicits.toFunctorOps
import com.digitalasset.canton.http.json.v2.{
  MockSchemaProcessor,
  MockTranscodePackageIdResolver,
  ProtocolConverter,
  ProtocolConverters,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, GeneratorsLapi, HasExecutionContext}
import org.scalacheck.Arbitrary
import org.scalatest.AppendedClues.convertToClueful
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class ProtocolConvertersTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  import GeneratorsLapi.*

  private val randomSamplesPerMappedClass = 10

  "check all Js mirrors mapping" in {
    forAll(mappings) { mapping =>
      mapping.check(randomSamplesPerMappedClass).futureValue
    }
  }

  private val mockSchemaProcessor = new MockSchemaProcessor()
  private val packageNameResolver = new MockTranscodePackageIdResolver(loggerFactory)
  private val converters = new ProtocolConverters(mockSchemaProcessor, packageNameResolver)

  import magnolify.scalacheck.auto.*

  private lazy val mappings: Seq[JsMapping[?, ?]] = Seq(
    JsMapping(converters.Commands),
    JsMapping(converters.InterfaceView),
    JsMapping(converters.Event),
    JsMapping(converters.Transaction),
    JsMapping(converters.SubmitAndWaitTransactionResponse),
    JsMapping(converters.SubmitAndWaitForReassignmentResponse),
    JsMapping(converters.SubmitAndWaitForTransactionRequest),
    JsMapping(converters.GetEventsByContractIdResponse),
    JsMapping(converters.CreatedEvent),
    JsMapping(converters.AssignedEvent),
    JsMapping(converters.ContractEntry),
    JsMapping(converters.GetActiveContractsResponse),
    JsMapping(converters.GetActiveContractsPageResponse),
    JsMapping(converters.ReassignmentEvent),
    JsMapping(converters.Reassignment),
    JsMapping(converters.GetUpdatesResponse),
//    JsMapping(converters.PrepareSubmissionRequest),//we only need toJson
//    JsMapping(converters.PrepareSubmissionResponse), // we only need toJson
//    JsMapping(converters.ExecuteSubmissionRequest), // we only need fromJson
    JsMapping(converters.PrefetchContractKey),
  )
}

final case class JsMapping[LAPI, JS](converter: ProtocolConverter[LAPI, JS])(implicit
    arb: Arbitrary[LAPI],
    lapClassTag: ClassTag[LAPI],
) {
  def check(samples: Int)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Unit] =
    Future
      .traverse(Vector.from(1 to samples)) { _ =>
        arb.arbitrary.sample
          .map { lapiValue =>
            for {
              jsValue <- converter.toJson(lapiValue)
              lapiValueReconstructed <- converter.fromJson(jsValue)
            } yield lapiValue shouldBe lapiValueReconstructed withClue s"Mapping for $lapClassTag"
          }
          .getOrElse(throw new RuntimeException(s"Failed to generate LAPI value for $lapClassTag"))
      }
      .void
}
