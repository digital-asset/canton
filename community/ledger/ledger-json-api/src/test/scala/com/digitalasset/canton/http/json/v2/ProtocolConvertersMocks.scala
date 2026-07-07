// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPackageName, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.{ExecutionContext, Future}

/** Fixed values returned by [[MockSchemaProcessor]].
  *
  * Kept in a single place so that round-trip tests can align their generators with the values the
  * mock produces.
  */
object ProtocolConvertersMocks {
  val defaultJsValue: ujson.Value = ujson.Obj("key" -> ujson.Str("value"))
  val defaultLapiRecord: Record =
    Record(fields = Seq(RecordField(value = Some(Value(Value.Sum.Text("quantumly-random"))))))
  val defaultLapiValue: Value = Value(sum = Value.Sum.Record(value = defaultLapiRecord))
  val defaultKeyJsValue: ujson.Value = ujson.Obj("key" -> ujson.Str("contract-key-marker"))
  val defaultKeyLapiValue: Value = Value(sum = Value.Sum.Text("contract-key-marker"))
}

/** Reusable no-op [[SchemaProcessors]] that returns fixed values regardless of the input. */
class MockSchemaProcessor()(implicit val executionContext: ExecutionContext)
    extends SchemaProcessors {

  private val simpleJsValue = Future.successful(ProtocolConvertersMocks.defaultJsValue)
  private val simpleLapiValue = Future.successful(ProtocolConvertersMocks.defaultLapiValue)
  private val contractKeyJsValue = Future.successful(ProtocolConvertersMocks.defaultKeyJsValue)
  private val contractKeyLapiValue = Future.successful(ProtocolConvertersMocks.defaultKeyLapiValue)

  override def contractArgFromJsonToProto(template: Identifier, jsonArgsValue: ujson.Value)(implicit
      traceContext: TraceContext
  ): Future[Value] = simpleLapiValue

  override def contractArgFromProtoToJson(template: Identifier, protoArgs: Record)(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = simpleJsValue

  override def choiceArgsFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      jsonArgsValue: ujson.Value,
  )(implicit traceContext: TraceContext): Future[Value] = simpleLapiValue

  override def choiceArgsFromProtoToJson(
      template: Identifier,
      choiceName: IdString.Name,
      protoArgs: Value,
  )(implicit traceContext: TraceContext): Future[ujson.Value] = simpleJsValue

  override def keyArgFromProtoToJson(template: Identifier, protoArgs: Value)(implicit
      traceContext: TraceContext
  ): Future[ujson.Value] = contractKeyJsValue

  override def keyArgFromJsonToProto(template: Identifier, jsonArgsValue: ujson.Value)(implicit
      traceContext: TraceContext
  ): Future[Value] = contractKeyLapiValue

  override def exerciseResultFromProtoToJson(
      template: Identifier,
      choiceName: IdString.Name,
      v: Value,
  )(implicit traceContext: TraceContext): Future[ujson.Value] = simpleJsValue

  override def exerciseResultFromJsonToProto(
      template: Identifier,
      choiceName: IdString.Name,
      value: ujson.Value,
  )(implicit traceContext: TraceContext): Future[Option[Value]] =
    simpleLapiValue.map(Some(_))
}

/** Reusable no-op [[TranscodePackageIdResolver]] that resolves every package-name to an empty map.
  */
class MockTranscodePackageIdResolver(
    override val loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root
)(implicit val ec: ExecutionContext)
    extends TranscodePackageIdResolver {

  override protected def resolvePackageNamesInternal(
      packageNames: NonEmpty[Set[LfPackageName]],
      party: LfPartyId,
      packageIdSelectionPreferences: Set[LfPackageId],
      synchronizerIdO: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPackageName, LfPackageId]] =
    FutureUnlessShutdown.pure(Map.empty)
}
