// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package refinement

import cats.data.NonEmptySet
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, PackageId, QualifiedName}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value

object Enricher {

  // impoverish removes redundant information from a value added by
  // enrichValue.
  // we have the following invariant:
  // impoverish(enrich(type, impoverish(value))) == impoverish(value)
  def impoverish(value: Value): Value = {
    def go(value0: Value, nesting: Int): Value =
      if (nesting > Value.MAXIMUM_NESTING) {
        throw Error.Preprocessing.ValueNesting(value)
      } else {
        val newNesting = nesting + 1
        value0 match {
          case Value.ValueEnum(_, cons) =>
            Value.ValueEnum(None, cons)
          case _: Value.ValueCidLessAtom | _: Value.ValueContractId => value0
          case Value.ValueRecord(_, fields) =>
            val n = fields.reverseIterator.dropWhile(_._2 == Value.ValueNone).size
            Value.ValueRecord(
              tycon = None,
              fields = fields.iterator
                .take(n)
                .map { case (_, v) => None -> go(v, newNesting) }
                .to(ImmArray),
            )
          case Value.ValueVariant(_, variant, value) =>
            Value.ValueVariant(
              tycon = None,
              variant = variant,
              value = go(value, newNesting),
            )
          case Value.ValueList(values) =>
            Value.ValueList(
              values = values.map(go(_, newNesting))
            )
          case Value.ValueOptional(value) =>
            Value.ValueOptional(
              value = value.map(go(_, newNesting))
            )
          case Value.ValueTextMap(value) =>
            Value.ValueTextMap(
              value = value.mapValue(go(_, newNesting))
            )
          case Value.ValueGenMap(entries) =>
            Value.ValueGenMap(
              entries = entries.map { case (k, v) =>
                go(k, newNesting) -> go(v, newNesting)
              }
            )
        }
      }

    go(value, 0)
  }

  private def impoverish(key: GlobalKey): GlobalKey =
    GlobalKey.assertBuild(
      templateId = key.templateId,
      key.packageName,
      key = impoverish(key.key),
      key.hash,
    )

  private def impoverish(key: GlobalKeyWithMaintainers): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers(
      globalKey = impoverish(key.globalKey),
      maintainers = key.maintainers,
    )

  private def impoverish(create: Node.Create): Node.Create = {
    import create._
    Node.Create(
      coid = coid,
      packageName = packageName,
      templateId = templateId,
      arg = impoverish(arg),
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt.map(impoverish),
      version = version,
    )
  }

  private def impoverish(node: Node): Node =
    node match {
      case create: Node.Create =>
        impoverish(create)
      case fetch: Node.Fetch =>
        import fetch._
        Node.Fetch(
          coid = coid,
          packageName = packageName,
          templateId = templateId,
          actingParties = actingParties,
          signatories = signatories,
          stakeholders = stakeholders,
          keyOpt = keyOpt.map(impoverish),
          byKey = byKey,
          interfaceId = interfaceId,
          version = version,
        )
      case lookup: Node.QueryByKey =>
        import lookup._
        Node.QueryByKey(
          packageName = packageName,
          templateId = templateId,
          key = impoverish(key),
          result = result,
          exhaustive = exhaustive,
          version = version,
        )
      case exe: Node.Exercise =>
        import exe._
        Node.Exercise(
          targetCoid = targetCoid,
          packageName = packageName,
          templateId = templateId,
          interfaceId = interfaceId,
          choiceId = choiceId,
          consuming = consuming,
          actingParties = actingParties,
          chosenValue = impoverish(chosenValue),
          stakeholders = stakeholders,
          signatories = signatories,
          choiceObservers = choiceObservers,
          choiceAuthorizers = choiceAuthorizers,
          children = children,
          exerciseResult = exerciseResult.map(impoverish),
          keyOpt = keyOpt.map(impoverish),
          byKey = byKey,
          version = version,
        )
      case rb: Node.Rollback => rb
    }

  def impoverish(tx: VersionedTransaction): VersionedTransaction =
    VersionedTransaction(
      version = tx.version,
      nodes = tx.nodes.map { case (nid, node) => nid -> impoverish(node) },
      roots = tx.roots,
    )

  def impoverish(contract: FatContractInstance): FatContractInstance = {
    val create = impoverish(contract.toCreateNode)
    FatContractInstance.fromCreateNode(
      create = create,
      createTime = contract.createdAt,
      authenticationData = contract.authenticationData,
    )
  }

  def apply(
      engine: Engine,
      addTypeInfo: Boolean = true,
      addFieldNames: Boolean = true,
      addTrailingNoneFields: Boolean = true,
      forbidLocalContractIds: Boolean = true,
  ): Enricher =
    apply(
      engine.compiledPackages,
      engine.loadPackage,
      addTypeInfo = addTypeInfo,
      addFieldNames = addFieldNames,
      addTrailingNoneFields = addTrailingNoneFields,
      forbidLocalContractIds,
      engine.loggerFactory,
    )

  private[lf] def apply(
      compiledPackages: CompiledPackages,
      loadPackage: (PackageId, language.Reference) => Result[Unit],
      addTypeInfo: Boolean,
      addFieldNames: Boolean,
      addTrailingNoneFields: Boolean,
      forbidLocalContractIds: Boolean,
      loggerFactory: NamedLoggerFactory,
  ): Enricher =
    new EnricherImpl(
      compiledPackages,
      loadPackage,
      addTypeInfo,
      addFieldNames,
      addTrailingNoneFields,
      forbidLocalContractIds,
      loggerFactory,
    )
}

// Provides methods to add missing information in values (and value containers):
// - type constructor in records, variants, and enums
// - records' field names
trait Enricher {

  def enrichValue(typ: Ast.Type, value: Value)(implicit traceContext: TraceContext): Result[Value]

  def enrichContract(tyCon: Identifier, value: Value)(implicit
      traceContext: TraceContext
  ): Result[Value]

  def enrichContract(
      contract: FatContractInstance
  )(implicit traceContext: TraceContext): Result[FatContractInstance]

  /** Verifies that [contract] hashes to the same value according to all packages in [packageIds] (i.e. renders to
    * the same value modulo package IDs) and returns the contract enriched by the smallest package ID.
    *
    * Returns a Left if:
    *   - the contract hashes to different values according to different packages
    *   - the contract contains a key.
    *
    * Returns a ResultError if the enrichment fails for unexpected reasons (e.g. typechecking fails).
    */
  def enrichContractWithPackages(
      contract: FatContractInstance,
      packageIds: NonEmptySet[PackageId],
  )(implicit traceContext: TraceContext): Result[Either[String, FatContractInstance]]

  def enrichContractKey(tyCon: Identifier, value: Value)(implicit
      traceContext: TraceContext
  ): Result[Value]

  // deprecated
  def enrichChoiceArgument(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value]

  def enrichChoiceArgument(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value]

  // deprecated
  def enrichChoiceResult(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value]

  def enrichChoiceResult(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value]

  // View enrichment

  def enrichView(
      interfaceId: Identifier,
      viewValue: Value,
  )(implicit traceContext: TraceContext): Result[Value]

  def enrichVersionedTransaction(versionedTx: VersionedTransaction)(implicit
      traceContext: TraceContext
  ): Result[VersionedTransaction]

  def enrichIncompleteTransaction(
      incompleteTx: IncompleteTransaction
  )(implicit traceContext: TraceContext): Result[IncompleteTransaction]

}
