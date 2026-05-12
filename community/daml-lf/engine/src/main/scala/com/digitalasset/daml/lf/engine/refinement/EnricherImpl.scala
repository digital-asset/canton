// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package refinement

import cats.data.NonEmptySet
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, PackageId, QualifiedName}
import com.digitalasset.daml.lf.language.{Ast, Reference}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value

// Provide methods to add missing information in values (and value containers):
// - type constructor in records, variants, and enums
// - Records' field names
private[refinement] final class EnricherImpl(
    compiledPackages: CompiledPackages,
    loadPackage: (PackageId, language.Reference) => Result[Unit],
    addTypeInfo: Boolean,
    addFieldNames: Boolean,
    addTrailingNoneFields: Boolean,
    forbidLocalContractIds: Boolean,
    override val loggerFactory: NamedLoggerFactory,
) extends SafePackageResolver(compiledPackages, loadPackage, loggerFactory)
    with Enricher {
  import compiledPackages.pkgInterface

  private[this] val translator = new ValueTranslator(
    pkgInterface = pkgInterface,
    forbidLocalContractIds = forbidLocalContractIds,
  )

  private[this] def toValue(sValue: SValue): Value =
    sValue.toValue(
      keepTypeInfo = addTypeInfo,
      keepFieldName = addFieldNames,
      keepTrailingNoneFields = addTrailingNoneFields,
    )

  private[this] def unsafeTranslateValue(typ: Ast.Type, value: Value): SValue =
    translator.unsafeTranslateValue(typ, value, extendLocalIdForbiddanceToRelativeV2 = false)

  private[this] def unsafeEnrichValue(typ: Ast.Type, value: Value): Value =
    toValue(unsafeTranslateValue(typ, value))

  override def enrichValue(typ: Ast.Type, value: Value)(implicit
      traceContext: TraceContext
  ): Result[Value] =
    safelyRun(pullTypePackages(typ))(unsafeEnrichValue(typ, value))

  override def enrichContract(
      contract: FatContractInstance
  )(implicit traceContext: TraceContext): Result[FatContractInstance] =
    safelyRun(
      pullPackages(
        Map(
          contract.templateId.packageId -> language.Reference.Template(contract.templateId.toRef)
        )
      )
    )(
      FatContractInstance.fromCreateNode(
        unsafeEnrichCreate(contract.toCreateNode),
        contract.createdAt,
        contract.authenticationData,
      )
    )

  override def enrichContractWithPackages(
      contract: FatContractInstance,
      packageIds: NonEmptySet[PackageId],
  )(implicit traceContext: TraceContext): Result[Either[String, FatContractInstance]] = {
    def neededPkgs = packageIds.toNonEmptyList.iterator.map(pkgId =>
      pkgId -> language.Reference.Template(
        Identifier(pkgId, contract.templateId.qualifiedName).toRef
      )
    )
    safelyRun(pullPackages(neededPkgs.toMap))(
      unsafeEnrichCreateWithPackages(contract.toCreateNode, packageIds).map(
        FatContractInstance.fromCreateNode(_, contract.createdAt, contract.authenticationData)
      )
    )
  }

  override def enrichView(
      interfaceId: Identifier,
      viewValue: Value,
  )(implicit traceContext: TraceContext): Result[Value] =
    safelyRun(
      pullPackages(Map(interfaceId.packageId -> language.Reference.Interface(interfaceId)))
    ) {
      val iface = handleLookup(pkgInterface.lookupInterface(interfaceId))
      unsafeEnrichValue(iface.view, viewValue)
    }

  override def enrichContract(tyCon: Identifier, value: Value)(implicit
      traceContext: TraceContext
  ): Result[Value] =
    safelyRun(pullPackages(Map(tyCon.packageId -> language.Reference.Template(tyCon.toRef))))(
      unsafeEnrichValue(Ast.TTyCon(tyCon), value)
    )

  // deprecated
  override def enrichChoiceArgument(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value] = enrichChoiceArgument(
    templateId.packageId,
    templateId.qualifiedName,
    interfaceId,
    choiceName,
    value,
  )

  private[this] def choicePkgs(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
  ): List[(PackageId, Reference)] = {
    val tyCon = Identifier(packageId, templateName)
    interfaceId match {
      case Some(ifaceId) =>
        List(
          ifaceId.packageId -> language.Reference.Choice(ifaceId, choiceName),
          packageId -> language.Reference.Template(tyCon.toRef),
        )
      case None =>
        List(
          packageId -> language.Reference.Choice(tyCon, choiceName)
        )
    }
  }

  private[this] def pullChoicePkgs(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
  ): Result[Unit] =
    pullPackages(choicePkgs(packageId, templateName, interfaceId, choiceName).toMap)

  override def enrichChoiceArgument(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value] =
    safelyRun(pullChoicePkgs(packageId, templateName, interfaceId, choiceName))(
      unsafeEnrichChoiceArgument(packageId, templateName, interfaceId, choiceName, value)
    )

  private[this] def unsafeEnrichChoiceArgument(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Value = {
    val choice = handleLookup(
      pkgInterface.lookupChoice(Identifier(packageId, templateName), interfaceId, choiceName)
    )
    unsafeEnrichValue(choice.argBinder._2, value)
  }

  // deprecated
  override def enrichChoiceResult(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value] = enrichChoiceResult(
    templateId.packageId,
    templateId.qualifiedName,
    interfaceId,
    choiceName,
    value,
  )

  override def enrichChoiceResult(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  )(implicit traceContext: TraceContext): Result[Value] =
    safelyRun(pullChoicePkgs(packageId, templateName, interfaceId, choiceName))(
      unsafeEnrichChoiceResult(packageId, templateName, interfaceId, choiceName, value)
    )

  private[this] def unsafeEnrichChoiceResult(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Value = {
    val choice = handleLookup(
      pkgInterface.lookupChoice(Identifier(packageId, templateName), interfaceId, choiceName)
    )
    unsafeEnrichValue(choice.returnType, value)
  }

  private[this] def unsafeEnrichContractKey(tyCon: Identifier, value: Value) = {
    val key = handleLookup(pkgInterface.lookupTemplateKey(tyCon))
    unsafeEnrichValue(key.typ, value)
  }

  override def enrichContractKey(tyCon: Identifier, value: Value)(implicit
      traceContext: TraceContext
  ): Result[Value] =
    safelyRun(
      pullPackages(
        Map(
          tyCon.packageId -> language.Reference.TemplateKey(tyCon)
        )
      )
    )(unsafeEnrichContractKey(tyCon, value))

  private[this] def unsafeEnrichContractKey(
      key: GlobalKeyWithMaintainers
  ): GlobalKeyWithMaintainers =
    key.copy(globalKey =
      GlobalKey.assertWithRenormalizedValue(
        key.globalKey,
        unsafeEnrichContractKey(key.globalKey.templateId, key.globalKey.key),
      )
    )

  private[lf] def unsafeEnrichContractKey(
      key: Option[GlobalKeyWithMaintainers]
  ): Option[GlobalKeyWithMaintainers] =
    key.map(unsafeEnrichContractKey)

  private[this] def unsafeEnrichCreate(
      create: Node.Create
  ): Node.Create = {
    val arg = unsafeEnrichValue(Ast.TTyCon(create.templateId), create.arg)
    val key = unsafeEnrichContractKey(create.keyOpt)
    create.copy(arg = arg, keyOpt = key)
  }

  /** Verifies that [create] hashes to the same value according to all packages in [packageIds] (i.e. renders to
    * the same value modulo package IDs) and returns the Create node enriched by the smallest package ID.
    *
    * Returns a Left if:
    *   - the contract hashes to different values according to different packages
    *   - the contract contains a key.
    *
    * Returns a ResultError if the enrichment fails for unexpected reasons (e.g. typechecking fails).
    */
  private[lf] def unsafeEnrichCreateWithPackages(
      create: Node.Create,
      packageIds: NonEmptySet[PackageId],
  ): Either[String, Node.Create] = {
    def translateWithPackage(pkgId: PackageId): (PackageId, SValue) = (
      pkgId ->
        unsafeTranslateValue(
          Ast.TTyCon(Identifier(pkgId, create.templateId.qualifiedName)),
          create.arg,
        )
    )

    def hashSValue(pkgId: PackageId, sValue: SValue): Hash =
      // despite its name, assertHashContractInstance, never fails with an HashingError
      SValueHash
        .assertHashContractInstance(
          compiledPackages.signatures(pkgId).pkgName,
          create.templateId.qualifiedName,
          sValue,
        )

    val packageIdList = packageIds.toNonEmptyList
    val targetPkgId = packageIdList.iterator.min
    val (_, sValue) = translateWithPackage(targetPkgId)
    val others = packageIdList.filter(_ != targetPkgId).map(translateWithPackage)
    val hash = hashSValue(targetPkgId, sValue)
    val otherHashes = others.map { case (pkgId, sval) => hashSValue(pkgId, sval) }

    val enrichedKey = unsafeEnrichContractKey(create.keyOpt)
    otherHashes.find(_ != hash) match {
      case Some(otherPkgId) =>
        Left(
          s"Contract ${create.coid} enriches to different values for packages ${targetPkgId} and ${otherPkgId}"
        )
      case None =>
        Right(create.copy(arg = toValue(sValue), keyOpt = enrichedKey))
    }
  }

  private[this] def unsafeEnrichNode(node: Node): Node =
    node match {
      case rb @ Node.Rollback(_) =>
        rb
      case create: Node.Create =>
        unsafeEnrichCreate(create)
      case fetch: Node.Fetch =>
        fetch.copy(keyOpt = unsafeEnrichContractKey(fetch.keyOpt))
      case query: Node.QueryByKey =>
        query.copy(key = unsafeEnrichContractKey(query.key))
      case exe: Node.Exercise =>
        exe.copy(
          chosenValue = unsafeEnrichChoiceArgument(
            exe.templateId.packageId,
            exe.templateId.qualifiedName,
            exe.interfaceId,
            exe.choiceId,
            exe.chosenValue,
          ),
          exerciseResult = exe.exerciseResult.map(
            unsafeEnrichChoiceResult(
              exe.templateId.packageId,
              exe.templateId.qualifiedName,
              exe.interfaceId,
              exe.choiceId,
              _,
            )
          ),
          keyOpt = unsafeEnrichContractKey(exe.keyOpt),
        )
    }

  private[this] def templatePkg(tyCon: Identifier) =
    List(tyCon.packageId -> language.Reference.Template(tyCon.toRef))

  private[this] def pullTransactionPkgs(nodes: Iterable[Node]) = {
    val pkgIds = nodes.flatMap {
      case leaf: Node.LeafOnlyAction => templatePkg(leaf.templateId)
      case exe: Node.Exercise =>
        choicePkgs(
          exe.templateId.packageId,
          exe.templateId.qualifiedName,
          exe.interfaceId,
          exe.choiceId,
        )
      case _: Node.Rollback => List.empty
    }
    pullPackages(pkgIds.toMap)
  }

  override def enrichVersionedTransaction(
      versionedTx: VersionedTransaction
  )(implicit traceContext: TraceContext): Result[VersionedTransaction] =
    safelyRun(pullTransactionPkgs(versionedTx.nodes.values))(
      VersionedTransaction(
        versionedTx.version,
        nodes = versionedTx.nodes.transform((_, node) => unsafeEnrichNode(node)),
        versionedTx.roots,
      )
    )

  override def enrichIncompleteTransaction(
      incompleteTx: IncompleteTransaction
  )(implicit traceContext: TraceContext): Result[IncompleteTransaction] =
    safelyRun(pullTransactionPkgs(incompleteTx.transaction.nodes.values))(
      incompleteTx.copy(
        transaction = incompleteTx.transaction.copy(
          nodes = incompleteTx.transaction.nodes.transform((_, node) => unsafeEnrichNode(node))
        )
      )
    )
}
