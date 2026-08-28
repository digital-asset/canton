// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast.{Package, PackageSignature}
import com.digitalasset.daml.lf.language.{LanguageVersion, PackageInterface, Util}
import com.digitalasset.daml.lf.speedy.SExpr.SDefinitionRef
import com.digitalasset.daml.lf.speedy.{Compiler, SDefinition}
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2

/** Trait to abstract over a collection holding onto Daml-LF package definitions + the compiled
  * speedy expressions.
  */
private[lf] abstract class CompiledPackages(
    val compilerConfig: Compiler.Config
) {
  def signatures: collection.Map[PackageId, PackageSignature]
  def getDefinition(ref: SDefinitionRef): Option[SDefinition]

  /** Get the transitive dependencies of the given package. Returns 'None' should this function call
    * fail or error.
    */
  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]]

  final def compiler: Compiler = new Compiler(pkgInterface, compilerConfig)
  final def pkgInterface = new PackageInterface(signatures)
  final def contains(pkgId: PackageId): Boolean = signatures.contains(pkgId)
}

private[lf] object CompiledPackages {

  case class StablePackagesInfo(
      packageSignatures: Map[PackageId, PackageSignature],
      defs: Map[SDefinitionRef, SDefinition],
      deps: Map[PackageId, Set[PackageId]],
  )

  def stablePackagesInfoFromCompilerConfig(compilerConfig: Compiler.Config): StablePackagesInfo =
    stablePackagesInfos(compilerConfig.allowedLanguageVersions.max)

  val stablePackagesInfos: Map[LanguageVersion, StablePackagesInfo] =
    LanguageVersion.allLfVersions.map { languageVersion =>
      val (stablePackageSignatures, stableDefs) = {
        val stablePackages = StablePackagesV2.packagesMap.filter { case (_, pkg) =>
          pkg.languageVersion <= languageVersion
        }
        val signatures = Util.toSignatures(stablePackages)
        def defs = data.assertRight(
          Compiler.compilePackages(
            new PackageInterface(signatures),
            stablePackages,
            Compiler.Config.Dev,
          )
        )
        (signatures, defs)
      }

      val stableDeps: Map[PackageId, Set[PackageId]] = {
        val directDeps = stablePackageSignatures.transform { case (_, pkg) => pkg.directDeps }
        language.Graphs.transitiveClosure(directDeps)
      }
      (languageVersion, StablePackagesInfo(stablePackageSignatures, stableDefs, stableDeps))
    }.toMap

}

/** Important: use the constructor only if you _know_ you have all the definitions! Otherwise use
  * the apply in the companion object, which will compile them for you.
  */
private[lf] final class PureCompiledPackages(
    override val signatures: Map[PackageId, PackageSignature],
    val definitions: Map[SDefinitionRef, SDefinition],
    override val compilerConfig: Compiler.Config,
) extends CompiledPackages(compilerConfig) {
  private[this] val transitiveDeps: Map[PackageId, Set[PackageId]] = {
    val directDeps = signatures.transform { case (_, pkg) => pkg.directDeps }
    language.Graphs.transitiveClosure(directDeps)
  }

  override def getDefinition(ref: SDefinitionRef): Option[SDefinition] = definitions.get(ref)

  override def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]] =
    transitiveDeps.get(pkgId)
}

private[lf] object PureCompiledPackages {

  import CompiledPackages.*

  /** Important: use this method only if you _know_ you have all the definitions! Otherwise use the
    * other apply, which will compile them for you.
    */
  def apply(
      packages: Map[PackageId, PackageSignature],
      definitions: Map[SDefinitionRef, SDefinition],
      compilerConfig: Compiler.Config,
  ): PureCompiledPackages =
    new PureCompiledPackages(packages, definitions, compilerConfig)

  def build(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): Either[String, PureCompiledPackages] = {
    val stablePackagesInfo = stablePackagesInfoFromCompilerConfig(compilerConfig)
    val signatures = Util.toSignatures(packages) ++ stablePackagesInfo.packageSignatures
    Compiler
      .compilePackages(
        pkgInterface = new PackageInterface(signatures),
        packages = packages.filterNot { case (pkgId, _) =>
          stablePackagesInfo.packageSignatures.isDefinedAt(pkgId)
        },
        compilerConfig = compilerConfig,
      )
      .map(defs => apply(signatures, defs ++ stablePackagesInfo.defs, compilerConfig))
  }

  def assertBuild(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
  ): PureCompiledPackages =
    data.assertRight(build(packages, compilerConfig))

  def Empty(compilerConfig: Compiler.Config): PureCompiledPackages = {
    val stablePackagesInfo = stablePackagesInfoFromCompilerConfig(compilerConfig)
    PureCompiledPackages(
      stablePackagesInfo.packageSignatures,
      stablePackagesInfo.defs,
      compilerConfig,
    )
  }

}
