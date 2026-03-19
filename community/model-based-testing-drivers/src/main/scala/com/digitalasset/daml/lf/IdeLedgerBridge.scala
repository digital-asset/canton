// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.preprocessing.CommandPreprocessor
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.script.{IdeLedger, IdeLedgerRunner}
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.transaction.FatContractInstance

/** Minimal bridge to access private[lf] APIs from the model-based-testing module.
  *
  * This object provides a public facade over:
  *   - PureCompiledPackages
  *   - CommandPreprocessor
  *   - Compiler
  *   - IdeLedgerRunner
  */
object IdeLedgerBridge {

  /** Holds a compiled set of packages ready for interpretation. */
  final class CompiledPackagesHandle private[IdeLedgerBridge] (
      private[IdeLedgerBridge] val compiledPackages: PureCompiledPackages
  )

  /** Build compiled packages from decoded DAR packages.
    */
  def buildCompiledPackages(
      packages: Map[Ref.PackageId, Ast.Package],
      enableLfDev: Boolean,
  ): CompiledPackagesHandle = {
    val compilerConfig =
      if (enableLfDev) Compiler.Config.Dev else Compiler.Config.Default
    val compiled = PureCompiledPackages.assertBuild(packages, compilerConfig)
    new CompiledPackagesHandle(compiled)
  }

  /** Derive the next submission seed from the current one. */
  def nextSeed(seed: crypto.Hash): crypto.Hash =
    IdeLedgerRunner.nextSeed(seed)

  /** Result of a submission to the IDE ledger. */
  sealed trait SubmitResult
  final case class SubmitSuccess(
      newLedger: IdeLedger,
      commitResult: IdeLedger.CommitResult,
  ) extends SubmitResult
  final case class SubmitError(
      error: String
  ) extends SubmitResult

  /** Translate a sequence of API commands into a speedy SExpr, then submit it against the given IDE
    * ledger.
    */
  def submitApiCommands(
      handle: CompiledPackagesHandle,
      ledger: IdeLedger,
      committers: Set[Ref.Party],
      readAs: Set[Ref.Party],
      apiCommands: ImmArray[ApiCommand],
      seed: crypto.Hash,
      disclosures: Iterable[FatContractInstance] = Iterable.empty,
  )(implicit loggingContext: LoggingContext): SubmitResult = {
    val compiledPackages = handle.compiledPackages
    val preprocessor = new CommandPreprocessor(
      pkgInterface = compiledPackages.pkgInterface,
      forbidLocalContractIds = true,
    )

    val speedyCommands = preprocessor.unsafePreprocessApiCommands(Map.empty, apiCommands)
    val sexpr = compiledPackages.compiler.unsafeCompile(speedyCommands)
    val ledgerApi = IdeLedgerRunner.ScriptLedgerApi(ledger)
    val result = IdeLedgerRunner.submit(
      compiledPackages = compiledPackages,
      disclosures = disclosures,
      ledger = ledgerApi,
      committers = committers,
      readAs = readAs,
      commands = sexpr,
      location = None,
      seed = seed,
    )

    result.resolve() match {
      case Right(commit) =>
        SubmitSuccess(
          newLedger = commit.result.newLedger,
          commitResult = commit.result,
        )
      case Left(err) =>
        SubmitError(script.Pretty.prettyError(err.error).render(80))
    }
  }
}
