// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package refinement

import com.daml.nameof.NameOf
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.command.{ApiContractKey, ReplayCommand}
import com.digitalasset.daml.lf.data.{CostModel, ImmArray, Ref}
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.{
  GlobalKey,
  MaxContractKeyFetches,
  Node,
  SubmittedTransaction,
}
import com.digitalasset.daml.lf.value.Value

/** The Command Preprocessor is responsible of the following tasks:
  *  - normalizes value representation (e.g. resolves missing type
  *    reference in record/variant/enumeration, infers missing labeled
  *    record fields, orders labeled record fields, ...);
  *  - checks value nesting does not overpass 100;
  *  - checks a LF command/value is properly typed according the
  *    Daml-LF package definitions;
  *  - checks for Contract ID suffix (see [[forbidLocalContractIds]]);
  *  - translates a LF command/value into speedy command/value; and
  *  - translates a complete transaction into a list of speedy
  *    commands.
  *
  * @param compiledPackages a [[MutableCompiledPackages]] contains the
  *   Daml-LF package definitions against the command should
  *   resolved/typechecked. It is updated dynamically each time the
  *   [[ResultNeedPackage]] continuation is called.
  * @param forbidLocalContractIds when `true` the preprocessor will reject
  *   any value/command/transaction that contains a local Contract ID.
  * @param costModel the preprocessor input cost model that should be used
  * @param initialInputCost initial input cost
  * @param maxInputCost maximum input cost. Any input cost that exceeds this value will cause engine evaluation to
  *   terminate with an error.
  */
private[engine] final class Preprocessor(
    compiledPackages: CompiledPackages,
    loadPackage: (Ref.PackageId, language.Reference) => Result[Unit],
    forbidLocalContractIds: Boolean = true,
    costModel: CostModel.CostModelImplicits = CostModel.EmptyCostModelImplicits,
    initialInputCost: CostModel.Cost = 0L,
    maxInputCost: CostModel.Cost = Long.MaxValue,
    loggerFactory: NamedLoggerFactory,
) extends SafePackageResolver(compiledPackages, loadPackage, loggerFactory) {

  import compiledPackages.pkgInterface
  import costModel._

  private[this] var inputCost: CostModel.Cost = initialInputCost

  def updateInputCost[A](value: A)(implicit cost: A => CostModel.Cost): Unit = {
    inputCost += cost(value)
  }

  def getInputCost(implicit traceContext: TraceContext): Result[CostModel.Cost] = {
    if (inputCost <= maxInputCost) {
      ResultDone(inputCost)
    } else {
      ResultError(
        Error.Preprocessing.Internal(
          NameOf.qualifiedNameOfCurrentFunc,
          "Preprocessing input cost budget exceeded",
          None,
        )
      )
    }
  }

  val commandPreprocessor =
    new CommandPreprocessor(
      pkgInterface = pkgInterface,
      forbidLocalContractIds = forbidLocalContractIds,
    )

  val transactionPreprocessor = new TransactionPreprocessor(commandPreprocessor)

  /** Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value)(implicit
      traceContext: TraceContext
  ): Result[SValue] =
    safelyRun(pullTypePackages(ty0)) {
      // this is used only by the value enricher, strict translation is the way to go
      commandPreprocessor.unsafeTranslateValue(
        ty0,
        v0,
        extendLocalIdForbiddanceToRelativeV2 = false,
      )
    }

  private[engine] def preprocessApiCommand(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmd: command.ApiCommand,
  ): Result[speedy.Command] =
    safelyRun(pullPackage(pkgResolution, List(cmd.typeRef))) {
      commandPreprocessor.unsafePreprocessApiCommand(pkgResolution, cmd)
    }

  private[lf] val EmptyPackageResolution: Result[Map[Ref.PackageName, Ref.PackageId]] = ResultDone(
    Map.empty
  )

  def buildPackageResolution(
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      packagePreference: Set[Ref.PackageId] = Set.empty,
  )(implicit traceContext: TraceContext): Result[Map[Ref.PackageName, Ref.PackageId]] = {
    updateInputCost(packageMap)
    updateInputCost(packagePreference)

    packagePreference.foldLeft(EmptyPackageResolution)((acc, pkgId) =>
      for {
        pkgName <- packageMap.get(pkgId) match {
          case Some((pkgName, _)) => ResultDone(pkgName)
          case None =>
            ResultError(Error.Preprocessing.Lookup(language.LookupError.MissingPackage(pkgId)))
        }
        m <- acc
        _ <- m.get(pkgName) match {
          case None => Result.Unit
          case Some(pkgId0) =>
            ResultError(
              Error.Preprocessing.Internal(
                NameOf.qualifiedNameOfCurrentFunc,
                s"package $pkgId0 and $pkgId have the same name $pkgName",
                None,
              )
            )
        }
      } yield m.updated(pkgName, pkgId)
    )
  }

  def buildGlobalKey(
      templateId: Ref.TypeConId,
      contractKey: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): Result[GlobalKey] = {
    safelyRun(pullPackage(Seq(templateId))) {
      commandPreprocessor.unsafePreprocessContractKey(
        contractKey,
        templateId,
        extendLocalIdForbiddanceToRelativeV2,
      )
    }
  }

  /** Translates  LF commands to a speedy commands.
    */
  def preprocessApiCommands(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmds: data.ImmArray[command.ApiCommand],
  ): Result[ImmArray[speedy.ApiCommand]] = {
    updateInputCost(cmds)

    safelyRun(pullPackage(pkgResolution, cmds.toSeq.view.map(_.typeRef))) {
      commandPreprocessor.unsafePreprocessApiCommands(pkgResolution, cmds)
    }
  }

  private[engine] def preprocessReplayCommand(
      cmd: ReplayCommand
  ): Result[speedy.Command] = {
    def templateAndInterfaceIds =
      cmd match {
        case ReplayCommand.Create(templateId, _) => List(templateId)
        case ReplayCommand.Exercise(templateId, interfaceId, _, _, _) =>
          templateId :: interfaceId.toList
        case ReplayCommand.ExerciseByKey(templateId, _, _, _) => List(templateId)
        case ReplayCommand.Fetch(templateId, interfaceId, _) =>
          templateId :: interfaceId.toList
        case ReplayCommand.FetchByKey(templateId, _) => List(templateId)
        case ReplayCommand.LookupByKey(templateId, _) => List(templateId)
      }
    safelyRun(pullPackage(templateAndInterfaceIds)) {
      commandPreprocessor.unsafePreprocessReplayCommand(cmd)
    }
  }

  /** Translates a complete transaction. Assumes no contract ID suffixes are used */
  def translateTransactionRoots(
      tx: SubmittedTransaction
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(
      pullPackage(
        tx.nodes.values.collect { case action: Node.Action => action.templateId }
      )
    ) {
      transactionPreprocessor.unsafeTranslateTransactionRoots(tx)
    }

  def preprocessInterfaceView(
      templateId: Ref.Identifier,
      argument: Value,
      interfaceId: Ref.Identifier,
  ): Result[speedy.InterfaceView] =
    safelyRun(
      pullPackage(Seq(templateId)).flatMap(_ => pullPackage(Seq(interfaceId)))
    ) {
      commandPreprocessor.unsafePreprocessInterfaceView(templateId, argument, interfaceId)
    }

  def preprocessApiContractKeys(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      keys: Seq[ApiContractKey],
  ): Result[Seq[(GlobalKey, Int)]] = {
    updateInputCost(keys)

    safelyRun(pullPackage(pkgResolution, keys.view.map(_.templateRef))) {
      commandPreprocessor.unsafePreprocessApiContractKeys(pkgResolution, keys)
    }
  }

  private[engine] def prefetchContractIdsAndKeys(
      commands: ImmArray[speedy.ApiCommand],
      prefetchKeys: Seq[(GlobalKey, Int)],
      unprocessedCommands: Option[ImmArray[command.ApiCommand]] = None,
  )(implicit traceContext: TraceContext): Result[Unit] = {
    // TODO: https://github.com/digital-asset/daml/issues/21953: implement size cost model for speedy.ApiCommand and speedy.SValue
    unprocessedCommands.foreach { cmds =>
      updateInputCost(cmds)
    }
    updateInputCost(prefetchKeys.map(_._1))

    safelyRun(
      ResultError(
        Error.Preprocessing.Internal(
          NameOf.qualifiedNameOfCurrentFunc,
          "unsafePrefetchKeys should not need packages",
          None,
        )
      )
    ) {
      val keysToPrefetch = unsafePrefetchKeys(commands, prefetchKeys)
      val contractIdsToPrefetch = unsafePrefetchContractIds(commands)
      (keysToPrefetch, contractIdsToPrefetch)
    }.flatMap { case (keysToPrefetch, contractIdsToPrefetch) =>
      if (keysToPrefetch.nonEmpty || contractIdsToPrefetch.nonEmpty)
        ResultPrefetch(contractIdsToPrefetch.toSeq, keysToPrefetch, () => Result.Unit)
      else Result.Unit
    }
  }

  private def unsafePrefetchContractIds(
      commands: ImmArray[speedy.ApiCommand]
  ): Set[Value.ContractId] =
    commands.iterator.foldLeft(Set.empty[Value.ContractId])((acc, cmd) =>
      cmd match {
        case speedy.Command.ExerciseTemplate(_, contractId, _, argument) =>
          SValue.addContractIds(argument, acc + contractId.value)
        case speedy.Command.ExerciseInterface(_, contractId, _, argument) =>
          SValue.addContractIds(argument, acc + contractId.value)
        case speedy.Command.ExerciseByKey(_, _, _, argument) =>
          // No need to look at the key because keys cannot contain contract IDs
          SValue.addContractIds(argument, acc)
        case speedy.Command.Create(_, argument) =>
          SValue.addContractIds(argument, acc)
        case speedy.Command.CreateAndExercise(_, createArgument, _, choiceArgument) =>
          SValue.addContractIds(choiceArgument, SValue.addContractIds(createArgument, acc))
      }
    )

  private def unsafePrefetchKeys(
      commands: ImmArray[speedy.Command],
      prefetchKeys: Seq[(GlobalKey, Int)],
  ): Map[GlobalKey, Int] = {
    // ExerciseByKey commands exercise exactly one contract per key, so they get max_contracts=1.
    val exercisedKeys: Iterator[(GlobalKey, Int)] = commands.iterator.collect {
      case speedy.Command.ExerciseByKey(templateId, contractKey, _, _) =>
        val key = speedy.Speedy.Machine
          .globalKey(pkgInterface, templateId, contractKey)
          .getOrElse(
            throw Error.Preprocessing.ContractIdInContractKey(contractKey.toUnnormalizedValue)
          )
        key -> 1
    }
    (exercisedKeys.toMap ++ prefetchKeys).transform((_, x) => x min MaxContractKeyFetches)
  }
}

private[lf] object Preprocessor {

  def forTesting(
      compilerConfig: speedy.Compiler.Config,
      loggerFactory: NamedLoggerFactory,
  ): Preprocessor =
    forTesting(new ConcurrentCompiledPackages(compilerConfig), loggerFactory)

  def forTesting(pkgs: MutableCompiledPackages, loggerFactory: NamedLoggerFactory): Preprocessor =
    new Preprocessor(
      pkgs,
      (pkgId, _) =>
        ResultNeedPackage(
          pkgId,
          {
            case Some(pkg) => pkgs.addPackage(pkgId, pkg)
            case None =>
              ResultError(
                Error.Preprocessing(
                  Error.Preprocessing.Lookup(LookupError.MissingPackage(pkgId))
                )
              )
          },
        ),
      loggerFactory = loggerFactory,
    )

}
