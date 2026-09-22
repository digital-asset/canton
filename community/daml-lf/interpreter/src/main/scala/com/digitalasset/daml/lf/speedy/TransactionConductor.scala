// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import cats.implicits.*
import com.daml.nameof.NameOf
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.interpretation.Error as IError
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.Speedy.*
import com.digitalasset.daml.lf.speedy.metrics.{FetchNodeCount, MetricPlugin, TxNodeCount}
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value as V}
import com.google.common.annotations.VisibleForTesting

import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.collection.immutable.{ArraySeq, TreeSet}

/** Standalone driver for the [[Speedy.CmdMachine]] two-layer architecture.
  *
  * `TransactionConductor` owns the ledger state (the [[PartialTransaction]] and the contract
  * caches) and applies each [[Question.Cmd]] a [[Speedy.CmdMachine]] emits. It is NOT a
  * [[Speedy.Machine]] and never interprets Daml-LF: program interpretation runs on
  * [[Speedy.CmdMachine]]s and pure computations on a fresh [[Speedy.PureMachine]] via [[runPure]].
  * Its [[Speedy.Control]] is therefore only ever a `Question` / `Complete` / `Error`, so [[run]] is
  * a small trampoline over `Question.Update`.
  *
  * The cmd path has no exception handling: an uncaught Daml `throw` crashes (`SErrorCrash`) instead
  * of becoming a failure status.
  */
private[lf] final class TransactionConductor(
    private[speedy] var compiledPackages: CompiledPackages,
    val committers: Set[Ref.Party],
    val preparationTime: Time.Timestamp,
    val contractIdVersion: ContractIdVersion,
    val packageResolution: Map[Ref.PackageName, Ref.PackageId],
    val limits: interpretation.Limits,
    val logger: MachineLogger,
    val iterationsBetweenInterruptions: Long,
    val profile: Profile,
    private[speedy] var ptx: PartialTransaction,
    metricPlugins: Seq[MetricPlugin],
    transactionTraceMaxLength: Int = 10,
) {

  import TransactionConductor.*

  private[lf] val metrics: Speedy.Metrics = new Speedy.Metrics(metricPlugins)

  // The driver never interprets Daml-LF, so it has no notion of a "current" source location.
  // Ledger nodes are therefore inserted without location info.
  // TODO(https://github.com/digital-asset/daml/issues/23173) handle locations
  private def getLastLocation: Option[Ref.Location] = None

  private def assignSerializationVersion(hasKey: Boolean): SerializationVersion =
    Speedy.Machine.assignSerializationVersion(hasKey)

  private def tmplId2PackageName(tmplId: Ref.TypeConId): Ref.PackageName =
    Speedy.Machine.tmplId2PackageName(compiledPackages.pkgInterface, tmplId)

  // ---------------------------------------------------------------------------
  // Driver run loop (host-facing)
  // ---------------------------------------------------------------------------

  private[this] def driveCmdMachine(
      cmdMachine: Speedy.CmdMachine
  ): Upd.WithException[SValue] =
    cmdMachine.run() match {
      case SResult.SResultFinal(value) =>
        Upd.WithException.pure(value)
      case SResult.SResultInterruption =>
        driveCmdMachine(cmdMachine)
      case SResult.SResultError(err) =>
        err match {
          case SError.InterpretationError(error) =>
            Upd.WithException.raiseError(error)
          case SError.UnhandledException(excp) =>
            Upd.WithException.raiseException(excp)
          case crash =>
            throw crash
        }
      case SResult.SResultQuestion(cmd) =>
        Upd.WithException.liftEither(
          for {
            result <- handleCmd(cmd).value
            ctrl = result match {
              case Right(value) => Speedy.Control.Value(value)
              case Left(excp) => Speedy.Control.Expression(SBuiltinFun.SBThrow(SExpr.SEValue(excp)))
            }
            _ = cmdMachine.setControl(ctrl)
            value <- driveCmdMachine(cmdMachine).value
          } yield value
        )
    }

  // Spawns a nested CmdMachine to interpret `cmdSExpr` (e.g. an choice body).
  private[this] def runNestedCmdMachine(cmdSExpr: SExpr.SExpr): Upd.WithException[SValue] = {
    val nested =
      Speedy.Machine.fromCmdSExpr(
        compiledPackages = compiledPackages,
        expr = cmdSExpr,
        logger = logger,
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
        profile = profile,
      )
    driveCmdMachine(nested)
  }

  // ---------------------------------------------------------------------------
  // Command-driving entry (host-facing)
  // -----------------------------------------------------------------  ----------

  @scala.annotation.nowarn
  def handleCommands(cmds: Seq[Command]): Upd.T[Unit] =
    cmds
      .traverse {
        case Command.Create(templateId, argument: SValue.SRecord) =>
          handleCommand(Question.Cmd.Create(templateId, argument))
        case Command.ExerciseTemplate(
              templateId,
              SValue.SContractId(contractId),
              choiceId,
              argument,
            ) =>
          handleCommand(Question.Cmd.ExerciseTemplate(templateId, contractId, choiceId, argument))
        case Command.ExerciseInterface(
              interfaceId,
              SValue.SContractId(contractId),
              choiceId,
              argument,
            ) =>
          handleCommand(
            Question.Cmd
              .ExerciseInterface(interfaceId, contractId, choiceId, argument)
          )
        case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
          handleCommand(Question.Cmd.ExerciseByKey(templateId, contractKey, choiceId, argument))
        case Command.CreateAndExercise(
              templateId,
              createArgument: SValue.SRecord,
              choiceId,
              choiceArgument,
            ) =>
          handleCreateThenExercise(
            Question.Cmd.Create(templateId, createArgument),
            Question.Cmd.ExerciseTemplate(templateId, _, choiceId, choiceArgument),
          )
        case Command.FetchTemplate(templateId, SValue.SContractId(contractId)) =>
          handleCommand(Question.Cmd.FetchTemplate(templateId, contractId))
        case Command.FetchInterface(interfaceId, SValue.SContractId(contractId)) =>
          handleCommand(Question.Cmd.FetchInterface(interfaceId, contractId))
        case Command.FetchByKey(templateId, key) =>
          handleCommand(Question.Cmd.FetchByKey(templateId, key))
      }
      .map(_ => ())

  def finish: Either[SError.Crash, Result] = ptx.finish.map { case (tx, seeds) =>
    Result(
      tx,
      zipSameLength(seeds, ptx.actionNodeSeeds.toImmArray),
      ptx.csmJournal.keyInputs.transform((_, v) => v.queue),
      ptx.csmJournal.contractOrder,
    )
  }

  /** Public seam: interpret a single ledger command, yielding a program to drive. */
  def handleCommand(cmd: Question.Cmd): Upd.T[SValue] =
    handleCmd(cmd).valueOrF(raiseFailureWithStatus(_))

  /** Runs `create`, then feeds the resulting contract id to `exerciseOn` and runs that command.
    * Used by tests that must set up a contract (e.g. a helper) before exercising it.
    */
  def handleCreateThenExercise(
      create: Question.Cmd.Create,
      exerciseOn: V.ContractId => Question.Cmd,
  ): Upd.T[SValue] =
    handleCmd(create)
      .flatMap {
        case SValue.SContractId(cid) => handleCmd(exerciseOn(cid))
        case other =>
          throw SError.Crash(
            NameOf.qualifiedNameOfCurrentFunc,
            s"expected a contract id from create, got $other",
          )
      }
      .valueOrF(raiseFailureWithStatus(_))

  // ---------------------------------------------------------------------------
  // Host questions (Question.Update)
  // ---------------------------------------------------------------------------

  private[this] var timeBoundaries: Time.Range = Time.Range.unconstrained

  def getTimeBoundaries: Time.Range = timeBoundaries

  private[speedy] def setTimeBoundaries(newTimeBoundaries: Time.Range): Unit =
    timeBoundaries = newTimeBoundaries

  private def needTime: Upd.T[Time.Timestamp] =
    Upd.lift(Upd.NeedTime).map { time =>
      require(
        timeBoundaries.min <= time && time <= timeBoundaries.max,
        s"NeedTime post-condition failed: time $time lies outside time boundaries $timeBoundaries",
      )
      time
    }

  private def needContract(
      contractId: V.ContractId
  ): Upd.T[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)] =
    Upd.lift(Upd.NeedContract(contractId, committers))

  private def needPackage(
      packageId: Ref.PackageId,
      context: language.Reference,
  ): Upd.T[Unit] =
    Upd.lift(Upd.NeedPackage(packageId, context)).map { packages =>
      this.compiledPackages = packages
      assert(compiledPackages.contains(packageId))
    }

  private def needKeys(
      key: GlobalKey,
      n: Int,
      progress: NeedKeyProgression.CanContinue,
  ): Upd.T[
    (
        Vector[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)],
        NeedKeyProgression.HasStarted,
    )
  ] =
    Upd.lift(Upd.NeedKey(key, n, progress, committers))

  // ---------------------------------------------------------------------------
  // Ledger state: caches
  // ---------------------------------------------------------------------------

  private[this] var contractLookupCache =
    Map.empty[V.ContractId, (FatContractInstance, Hash.HashingMethod, Hash => Boolean)]

  private def lookupContract(
      coid: V.ContractId
  ): Upd.T[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)] =
    contractLookupCache.get(coid) match {
      case Some(res) =>
        Upd.pure(res)
      case None =>
        needContract(coid).map { case entry =>
          contractLookupCache = contractLookupCache.updated(coid, entry)
          entry
        }
    }

  private[this] var localContractStore: Map[V.ContractId, (Ref.TypeConId, SValue)] = Map.empty
  private def getIfLocalContract(coid: V.ContractId): Option[(Ref.TypeConId, SValue)] =
    localContractStore.get(coid)
  private def storeLocalContract(
      coid: V.ContractId,
      templateId: Ref.TypeConId,
      templateArg: SValue,
  ): Unit =
    localContractStore = localContractStore + (coid -> (templateId, templateArg))

  private[this] var contractInfoCache_ : Map[(V.ContractId, Ref.PackageId), Speedy.ContractInfo] =
    Map.empty
  private def contractInfoCache: Map[(V.ContractId, Ref.PackageId), Speedy.ContractInfo] =
    contractInfoCache_
  private def insertContractInfoCache(coid: V.ContractId, contract: Speedy.ContractInfo): Unit = {
    val pkgId = contract.templateId.packageId
    contractInfoCache_ = contractInfoCache_.updated((coid, pkgId), contract)
  }

  private def ensurePackageIsLoaded(
      packageId: Ref.PackageId,
      ref: => language.Reference,
  ): Upd.T[Unit] =
    if (compiledPackages.contains(packageId)) Upd.unit
    else needPackage(packageId, ref)

  // ---------------------------------------------------------------------------
  // Pure computations (run on a fresh PureMachine)
  // ---------------------------------------------------------------------------

  private def transactionTrace(numOfCmds: Int): String =
    "to be implemented"

  private def runPure(
      defRef: SExpr.SDefinitionRef,
      args: ArraySeq[SValue],
  ): Upd.WithException[SValue] = {

    val machine = Speedy.Machine
      .fromPureSExpr(compiledPackages, SExpr.SEApp(SExpr.SEVal(defRef), args), logger)

    val abort = { () =>
      machine.abort()
      Some(transactionTrace(transactionTraceMaxLength))
    }

    @scala.annotation.nowarn("msg=dead code following this construct")
    def loop: Upd.T[Either[SValue.SAny, SValue]] =
      machine.run() match {
        case SResult.SResultFinal(v) =>
          Upd.pure(Right(v))
        case SResult.SResultError(err) =>
          err match {
            case SError.InterpretationError(err) =>
              Upd.raise(err)
            case SError.UnhandledException(excp) =>
              Upd.pure(Left(excp))
            case crash =>
              throw crash
          }
        case SResult.SResultInterruption =>
          Upd.lift(Upd.NeedInterruption(abort)).map(_ => Right(SValue.SUnit))
        case SResult.SResultQuestion(nothing) =>
          nothing
      }

    for {
      _ <-
        if (compiledPackages.getDefinition(defRef).isDefined)
          Upd.WithException.pure(())
        else
          Upd.WithException.liftSuccess(
            needPackage(defRef.packageId, language.Reference.Value(defRef.ref))
          )
      result <- Upd.WithException.liftEither(loop)
    } yield result
  }

  private def computeContractSignatories(
      tmplId: Ref.TypeConId,
      createArg: SValue,
  ): Upd.WithException[TreeSet[Ref.Party]] =
    runPure(SExpr.SignatoriesDefRef(tmplId), ArraySeq(createArg))
      .map(TransactionConductor.extractParties("computeContractSignatories", _))

  private def computeContractObservers(
      tmplId: Ref.TypeConId,
      createArg: SValue,
  ): Upd.WithException[TreeSet[Ref.Party]] =
    runPure(SExpr.ObserversDefRef(tmplId), ArraySeq(createArg))
      .map(TransactionConductor.extractParties("computeContractObservers", _))

  private def computeKeyOpt(
      tmplId: Ref.TypeConId,
      createArg: SValue,
  ): Upd.WithException[Option[GlobalKeyWithMaintainers]] = {
    val keyDefRef = SExpr.ContractKeyDefRef(tmplId)
    if (compiledPackages.getDefinition(keyDefRef).isDefined)
      for {
        keyValue <- runPure(keyDefRef, ArraySeq(createArg))
        gkey <- computeKeyWithMaintainers(tmplId, keyValue)
      } yield Some(gkey)
    else
      Upd.WithException.none
  }

  private def computeKeyWithMaintainers(
      tmplId: Ref.TypeConId,
      keyValue: SValue,
  ): Upd.WithException[GlobalKeyWithMaintainers] =
    // TODO(https://github.com/digital-asset/daml/issues/23276)
    //   check if we want to fail first on ContractIdInContractKey before evaluation maintainers
    runPure(SExpr.KeyMaintainersDefRef(tmplId), ArraySeq(keyValue)).flatMap { maintainersValue =>
      Speedy.Machine.globalKey(tmplId2PackageName(tmplId), tmplId, keyValue) match {
        case Some(gkey) =>
          Upd.WithException.pure(
            GlobalKeyWithMaintainers(
              gkey,
              TransactionConductor.extractParties("computeKeyWithMaintainers", maintainersValue),
            )
          )
        case None =>
          Upd.WithException.raiseError(
            IError.ContractIdInContractKey(keyValue.toUnnormalizedValue)
          )
      }
    }

  private def checkPrecondition(
      tmplId: Ref.TypeConId,
      createArg: SValue,
  ): Upd.WithException[Unit] =
    runPure(SExpr.TemplatePreConditionDefRef(tmplId), ArraySeq(createArg)).flatMap {
      case SValue.SBool(true) =>
        Upd.WithException.unit
      case SValue.SBool(false) =>
        Upd.WithException.raiseError(
          IError.TemplatePreconditionViolated(tmplId, None, createArg.toUnnormalizedValue)
        )
      case other =>
        throw SError.Crash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"template precondition returned a non-boolean value: $other",
        )
    }

  private def computeChoiceParties(
      defRef: SExpr.SDefinitionRef,
      label: String,
      thisValue: SValue,
      choiceArg: SValue,
  ): Upd.T[TreeSet[Ref.Party]] =
    runPure(defRef, ArraySeq(thisValue, choiceArg))
      .valueOrF(raiseFailureWithStatus)
      .map(TransactionConductor.extractParties(label, _))

  // ---------------------------------------------------------------------------
  // Contract fetch/validation helpers (duplicated from SBuiltinFun, machine-free)
  // ---------------------------------------------------------------------------

  private def importCreateArg(
      coidOpt: Option[V.ContractId],
      srcTmplId: Ref.TypeConId,
      dstTmplId: Ref.TypeConId,
      createArg: V,
      forbidLocalContractIds: Boolean,
      forbidTrailingNones: Boolean,
  ): Upd.T[SValue] =
    new ValueTranslator(
      compiledPackages.pkgInterface,
      forbidLocalContractIds = forbidLocalContractIds,
      forbidTrailingNones = forbidTrailingNones,
    ).translateValue(Ast.TTyCon(dstTmplId), createArg) match {
      case Right(svalue) =>
        Upd.pure(svalue)
      case Left(translationError) =>
        Upd.raise(
          IError.Upgrade(
            IError.Upgrade
              .TranslationFailed(coidOpt, srcTmplId, dstTmplId, createArg, translationError)
          )
        )
    }

  private def getContractInfo(
      coid: V.ContractId,
      templateId: Ref.TypeConId,
      templateArg: SValue,
  ): Upd.T[ContractInfo] =
    contractInfoCache.get((coid, templateId.packageId)) match {
      case Some(contract) =>
        assert(contract.templateId == templateId)
        Upd.pure(contract)
      case None =>
        computeContractInfo(templateId, templateArg).value.flatMap {
          case Right(contract) =>
            insertContractInfoCache(coid, contract)
            Upd.pure(contract)
          case Left(excpt) =>
            raiseFailureWithStatus(excpt)
        }
    }

  // Mirrors ToContractInfoDefRef: verifies the precondition, then computes the contract metadata.
  // Used by both the create and fetch paths, matching the UpdateMachine (the precondition therefore
  // runs on fetch too). The empty-key-maintainers check is create-only; see handleCreate.
  private def computeContractInfo(
      tmplId: Ref.TypeConId,
      createArg: SValue,
  ): Upd.WithException[ContractInfo] =
    for {
      _ <- checkPrecondition(tmplId, createArg)
      signatories <- computeContractSignatories(tmplId, createArg)
      observers <- computeContractObservers(tmplId, createArg)
      keyOpt <- computeKeyOpt(tmplId, createArg)
      lfArg <- Upd.WithException.safely(createArg.toNormalizedValue)
      pkgName = tmplId2PackageName(tmplId)
    } yield ContractInfo(
      version = assignSerializationVersion(keyOpt.isDefined),
      packageName = pkgName,
      templateId = tmplId,
      createArg = lfArg,
      hash = SValueHash.assertHashContractInstance(pkgName, tmplId.qualifiedName, createArg),
      signatories = signatories,
      observers = observers,
      keyOpt = keyOpt,
    )

  private def ensureContractActive(
      coid: V.ContractId,
      templateId: Ref.TypeConId,
  ): Upd.T[Unit] =
    ptx.consumedByOrInactive(coid) match {
      case Some(Left(nid)) =>
        Upd.raise(IError.ContractNotActive(coid, templateId, nid))
      case Some(Right(())) =>
        Upd.raise(IError.ContractNotFound(coid))
      case None =>
        Upd.unit
    }

  private def interfaceInstanceExists(
      interfaceId: Ref.TypeConId,
      templateId: Ref.TypeConId,
  ): Boolean = {
    def mkRef(parent: Ref.TypeConId) =
      SExpr.InterfaceInstanceDefRef(parent, interfaceId, templateId)

    List(mkRef(templateId), mkRef(interfaceId)).exists(ref =>
      compiledPackages.getDefinition(ref).nonEmpty
    )
  }

  private def fetchValidateDstContract(
      coid: V.ContractId,
      srcTmplId: Ref.TypeConId,
      srcPkgName: Ref.PackageName,
      srcContractMetadata: ContractMetadata,
      dstTmplId: Ref.TypeConId,
      dstTmplArg: SValue,
      mbTypedNormalFormAuthenticator: Option[Hash => Boolean],
  ): Upd.T[(Ref.TypeConId, SValue, ContractInfo)] =
    for {
      dstContract <- getContractInfo(coid, dstTmplId, dstTmplArg)
      _ <- ensureContractActive(coid, dstContract.templateId)
      _ <- checkContractUpgradable(
        coid,
        srcTmplId,
        dstTmplId,
        srcPkgName,
        dstContract.packageName,
        srcContractMetadata,
        dstContract.metadata,
      )
      _ <- mbTypedNormalFormAuthenticator match {
        case Some(authenticator) =>
          Upd.assert(
            authenticator(
              SValueHash.assertHashContractInstance(
                srcPkgName,
                dstContract.templateId.qualifiedName,
                dstTmplArg,
              )
            )
          )(
            IError.Upgrade(
              IError.Upgrade.AuthenticationFailed(
                coid = coid,
                srcTemplateId = srcTmplId,
                dstTemplateId = dstTmplId,
                createArg = dstContract.createArg,
                msg = s"failed to authenticate contract",
              )
            )
          )
        case None => Upd.unit
      }
    } yield (dstTmplId, dstTmplArg, dstContract)

  private def checkContractUpgradable(
      coid: V.ContractId,
      srcTemplateId: Ref.TypeConId,
      recomputedTemplateId: Ref.TypeConId,
      srcPkgName: Ref.PackageName,
      dstPkgName: Ref.PackageName,
      original: ContractMetadata,
      recomputed: ContractMetadata,
  ): Upd.T[Unit] = {
    def check[T](getter: ContractMetadata => T, desc: String): Option[String] =
      Option.when(getter(recomputed) != getter(original))(
        s"$desc mismatch: $original vs $recomputed"
      )

    List(
      check(_.signatories, "signatories"),
      check(_.nonSignatoryStakeholders, "nonSignatoryStakeholders"),
      check(_.keyOpt.map(_.maintainers), "key maintainers"),
      check(_.keyOpt.map(_.globalKey.key), "key value"),
      Option.when(srcPkgName != dstPkgName)(
        s"package name mismatch: $srcPkgName vs $dstPkgName"
      ),
    ).flatten match {
      case Nil => Upd.unit
      case errors =>
        Upd.raise(
          IError.Upgrade(
            IError.Upgrade.ValidationFailed(
              coid = coid,
              srcTemplateId = srcTemplateId,
              dstTemplateId = recomputedTemplateId,
              srcPackageName = srcPkgName,
              dstPackageName = dstPkgName,
              originalSignatories = original.signatories,
              originalNonSignatoryStakeholders = original.nonSignatoryStakeholders,
              originalKeyOpt = original.keyOpt,
              recomputedSignatories = recomputed.signatories,
              recomputedNonSignatoryStakeholders = recomputed.nonSignatoryStakeholders,
              recomputedKeyOpt = recomputed.keyOpt,
              msg = errors.mkString("['", "', '", "']"),
            )
          )
        )
    }
  }

  // Derives the authenticator and trailing-None policy from how a fetched contract was hashed.
  private def hashingMethodFlags(
      hashingMethod: Hash.HashingMethod,
      authenticator: Hash => Boolean,
  ): (Option[Hash => Boolean], Boolean) =
    hashingMethod match {
      case Hash.HashingMethod.TypedNormalForm => (Some(authenticator), true)
      case Hash.HashingMethod.Legacy => (None, false)
      case Hash.HashingMethod.UpgradeFriendlyUnsafe => (None, true)
    }

  private def resolveLedgerContract[A](
      coid: V.ContractId
  ): Freer[Upd, interpretation.Error, (FatContractInstance, Option[Hash => Boolean], Boolean)] =
    for {
      entry <- lookupContract(coid)
      (coinst, hashingMethod, authenticator) = entry
      (mbAuthenticator, forbidTrailingNones) = hashingMethodFlags(hashingMethod, authenticator)
      _ <- TransactionConductor.authenticateIfLegacyContract(
        coid,
        coinst,
        hashingMethod,
        authenticator,
      )
      _ <- ensureContractActive(coid, coinst.templateId)
    } yield (coinst, mbAuthenticator, forbidTrailingNones)

  /** Fetches the requested contract ID and:
    *   - authenticates the contract against its contract ID if the contract ID uses a legacy
    *     hashing method
    *   - ensures that the contract is still active according to the contract state machine
    *   - verifies that the source template's qualified name matches that of the target template
    *   - loads the package of the target template
    *   - typechecks and converts to an SValue the argument of the source contract according to the
    *     target template
    *   - computes the metadata of the contract according to the target template (including the
    *     ensure clause), caches the result, and verifies that it matches the metadata of the source
    *     contract
    *   - authenticates the contract against its contract ID if the contract ID uses the
    *     TypedNormalForm hashing method
    *   - returns the converted argument
    */
  private def fetchAndValidateContractByTemplate(
      dstTmplId: Ref.TypeConId,
      coid: V.ContractId,
  ): Upd.T[(SValue, ContractInfo)] = {
    def processSrcContract(
        srcTmplId: Ref.TypeConId,
        srcPkgName: Ref.PackageName,
        srcMetadata: ContractMetadata,
        srcArg: V,
        mbTypedNormalFormAuthenticator: Option[Hash => Boolean],
        forbidLocalContractIds: Boolean,
        forbidTrailingNones: Boolean,
    ): Upd.T[(SValue, ContractInfo)] =
      if (srcTmplId.qualifiedName != dstTmplId.qualifiedName)
        Upd.raise(IError.WronglyTypedContract(coid, dstTmplId, srcTmplId))
      else
        for {
          _ <- ensurePackageIsLoaded(
            dstTmplId.packageId,
            language.Reference.Template(dstTmplId.toRef),
          )
          dstSArg <- importCreateArg(
            Some(coid),
            srcTmplId,
            dstTmplId,
            srcArg,
            forbidLocalContractIds = forbidLocalContractIds,
            forbidTrailingNones = forbidTrailingNones,
          )
          result <- fetchValidateDstContract(
            coid,
            srcTmplId,
            srcPkgName,
            srcMetadata,
            dstTmplId,
            dstSArg,
            mbTypedNormalFormAuthenticator,
          )
          (_, value, contract) = result
        } yield (value, contract)

    getIfLocalContract(coid) match {
      case Some((srcTmplId, srcSArg)) =>
        ensureContractActive(coid, srcTmplId).flatMap { _ =>
          if (srcTmplId == dstTmplId) {
            contractInfoCache.get((coid, srcTmplId.packageId)) match {
              case Some(contract) =>
                Upd.pure((srcSArg, contract))
              case None =>
                throw SError.Crash(
                  NameOf.qualifiedNameOfCurrentFunc,
                  s"Contract info for local contract $coid with template ID $srcTmplId",
                )
            }
          } else
            for {
              srcContractInfo <- getContractInfo(coid, srcTmplId, srcSArg)
              result <- processSrcContract(
                srcTmplId = srcTmplId,
                srcPkgName = srcContractInfo.packageName,
                srcMetadata = srcContractInfo.metadata,
                srcArg = srcContractInfo.createArg,
                mbTypedNormalFormAuthenticator = Some(_ == srcContractInfo.hash),
                forbidLocalContractIds = false,
                forbidTrailingNones = true,
              )
            } yield result
        }
      case None =>
        resolveLedgerContract(coid).flatMap { case (coinst, mbAuthenticator, forbidTrailingNones) =>
          processSrcContract(
            srcTmplId = coinst.templateId,
            srcPkgName = coinst.packageName,
            srcMetadata = ContractMetadata(
              coinst.signatories,
              coinst.nonSignatoryStakeholders,
              coinst.contractKeyWithMaintainers,
            ),
            srcArg = coinst.createArg,
            mbTypedNormalFormAuthenticator = mbAuthenticator,
            forbidLocalContractIds = true,
            forbidTrailingNones = forbidTrailingNones,
          )
        }
    }
  }

  /* Similar as fetchAndValidateContractByTemplate but perform a dynamic package resolution */
  private def fetchAndValidateContractByInterface(
      coid: V.ContractId,
      interfaceId: Ref.TypeConId,
  ): Upd.T[(SValue.SAny, ContractInfo)] = {
    def processSrcContract(
        srcPackageName: Ref.PackageName,
        srcTmplId: Ref.TypeConId,
        srcMetadata: ContractMetadata,
        srcArg: V,
        mbTypedNormalFormAuthenticator: Option[Hash => Boolean],
        forbidLocalContractIds: Boolean,
        forbidTrailingNones: Boolean,
    ): Upd.T[(SValue.SAny, ContractInfo)] =
      for {
        pkgId <- Upd.from(
          packageResolution
            .get(srcPackageName)
            .toRight(IError.UnresolvedPackageName(srcPackageName))
        )
        dstTmplId = srcTmplId.copy(pkg = pkgId)
        _ <- ensurePackageIsLoaded(
          dstTmplId.packageId,
          language.Reference.Template(dstTmplId.toRef),
        )
        _ <- Upd.assert(
          interfaceInstanceExists(interfaceId, dstTmplId)
        )(
          IError.ContractDoesNotImplementInterface(interfaceId, coid, dstTmplId)
        )
        dstSArg <- importCreateArg(
          Some(coid),
          srcTmplId,
          dstTmplId,
          srcArg,
          forbidLocalContractIds = forbidLocalContractIds,
          forbidTrailingNones = forbidTrailingNones,
        )
        result <- fetchValidateDstContract(
          coid,
          srcTmplId,
          srcPackageName,
          srcMetadata,
          dstTmplId,
          dstSArg,
          mbTypedNormalFormAuthenticator,
        )
        (resolvedTmplId, dstArg, contract) = result
      } yield (SValue.SAny(Ast.TTyCon(resolvedTmplId), dstArg), contract)

    getIfLocalContract(coid) match {
      case Some((srcTmplId, srcSArg)) =>
        for {
          _ <- ensureContractActive(coid, srcTmplId)
          // We retrieve (or compute for the first time) the contract info of the local contract in order to extract
          // its metadata.
          // We do not need to load the package of srcTmplId because if the contract was created locally, then the
          // package is already loaded.
          srcContractInfo <- getContractInfo(coid, srcTmplId, srcSArg)
          result <- processSrcContract(
            srcPackageName = srcContractInfo.packageName,
            srcTmplId = srcTmplId,
            srcMetadata = srcContractInfo.metadata,
            srcArg = srcContractInfo.createArg,
            mbTypedNormalFormAuthenticator = Some(_ == srcContractInfo.hash),
            forbidLocalContractIds = false,
            forbidTrailingNones = true,
          )
        } yield result
      case None =>
        resolveLedgerContract(coid).flatMap { case (coinst, mbAuthenticator, forbidTrailingNones) =>
          processSrcContract(
            srcPackageName = coinst.packageName,
            srcTmplId = coinst.templateId,
            srcMetadata = ContractMetadata(
              coinst.signatories,
              coinst.nonSignatoryStakeholders,
              coinst.contractKeyWithMaintainers,
            ),
            srcArg = coinst.createArg,
            mbTypedNormalFormAuthenticator = mbAuthenticator,
            forbidLocalContractIds = true,
            forbidTrailingNones = forbidTrailingNones,
          )
        }
    }
  }

  // Crashes if the ledger returned a contract whose key does not match the requested key.
  private def assertKeyMatches(
      globalKey: GlobalKey,
      result: Vector[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)],
  ): Unit =
    result
      .collectFirst {
        case (contract, _, _)
            if contract.contractKeyWithMaintainers.forall(_.globalKey != globalKey) =>
          contract
      }
      .foreach { contract =>
        throw SError.Crash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"Contract key mismatch: the ledger returned a contract whose key does not " +
            s"match the requested key. Requested key: $globalKey, returned " +
            s"contract id: ${contract.contractId}",
        )
      }

  private def resolveContractKey(
      context: => String,
      tmplId: Ref.TypeConId,
      keyValue: SValue,
  ): Upd.WithException[V.ContractId] =
    for {
      keyWithM <- computeKeyWithMaintainers(tmplId, keyValue)
      coid <- Upd.WithException.liftSuccess(resolveContractKey(context, keyWithM))
    } yield coid

  private def resolveContractKey(
      context: => String,
      keyWithM: GlobalKeyWithMaintainers,
  ): Upd.T[V.ContractId] = {
    def loop(
        keyWithM: GlobalKeyWithMaintainers,
        queryResult: Either[NeedKey[CSMJournal], Either[TransactionError, (KeyMapping, CSMJournal)]],
    ): Upd.T[V.ContractId] =
      queryResult match {
        case Left(NeedKey(n, progression, resume)) =>
          needKeys(keyWithM.globalKey, n, progression)
            .flatMap { case (result, newProgression) =>
              assertKeyMatches(keyWithM.globalKey, result)
              loop(keyWithM, resume(result.view.map(_._1.contractId), newProgression))
            }
        case Right(Right((mapping, next))) =>
          ptx = ptx.copy(csmJournal = next)
          mapping.queue.headOption match {
            case Some(coid) => Upd.pure(coid)
            case None => Upd.raise(IError.ContractKeyNotFound(keyWithM.globalKey))
          }
        case Right(Left(error)) =>
          Upd.raise(convTxError(ptx.nodes, context, error))
      }

    for {
      _ <- Upd.assert(keyWithM.maintainers.nonEmpty)(
        IError.FetchEmptyContractKeyMaintainers(
          keyWithM.globalKey.templateId,
          keyWithM.globalKey.key,
          keyWithM.globalKey.packageName,
        )
      )
      coid <- loop(keyWithM, ptx.csmJournal.queryNByKey(keyWithM.globalKey, 1))
    } yield coid
  }

  // ---------------------------------------------------------------------------
  // Command handlers
  // ---------------------------------------------------------------------------

  private def handleCreate(
      tmplId: Ref.TypeConId,
      createArg: SValue,
  ): Upd.WithException[SValue] =
    computeContractInfo(tmplId, createArg).semiflatMap(handleCreate(_, createArg))

  private def handleCreate(
      contract: ContractInfo,
      createArg: SValue,
  ): Upd.T[SValue] =
    for {
      _ <- contract.keyOpt match {
        case Some(key) if key.maintainers.isEmpty =>
          Upd.raise(
            IError.CreateEmptyContractKeyMaintainers(
              contract.templateId,
              contract.createArg,
              key.value,
            )
          )
        case _ => Upd.unit
      }
      coid <- ptx.insertCreate(
        preparationTime = preparationTime,
        contract = contract,
        optLocation = getLastLocation,
        contractIdVersion = contractIdVersion,
      ) match {
        case Right((createNode, newPtx)) =>
          val coid = createNode.coid
          storeLocalContract(coid, contract.templateId, createArg)
          ptx = newPtx
          insertContractInfoCache(coid, contract)
          metrics.incrCount[TxNodeCount]()
          Upd.pure(SValue.SContractId(coid))
        case Left((newPtx, err)) =>
          ptx = newPtx
          Upd.raise(err)
      }
    } yield coid

  private def insertFetchNode(
      coid: V.ContractId,
      contract: ContractInfo,
      byKey: Boolean,
      interfaceId: Option[Ref.TypeConId],
  ): Upd.T[Unit] =
    ptx.insertFetch(
      coid = coid,
      contract = contract,
      optLocation = getLastLocation,
      byKey = byKey,
      version = assignSerializationVersion(hasKey = contract.keyOpt.isDefined),
      interfaceId = interfaceId,
    ) match {
      case Right(newPtx) =>
        ptx = newPtx
        metrics.incrCount[TxNodeCount]()
        metrics.incrCount[FetchNodeCount]()
        Upd.unit
      case Left(err) =>
        Upd.raise(err)
    }

  private def handleFetchTemplate(
      dstTmplId: Ref.TypeConId,
      coid: V.ContractId,
      byKey: Boolean = false,
  ): Upd.T[SValue] =
    for {
      resolved <- fetchAndValidateContractByTemplate(dstTmplId, coid)
      (value, contract) = resolved
      _ <- insertFetchNode(coid, contract, byKey = byKey, interfaceId = None)
    } yield value

  private def handleFetchInterface(
      interfaceId: Ref.TypeConId,
      coid: V.ContractId,
  ): Upd.T[SValue] =
    for {
      resolved <- fetchAndValidateContractByInterface(coid, interfaceId)
      (sAny, contract) = resolved
      _ <- insertFetchNode(coid, contract, byKey = false, interfaceId = Some(interfaceId))
    } yield sAny

  private def handleFetchByKey(
      tmplId: Ref.TypeConId,
      keyValue: SValue,
  ): Upd.WithException[SValue] =
    for {
      coid <- resolveContractKey("FetchByKey", tmplId, keyValue)
      templateArg <- Upd.WithException.liftSuccess(handleFetchTemplate(tmplId, coid, byKey = true))
    } yield SValue.SPair(SValue.SContractId(coid), templateArg)

  private def handleQueryContractKey(
      tmplId: Ref.TypeConId,
      keyValue: SValue,
      n: Int,
  ): Upd.WithException[SValue] =
    computeKeyWithMaintainers(tmplId, keyValue).semiflatMap(handleQueryContractKey(_, n))

  private def handleQueryContractKey(
      keyWithM: GlobalKeyWithMaintainers,
      n: Int,
  ): Upd.T[SValue] = {
    def loop(
        keyWithM: GlobalKeyWithMaintainers,
        queryResult: Either[NeedKey[CSMJournal], Either[TransactionError, (KeyMapping, CSMJournal)]],
    ): Upd.T[(KeyMapping, List[SValue])] =
      queryResult match {
        case Left(NeedKey(needN, progression, resume)) =>
          needKeys(keyWithM.globalKey, needN, progression)
            .flatMap { case (result, newProgression) =>
              assertKeyMatches(keyWithM.globalKey, result)
              loop(keyWithM, resume(result.view.map(_._1.contractId), newProgression))
            }
        case Right(Right((mapping, next))) =>
          mapping.queue.toList
            .traverse(coid =>
              fetchAndValidateContractByTemplate(keyWithM.globalKey.templateId, coid).map(_._1)
            )
            .map { payloads =>
              ptx = ptx.copy(csmJournal = next)
              (mapping, payloads)
            }
        case Right(Left(error)) =>
          Upd.raise(convTxError(ptx.nodes, "QueryContractKey", error))
      }

    for {
      _ <- Upd.assert(keyWithM.maintainers.nonEmpty)(
        IError.FetchEmptyContractKeyMaintainers(
          keyWithM.globalKey.templateId,
          keyWithM.value,
          keyWithM.globalKey.packageName,
        )
      )
      _ <- Upd.from(ptx.authorizeQueryByKey(getLastLocation, keyWithM))
      resolved <- loop(keyWithM, ptx.csmJournal.queryNByKey(keyWithM.globalKey, n))
      (mapping, payloads) = resolved
      _ = {
        ptx = ptx.insertQueryByKey(
          optLocation = getLastLocation,
          key = keyWithM,
          result = mapping,
          keyVersion = assignSerializationVersion(hasKey = true),
        )
        metrics.incrCount[TxNodeCount]()
      }
    } yield SValue.SOptional(
      Some(
        SValue.SList(
          (mapping.queue.view.map(SValue.SContractId(_)) zip payloads)
            .map { case (cid, payload) => SValue.SPair(cid, payload) }
            .to(FrontStack)
        )
      )
    )
  }

  private def beginExercise(
      tmplId: Ref.TypeConId,
      ifaceId: Option[Ref.TypeConId],
      choiceName: Ref.ChoiceName,
      coid: V.ContractId,
      thisValue: SValue,
      contract: ContractInfo,
      choiceArg: SValue,
      byKey: Boolean,
  ): Upd.T[Unit] = {
    val choice = compiledPackages.pkgInterface.lookupChoice(tmplId, ifaceId, choiceName) match {
      case Left(lookupError) =>
        throw SError.Crash(NameOf.qualifiedNameOfCurrentFunc, lookupError.pretty)
      case Right(choice) => choice
    }
    val choiceTypeId = ifaceId.getOrElse(tmplId)
    for {
      controllers <- computeChoiceParties(
        SExpr.ChoiceControllerDefRef(choiceTypeId, choiceName),
        "computeChoiceControllers",
        thisValue,
        choiceArg,
      )
      observers <- computeChoiceParties(
        SExpr.ChoiceObserverDefRef(choiceTypeId, choiceName),
        "computeChoiceObservers",
        thisValue,
        choiceArg,
      )
      authorizersOpt <-
        if (choice.choiceAuthorizers.isDefined)
          computeChoiceParties(
            SExpr.ChoiceAuthorizersDefRef(choiceTypeId, choiceName),
            "computeChoiceAuthorizers",
            thisValue,
            choiceArg,
          )
            .map(Some(_))
        else
          Upd.none
      chosenValue <- Upd.safely(choiceArg.toNormalizedValue)
      _ <- ptx.beginExercises(
        packageName = tmplId2PackageName(tmplId),
        templateId = tmplId,
        targetId = coid,
        contract = contract,
        interfaceId = ifaceId,
        choiceId = choiceName,
        optLocation = getLastLocation,
        consuming = choice.consuming,
        actingParties = controllers,
        choiceObservers = observers,
        choiceAuthorizers = authorizersOpt,
        byKey = byKey,
        chosenValue = chosenValue,
        version = assignSerializationVersion(hasKey = contract.keyOpt.isDefined),
      ) match {
        case Right(newPtx) =>
          ptx = newPtx
          metrics.incrCount[TxNodeCount]()
          Upd.unit
        case Left(err) =>
          Upd.raise(err)
      }
    } yield ()
  }

  private def handleExercise(
      tmplId: Ref.TypeConId,
      interfaceId: Option[Ref.TypeConId],
      choiceName: Ref.ChoiceName,
      coid: V.ContractId,
      thisValue: SValue,
      contract: ContractInfo,
      choiceArg: SValue,
      byKey: Boolean,
      choiceBodyDefRef: SExpr.SDefinitionRef,
  ): Upd.WithException[SValue] =
    for {
      _ <- Upd.WithException.liftSuccess(
        beginExercise(tmplId, interfaceId, choiceName, coid, thisValue, contract, choiceArg, byKey)
      )
      choiceResult <- Upd.WithException.liftEither(
        runNestedCmdMachine(
          SExpr.SEApp(
            SExpr.SEVal(choiceBodyDefRef),
            ArraySeq(thisValue, choiceArg, SValue.SContractId(coid), SValue.SToken),
          )
        ).value.flatMap {
          case right @ Right(result) =>
            for {
              value <- Upd.safely(result.toNormalizedValue)
              _ = ptx = ptx.endExercises(value)
            } yield right
          case left @ Left(_) =>
            ptx = ptx.abortExercises
            Upd.pure(left)
        }
      )
    } yield choiceResult

  private def handleExerciseTemplate(
      tmplId: Ref.TypeConId,
      choiceName: Ref.ChoiceName,
      coid: V.ContractId,
      choiceArg: SValue,
      byKey: Boolean = false,
  ): Upd.WithException[SValue] =
    for {
      entry <- Upd.WithException.liftSuccess(fetchAndValidateContractByTemplate(tmplId, coid))
      (contractArg, contract) = entry
      result <- handleExercise(
        tmplId = tmplId,
        interfaceId = None,
        choiceName = choiceName,
        coid = coid,
        thisValue = contractArg,
        contract = contract,
        choiceArg = choiceArg,
        byKey = byKey,
        choiceBodyDefRef = SExpr.CmdChoiceBodyDefRef(tmplId, choiceName),
      )
    } yield result

  private def handleExerciseByKey(
      tmplId: Ref.TypeConId,
      choiceName: Ref.ChoiceName,
      keyValue: SValue,
      choiceArg: SValue,
  ): Upd.WithException[SValue] =
    resolveContractKey("ExerciseByKey", tmplId, keyValue).flatMap { coid =>
      handleExerciseTemplate(
        tmplId,
        choiceName,
        coid,
        choiceArg,
        byKey = true,
      )
    }

  private def handleExerciseInterface(
      ifaceId: Ref.TypeConId,
      choiceName: Ref.ChoiceName,
      coid: V.ContractId,
      choiceArg: SValue,
  ): Upd.WithException[SValue] =
    Upd.WithException.liftSuccess(fetchAndValidateContractByInterface(coid, ifaceId)).flatMap {
      case (sAny @ SValue.SAny(Ast.TTyCon(tmplId), _), contract) =>
        handleExercise(
          tmplId = tmplId,
          interfaceId = Some(ifaceId),
          choiceName = choiceName,
          coid = coid,
          thisValue = sAny,
          contract = contract,
          choiceArg = choiceArg,
          byKey = false,
          choiceBodyDefRef = SExpr.CmdInterfaceChoiceBodyDefRef(ifaceId, choiceName),
        )
      case other =>
        throw SError.Crash(
          NameOf.qualifiedNameOfCurrentFunc,
          s"fetchAndValidateContractByInterface returned an unexpected value: $other",
        )
    }

  private def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHex: String,
      inputHex: String,
  ): Upd.T[SValue] =
    (Bytes.fromString(configHex), Bytes.fromString(inputHex)) match {
      case (Right(config), Right(input)) =>
        Upd
          .lift(Upd.NeedExternalCall(extensionId, functionId, configHex, inputHex))
          .flatMap {
            case Right(responseBodyRaw) =>
              Bytes.fromString(responseBodyRaw) match {
                case Right(output) =>
                  ptx.recordExternalCallResult(
                    extensionId = extensionId,
                    functionId = functionId,
                    config = config,
                    input = input,
                    output = output,
                  ) match {
                    case Some(updatedPtx) =>
                      ptx = updatedPtx
                      Upd.pure(SValue.SText(responseBodyRaw))
                    case None =>
                      throw SError.Crash(
                        NameOf.qualifiedNameOfCurrentFunc,
                        s"lost enclosing exercise context while resuming external call " +
                          s"(extensionId=$extensionId, functionId=$functionId)",
                      )
                  }
                case Left(_) =>
                  Upd.raise(
                    IError.ExternalCall(
                      IError.ExternalCall.ExecutionFailed(
                        extensionId,
                        functionId,
                        IError.ExternalCall.ExecutionFailed.InvalidOutput(
                          "Invalid external call output: expected canonical lowercase hex"
                        ),
                      )
                    )
                  )
              }
            case Left(error) =>
              Upd.raise(
                IError.ExternalCall(
                  IError.ExternalCall.ExecutionFailed(
                    extensionId,
                    functionId,
                    IError.ExternalCall.ExecutionFailed.CallFailed(error.message),
                  )
                )
              )
          }
      case _ =>
        Upd.raise(
          IError.ExternalCall(
            IError.ExternalCall.PreparationFailed(
              extensionId,
              functionId,
              "Invalid external call config or input: expected canonical lowercase hex",
            )
          )
        )
    }

  private def raiseFailureWithStatus(name: Ref.TypeConId, message: String) =
    Upd.raise(
      IError.FailureStatus(
        "UNHANDLED_EXCEPTION/" + name.qualifiedName.toString,
        Ast.FCInvalidGivenCurrentSystemStateOther.cantonCategoryId,
        message,
        Map(),
      )
    )

  private def raiseFailureWithStatus[A](excp: SValue.SAny): Upd.T[A] = excp match {
    case Speedy.SArithmeticError(msg) =>
      raiseFailureWithStatus(Speedy.SArithmeticError.tyCon, msg)
    case SValue.SAny(Ast.TTyCon(excTyp), svalue) =>
      Speedy.Machine
        .fromPureSExpr(
          compiledPackages,
          SExpr.SEApp(SExpr.SEVal(SExpr.ExceptionMessageDefRef(excTyp)), ArraySeq(svalue)),
          logger,
        )
        .runPure() match {
        case Right(value) =>
          value match {
            case SValue.SText(msg) =>
              raiseFailureWithStatus(excTyp, msg)
            case otherwise =>
              throw SError.Crash(
                NameOf.qualifiedNameOfCurrentFunc,
                s"expected a Text got ${otherwise.toUnnormalizedValue}",
              )
          }
        case Left(err) =>
          err match {
            case SError.InterpretationError(error) => Upd.raise(error)
            case SError.UnhandledException(SValue.SAny(Ast.TTyCon(exceptionId), _)) =>
              raiseFailureWithStatus(
                excTyp,
                s"<Failed to calculate message as ${exceptionId.qualifiedName.toString} was thrown during conversion>",
              )
            case crash =>
              throw crash
          }
      }
    case _ =>
      throw SError.Crash(
        NameOf.qualifiedNameOfCurrentFunc,
        "ill-formed exception",
      )
  }

  private def handleCmd(cmd: Question.Cmd): Upd.WithException[SValue] =
    cmd match {
      case Question.Cmd.Create(tmplId, createArg) =>
        handleCreate(tmplId, createArg)
      case Question.Cmd.FetchTemplate(tmplId, coid) =>
        Upd.WithException.liftSuccess(handleFetchTemplate(tmplId, coid))
      case Question.Cmd.FetchInterface(interfaceId, coid) =>
        Upd.WithException.liftSuccess(handleFetchInterface(interfaceId, coid))
      case Question.Cmd.FetchByKey(tmplId, key) =>
        handleFetchByKey(tmplId, key)
      case Question.Cmd.QueryContractKey(tmplId, key, n) =>
        handleQueryContractKey(tmplId, key, n)
      case Question.Cmd.ExerciseTemplate(
            tmplId,
            coid,
            choiceName,
            choiceArg,
          ) =>
        handleExerciseTemplate(tmplId, choiceName, coid, choiceArg)
      case Question.Cmd.ExerciseByKey(
            tmplId,
            key,
            choiceName,
            choiceArg,
          ) =>
        handleExerciseByKey(tmplId, choiceName, key, choiceArg)
      case Question.Cmd.ExerciseInterface(
            ifaceId,
            coid,
            choiceName,
            choiceArg,
          ) =>
        handleExerciseInterface(ifaceId, choiceName, coid, choiceArg)
      case Question.Cmd.GetTime =>
        Upd.WithException.liftSuccess(needTime.map(time => SValue.STimestamp(time)))
      case Question.Cmd.ExternalCall(extensionId, functionId, configHash, input) =>
        Upd.WithException.liftSuccess(
          handleExternalCall(extensionId, functionId, configHash, input)
        )
      case Question.Cmd.CheckLedgerTimeLT(time) =>
        Upd.WithException.liftSuccess(needTime.map { now =>
          val Time.Range(lb, ub) = getTimeBoundaries
          if (now < time) {
            val newUb = time.subtract(Duration.of(1, ChronoUnit.MICROS))
            setTimeBoundaries(Time.Range(lb, if (newUb < ub) newUb else ub))
            SValue.SBool(true)
          } else {
            setTimeBoundaries(Time.Range(if (lb < time) time else lb, ub))
            SValue.SBool(false)
          }
        })
      case Question.Cmd.OpenTry =>
        ptx = ptx.beginTry
        Upd.WithException.sunit
      case Question.Cmd.CloseTry =>
        ptx = ptx.endTry
        Upd.WithException.sunit
      case Question.Cmd.AbortTry =>
        ptx.rollbackTry match {
          case Left(err) =>
            Upd.WithException.raiseError(err)
          case Right(newPtx) =>
            ptx = newPtx
            Upd.WithException.sunit
        }
    }
}

private[lf] object TransactionConductor {

  sealed abstract class Upd[+A] extends Product with Serializable
  object Upd extends data.Freer.Companion {

    type F[X] = Upd[X]
    type E = interpretation.Error

    def safely[A](a: => A): Upd.T[A] =
      try pure(a)
      catch {
        case SError.InterpretationError(err) => raise(err)
      }

    private[TransactionConductor] type WithException[X] = cats.data.EitherT[Upd.T, SValue.SAny, X]

    private[TransactionConductor] object WithException {
      def liftEither[A](u: Upd.T[Either[SValue.SAny, A]]): WithException[A] = cats.data.EitherT(u)
      def liftSuccess[A](u: Upd.T[A]): WithException[A] = cats.data.EitherT.right(u)
      val unit: WithException[Unit] = pure(())
      def sunit: WithException[SValue] = pure(SValue.SUnit)
      def none[A]: WithException[Option[A]] = pure(None)
      def pure[A](a: A): WithException[A] = liftEither(Upd.pure[Either[SValue.SAny, A]](Right(a)))

      /* raise interpretation error */
      def raiseError[A](e: E): WithException[A] = liftEither(Upd.raise(e))

      /* throw daml exception */
      def raiseException[A](e: SValue.SAny): WithException[A] = liftEither(
        Upd.pure[Either[SValue.SAny, A]](Left(e))
      )

      def safely[A](a: => A): WithException[A] =
        try pure(a)
        catch {
          case SError.InterpretationError(err) => raiseError(err)
          case SError.UnhandledException(e) => raiseException(e)
        }
    }

    /** Update interpretation requires the current ledger time.
      */
    final case object NeedTime extends Upd[Time.Timestamp]

    /** Update interpretation requires access to a contract on the ledger. */
    final case class NeedContract(
        contractId: V.ContractId,
        committers: Set[Ref.Party],
    ) extends Upd[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)]

    /** Machine needs a definition that was not present when the machine was initialized. The caller
      * must retrieve the definition and fill it in the packages cache it had provided to initialize
      * the machine.
      */
    final case class NeedPackage(
        pkg: Ref.PackageId,
        context: language.Reference,
    ) extends Upd[CompiledPackages]

    // Requests up to `limit` FatContractInstances matching `key`, delivered via `callback`.
    // `callback` takes at most `limit` contracts and a progression token:
    //   - Finished when all matches have been delivered (only valid with strictly fewer than `limit` results),
    //   - InProgress when more results may follow.
    // `progression` is Unstarted on the first call, InProgress on continuations.
    final case class NeedKey(
        key: GlobalKey,
        limit: Int,
        progression: NeedKeyProgression.CanContinue,
        committers: Set[Ref.Party],
    ) extends Upd[
          (
              Vector[(FatContractInstance, Hash.HashingMethod, Hash => Boolean)],
              NeedKeyProgression.HasStarted,
          )
        ]

    /** Update interpretation requires an external-call result from the host. The engine suspends
      * until the host resumes the request. The request fields use canonical lowercase hexadecimal
      * encoding. To resume a successful external call, the host must provide the output using the
      * same canonical encoding.
      *
      * @param extensionId
      *   Identifier of the configured extension
      * @param functionId
      *   Function identifier within the extension
      * @param configHash
      *   Configuration hash as canonical lowercase hex
      * @param input
      *   Input data as canonical lowercase hex
      * @param callback
      *   Callback to provide the result or error
      */
    final case class NeedExternalCall(
        extensionId: String,
        functionId: String,
        configHash: String,
        input: String,
    ) extends Upd[Either[NeedExternalCall.Error, String]]

    object NeedExternalCall {

      /** Error information from external call failures */
      final case class Error(message: String)
    }

    final case class NeedInterruption(abort: () => Option[String]) extends Upd[Unit]
  }

  private val iterationsBetweenInterruptions: Long = 10000

  // Machine-free copies of the SBuiltinFun helpers, so the conductor does not depend on that object.

  private def extractParties(where: String, v: SValue): TreeSet[Ref.Party] =
    v match {
      case SValue.SList(vs) =>
        TreeSet.empty(Ref.Party.ordering) ++ vs.iterator.map {
          case SValue.SParty(p) => p
          case x => throw SError.Crash(where, s"non-party value in list: $x")
        }
      case SValue.SParty(p) =>
        TreeSet(p)(Ref.Party.ordering)
      case _ =>
        throw SError.Crash(where, s"value not a list of parties or party: $v")
    }

  private def authenticateIfLegacyContract(
      coid: V.ContractId,
      coinst: FatContractInstance,
      hashingMethod: Hash.HashingMethod,
      authenticator: Hash => Boolean,
  ): Upd.T[Unit] = {
    def authError(msg: String): Upd.T[Nothing] =
      Upd.raise(
        IError.Upgrade(
          IError.Upgrade.AuthenticationFailed(
            coid = coid,
            srcTemplateId = coinst.templateId,
            dstTemplateId = coinst.templateId,
            createArg = coinst.createArg,
            msg = msg,
          )
        )
      )

    hashingMethod match {
      // Not a legacy contract; authenticated after translation to SValue.
      case Hash.HashingMethod.TypedNormalForm =>
        Upd.unit
      case _ =>
        val upgradeFriendlyUnsafe = hashingMethod == Hash.HashingMethod.UpgradeFriendlyUnsafe
        Hash.hashContractInstance(
          coinst.templateId,
          coinst.createArg,
          coinst.packageName,
          upgradeFriendlyUnsafe = upgradeFriendlyUnsafe,
        ) match {
          case Right(hash) if authenticator(hash) => Upd.unit
          case Right(_) => authError("failed to authenticate contract")
          case Left(msg) => authError(msg)
        }
    }
  }

  @annotation.unused("cat=unused&msg=parameter interpretationConfig")
  def apply(
      compiledPackages: CompiledPackages,
      preparationTime: Time.Timestamp,
      initialSeeding: InitialSeeding,
      committers: Set[Ref.Party],
      logger: MachineLogger,
      authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
      iterationsBetweenInterruptions: Long = TransactionConductor.iterationsBetweenInterruptions,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      interpretationConfig: interpretation.InterpretationConfig =
        interpretation.InterpretationConfig.Default,
      contractIdVersion: ContractIdVersion = ContractIdVersion.V1,
      limits: interpretation.Limits = interpretation.Limits.Lenient,
      metricPlugins: Seq[MetricPlugin] = Seq.empty,
  ): TransactionConductor =
    new TransactionConductor(
      compiledPackages = compiledPackages,
      committers = committers,
      preparationTime = preparationTime,
      contractIdVersion = contractIdVersion,
      packageResolution = packageResolution,
      limits = limits,
      logger = logger,
      iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      profile = new Profile(),
      ptx = PartialTransaction.initial(
        initialSeeding,
        committers,
        authorizationChecker,
      ),
      metricPlugins = metricPlugins,
    )

  @VisibleForTesting
  private[speedy] def buildForTest(
      compiledPackages: CompiledPackages,
      transactionSeed: crypto.Hash,
      committers: Set[Ref.Party],
      logger: MachineLogger,
      authorizationChecker: AuthorizationChecker = DefaultAuthorizationChecker,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
      interpretationConfig: interpretation.InterpretationConfig =
        interpretation.InterpretationConfig.Default,
      limits: interpretation.Limits = interpretation.Limits.Lenient,
  ): TransactionConductor =
    apply(
      compiledPackages = compiledPackages,
      preparationTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(transactionSeed),
      committers = committers,
      logger = logger,
      authorizationChecker = authorizationChecker,
      packageResolution = packageResolution,
      interpretationConfig = interpretationConfig,
      limits = limits,
    )

  private[lf] final case class Result(
      tx: SubmittedTransaction,
      seeds: ImmArray[(NodeId, crypto.Hash)],
      globalKeyMapping: Map[GlobalKey, Vector[V.ContractId]],
      contractOrder: List[V.ContractId],
  )

  @throws[IllegalArgumentException]
  private def zipSameLength[X, Y](xs: ImmArray[X], ys: ImmArray[Y]): ImmArray[(X, Y)] = {
    val n1 = xs.length
    val n2 = ys.length
    if (n1 != n2) {
      throw new IllegalArgumentException(s"sameLengthZip, $n1 /= $n2")
    }
    xs.zip(ys)
  }

}
