// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLoggingContext}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.Error.Interpretation
import com.digitalasset.daml.lf.engine.Result.Need
import com.digitalasset.daml.lf.engine.refinement.Enricher as LfEnricher
import com.digitalasset.daml.lf.engine.{Engine, Result, TransactionCoder}
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.speedy.*
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.digitalasset.daml.lf.speedy.SResult.*
import com.digitalasset.daml.lf.testing.snapshot.Snapshot
import com.digitalasset.daml.lf.transaction.{NextGenContractStateMachine as CSMachine, *}
import com.digitalasset.daml.lf.value.Value.ContractId

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.annotation.tailrec
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

private[lf] object IdeLedgerRunner {

  private def crash(reason: String) =
    throw Error.Internal(reason)

  private def handleUnsafe[T](unsafe: => T): Either[Error, T] =
    Try(unsafe) match {
      case Failure(err: Error) => Left(err: Error)
      case Failure(other) => throw other
      case Success(t) => Right(t)
    }

  sealed abstract class SubmissionResult[+R] {
    @tailrec
    private[lf] final def resolve(): Either[SubmissionError, Commit[R]] =
      this match {
        case commit: Commit[R] => Right(commit)
        case error: SubmissionError => Left(error)
        case Interruption(continue) => continue().resolve()
      }
  }

  final case class Commit[+R](
      result: R,
      tx: IncompleteTransaction,
  ) extends SubmissionResult[R]

  final case class SubmissionError(error: Error, tx: IncompleteTransaction)
      extends SubmissionResult[Nothing]

  final case class Interruption[R](continue: () => SubmissionResult[R]) extends SubmissionResult[R]

  // The interface we need from a ledger during submission. We allow abstracting over this so we can play
  // tricks like caching all responses in some benchmarks.
  private[lf] abstract class LedgerApi[R] {
    def lookupContract(
        coid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        cbPresent: FatContractInstance => Unit,
    ): Either[Error, Unit]
    def lookupKey(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
    ): Either[Error, Vector[FatContractInstance]]
    def currentTime: Time.Timestamp
    def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: CommittedTransaction,
        locationInfo: Map[NodeId, Location],
    ): Either[Error, R]
    def csmMode: CSMachine.Mode
  }

  private[lf] case class ScriptLedgerApi(ledger: IdeLedger)
      extends LedgerApi[IdeLedger.CommitResult] {

    override def lookupContract(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: FatContractInstance => Unit,
    ): Either[Error, Unit] =
      handleUnsafe(lookupContractUnsafe(acoid, actAs, readAs, callback))

    private def lookupContractUnsafe(
        acoid: ContractId,
        actAs: Set[Party],
        readAs: Set[Party],
        callback: FatContractInstance => Unit,
    ) = {

      val effectiveAt = ledger.currentTime

      ledger.lookupGlobalContract(
        actAs,
        readAs,
        effectiveAt = effectiveAt,
        acoid,
      ) match {
        case IdeLedger.LookupOk(coinst) =>
          callback(coinst)

        case IdeLedger.LookupContractNotFound(coid) =>
          // This should never happen, hence we don't have a specific
          // error for this.
          throw Error.Internal(s"contract ${coid.coid} not found")

        case IdeLedger.LookupContractNotEffective(coid, tid, effectiveAt) =>
          throw Error.ContractNotEffective(coid, tid, effectiveAt)

        case IdeLedger.LookupContractNotActive(coid, tid, consumedBy) =>
          throw Error.ContractNotActive(coid, tid, consumedBy)

        case IdeLedger.LookupContractNotVisible(coid, tid, observers, stakeholders @ _) =>
          throw Error.ContractNotVisible(coid, tid, actAs, readAs, observers)
      }
    }

    override def lookupKey(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
    ): Either[Error, Vector[FatContractInstance]] =
      handleUnsafe(lookupKeyUnsafe(gk, actAs, readAs))

    private def lookupKeyUnsafe(
        gk: GlobalKey,
        actAs: Set[Party],
        readAs: Set[Party],
    ): Vector[FatContractInstance] = {
      val acoids = ledger.ledgerData.activeKeys.getOrElse(gk, Vector.empty)
      acoids.collect(Function.unlift { acoid =>
        ledger.lookupGlobalContract(
          actAs,
          readAs,
          effectiveAt = ledger.currentTime,
          acoid,
        ) match {
          case IdeLedger.LookupOk(contract) => Some(contract)
          case IdeLedger.LookupContractNotFound(_) => None
          case IdeLedger.LookupContractNotEffective(_, _, _) => None
          case IdeLedger.LookupContractNotActive(_, _, _) => None
          case IdeLedger.LookupContractNotVisible(_, _, _, _) => None
        }
      })
    }

    override def currentTime = ledger.currentTime

    override def commit(
        committers: Set[Party],
        readAs: Set[Party],
        location: Option[Location],
        tx: CommittedTransaction,
        locationInfo: Map[NodeId, Location],
    ): Either[Error, IdeLedger.CommitResult] = {
      val result = IdeLedger.commitTransaction(
        actAs = committers,
        readAs = readAs,
        effectiveAt = ledger.currentTime,
        optLocation = location,
        tx = tx,
        locationInfo = locationInfo,
        l = ledger,
      )
      Right(result)
    }

    override val csmMode = ledger.csmMode
  }

  private[this] abstract class Enricher {
    def enrich(tx: VersionedTransaction)(implicit traceContext: TraceContext): VersionedTransaction
    def enrich(tx: IncompleteTransaction)(implicit
        traceContext: TraceContext
    ): IncompleteTransaction
  }

  private[this] object NoEnricher extends Enricher {
    override def enrich(tx: VersionedTransaction)(implicit
        traceContext: TraceContext
    ): VersionedTransaction = tx
    override def enrich(tx: IncompleteTransaction)(implicit
        traceContext: TraceContext
    ): IncompleteTransaction = tx
  }

  private[this] class EnricherImpl(
      compiledPackages: CompiledPackages,
      loggerFactory: NamedLoggerFactory,
  ) extends Enricher {
    val config = Engine.DevConfig
    def loadPackage(pkgId: PackageId, context: language.Reference): Result[Unit] =
      crash(LookupError.MissingPackage.pretty(pkgId, context))
    val strictEnricher = LfEnricher(
      compiledPackages = compiledPackages,
      loadPackage = loadPackage,
      addTypeInfo = true,
      addFieldNames = true,
      addTrailingNoneFields = true,
      forbidLocalContractIds = true,
      loggerFactory = loggerFactory,
    )
    val lenientEnricher = LfEnricher(
      compiledPackages = compiledPackages,
      loadPackage = loadPackage,
      addTypeInfo = true,
      addFieldNames = true,
      addTrailingNoneFields = true,
      forbidLocalContractIds = false,
      loggerFactory = loggerFactory,
    )
    def consume[V](res: Result[V]): V =
      res.start match {
        case Result.Step.Pure(x) => x
        case x => crash(s"unexpected Result when enriching value: $x")
      }
    override def enrich(tx: VersionedTransaction)(implicit
        traceContext: TraceContext
    ): VersionedTransaction =
      consume(strictEnricher.enrichVersionedTransaction(tx))
    override def enrich(tx: IncompleteTransaction)(implicit
        traceContext: TraceContext
    ): IncompleteTransaction =
      consume(lenientEnricher.enrichIncompleteTransaction(tx))
  }

  /** A class for suffixing all the local contract IDs of a transaction with the TypedNormalForm
    * hash of their Create argument. Assumes that the creation package of these contract and its
    * transitive dependencies are all present in [compiledPackages].
    */
  private[this] class CidSuffixer(compiledPackages: CompiledPackages) {
    private[this] val valueTranslator = new speedy.ValueTranslator(
      compiledPackages.pkgInterface,
      forbidLocalContractIds = false,
      forbidTrailingNones = false,
    )

    private[this] def hashCreateNode(createNode: Node.Create): crypto.Hash = {
      val sValue = valueTranslator
        .translateValue(Ast.TTyCon(createNode.templateId), createNode.arg)
        .fold(
          e => crash(s"unexpected error when enriching a Create node produced by the engine: $e"),
          identity,
        )
      SValueHash
        .hashContractInstance(createNode.packageName, createNode.templateId.qualifiedName, sValue)
        .fold(
          e => crash(s"unexpected error when hashing a Create node produced by the engine: $e"),
          identity,
        )
    }

    def suffixCids(tx: VersionedTransaction): VersionedTransaction = {
      val cidMapping = tx
        .foldInExecutionOrder(Map.empty[ContractId, ContractId].withDefault(identity))(
          exerciseBegin = (mapping, _, _) => (mapping, Transaction.ChildrenRecursion.DoRecurse),
          rollbackBegin = (mapping, _, _) => (mapping, Transaction.ChildrenRecursion.DoRecurse),
          leaf = (mapping, _, leaf) => {
            leaf match {
              case create: Node.Create =>
                val suffix = hashCreateNode(create.mapCid(mapping)).bytes
                mapping + (create.coid -> data.assertRight(
                  create.coid.suffixCid(_ => suffix, _ => suffix)
                ))
              case _ =>
                mapping
            }
          },
          exerciseEnd = (mapping, _, _) => mapping,
          rollbackEnd = (mapping, _, _) => mapping,
        )
      tx.mapCid(cidMapping)
    }
  }

  def submit[R](
      compiledPackages: CompiledPackages,
      disclosures: Iterable[FatContractInstance],
      ledger: LedgerApi[R],
      committers: Set[Party],
      readAs: Set[Party],
      commands: SExpr,
      location: Option[Location],
      seed: crypto.Hash,
      machineLogger: MachineLogger,
      packageResolution: Map[PackageName, PackageId] = Map.empty,
      doEnrichment: Boolean = true,
      snapshotDir: Option[Path] = None,
  )(implicit loggingContext: NamedLoggingContext): SubmissionResult[R] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext

    val disclosuresByCoid = disclosures.view.map(fci => fci.contractId -> fci).toMap

    val disclosuresByKey =
      disclosures
        .filter(_.contractKeyWithMaintainers.isDefined)
        .groupMapReduce(_.contractKeyWithMaintainers.get.globalKey)(Vector(_))(_ ++ _)

    val ledgerMachine = Speedy.UpdateMachine(
      packageResolution = packageResolution,
      compiledPackages = compiledPackages,
      preparationTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SEApp(commands, ArraySeq(SValue.SToken)),
      committers = committers,
      readAs = readAs,
      commitLocation = location,
      limits = interpretation.Limits.Lenient,
      interpretationConfig =
        interpretation.InterpretationConfig.Default.copy(contractStateMode = ledger.csmMode),
      logger = machineLogger,
    )
    // TODO (drsk) validate and propagate errors back to submitter
    // https://github.com/digital-asset/daml/issues/14108
    val enricher =
      if (doEnrichment) new EnricherImpl(compiledPackages, loggingContext.loggerFactory)
      else NoEnricher
    import enricher.*
    val suffixer = new CidSuffixer(compiledPackages)

    // If any compiled package lookup fails, then we return None. Otherwise, we return the defined
    // set of package dependencies for each package in our transaction.
    def deps(tx: SubmittedTransaction): Option[Set[PackageId]] = {
      val nodePkgIds =
        tx.nodes.values.collect { case node: Node.Action => node.packageIds }.flatten.toSet

      nodePkgIds.foldLeft[Option[Set[PackageId]]](Some(nodePkgIds)) {
        case (None, _) =>
          None
        case (Some(acc), pkgId) =>
          compiledPackages.getPackageDependencies(pkgId).map(acc | _)
      }
    }

    def saveTransaction(result: Speedy.UpdateMachine.Result, snapshotDir: Path): Option[String] = {
      val Speedy.UpdateMachine.Result(tx, _, _, nodeSeeds, globalKeyMapping, contractOrder) = result

      deps(tx).flatMap { deps =>
        val meta = Transaction.Metadata(
          submissionSeed = None,
          preparationTime = ledgerMachine.preparationTime,
          usedPackages = deps,
          timeBoundaries = ledgerMachine.getTimeBoundaries,
          nodeSeeds = nodeSeeds,
          globalKeyMapping = globalKeyMapping,
          contractOrder = contractOrder,
        )
        val snapshotFile = snapshotDir.resolve("snapshot-ide-ledger.bin")

        TransactionCoder
          .encodeTransaction(tx)
          .fold(
            err => Some(s"TransactionCoder.encodeTransaction: $err"),
            encoded => {
              val txEntry = Snapshot.TransactionEntry
                .newBuilder()
                .setRawTransaction(encoded.toByteString)
                .setParticipantId("ide-ledger")
                .addAllSubmitters(committers.map(_.toString).asJava)
                .setLedgerTime(ledger.currentTime.micros)
                .setPreparationTime(meta.preparationTime.micros)
                .build()
              val txSubmission = Snapshot.SubmissionEntry
                .newBuilder()
                .setTransaction(txEntry)
                .build()

              txSubmission.writeDelimitedTo(
                Files.newOutputStream(
                  snapshotFile,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.APPEND,
                )
              )

              None
            },
          )
      }
    }

    def continue = () => go()

    // Wrapper for Vector[FatContractInstance] to avoid type erasure of `FatContractInstance` within the Vector when casting from Any
    final case class FatContractInstanceVector(getInstances: Vector[FatContractInstance])
        extends NeedKeyProgression.Token

    def submissionError(err: Error) =
      SubmissionError(err, enrich(ledgerMachine.incompleteTransaction))

    // Speedy reports an unhandled exception as `UnhandledException`; turn it into a FailureStatus
    // by running the exception's message function, mirroring the Engine's behaviour.
    def raiseFailureWithStatus(excp: SValue.SAny): SubmissionResult[R] = {
      @scala.annotation.nowarn("msg=dead code following this construct")
      def loop(step: Result.Step[Nothing]): SubmissionResult[Nothing] =
        step match {
          case Result.Step.Error(error) =>
            error match {
              case engine.Error.Interpretation(Interpretation.DamlException(error), _) =>
                submissionError(Error.RunnerException(SError.InterpretationError(error)))
              case otherwise =>
                submissionError(Error.Internal(s"unexpected Error $otherwise"))
            }
          case impure: Result.Step.Impure[x, Nothing] =>
            impure.fx match {
              case Need.Interruption(_) =>
                Interruption(() => loop(impure.resume(())))
              case otherwise =>
                submissionError(Error.Internal(s"unexpected question $otherwise"))
            }
          case Result.Step.Pure(nothing) =>
            nothing
        }

      loop(
        Engine
          .raiseFailureWithStatus(
            excp = excp,
            compiledPackages = compiledPackages,
            machineLogger = machineLogger,
            iterationsBetweenInterruptions = ledgerMachine.iterationsBetweenInterruptions,
            detailMsg = None,
          )
          .start
      )
    }

    @tailrec
    def go(): SubmissionResult[R] =
      ledgerMachine.run() match {
        case SResultQuestion(question) =>
          question match {
            case Question.Update.NeedContract(coid, committers, callback) =>
              disclosuresByCoid.get(coid) match {
                case Some(fcoinst) =>
                  callback(
                    fcoinst.nonVerboseWithoutTrailingNones,
                    Hash.HashingMethod.TypedNormalForm,
                    h =>
                      fcoinst.contractId match {
                        case ContractId.V1(_, suffix) => h.bytes == suffix
                        case ContractId.V2(_, suffix) => h.bytes == suffix
                      },
                  )
                  go()
                case None =>
                  ledger.lookupContract(
                    coid,
                    committers,
                    readAs,
                    (fcoinst: FatContractInstance) =>
                      callback(
                        fcoinst.nonVerboseWithoutTrailingNones,
                        Hash.HashingMethod.TypedNormalForm,
                        h =>
                          fcoinst.contractId match {
                            case ContractId.V1(_, suffix) => h.bytes == suffix
                            case ContractId.V2(_, suffix) => h.bytes == suffix
                          },
                      ),
                  ) match {
                    case Left(err) => submissionError(err)
                    case Right(_) => go()
                  }
              }
            case Question.Update.NeedKey(
                  gkey,
                  limit,
                  progress,
                  committers,
                  callback,
                ) =>
              val contracts = progress match {
                case NeedKeyProgression.Unstarted =>
                  val disclosedInsts =
                    disclosuresByKey.getOrElse(gkey, Vector.empty)
                  for {
                    globalInsts <- ledger
                      .lookupKey(gkey, committers, readAs)
                      .left
                      .map(submissionError(_))
                  } yield disclosedInsts ++ globalInsts.diff(disclosedInsts)
                case NeedKeyProgression.InProgress(FatContractInstanceVector(rest)) => Right(rest)
                case NeedKeyProgression.InProgress(token) =>
                  throw new IllegalStateException(
                    s"unexpected continuation token of type ${token.getClass}"
                  )
              }
              contracts match {
                case Left(err) => err
                case Right(contracts) =>
                  val (result, rest) = contracts.splitAt(limit)
                  callback(
                    result.map(x => (x, Hash.HashingMethod.TypedNormalForm, (_: Hash) => true)),
                    // it is important not return `Finished` if result.length == n
                    // See NeedKeyProgression for more details
                    if (result.length == limit)
                      NeedKeyProgression.InProgress(FatContractInstanceVector(rest))
                    else
                      NeedKeyProgression.Finished,
                  )
                  go()
              }
            case Question.Update.NeedTime(callback) =>
              callback(ledger.currentTime)
              go()
            case Question.Update.NeedExternalCall(_, _, _, _, _) =>
              throw Error.Internal("External calls are not supported in the IDE ledger")
            case res: Question.Update.NeedPackage =>
              throw Error.Internal(s"unexpected $res")
          }
        case SResultInterruption =>
          Interruption(continue)
        case SResult.SResultFinal(_) =>
          ledgerMachine.finish match {
            case Right(result @ Speedy.UpdateMachine.Result(tx, _, locationInfo, _, _, _)) =>
              snapshotDir.foreach(saveTransaction(result, _)) // FIXME:
              val committedTx = CommittedTransaction(enrich(suffixer.suffixCids(tx)))
              ledger.commit(committers, readAs, location, committedTx, locationInfo) match {
                case Left(err) => submissionError(err)
                case Right(r) => Commit(r, enrich(ledgerMachine.incompleteTransaction))
              }
            case Left(err) =>
              throw err
          }
        case SResultError(err) =>
          err match {
            case SError.UnhandledException(excp) =>
              raiseFailureWithStatus(excp)
            case _ =>
              submissionError(Error.RunnerException(err))
          }
      }
    go()
  }

  private[lf] def nextSeed(submissionSeed: crypto.Hash): crypto.Hash =
    crypto.Hash.deriveTransactionSeed(
      submissionSeed,
      Ref.ParticipantId.assertFromString("script-service"),
      // MinValue makes no sense here but this is what we did before so
      // to avoid breaking all tests we keep it for now at least.
      Time.Timestamp.MinValue,
    )
}
