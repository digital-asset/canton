// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig, Error, TransactionCoder as TxCoder}
import com.digitalasset.daml.lf.interpretation.{ExecutionMode, InterpretationConfig}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Util as AstUtil}
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.metrics.MetricPlugins
import com.digitalasset.daml.lf.transaction.{
  ContractInstanceCoder,
  FatContractInstance,
  GlobalKey,
  SubmittedTransaction,
}
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.digitalasset.daml.lf.value.Value.ContractId

import java.nio.file.Paths
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** A corpus unit decoded for replay. */
final case class ReplayUnit(
    unit: CorpusUnit,
    submitters: Set[Ref.Party],
    transaction: SubmittedTransaction,
    ledgerTime: Time.Timestamp,
    preparationTime: Time.Timestamp,
    transactionSeed: crypto.Hash,
    contractIdVersion: ContractIdVersion,
    packageResolution: Map[Ref.PackageName, Ref.PackageId],
    usedPackages: Set[Ref.PackageId],
    contracts: Map[ContractId, FatContractInstance],
    keys: Map[GlobalKey, Vector[FatContractInstance]],
)

/** Replays and verifies corpus units, each from the unit alone. */
final class CorpusReplay(corpus: Corpus, loggerFactory: NamedLoggerFactory) {
  import TraceContext.Implicits.Empty.emptyTraceContext

  val packages: Map[Ref.PackageId, Ast.Package] = corpus.packages(
    corpus.manifest.getSuitesList.asScala.view
      .flatMap(suite => corpus.units(suite.getName))
      .flatMap(_.entry.getUsedPackagesList.asScala)
      .toSet
      .map(Ref.PackageId.assertFromString)
  )

  // the conductor is where interpretation is heading, so that's the mode worth benchmarking
  def newEngine(): Engine =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = LanguageVersion.allLfVersions,
        executionMode = ExecutionMode.Conductor,
      ),
      loggerFactory,
    )

  /** An engine with all the corpus's packages already loaded. */
  def newPreloadedEngine(): Engine = {
    val engine = newEngine()
    AstUtil.dependenciesInTopologicalOrder(packages.keys.toList, packages).foreach { pkgId =>
      engine
        .preloadPackage(pkgId, packages(pkgId))
        .consume(lookupHandler())
        .fold(err => sys.error(s"preload package $pkgId: ${err.message}"), identity)
    }
    engine
  }

  def prepare(unit: CorpusUnit): Either[String, ReplayUnit] = {
    val entry = unit.entry
    for {
      tx <- TxCoder
        .decodeTransaction(Snapshot.Transaction.parseFrom(entry.getRawTransaction))
        .left
        .map(err => s"decode transaction: ${err.errorMessage}")
      inputs <- entry.getInputContractsList.asScala.toSeq
        .map(ContractInstanceCoder.decodeFatContractInstance)
        .foldLeft[Either[String, Map[ContractId, FatContractInstance]]](Right(Map.empty)) {
          case (acc, contract) =>
            for {
              map <- acc
              c <- contract.left.map(err => s"decode input contract: $err")
            } yield map.updated(c.contractId, c)
        }
      keys <- entry.getKeyLookupsList.asScala.toSeq
        .map { lookup =>
          lookup.getContractIdsList.asScala.toVector
            .map(bytes => ContractId.fromBytes(data.Bytes.fromByteString(bytes)))
            .map(_.left.map(err => s"decode key lookup contract ID: $err"))
            .map(_.flatMap(cid => inputs.get(cid).toRight("key lookup contract not an input")))
            .foldLeft[Either[String, Vector[FatContractInstance]]](Right(Vector.empty)) {
              case (acc, c) => acc.flatMap(cs => c.map(cs :+ _))
            }
            .flatMap(contracts =>
              contracts.headOption
                .flatMap(_.contractKeyWithMaintainers)
                .map(_.globalKey -> contracts)
                .toRight("key lookup contract has no key")
            )
        }
        .foldLeft[Either[String, Map[GlobalKey, Vector[FatContractInstance]]]](Right(Map.empty)) {
          case (acc, kv) => acc.flatMap(map => kv.map(map + _))
        }
    } yield ReplayUnit(
      unit = unit,
      submitters = entry.getSubmittersList.asScala.map(Ref.Party.assertFromString).toSet,
      transaction = SubmittedTransaction(tx),
      ledgerTime = Time.Timestamp.assertFromLong(entry.getLedgerTime),
      preparationTime = Time.Timestamp.assertFromLong(entry.getPreparationTime),
      transactionSeed = crypto.Hash.assertFromByteArray(entry.getTransactionSeed.toByteArray),
      contractIdVersion = entry.getContractIdVersion match {
        case Snapshot.ContractIdVersion.CONTRACT_ID_VERSION_V1 => ContractIdVersion.V1
        case _ => ContractIdVersion.V2
      },
      packageResolution = entry.getPackageResolutionMap.asScala.map { case (name, id) =>
        Ref.PackageName.assertFromString(name) -> Ref.PackageId.assertFromString(id)
      }.toMap,
      usedPackages = entry.getUsedPackagesList.asScala.map(Ref.PackageId.assertFromString).toSet,
      contracts = inputs,
      keys = keys,
    )
  }

  // TODO(36899) replay with the recorded interpretation config
  private val interpretationConfig = InterpretationConfig.Dev

  private def handler(u: ReplayUnit) =
    lookupHandler(u.contracts, packages.view.filterKeys(u.usedPackages).toMap, u.keys)

  private def describe(err: Error): String = s"${err.getClass.getSimpleName}: ${err.message}"

  /** Reinterprets a unit, without checking the result. */
  def replay(
      engine: Engine,
      u: ReplayUnit,
      metricPlugins: Option[MetricPlugins] = None,
  ): Either[String, Speedy.Metrics] =
    engine
      .replayWithTransactionSeedAndCollectMetrics(
        submitters = u.submitters,
        tx = u.transaction,
        ledgerEffectiveTime = u.ledgerTime,
        preparationTime = u.preparationTime,
        transactionSeed = u.transactionSeed,
        contractIdVersion = u.contractIdVersion,
        interpretationConfig = interpretationConfig,
        packageResolution = u.packageResolution,
        metricPlugins = metricPlugins,
      )
      .consume(handler(u))
      .map { case (_, _, metrics) => metrics }
      .left
      .map(describe)

  /** Reinterprets a unit and checks the result equals the recorded transaction. */
  def validate(engine: Engine, u: ReplayUnit): Either[String, Unit] =
    engine
      .validateWithTransactionSeedAndCollectMetrics(
        submitters = u.submitters,
        tx = u.transaction,
        ledgerEffectiveTime = u.ledgerTime,
        preparationTime = u.preparationTime,
        transactionSeed = u.transactionSeed,
        contractIdVersion = u.contractIdVersion,
        interpretationConfig = interpretationConfig,
        packageResolution = u.packageResolution,
      )
      .consume(handler(u))
      .map(_ => ())
      .left
      .map(describe)

  def verify(engine: Engine, unit: CorpusUnit): Either[String, Unit] =
    prepare(unit).flatMap(validate(engine, _))
}

/** Replays every unit of a corpus and summarises the failures. Usage: CorpusReplay CORPUS_DIR */
object CorpusReplay {

  // contract IDs, hashes and package IDs vary between examples of the same failure
  private def kind(error: String): String =
    error.replaceAll("[0-9a-f]{12,}", "#").take(200)

  def main(args: Array[String]): Unit = {
    val corpus = Corpus.read(Paths.get(args(0)))
    val replay = new CorpusReplay(corpus, NamedLoggerFactory.root)
    val shared = replay.newEngine()
    val start = System.nanoTime()
    var ok = 0
    val failed = scala.collection.mutable.Map.empty[String, Vector[(CorpusUnit, String)]]
    corpus.units.foreach { unit =>
      // a fresh engine for each test's first unit checks its used packages are complete
      val engine = if (unit.index == 0) replay.newEngine() else shared
      val result =
        try replay.verify(engine, unit)
        catch { case NonFatal(e) => Left(s"exception: $e") }
      result match {
        case Right(()) => ok += 1
        case Left(error) =>
          failed.updateWith(kind(error))(prev =>
            Some(prev.getOrElse(Vector.empty) :+ (unit -> error))
          )
      }
    }
    val total = ok + failed.values.map(_.size).sum
    val seconds = (System.nanoTime() - start) / 1e9
    println(f"%%%%%% $ok of $total units replayed and verified in $seconds%.1f s")
    val entries = corpus.units.map(_.entry).toSeq
    println(
      s"%%% units with input contracts: ${entries.count(_.getInputContractsCount > 0)}, " +
        s"with key lookups: ${entries.count(_.getKeyLookupsCount > 0)}, " +
        s"with package resolution: ${entries.count(_.getPackageResolutionCount > 0)}"
    )
    failed.toSeq.sortBy(-_._2.size).foreach { case (kind, examples) =>
      println(s"%%% ${examples.size} x $kind")
      examples.take(3).foreach { case (unit, error) =>
        println(s"%%%     ${unit.suite} ${unit.test} #${unit.index}: ${error.take(1000)}")
      }
    }
  }
}
