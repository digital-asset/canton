// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import cats.data.NonEmptyList
import cats.syntax.traverse._
import com.daml.lf.data.ImmArray
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.{
  ContractKeyUniquenessMode,
  Transaction,
  TransactionVersion,
  Versioned,
  VersionedTransaction,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.VersionedValue
import com.digitalasset.canton.protocol.{
  LfActionNode,
  LfNode,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeFetch,
  LfNodeId,
  LfNodeLookupByKey,
  LfNodeRollback,
  LfTransactionVersion,
}

import java.nio.file.Path

/** As part of upstream Daml-LF refactoring, previously accessible capabilities have become Canton-private. This
  * enables Daml-LF to limit it's API surface area while still allowing Canton deeper visibility into transaction
  * internals.
  */
// TODO(i3065): Get rid of lf.CantonOnly again
object CantonOnly {

  // 1. Value normalizer to strip LF-1.12 value type information
  //
  object TransactionNormalizer {

    import scala.Ordering.Implicits._
    import com.daml.lf.transaction.Util._

    private def doOnlyIfMinVersionTypeErasure[T](version: LfTransactionVersion, id: T)(f: => T): T =
      if (version >= LfTransactionVersion.minTypeErasure) f else id

    private def normalizeNode(node: LfNode): Either[String, LfNode] = node match {
      case an: LfActionNode =>
        doOnlyIfMinVersionTypeErasure(an.version, Right(an): Either[String, LfActionNode]) {
          an match {
            case cn: LfNodeCreate =>
              for {
                normalizedArg <- normalizeValue(cn.arg, cn.version)
                normalizedKey <- normalizeOptKey(cn.key, cn.version)
              } yield cn.copy(arg = normalizedArg, key = normalizedKey)
            case en: LfNodeExercises =>
              for {
                normalizedChosenValue <- normalizeValue(en.chosenValue, en.version)
                normalizedExerciseResult <- en.exerciseResult.traverse(
                  normalizeValue(_, en.version)
                )
                normalizedKey <- normalizeOptKey(en.key, en.version)
              } yield en.copy(
                chosenValue = normalizedChosenValue,
                exerciseResult = normalizedExerciseResult,
                key = normalizedKey,
              )
            case fn: LfNodeFetch =>
              for {
                normalizedKey <- normalizeOptKey(fn.key, fn.version)
              } yield fn.copy(key = normalizedKey)
            case ln: LfNodeLookupByKey =>
              for {
                normalizedKey <- normalizeKey(ln.key, ln.version)
              } yield ln.copy(key = normalizedKey)
          }
        }
      case rn: LfNodeRollback => Right(rn)
    }

    def normalizeTransaction(
        transaction: LfVersionedTransaction
    ): Either[String, VersionedTransaction] =
      doOnlyIfMinVersionTypeErasure(
        transaction.version,
        Right(transaction): Either[String, VersionedTransaction],
      ) {
        for {
          normalizedNodes <- transaction.nodes.toList
            .traverse { case (nid, node) =>
              normalizeNode(node).map(nid -> _)
            }
            .map(_.toMap)
        } yield setTransactionNodes(transaction, normalizedNodes)
      }
  }

  // 2. Access to Transaction "hidden" / package-private inside LfVersionedTransaction
  //
  type LfTransaction = Transaction
  val LfTransaction: Transaction.type = Transaction

  type LfVersionedTransaction = VersionedTransaction
  val LfVersionedTransaction: VersionedTransaction.type = VersionedTransaction

  def lfVersionedTransaction(
      version: LfTransactionVersion,
      nodes: Map[LfNodeId, LfNode],
      roots: ImmArray[LfNodeId],
  ): LfVersionedTransaction =
    LfVersionedTransaction(version, nodes, roots)

  def lfVersionedTransaction(
      nodes: Map[LfNodeId, LfNode],
      roots: ImmArray[LfNodeId],
  ): LfVersionedTransaction =
    LfTransactionVersion.asVersionedTransaction(LfTransaction(nodes, roots))

  def unwrapVersionedTransaction(versionedTransaction: LfVersionedTransaction): LfTransaction =
    versionedTransaction.transaction

  // Replace LfTransactionVersion nodes, keeping transaction version and roots the same.
  def setTransactionNodes(tx: LfVersionedTransaction, nodes: Map[LfNodeId, LfNode]) =
    LfVersionedTransaction(tx.version, nodes, tx.roots)

  def mapNodeId(
      versionedTransaction: LfVersionedTransaction,
      f: LfNodeId => LfNodeId,
  ): VersionedTransaction =
    versionedTransaction.mapNodeId(f)

  // 3. Canton supported version ranges
  def newDamlEngine(uniqueContractKeys: Boolean, profileDir: Option[Path] = None): Engine =
    new Engine(
      EngineConfig(
        allowedLanguageVersions =
          VersionRange(LanguageVersion.v1_14, LanguageVersion.StableVersions.max),
        // The package store contains only validated packages, so we can skip validation upon loading
        packageValidation = false,
        profileDir = profileDir,
        forbidV0ContractId = true,
        requireSuffixedGlobalContractId = true,
        contractKeyUniqueness =
          if (uniqueContractKeys) ContractKeyUniquenessMode.On else ContractKeyUniquenessMode.Off,
      )
    )

  val DummyTransactionVersion: LfTransactionVersion = TransactionVersion.maxVersion

  def lookupTransactionVersion(versionP: String): Either[String, LfTransactionVersion] =
    TransactionVersion.All
      .find(_.protoValue == versionP)
      .toRight(s"Unsupported transaction version ${versionP}")

  def asVersionedValue(value: Value, transactionVersion: LfTransactionVersion): VersionedValue =
    Versioned(transactionVersion, value)

  def assertAsVersionedValue(
      value: Value,
      transactionVersion: LfTransactionVersion,
  ): VersionedValue =
    asVersionedValue(value, transactionVersion)

  def maxTransactionVersion(versions: NonEmptyList[LfTransactionVersion]): LfTransactionVersion =
    versions.reduceLeft[LfTransactionVersion](LfTransactionVersion.Ordering.max)

}
