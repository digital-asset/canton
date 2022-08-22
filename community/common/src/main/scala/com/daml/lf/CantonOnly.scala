// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.speedy.Compiler
import com.daml.lf.transaction.{
  ContractKeyUniquenessMode,
  Transaction,
  TransactionVersion,
  Versioned,
  VersionedTransaction,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.VersionedValue
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.protocol.{LfNode, LfNodeId, LfTransactionVersion}

import java.nio.file.Path

/** As part of upstream Daml-LF refactoring, previously accessible capabilities have become Canton-private. This
  * enables Daml-LF to limit it's API surface area while still allowing Canton deeper visibility into transaction
  * internals.
  */
// TODO(i3065): Get rid of lf.CantonOnly again
object CantonOnly {

  // 1. Access to Transaction "hidden" / package-private inside LfVersionedTransaction
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

  // 2. Canton supported version ranges
  def newDamlEngine(
      uniqueContractKeys: Boolean,
      enableLfDev: Boolean,
      profileDir: Option[Path] = None,
  ): Engine =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = VersionRange(
          LanguageVersion.v1_14,
          if (enableLfDev) LanguageVersion.DevVersions.max else LanguageVersion.StableVersions.max,
        ),
        // The package store contains only validated packages, so we can skip validation upon loading
        packageValidation = false,
        profileDir = profileDir,
        forbidV0ContractId = true,
        requireSuffixedGlobalContractId = true,
        contractKeyUniqueness =
          if (uniqueContractKeys) ContractKeyUniquenessMode.Strict
          else ContractKeyUniquenessMode.Off,
      )
    )

  val DummyTransactionVersion: LfTransactionVersion = TransactionVersion.maxVersion

  def lookupTransactionVersion(versionP: String): Either[String, LfTransactionVersion] =
    TransactionVersion.All
      .find(_.protoValue == versionP)
      .toRight(s"Unsupported transaction version ${versionP}")

  def asVersionedValue(value: Value, transactionVersion: LfTransactionVersion): VersionedValue =
    Versioned(transactionVersion, value)

  def maxTransactionVersion(versions: NonEmpty[Seq[LfTransactionVersion]]): LfTransactionVersion =
    versions.reduceLeft[LfTransactionVersion](LfTransactionVersion.Ordering.max)

  def tryBuildCompiledPackages(
      darMap: Map[PackageId, Ast.Package],
      enableLfDev: Boolean,
  ): PureCompiledPackages = {
    PureCompiledPackages.assertBuild(
      darMap,
      if (enableLfDev) Compiler.Config.Dev else Compiler.Config.Default,
    )
  }
}
