// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package speedy

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, PackageInterface}
import com.digitalasset.daml.lf.speedy.SResult.*
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey, NeedKeyProgression, SubmittedTransaction}
import com.digitalasset.daml.lf.validation.{Validation, ValidationError}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.annotation.tailrec

private[speedy] object SpeedyTestLib {

  sealed abstract class Error(msg: String) extends RuntimeException("SpeedyTestLib.run:" + msg)

  final case object UnexpectedSResultNeedTime extends Error("unexpected SResultNeedTime")

  final case class UnknownContract(contractId: Value.ContractId)
      extends Error(s"unknown contract '$contractId'")

  final case class UnknownPackage(packageId: PackageId)
      extends Error(s"unknown package '$packageId'")

  final case object UnexpectedSResultScenarioX extends Error("unexpected SResultScenarioX")

  @throws[SError.SErrorCrash]
  def run(
      machine: Speedy.Machine[Question.Update],
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]] =
        PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
      hashingMethod: ContractId => Hash.HashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
  ): Either[SError.SError, SValue] = {

    def onQuestion(question: Question.Update): Unit = question match {
      case Question.Update.NeedTime(callback) =>
        getTime.lift(()) match {
          case Some(value) =>
            callback(value)
          case None =>
            throw UnexpectedSResultNeedTime
        }
      case Question.Update.NeedContract(contractId, _, callback) =>
        getContract.lift(contractId) match {
          case Some(coinst) =>
            callback(
              coinst,
              hashingMethod(contractId),
              _ => true, // we assume authentication always succeeds in speedy tests for now
            )
          case None =>
            throw UnknownContract(contractId)
        }
      case Question.Update.NeedPackage(pkg, _, callback) =>
        getPkg.lift(pkg) match {
          case Some(value) =>
            callback(value)
          case None =>
            throw UnknownPackage(pkg)
        }
      case Question.Update.NeedKey(key, n, canContinue, _, callback) =>
        val (returned, hasStarted) =
          NeedKeyProgression.takeN(canContinue, n, getKeys.lift(key).getOrElse(Vector.empty))
        discard(
          callback(returned, hasStarted)
        )
    }
    runTxQ(onQuestion, machine) match {
      case Left(e) => Left(e)
      case Right(fv) => Right(fv)
    }
  }

  @throws[SError.SErrorCrash]
  def buildTransaction(
      machine: Speedy.UpdateMachine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]] =
        PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SubmittedTransaction] =
    run(machine, getPkg, getContract, getKeys, getTime) match {
      case Right(_) => machine.finish.map(_.tx)
      case Left(err) => Left(err)
    }

  @throws[SError.SErrorCrash]
  def buildTransactionCollectRequests(
      machine: Speedy.UpdateMachine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]] =
        PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SubmittedTransaction] =
    run(machine, getPkg, getContract, getKeys, getTime) match {
      case Right(_) => machine.finish.map(_.tx)
      case Left(err) => Left(err)
    }

  @throws[SError.SErrorCrash]
  def runTxQ[Q](
      onQuestion: Q => Unit,
      machine: Speedy.Machine[Q],
  ): Either[SError.SError, SValue] = {
    @tailrec
    def loop: Either[SError.SError, SValue] = {
      machine.run() match {
        case SResultQuestion(question) =>
          onQuestion(question)
          loop
        case SResultFinal(v) =>
          Right(v)
        case SResultInterruption =>
          loop
        case SResultError(err) =>
          Left(err)
      }
    }
    loop
  }

  @throws[ValidationError]
  def typeAndCompile(
      majorLanguageVersion: LanguageVersion.Major,
      pkgs: Map[PackageId, Ast.Package],
  ): PureCompiledPackages = {
    require(
      pkgs.values.forall(pkg => pkg.languageVersion.major == majorLanguageVersion), {
        val wrongPackages = pkgs.view
          .mapValues(_.languageVersion)
          .filter { case (_, v) => v.major != majorLanguageVersion }
          .toList
        s"these packages don't have the expected major language version $majorLanguageVersion: $wrongPackages"
      },
    )
    Validation.unsafeCheckPackages(
      PackageInterface(pkgs),
      pkgs,
    )
    PureCompiledPackages.assertBuild(
      pkgs,
      Compiler.Config.Dev
        .copy(stacktracing = Compiler.FullStackTrace),
    )
  }

  @throws[ValidationError]
  def typeAndCompile[X](pkg: Ast.Package)(implicit
      parserParameter: ParserParameters[X]
  ): PureCompiledPackages =
    typeAndCompile(
      pkg.languageVersion.major,
      StablePackages.stablePackages.packagesMap + (parserParameter.defaultPackageId -> pkg),
    )
}
